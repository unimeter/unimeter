//! UsageLog: core storage engine.
//! Owns WAL + active Segment + DedupRing + MetricRegistry.
//!
//! Two write paths:
//!   ingest()  — synchronous (used in tests, recovery)
//!   prepare() — fills caller buffers, no I/O; server drives async writes via io_uring

const std   = @import("std");
const Event = @import("../event.zig").Event;

const time_util = @import("../util/time.zig");

const wal_mod        = @import("wal.zig");
const segment_mod    = @import("segment.zig");
const Wal            = wal_mod.Wal;
const SegmentedWal   = wal_mod.SegmentedWal;
const Segment        = segment_mod.Segment;
const DEFAULT_MAX_EVENTS = segment_mod.DEFAULT_MAX_EVENTS;
const DedupRing      = @import("dedup.zig").DedupRing;
const MetricRegistry = @import("metric_registry.zig").MetricRegistry;
const fnv1a          = @import("metric_registry.zig").fnv1a;
const protocol       = @import("protocol.zig");
const WireEvent      = protocol.WireEvent;

pub const IngestResult = struct {
    last_offset:     u64,
    n_stored:        u32, // events written to segment
    n_duplicates:    u32, // events rejected by dedup
    unknown_metrics: bool,
};

/// Result of prepare(): describes what the caller must write via io_uring.
pub const PreparedBatch = struct {
    /// Total bytes in wal_buf to write: EntryHeader(16B) + Event payload.
    wal_entry_len: usize,
    /// CRC to pass to wal.advance() after the WAL write CQE arrives.
    wal_new_crc:   u32,
    /// Number of events in events_buf (same events as wal_buf payload).
    n_events:      usize,
    /// Summary for building the response to the client.
    result:        IngestResult,
};

pub const Config = struct {
    data_dir:    []const u8,
    max_events_per_segment: u32 = DEFAULT_MAX_EVENTS,
    dedup_window_ms: i64 = 5 * 60 * 1000,
    wal_segment_size: u64 = wal_mod.DEFAULT_SEGMENT_SIZE,
};

pub const UsageLog = struct {
    alloc:    std.mem.Allocator,
    wal:      SegmentedWal,
    segment:  Segment,
    dedup:    DedupRing,
    registry: MetricRegistry,
    data_dir: []const u8,
    offset:   u64, // next monotonic offset to assign

    pub fn init(alloc: std.mem.Allocator, cfg: Config) !UsageLog {
        // Ensure data directory exists.
        const disk_io = @import("../io/disk_io.zig");
        try disk_io.make_path(cfg.data_dir);

        // WAL directory inside data dir.
        var wal_dir_buf: [512]u8 = undefined;
        const wal_dir = try std.fmt.bufPrint(&wal_dir_buf, "{s}/wal", .{cfg.data_dir});
        const wal = try wal_mod.wal_open_segmented(wal_dir, cfg.wal_segment_size);
        errdefer @constCast(&wal).deinit();

        // Segment starts at offset 0.
        const segment = try segment_mod.segment_create(cfg.data_dir, 0, cfg.max_events_per_segment);
        errdefer @constCast(&segment).deinit();

        // MetricRegistry: load from file if present.
        var reg_path_buf: [512]u8 = undefined;
        const reg_path = try std.fmt.bufPrint(&reg_path_buf, "{s}/metric_registry.bin", .{cfg.data_dir});
        const registry = try MetricRegistry.load(alloc, reg_path);

        const dedup = DedupRing.init(alloc, cfg.dedup_window_ms);

        return .{
            .alloc    = alloc,
            .wal      = wal,
            .segment  = segment,
            .dedup    = dedup,
            .registry = registry,
            .data_dir = cfg.data_dir,
            .offset   = segment.next_offset(),
        };
    }

    pub fn deinit(self: *UsageLog) void {
        self.wal.deinit();
        self.segment.deinit();
        self.dedup.deinit();
        self.registry.deinit();
    }

    /// Ingest a batch of wire events.
    /// sync_mode=true: fsync WAL before returning.
    /// sync_mode=false: write to WAL without fsync (OS buffer).
    pub fn ingest(self: *UsageLog, wire_events: []const WireEvent, sync_mode: bool) !IngestResult {
        var result = IngestResult{
            .last_offset     = if (self.offset > 0) self.offset - 1 else 0,
            .n_stored        = 0,
            .n_duplicates    = 0,
            .unknown_metrics = false,
        };

        // Batch buffer: stack-allocate for small batches, heap for large.
        // Max 1024 events per call to keep stack usage bounded.
        const max_batch = @min(wire_events.len, 1024);
        var batch_buf: [1024]Event = undefined;
        var batch_len: usize = 0;

        const now_ns: i64 = @truncate(time_util.wallNanos());

        for (wire_events[0..max_batch], 0..) |*we, batch_idx| {
            const ts: i64 = if (we.timestamp == 0) now_ns + @as(i64, @intCast(batch_idx)) else we.timestamp;
            const ikey = self.derive_ikey_with_ts(we, ts);

            if (self.dedup.seen(ikey)) {
                result.n_duplicates += 1;
                continue;
            }

            const metric_code = fnv1a(std.mem.sliceTo(&we.metric_code_str, 0));
            if (self.registry.get(metric_code) == null) {
                result.unknown_metrics = true;
            }

            const ev = Event{
                .offset          = self.offset,
                .timestamp       = ts,
                .idempotency_key = ikey,
                .account_id      = we.account_id,
                .metric_code     = metric_code,
                .value           = we.value,
                .operation_type  = we.operation_type,
                ._pad            = .{ 0, 0, 0 },
                .checksum        = 0,
            };

            batch_buf[batch_len] = ev;
            batch_len += 1;
            self.offset += 1;

            try self.dedup.insert(ikey);
        }

        if (batch_len == 0) return result;

        // Compute checksums and write to WAL in one shot.
        const events_slice = batch_buf[0..batch_len];
        for (events_slice) |*ev| ev.checksum = event_checksum(ev);

        const payload = std.mem.sliceAsBytes(events_slice);
        try self.wal.append(.commit, payload);
        if (sync_mode) try self.wal.sync();

        // Write to segment.
        for (events_slice) |*ev| {
            if (self.segment.is_full()) {
                try self.rotate_segment();
            }
            try self.segment.append(ev);
        }
        if (sync_mode) try self.segment.sync();

        result.n_stored    = @intCast(batch_len);
        result.last_offset = self.offset - 1;
        return result;
    }

    /// Async-friendly write path: processes business logic (dedup, hashing),
    /// fills wal_buf and events_buf, then returns without doing any I/O.
    /// The caller (server) drives the actual writes via io_uring.
    ///
    /// wal_buf layout: [EntryHeader(16B)][Event × n_events]
    /// wal_buf must be at least 16 + MAX_EVENTS_PER_BATCH * @sizeOf(Event) bytes.
    pub fn prepare(
        self:       *UsageLog,
        wire_events: []const WireEvent,
        wal_buf:     []u8,
        events_buf:  []Event,
        wire_indices: ?[]u16,
    ) !PreparedBatch {
        const max_batch = @min(wire_events.len, events_buf.len);
        var n: usize = 0;
        var result = IngestResult{
            .last_offset     = if (self.offset > 0) self.offset - 1 else 0,
            .n_stored        = 0,
            .n_duplicates    = 0,
            .unknown_metrics = false,
        };

        const now_ns: i64 = @truncate(time_util.wallNanos());

        for (wire_events[0..max_batch], 0..) |*we, batch_idx| {
            // Assign server timestamp when client sends 0 (server-assign mode).
            const ts: i64 = if (we.timestamp == 0) now_ns + @as(i64, @intCast(batch_idx)) else we.timestamp;
            const ikey = self.derive_ikey_with_ts(we, ts);
            if (self.dedup.seen(ikey)) {
                result.n_duplicates += 1;
                continue;
            }
            const metric_code = fnv1a(std.mem.sliceTo(&we.metric_code_str, 0));
            if (self.registry.get(metric_code) == null) result.unknown_metrics = true;

            events_buf[n] = Event{
                .offset          = self.offset,
                .timestamp       = ts,
                .idempotency_key = ikey,
                .account_id      = we.account_id,
                .metric_code     = metric_code,
                .value           = we.value,
                .operation_type  = we.operation_type,
                ._pad            = .{ 0, 0, 0 },
                .checksum        = 0,
            };
            events_buf[n].checksum = event_checksum(&events_buf[n]);
            if (wire_indices) |wi| { wi[n] = @intCast(batch_idx); }
            n += 1;
            self.offset += 1;
            try self.dedup.insert(ikey);
        }

        const events_slice = events_buf[0..n];
        const payload = std.mem.sliceAsBytes(events_slice);
        const built = self.wal.build_entry(.commit, payload, wal_buf);

        result.n_stored    = @intCast(n);
        result.last_offset = if (self.offset > 0) self.offset - 1 else 0;

        return PreparedBatch{
            .wal_entry_len = built.len,
            .wal_new_crc   = built.crc,
            .n_events      = n,
            .result        = result,
        };
    }

    fn rotate_segment(self: *UsageLog) !void {
        self.segment.deinit();
        self.segment = try segment_mod.segment_create(self.data_dir, self.offset, self.segment.max_events);
    }

    /// Derive a 128-bit idempotency key from wire event fields and resolved timestamp.
    /// Uses the server-assigned timestamp (not the wire timestamp) so that
    /// events with timestamp=0 in the same batch get unique keys.
    fn derive_ikey_with_ts(self: *UsageLog, we: *const WireEvent, ts: i64) u128 {
        _ = self;
        var h = std.hash.Wyhash.init(0);
        h.update(&we.metric_code_str);
        h.update(std.mem.asBytes(&we.account_id));
        h.update(std.mem.asBytes(&ts));
        h.update(std.mem.asBytes(&we.value));
        const lo = h.final();
        var h2 = std.hash.Wyhash.init(0xDEADBEEF_CAFEBABE);
        h2.update(&we.metric_code_str);
        h2.update(std.mem.asBytes(&we.account_id));
        h2.update(std.mem.asBytes(&ts));
        const hi = h2.final();
        return (@as(u128, hi) << 64) | lo;
    }
};

fn event_checksum(e: *const Event) u32 {
    return std.hash.Crc32.hash(std.mem.asBytes(e)[0..60]);
}

test "usagelog: ingest stores events" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    const dir = "/tmp/billing_usagelog_test";
    // cleanup skipped (fs API moved in Zig 0.16)

    var log = try UsageLog.init(alloc, .{ .data_dir = dir });
    defer log.deinit();

    var we = std.mem.zeroes(WireEvent);
    @memcpy(we.metric_code_str[0..8], "api_call");
    we.account_id = 1;
    we.timestamp  = 1_000_000;
    we.value      = 500_000;

    const result = try log.ingest(&.{we}, false);
    try std.testing.expectEqual(@as(u32, 1), result.n_stored);
    try std.testing.expectEqual(@as(u32, 0), result.n_duplicates);
}

test "usagelog: dedup rejects second identical event" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    const dir = "/tmp/billing_usagelog_dedup_test";
    // cleanup skipped (fs API moved in Zig 0.16)

    var log = try UsageLog.init(alloc, .{ .data_dir = dir });
    defer log.deinit();

    var we = std.mem.zeroes(WireEvent);
    @memcpy(we.metric_code_str[0..8], "api_call");
    we.account_id = 1;
    we.timestamp  = 999;
    we.value      = 1;

    const r1 = try log.ingest(&.{we}, false);
    try std.testing.expectEqual(@as(u32, 1), r1.n_stored);

    const r2 = try log.ingest(&.{we}, false);
    try std.testing.expectEqual(@as(u32, 0), r2.n_stored);
    try std.testing.expectEqual(@as(u32, 1), r2.n_duplicates);
}
