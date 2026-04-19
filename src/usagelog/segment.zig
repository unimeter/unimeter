//! Segment file: append-only sequence of fixed-width Events.
//! Sparse index (.idx): one (event_offset, byte_pos) entry every INDEX_STRIDE events,
//! enabling O(log n) seeks without loading the full index into memory.

const std     = @import("std");
const Event   = @import("../event.zig").Event;
const disk_io = @import("../io/disk_io.zig");

pub const INDEX_STRIDE:       u32 = 1000;
pub const DEFAULT_MAX_EVENTS: u32 = 1_000_000; // ~64 MB per segment

/// Sparse index entry stored in the .idx file.
const IndexEntry = extern struct {
    event_offset: u64, // monotonic offset of this event in the global log
    byte_pos:     u64, // byte position in the .seg file
};

comptime {
    std.debug.assert(@sizeOf(IndexEntry) == 16);
}

/// Generic Segment parameterised on a file type.
/// Production code uses Segment = SegmentGeneric(RealFile).
/// VOPR uses SegmentGeneric(FakeFile) from fake_io.zig.
pub fn SegmentGeneric(comptime FileT: type) type {
    return struct {
        file:        FileT,
        index_file:  FileT,
        base_offset: u64,
        event_count: u64,
        max_events:  u32,

        const Self = @This();

        pub fn init_from_files(
            file:        FileT,
            index_file:  FileT,
            base_offset: u64,
            max_events:  u32,
            event_count: u64,
        ) Self {
            return .{
                .file        = file,
                .index_file  = index_file,
                .base_offset = base_offset,
                .event_count = event_count,
                .max_events  = max_events,
            };
        }

        pub fn deinit(self: *Self) void {
            self.file.close();
            self.index_file.close();
        }

        pub fn is_full(self: *const Self) bool {
            return self.event_count >= self.max_events;
        }

        /// Next monotonic offset that will be assigned on the next append.
        pub fn next_offset(self: *const Self) u64 {
            return self.base_offset + self.event_count;
        }

        /// Append one event. The event's offset field must already be set.
        pub fn append(self: *Self, event: *const Event) !void {
            // Write a sparse index entry every INDEX_STRIDE events.
            if (self.event_count % INDEX_STRIDE == 0) {
                const idx = IndexEntry{
                    .event_offset = self.base_offset + self.event_count,
                    .byte_pos     = self.event_count * @sizeOf(Event),
                };
                try self.index_file.write_all(std.mem.asBytes(&idx));
            }
            try self.file.write_all(std.mem.asBytes(event));
            self.event_count += 1;
        }

        /// Read the event at the given monotonic offset.
        pub fn read_at(self: *Self, offset: u64) !Event {
            if (offset < self.base_offset) return error.OffsetTooLow;
            const local = offset - self.base_offset;
            if (local >= self.event_count) return error.OffsetOutOfRange;

            const byte_pos = local * @sizeOf(Event);
            try self.file.seek_to(byte_pos);
            var event: Event = undefined;
            const n = try self.file.read(std.mem.asBytes(&event));
            if (n != @sizeOf(Event)) return error.ShortRead;
            return event;
        }

        pub fn sync(self: *Self) !void {
            try self.file.sync();
        }

        // ---- Async-friendly interface for io_uring path ----

        /// Byte offset in the segment file where the next batch should be written.
        pub fn next_write_offset(self: *const Self) u64 {
            return self.event_count * @sizeOf(Event);
        }

        /// Call after the io_uring write CQE for a batch of n events completes.
        pub fn advance(self: *Self, n: usize) void {
            self.event_count += n;
        }
    };
}

/// Production Segment backed by real files.
pub const Segment = SegmentGeneric(disk_io.RealFile);

/// Create (or open existing) segment files under dir_path.
/// File names are zero-padded decimal base offsets: e.g. "00000000000000000000.seg".
pub fn segment_create(dir_path: []const u8, base_offset: u64, max_events: u32) !Segment {
    var seg_path_buf: [512]u8 = undefined;
    const seg_path = try std.fmt.bufPrint(&seg_path_buf, "{s}/{d:0>20}.seg", .{ dir_path, base_offset });
    const rf = try disk_io.open_rw(seg_path);
    errdefer rf.close();

    var idx_path_buf: [512]u8 = undefined;
    const idx_path = try std.fmt.bufPrint(&idx_path_buf, "{s}/{d:0>20}.idx", .{ dir_path, base_offset });
    const rf_idx = try disk_io.open_rw(idx_path);
    errdefer rf_idx.close();

    const file_size  = try rf.size();
    const event_count = file_size / @sizeOf(Event);

    try rf.seek_end();
    try rf_idx.seek_end();

    return Segment.init_from_files(rf, rf_idx, base_offset, max_events, event_count);
}

// ---- Tests ----

test "segment: append and read_at" {
    const dir = "/tmp/billing_segment_append_test";
    try disk_io.make_path(dir);
    defer {
        disk_io.remove("/tmp/billing_segment_append_test/00000000000000000000.seg") catch {};
        disk_io.remove("/tmp/billing_segment_append_test/00000000000000000000.idx") catch {};
    }

    var seg = try segment_create(dir, 0, DEFAULT_MAX_EVENTS);
    defer seg.deinit();

    const e = Event{
        .offset          = 0,
        .timestamp       = 1_000_000,
        .idempotency_key = 0xDEADBEEF,
        .account_id      = 42,
        .metric_code     = 7,
        .value           = 100,
        .operation_type  = 0,
        ._pad            = .{ 0, 0, 0 },
        .checksum        = 0,
    };

    var ev = e;
    ev.offset = seg.next_offset();
    try seg.append(&ev);

    const got = try seg.read_at(0);
    try std.testing.expectEqual(ev.account_id, got.account_id);
    try std.testing.expectEqual(ev.value,      got.value);
}

test "segment: is_full" {
    const dir = "/tmp/billing_segment_full_test";
    try disk_io.make_path(dir);
    defer {
        disk_io.remove("/tmp/billing_segment_full_test/00000000000000000000.seg") catch {};
        disk_io.remove("/tmp/billing_segment_full_test/00000000000000000000.idx") catch {};
    }

    var seg = try segment_create(dir, 0, 2);
    defer seg.deinit();

    const e = Event{
        .offset = 0, .timestamp = 0, .idempotency_key = 1,
        .account_id = 1, .metric_code = 1, .value = 1,
        .operation_type = 0, ._pad = .{ 0, 0, 0 }, .checksum = 0,
    };

    var e1 = e; e1.offset = seg.next_offset(); try seg.append(&e1);
    try std.testing.expect(!seg.is_full());
    var e2 = e; e2.offset = seg.next_offset(); try seg.append(&e2);
    try std.testing.expect(seg.is_full());
}
