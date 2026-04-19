//! Query API: read-only access to aggregates, raw events, and alert log.
//!
//! Four entry points:
//!   get_realtime  — current period aggregate from memtable (O(1))
//!   get_usage     — range of periods, merging checkpoint + memtable
//!   list_events   — raw events from segment files by timestamp range
//!   get_alerts    — alert log entries since a given offset
//!
//! get_realtime accepts period_type and billing_cycle_day to support both
//! fixed and calendar period modes via resolve_period_id().

const std = @import("std");
const disk_io  = @import("../io/disk_io.zig");
const time_util = @import("../util/time.zig");

const Event = @import("../event.zig").Event;

const AggKey   = @import("memtable.zig").AggKey;
const AggValue = @import("memtable.zig").AggValue;
const Memtable = @import("memtable.zig").Memtable;

const checkpoint   = @import("checkpoint.zig");
const CheckpointEntry = checkpoint.CheckpointEntry;
const AlertLog     = @import("alert.zig").AlertLog;
const AlertEntry   = @import("alert.zig").AlertEntry;

const aggregators = @import("aggregators.zig");

/// One result row returned by get_realtime / get_usage.
pub const AggEntry = struct {
    key:   AggKey,
    value: AggValue,
};

/// Return the current-period aggregate for a specific (account_id, metric_code).
/// filter_hash=0 (Phase 2: no filter support yet).
/// Returns null when no events have been ingested for this combination.
pub fn get_realtime(
    memtable:          *const Memtable,
    account_id:        u64,
    metric_code:       u64,
    period_ns:         i64,
    period_type:       u8,
    billing_cycle_day: u8,
) ?AggEntry {
    const now_ns: i64 = @intCast(time_util.wallNanos());
    const pid = aggregators.resolve_period_id(now_ns, period_type, @bitCast(period_ns), billing_cycle_day);
    const key = AggKey{ .account_id = account_id, .period_id = pid, .metric_code = metric_code, .filter_hash = 0 };
    const val = memtable.get(key) orelse return null;
    return AggEntry{ .key = key, .value = val };
}

/// Return all aggregate entries for account_id in [period_start, period_end] (inclusive).
/// Closed periods are read from the checkpoint file; the current period from memtable.
/// out_buf must be large enough to hold all matching entries.
pub fn get_usage(
    alloc:          std.mem.Allocator,
    checkpoint_path: []const u8,
    memtable:       *const Memtable,
    account_id:     u64,
    period_start:   u32,
    period_end:     u32,
    out_buf:        []AggEntry,
) ![]AggEntry {
    var n: usize = 0;

    // Scan checkpoint for closed periods.
    const cp = checkpoint.load(alloc, checkpoint_path) catch |err| switch (err) {
        error.FileNotFound => null,
        else               => return err,
    };
    if (cp) |c| {
        var loaded = c;
        defer loaded.memtable.deinit();

        var it = loaded.memtable.map.iterator();
        while (it.next()) |kv| {
            const key = kv.key_ptr.*;
            if (key.account_id != account_id) continue;
            if (key.period_id < period_start or key.period_id > period_end) continue;
            if (n >= out_buf.len) return error.BufferTooSmall;
            out_buf[n] = .{ .key = key, .value = kv.value_ptr.* };
            n += 1;
        }
    }

    // Overlay current memtable (may have entries for the current period).
    var it = memtable.map.iterator();
    while (it.next()) |kv| {
        const key = kv.key_ptr.*;
        if (key.account_id != account_id) continue;
        if (key.period_id < period_start or key.period_id > period_end) continue;
        // Overwrite any checkpoint entry for the same key, or append.
        var found = false;
        for (out_buf[0..n]) |*e| {
            if (std.meta.eql(e.key, key)) {
                e.value = kv.value_ptr.*;
                found = true;
                break;
            }
        }
        if (!found) {
            if (n >= out_buf.len) return error.BufferTooSmall;
            out_buf[n] = .{ .key = key, .value = kv.value_ptr.* };
            n += 1;
        }
    }

    return out_buf[0..n];
}

/// Scan segment files in data_dir for events belonging to account_id
/// with timestamp in [ts_start, ts_end] (nanoseconds).
/// Results are written into out_buf; returns the populated slice.
pub fn list_events(
    data_dir:   []const u8,
    account_id: u64,
    ts_start:   i64,
    ts_end:     i64,
    out_buf:    []Event,
) ![]Event {
    const dir_fd = try disk_io.open_dir(data_dir);
    defer disk_io.close_dir(dir_fd);

    var n: usize = 0;
    var it = disk_io.DirIter.init(dir_fd);
    while (try it.next()) |entry_name| {
        if (!std.mem.endsWith(u8, entry_name, ".seg")) continue;
        if (n >= out_buf.len) break;

        var path_buf: [512]u8 = undefined;
        const path = try std.fmt.bufPrint(&path_buf, "{s}/{s}", .{ data_dir, entry_name });
        const file = try disk_io.open_ro(path);
        defer file.close();

        var event: Event = undefined;
        while (n < out_buf.len) {
            const read = try file.read(std.mem.asBytes(&event));
            if (read == 0) break;
            if (read != @sizeOf(Event)) return error.ShortRead;
            if (event.account_id != account_id) continue;
            if (event.timestamp < ts_start or event.timestamp > ts_end) continue;
            out_buf[n] = event;
            n += 1;
        }
    }
    return out_buf[0..n];
}

/// Return alert log entries starting from since_offset (entry index, 0-based).
/// Optionally filters by account_id when account_id != 0.
pub fn get_alerts(
    alert_log_path: []const u8,
    account_id:     u64, // 0 = return all accounts
    since_offset:   u64,
    out_buf:        []AlertEntry,
) ![]AlertEntry {
    var log = try AlertLog.open(alert_log_path);
    defer log.deinit();

    var raw: [256]AlertEntry = undefined;
    const entries = try log.scan(since_offset, &raw);

    if (account_id == 0) {
        const n = @min(entries.len, out_buf.len);
        @memcpy(out_buf[0..n], entries[0..n]);
        return out_buf[0..n];
    }

    var n: usize = 0;
    for (entries) |e| {
        if (e.account_id != account_id) continue;
        if (n >= out_buf.len) break;
        out_buf[n] = e;
        n += 1;
    }
    return out_buf[0..n];
}

// ---- tests ----

test "query: get_realtime returns current period" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var mt = Memtable.init(alloc);
    defer mt.deinit();

    const now_ns: i64 = @intCast(time_util.wallNanos());
    const period_ns: i64 = 30 * 24 * 3600 * std.time.ns_per_s;
    const pid = aggregators.period_id_of(now_ns, period_ns);

    const key = AggKey{ .account_id = 7, .period_id = pid, .metric_code = 99, .filter_hash = 0 };
    (try mt.get_or_put(key)).count = 42;

    const result = get_realtime(&mt, 7, 99, period_ns, 0, 0);
    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(u64, 42), result.?.value.count);
}

test "query: get_realtime returns null for unknown key" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();

    var mt = Memtable.init(gpa.allocator());
    defer mt.deinit();

    const result = get_realtime(&mt, 1, 1, 30 * 24 * 3600 * std.time.ns_per_s, 0, 0);
    try std.testing.expect(result == null);
}

test "query: get_usage reads from memtable when no checkpoint" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var mt = Memtable.init(alloc);
    defer mt.deinit();

    const key = AggKey{ .account_id = 1, .period_id = 5, .metric_code = 10, .filter_hash = 0 };
    (try mt.get_or_put(key)).sum = 3_000_000;

    var buf: [16]AggEntry = undefined;
    const entries = try get_usage(alloc, "/tmp/billing_query_nocp.bin", &mt, 1, 5, 5, &buf);
    try std.testing.expectEqual(@as(usize, 1), entries.len);
    try std.testing.expectEqual(@as(u128, 3_000_000), entries[0].value.sum);
}

test "query: get_usage merges checkpoint and memtable" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    const cp_path = "/tmp/billing_query_cp.bin";
    defer disk_io.remove(cp_path) catch {};

    // Save checkpoint with period 3.
    {
        var cp_mt = Memtable.init(alloc);
        defer cp_mt.deinit();
        const k3 = AggKey{ .account_id = 1, .period_id = 3, .metric_code = 1, .filter_hash = 0 };
        (try cp_mt.get_or_put(k3)).count = 10;
        try checkpoint.save(alloc, &cp_mt, 99, cp_path);
    }

    // Memtable has period 4.
    var mt = Memtable.init(alloc);
    defer mt.deinit();
    const k4 = AggKey{ .account_id = 1, .period_id = 4, .metric_code = 1, .filter_hash = 0 };
    (try mt.get_or_put(k4)).count = 20;

    var buf: [16]AggEntry = undefined;
    const entries = try get_usage(alloc, cp_path, &mt, 1, 3, 4, &buf);
    try std.testing.expectEqual(@as(usize, 2), entries.len);
}

test "query: list_events scans segment file" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();

    const data_dir = "/tmp/billing_query_events_test";
    disk_io.remove_tree(data_dir);
    try disk_io.make_path(data_dir);
    const seg = try disk_io.open_rw("/tmp/billing_query_events_test/00000000000000000000.seg");
    defer seg.close();

    const account_id: u64 = 5;
    for (0..5) |i| {
        const e = Event{
            .offset          = @intCast(i),
            .timestamp       = @intCast(1000 * (i + 1)),
            .idempotency_key = @intCast(i),
            .account_id      = if (i < 3) account_id else 99, // 3 match, 2 don't
            .metric_code     = 1,
            .value           = 1,
            .operation_type  = 0,
            ._pad            = .{ 0, 0, 0 },
            .checksum        = 0,
        };
        try seg.write_all(std.mem.asBytes(&e));
    }

    var buf: [10]Event = undefined;
    const events = try list_events(data_dir, account_id, 0, 5000, &buf);
    try std.testing.expectEqual(@as(usize, 3), events.len);
}

test "query: get_alerts filters by account_id" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();

    const path = "/tmp/billing_query_alerts.bin";
    defer disk_io.remove(path) catch {};

    var log = try AlertLog.open(path);
    _ = try log.append(.{ .account_id = 1, .metric_code = 1, .threshold_index = 0,
                      ._pad = .{ 0 } ** 7, .value_at_cross = 100, .event_timestamp = 1000 });
    _ = try log.append(.{ .account_id = 2, .metric_code = 1, .threshold_index = 0,
                      ._pad = .{ 0 } ** 7, .value_at_cross = 200, .event_timestamp = 2000 });
    _ = try log.append(.{ .account_id = 1, .metric_code = 2, .threshold_index = 0,
                      ._pad = .{ 0 } ** 7, .value_at_cross = 300, .event_timestamp = 3000 });
    log.deinit();

    var buf: [10]AlertEntry = undefined;
    const alerts = try get_alerts(path, 1, 0, &buf);
    try std.testing.expectEqual(@as(usize, 2), alerts.len);
    try std.testing.expectEqual(@as(u64, 1), alerts[0].account_id);
    try std.testing.expectEqual(@as(u64, 1), alerts[1].account_id);
}
