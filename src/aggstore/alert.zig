//! Alert system: threshold detection, flag management, and append-only alert log.
//!
//! update_alert_flags() is called after every aggregate update.
//! It sets/clears bits in AggValue.alert_flags and appends to AlertLog on crossing.

const std = @import("std");

const disk_io      = @import("../io/disk_io.zig");
const AggValue     = @import("memtable.zig").AggValue;
const MetricSchema = @import("../usagelog/metric_registry.zig").MetricSchema;

/// One alert log entry. 40 bytes, fixed-width for sequential scan.
pub const AlertEntry = extern struct {
    account_id:      u64,
    metric_code:     u64,
    threshold_index: u8,   // which threshold was crossed (0–7)
    _pad:            [7]u8,
    value_at_cross:  u64,
    event_timestamp: i64,  // timestamp of the event that triggered the crossing
};

comptime {
    std.debug.assert(@sizeOf(AlertEntry) == 40);
}

/// Append-only log of threshold crossings, written to alert_log.bin.
pub const AlertLog = struct {
    file:       disk_io.RealFile,
    next_index: u64 = 0, // entry index of the next append (0-based)

    pub fn open(path: []const u8) !AlertLog {
        const file = try disk_io.open_rw(path);
        try file.seek_end();
        // Initialise next_index from file size so the index matches what
        // `scan()` would use when reading the log back.
        const sz = try file.size();
        const next_index = sz / @sizeOf(AlertEntry);
        return .{ .file = file, .next_index = next_index };
    }

    pub fn deinit(self: *AlertLog) void {
        self.file.close();
    }

    /// Append one entry. Returns the entry index (not byte offset) that was
    /// assigned. Client-side push uses this index to track last-seen offset.
    pub fn append(self: *AlertLog, entry: AlertEntry) !u64 {
        try self.file.write_all(std.mem.asBytes(&entry));
        const idx = self.next_index;
        self.next_index += 1;
        return idx;
    }

    /// Read entries from since_offset (entry index, not byte offset) into out_buf.
    /// Returns the slice of entries that fit.
    pub fn scan(self: *AlertLog, since_offset: u64, out_buf: []AlertEntry) ![]AlertEntry {
        const byte_pos = since_offset * @sizeOf(AlertEntry);
        try self.file.seek_to(byte_pos);
        var n: usize = 0;
        while (n < out_buf.len) {
            const bytes = try self.file.read(std.mem.asBytes(&out_buf[n]));
            if (bytes == 0) break;
            if (bytes != @sizeOf(AlertEntry)) return error.CorruptAlertLog;
            n += 1;
        }
        return out_buf[0..n];
    }
};

/// Optional callback invoked after a crossing is durably appended to alert_log.
/// `log_index` is the entry index returned by `AlertLog.append`.
/// Server uses this to fan-out live pushes to subscribed client connections.
pub const PushFn = *const fn (ctx: *anyopaque, entry: AlertEntry, log_index: u64) void;

/// Check each active threshold in schema against current_value.
/// Sets/clears bits in agg.alert_flags and appends to alert_log on new crossings.
/// When `push_fn` is non-null, it is invoked once per new crossing AFTER the
/// append to alert_log succeeds. Log write failures are silently ignored to
/// avoid disrupting the main ingest path.
pub fn update_alert_flags(
    agg:             *AggValue,
    schema:          MetricSchema,
    current_value:   u64,
    account_id:      u64,
    event_timestamp: i64,
    alert_log:       *AlertLog,
    push_fn:         ?PushFn,
    push_ctx:        ?*anyopaque,
) void {
    const n = @min(schema.alert_count, 8);
    for (schema.alert_thresholds[0..n], 0..) |threshold, i| {
        const bit: u64 = @as(u64, 1) << @intCast(i);
        const was_set     = (agg.alert_flags & bit) != 0;
        const now_crossed = current_value >= threshold.value;

        if (now_crossed and (!was_set or threshold.recurring)) {
            agg.alert_flags |= bit;
            const entry = AlertEntry{
                .account_id      = account_id,
                .metric_code     = schema.code,
                .threshold_index = @intCast(i),
                ._pad            = .{ 0, 0, 0, 0, 0, 0, 0 },
                .value_at_cross  = current_value,
                .event_timestamp = event_timestamp,
            };
            const log_index = alert_log.append(entry) catch continue;
            if (push_fn) |f| {
                if (push_ctx) |c| f(c, entry, log_index);
            }
        }
        if (!now_crossed) {
            agg.alert_flags &= ~bit; // clear so recurring can fire again on next crossing
        }
    }
}

// ---- tests ----

fn make_schema_with_threshold(threshold_value: u64, recurring: bool) MetricSchema {
    var s = std.mem.zeroes(MetricSchema);
    s.code        = 1;
    s.alert_count = 1;
    s.alert_thresholds[0].value     = threshold_value;
    s.alert_thresholds[0].recurring = recurring;
    return s;
}

test "alert: flag set when threshold crossed" {
    const tmp_path = "/tmp/billing_alert_test1.bin";
    defer disk_io.remove(tmp_path) catch {};

    var log = try AlertLog.open(tmp_path);
    defer log.deinit();

    var agg    = AggValue{};
    const schema = make_schema_with_threshold(100, false);

    update_alert_flags(&agg, schema, 99, 1, 0, &log, null, null);
    try std.testing.expectEqual(@as(u64, 0), agg.alert_flags); // not yet crossed

    update_alert_flags(&agg, schema, 100, 1, 1, &log, null, null);
    try std.testing.expectEqual(@as(u64, 1), agg.alert_flags & 1); // bit 0 set
}

test "alert: flag cleared when value drops below threshold" {
    const tmp_path = "/tmp/billing_alert_test2.bin";
    defer disk_io.remove(tmp_path) catch {};

    var log = try AlertLog.open(tmp_path);
    defer log.deinit();

    var agg    = AggValue{};
    const schema = make_schema_with_threshold(100, false);

    update_alert_flags(&agg, schema, 150, 1, 0, &log, null, null);
    try std.testing.expectEqual(@as(u64, 1), agg.alert_flags & 1);

    update_alert_flags(&agg, schema, 50, 1, 1, &log, null, null);
    try std.testing.expectEqual(@as(u64, 0), agg.alert_flags & 1); // cleared
}

test "alert: non-recurring fires only once" {
    const tmp_path = "/tmp/billing_alert_test3.bin";
    defer disk_io.remove(tmp_path) catch {};

    var log = try AlertLog.open(tmp_path);
    defer log.deinit();

    var agg    = AggValue{};
    const schema = make_schema_with_threshold(100, false);

    update_alert_flags(&agg, schema, 200, 1, 0, &log, null, null);
    update_alert_flags(&agg, schema, 300, 1, 1, &log, null, null); // stays above — must not re-fire

    var buf: [10]AlertEntry = undefined;
    const entries = try log.scan(0, &buf);
    try std.testing.expectEqual(@as(usize, 1), entries.len); // only one entry logged
}

test "alert: recurring fires on each update above threshold" {
    const tmp_path = "/tmp/billing_alert_test4.bin";
    defer disk_io.remove(tmp_path) catch {};

    var log = try AlertLog.open(tmp_path);
    defer log.deinit();

    var agg    = AggValue{};
    const schema = make_schema_with_threshold(100, true); // recurring=true

    update_alert_flags(&agg, schema, 200, 1, 0, &log, null, null);
    update_alert_flags(&agg, schema, 300, 1, 1, &log, null, null);

    var buf: [10]AlertEntry = undefined;
    const entries = try log.scan(0, &buf);
    try std.testing.expectEqual(@as(usize, 2), entries.len);
}

test "alert: scan returns entries from offset" {
    const tmp_path = "/tmp/billing_alert_test5.bin";
    defer disk_io.remove(tmp_path) catch {};

    var log = try AlertLog.open(tmp_path);
    defer log.deinit();

    var agg    = AggValue{};
    const schema = make_schema_with_threshold(10, true);

    update_alert_flags(&agg, schema, 20, 42, 1000, &log, null, null);
    update_alert_flags(&agg, schema, 30, 42, 2000, &log, null, null);
    update_alert_flags(&agg, schema, 40, 42, 3000, &log, null, null);

    var buf: [10]AlertEntry = undefined;
    const from1 = try log.scan(1, &buf);
    try std.testing.expectEqual(@as(usize, 2), from1.len);
    try std.testing.expectEqual(@as(i64, 2000), from1[0].event_timestamp);
}
