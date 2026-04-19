//! Aggregate memtable: in-memory store for current-period aggregates.
//! Keyed by (account_id, period_id, metric_code, filter_hash).

const std = @import("std");

/// Key identifying one aggregate bucket.
/// extern struct: deterministic layout required for checkpoint serialization.
pub const AggKey = extern struct {
    account_id:  u64,
    period_id:   u32,  // floor(event.timestamp_ns / period_ns)
    _pad:        u32 = 0,
    metric_code: u64,
    filter_hash: u64,  // FNV-1a over filter key=value pairs; 0 = no filters
};

comptime {
    std.debug.assert(@sizeOf(AggKey) == 32);
}

/// Accumulated values for one aggregate bucket.
/// extern struct: deterministic layout required for checkpoint serialization.
pub const AggValue = extern struct {
    sum:             u128 = 0, // SUM: accumulates scaled integers (u128 prevents overflow)
    count:           u64  = 0, // COUNT / COUNT UNIQUE: event count or unique-set size
    max:             u64  = 0, // MAX
    last_value:      u64  = 0, // LATEST: value of the most recent event by timestamp
    last_timestamp:  i64  = 0, // LATEST: timestamp of the last recorded value
    last_seg_offset: u64  = 0, // exactly-once barrier: highest segment offset applied
    alert_flags:     u64  = 0, // bitmask: bit N set when alert threshold N is crossed
};

comptime {
    std.debug.assert(@sizeOf(AggValue) == 64);
}

pub const Memtable = struct {
    map: std.AutoHashMap(AggKey, AggValue),

    pub fn init(alloc: std.mem.Allocator) Memtable {
        return .{ .map = std.AutoHashMap(AggKey, AggValue).init(alloc) };
    }

    /// Initialize with pre-allocated capacity. Avoids rehash churn during ingest
    /// when the number of unique aggregate keys is roughly known up-front.
    pub fn init_capacity(alloc: std.mem.Allocator, capacity: u32) !Memtable {
        var map = std.AutoHashMap(AggKey, AggValue).init(alloc);
        try map.ensureTotalCapacity(capacity);
        return .{ .map = map };
    }

    pub fn deinit(self: *Memtable) void {
        self.map.deinit();
    }

    /// Return a pointer to the value for key.
    /// Inserts a zero-initialized AggValue when the key is absent.
    pub fn get_or_put(self: *Memtable, key: AggKey) !*AggValue {
        const gop = try self.map.getOrPut(key);
        if (!gop.found_existing) gop.value_ptr.* = .{};
        return gop.value_ptr;
    }

    pub fn get(self: *const Memtable, key: AggKey) ?AggValue {
        return self.map.get(key);
    }

    pub fn count(self: *const Memtable) usize {
        return self.map.count();
    }
};

// ---- tests ----

test "memtable: get_or_put inserts zero value" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();

    var mt = Memtable.init(gpa.allocator());
    defer mt.deinit();

    const key = AggKey{ .account_id = 1, .period_id = 202504, .metric_code = 42, .filter_hash = 0 };
    const v1 = try mt.get_or_put(key);
    try std.testing.expectEqual(@as(u128, 0), v1.sum);
    try std.testing.expectEqual(@as(u64, 0), v1.count);

    v1.sum = 1_000_000;
    const v2 = try mt.get_or_put(key);
    try std.testing.expectEqual(@as(u128, 1_000_000), v2.sum);
    try std.testing.expectEqual(@as(usize, 1), mt.count());
}

test "memtable: multiple keys are independent" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();

    var mt = Memtable.init(gpa.allocator());
    defer mt.deinit();

    for (0..10) |i| {
        const key = AggKey{ .account_id = @intCast(i), .period_id = 1, .metric_code = 1, .filter_hash = 0 };
        const v = try mt.get_or_put(key);
        v.count = @intCast(i * 2);
    }
    try std.testing.expectEqual(@as(usize, 10), mt.count());

    for (0..10) |i| {
        const key = AggKey{ .account_id = @intCast(i), .period_id = 1, .metric_code = 1, .filter_hash = 0 };
        const v = mt.get(key).?;
        try std.testing.expectEqual(@as(u64, @intCast(i * 2)), v.count);
    }
}

test "memtable: same account different periods" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();

    var mt = Memtable.init(gpa.allocator());
    defer mt.deinit();

    const k1 = AggKey{ .account_id = 1, .period_id = 1, .metric_code = 1, .filter_hash = 0 };
    const k2 = AggKey{ .account_id = 1, .period_id = 2, .metric_code = 1, .filter_hash = 0 };

    (try mt.get_or_put(k1)).sum = 100;
    (try mt.get_or_put(k2)).sum = 200;

    try std.testing.expectEqual(@as(u128, 100), mt.get(k1).?.sum);
    try std.testing.expectEqual(@as(u128, 200), mt.get(k2).?.sum);
}
