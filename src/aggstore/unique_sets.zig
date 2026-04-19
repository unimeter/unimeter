//! COUNT UNIQUE: per-bucket sets of unique field values.
//! Each bucket (AggKey) owns a HashSet(u64) of hashed field values.
//! operation_type=add → insert; operation_type=remove → delete.
//! agg.count is kept in sync with the set size after every update.

const std = @import("std");

const AggKey       = @import("memtable.zig").AggKey;
const AggValue     = @import("memtable.zig").AggValue;
const OperationType = @import("../event.zig").OperationType;

const Set = std.AutoHashMap(u64, void);

pub const UniqueSets = struct {
    map:   std.AutoHashMap(AggKey, Set),
    alloc: std.mem.Allocator,

    pub fn init(alloc: std.mem.Allocator) UniqueSets {
        return .{
            .map   = std.AutoHashMap(AggKey, Set).init(alloc),
            .alloc = alloc,
        };
    }

    pub fn deinit(self: *UniqueSets) void {
        var it = self.map.valueIterator();
        while (it.next()) |set| set.deinit();
        self.map.deinit();
    }

    /// Insert or remove field_value from the set for key, then sync agg.count.
    /// op=none/add → insert; op=remove → delete.
    pub fn update(self: *UniqueSets, key: AggKey, field_value: u64,
                  op: OperationType, agg: *AggValue) !void {
        const gop = try self.map.getOrPut(key);
        if (!gop.found_existing) gop.value_ptr.* = Set.init(self.alloc);
        const set = gop.value_ptr;

        switch (op) {
            .none, .add => try set.put(field_value, {}),
            .remove     => _ = set.remove(field_value),
        }
        agg.count = @intCast(set.count());
    }

    /// Return the number of unique values for key, or 0 if bucket absent.
    pub fn count(self: *const UniqueSets, key: AggKey) u64 {
        const set = self.map.get(key) orelse return 0;
        return @intCast(set.count());
    }

    /// Remove the set for key. Called during period eviction.
    pub fn remove_key(self: *UniqueSets, key: AggKey) void {
        if (self.map.fetchRemove(key)) |kv| {
            // fetchRemove returns a copy of the value; deinit the inner set.
            var set = kv.value;
            set.deinit();
        }
    }
};

// ---- tests ----

test "unique_sets: add increases count" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();

    var us = UniqueSets.init(gpa.allocator());
    defer us.deinit();

    const key = AggKey{ .account_id = 1, .period_id = 1, .metric_code = 1, .filter_hash = 0 };
    var agg = AggValue{};

    try us.update(key, 100, .add, &agg);
    try std.testing.expectEqual(@as(u64, 1), agg.count);

    try us.update(key, 200, .add, &agg);
    try std.testing.expectEqual(@as(u64, 2), agg.count);

    // Duplicate add — count must not change.
    try us.update(key, 100, .add, &agg);
    try std.testing.expectEqual(@as(u64, 2), agg.count);
}

test "unique_sets: remove decreases count" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();

    var us = UniqueSets.init(gpa.allocator());
    defer us.deinit();

    const key = AggKey{ .account_id = 1, .period_id = 1, .metric_code = 1, .filter_hash = 0 };
    var agg = AggValue{};

    try us.update(key, 100, .add, &agg);
    try us.update(key, 200, .add, &agg);
    try std.testing.expectEqual(@as(u64, 2), agg.count);

    try us.update(key, 100, .remove, &agg);
    try std.testing.expectEqual(@as(u64, 1), agg.count);

    // Remove non-existent — count must not go negative.
    try us.update(key, 999, .remove, &agg);
    try std.testing.expectEqual(@as(u64, 1), agg.count);
}

test "unique_sets: count matches set size" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();

    var us = UniqueSets.init(gpa.allocator());
    defer us.deinit();

    const key = AggKey{ .account_id = 42, .period_id = 5, .metric_code = 7, .filter_hash = 0 };
    var agg = AggValue{};

    for (0..20) |i| {
        try us.update(key, @intCast(i), .add, &agg);
    }
    try std.testing.expectEqual(@as(u64, 20), us.count(key));
    try std.testing.expectEqual(@as(u64, 20), agg.count);

    for (0..10) |i| {
        try us.update(key, @intCast(i), .remove, &agg);
    }
    try std.testing.expectEqual(@as(u64, 10), us.count(key));
    try std.testing.expectEqual(@as(u64, 10), agg.count);
}

test "unique_sets: remove_key frees memory" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();

    var us = UniqueSets.init(gpa.allocator());
    defer us.deinit();

    const key = AggKey{ .account_id = 1, .period_id = 1, .metric_code = 1, .filter_hash = 0 };
    var agg = AggValue{};

    try us.update(key, 1, .add, &agg);
    try us.update(key, 2, .add, &agg);

    us.remove_key(key);
    try std.testing.expectEqual(@as(u64, 0), us.count(key));
}

test "unique_sets: independent buckets" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();

    var us = UniqueSets.init(gpa.allocator());
    defer us.deinit();

    const k1 = AggKey{ .account_id = 1, .period_id = 1, .metric_code = 1, .filter_hash = 0 };
    const k2 = AggKey{ .account_id = 2, .period_id = 1, .metric_code = 1, .filter_hash = 0 };
    var a1 = AggValue{};
    var a2 = AggValue{};

    try us.update(k1, 10, .add, &a1);
    try us.update(k1, 20, .add, &a1);
    try us.update(k2, 10, .add, &a2); // same field_value, different bucket

    try std.testing.expectEqual(@as(u64, 2), a1.count);
    try std.testing.expectEqual(@as(u64, 1), a2.count);
}
