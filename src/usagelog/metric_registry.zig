//! MetricRegistry: per-metric aggregation configuration.
//! Supports atomic hot-reload: build a new registry, then swap the pointer.
//! Persisted as a flat binary file of fixed-size MetricSchema records (1704B each).
//!
//! MetricSchema includes:
//!   period_type (0=fixed, 1=calendar) — controls how period_id is computed
//!   billing_cycle_day (1-28) — day of month when calendar billing period starts
//!   filters (up to 4 DimensionFilters) — per-dimension and AND-filter aggregation
//!   alert_thresholds (up to 8) — threshold-based alert firing

const std     = @import("std");
const disk_io = @import("../io/disk_io.zig");

pub const AggType = enum(u8) {
    count        = 0,
    sum          = 1,
    max          = 2,
    latest       = 3,
    count_unique = 4,
};

/// One alert threshold attached to a metric. 48 bytes.
pub const AlertThreshold = extern struct {
    code:      [32]u8, // human label, e.g. "soft", "hard", "80pct"
    value:     u64,    // threshold in the same units as the aggregate
    recurring: bool,   // re-fire on every update that stays above the threshold
    _pad:      [7]u8,
};

comptime {
    std.debug.assert(@sizeOf(AlertThreshold) == 48);
}

/// One dimension filter: match events where key ∈ values. 292 bytes.
/// Wire-compatible with DimensionFilterWire in the Go SDK.
pub const DimensionFilter = extern struct {
    key:         [32]u8,    // property key, null-padded
    value_count: u8,        // number of active values (0..8)
    _pad:        [3]u8,
    values:      [8][32]u8, // allowed values, null-padded
};

comptime {
    std.debug.assert(@sizeOf(DimensionFilter) == 292);
}

pub const MAX_FILTERS: u8 = 4;

pub const PeriodType = enum(u8) {
    fixed    = 0, // floor(timestamp_ns / period_ns)
    calendar = 1, // calendar month with optional billing_cycle_day offset
};

/// Fixed-size schema record. 1704 bytes.
pub const MetricSchema = extern struct {
    code:              u64,                        //    8B @    0
    code_str:          [64]u8,                     //   64B @    8
    agg_type:          u8,                         //    1B @   72  AggType value
    field_name:        [64]u8,                     //   64B @   73  property field for aggregation
    recurring:         bool,                       //    1B @  137  carry value across billing periods
    alert_count:       u8,                         //    1B @  138  number of active alert_thresholds (0..8)
    filter_count:      u8,                         //    1B @  139  number of active filters (0..MAX_FILTERS)
    period_type:       u8,                         //    1B @  140  PeriodType: 0=fixed, 1=calendar
    billing_cycle_day: u8,                         //    1B @  141  1-28, day of month when billing period starts (calendar only)
    _pad:              [2]u8,                      //    2B @  142
    period_ns:         u64,                        //    8B @  144  billing period length in nanoseconds (fixed only)
    alert_thresholds:  [8]AlertThreshold,          //  384B @  152
    filters:           [MAX_FILTERS]DimensionFilter, // 1168B @  536
};

comptime {
    std.debug.assert(@sizeOf(MetricSchema) == 1704);
}

pub const MetricRegistry = struct {
    alloc:   std.mem.Allocator,
    schemas: std.AutoHashMap(u64, MetricSchema),

    pub fn init(alloc: std.mem.Allocator) MetricRegistry {
        return .{
            .alloc   = alloc,
            .schemas = std.AutoHashMap(u64, MetricSchema).init(alloc),
        };
    }

    pub fn deinit(self: *MetricRegistry) void {
        self.schemas.deinit();
    }

    pub fn put(self: *MetricRegistry, schema: MetricSchema) !void {
        try self.schemas.put(schema.code, schema);
    }

    pub fn get(self: *const MetricRegistry, code: u64) ?MetricSchema {
        return self.schemas.get(code);
    }

    pub fn remove(self: *MetricRegistry, code: u64) void {
        _ = self.schemas.remove(code);
    }

    /// Persist to file atomically: write to <path>.tmp, then rename.
    pub fn save(self: *const MetricRegistry, path: []const u8) !void {
        var tmp_buf: [512]u8 = undefined;
        const tmp_path = try std.fmt.bufPrint(&tmp_buf, "{s}.tmp", .{path});

        {
            const f = try disk_io.open_rw(tmp_path);
            defer f.close();
            var it = self.schemas.valueIterator();
            while (it.next()) |s| try f.write_all(std.mem.asBytes(s));
        }
        try disk_io.rename(tmp_path, path);
    }

    /// Load from file. Returns empty registry if file does not exist.
    pub fn load(alloc: std.mem.Allocator, path: []const u8) !MetricRegistry {
        var reg = MetricRegistry.init(alloc);
        errdefer reg.deinit();

        const f = disk_io.open_ro(path) catch |err| switch (err) {
            error.FileNotFound => return reg,
            else               => return err,
        };
        defer f.close();

        var schema: MetricSchema = undefined;
        while (true) {
            const n = try f.read(std.mem.asBytes(&schema));
            if (n == 0) break;
            if (n != @sizeOf(MetricSchema)) return error.CorruptRegistry;
            try reg.put(schema);
        }
        return reg;
    }
};

/// FNV-1a 64-bit hash. Stops at the first null byte or end of slice.
pub fn fnv1a(s: []const u8) u64 {
    const prime:  u64 = 0x00000100000001B3;
    const offset: u64 = 0xcbf29ce484222325;
    var h = offset;
    for (s) |c| {
        if (c == 0) break;
        h ^= c;
        h *%= prime;
    }
    return h;
}

test "metric_registry: put get remove" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();

    var reg = MetricRegistry.init(gpa.allocator());
    defer reg.deinit();

    var schema = std.mem.zeroes(MetricSchema);
    schema.code = fnv1a("api_calls");
    schema.agg_type = @intFromEnum(AggType.count);

    try reg.put(schema);
    try std.testing.expect(reg.get(schema.code) != null);
    reg.remove(schema.code);
    try std.testing.expect(reg.get(schema.code) == null);
}

test "metric_registry: save and load" {
    const path = "/tmp/billing_registry_test.bin";
    defer disk_io.remove(path) catch {};

    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var schema = std.mem.zeroes(MetricSchema);
    schema.code = fnv1a("bytes_in");
    schema.agg_type = @intFromEnum(AggType.sum);

    {
        var reg = MetricRegistry.init(alloc);
        defer reg.deinit();
        try reg.put(schema);
        try reg.save(path);
    }

    var reg2 = try MetricRegistry.load(alloc, path);
    defer reg2.deinit();

    const got = reg2.get(schema.code);
    try std.testing.expect(got != null);
    try std.testing.expectEqual(schema.agg_type, got.?.agg_type);
}

test "fnv1a: null-terminated string" {
    // Same hash regardless of trailing nulls.
    var buf: [64]u8 = std.mem.zeroes([64]u8);
    @memcpy(buf[0..8], "api_call");
    try std.testing.expectEqual(fnv1a("api_call"), fnv1a(buf[0..]));
}
