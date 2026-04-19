//! AggWorker: reads UsageLog segment files and aggregates events into Memtable.
//!
//! Designed for sequential, synchronous reads — no io_uring needed for this path.
//!
//! Filter hashing: compute_per_dimension_hashes() produces per-dimension hashes
//! (one per matched DimensionFilter) plus a combined AND hash when 2+ dimensions
//! match. The combined hash = XOR of all per-dimension hashes, enabling queries
//! like region=us AND tier=pro as a single AggKey lookup.
//!
//! Period resolution: apply_batch uses resolve_period_id() which dispatches to
//! fixed (floor division) or calendar (year/month) based on MetricSchema.period_type.

const std = @import("std");

const disk_io      = @import("../io/disk_io.zig");
const Event         = @import("../event.zig").Event;
const OperationType = @import("../event.zig").OperationType;

const MetricRegistry    = @import("../usagelog/metric_registry.zig").MetricRegistry;
const MetricSchema      = @import("../usagelog/metric_registry.zig").MetricSchema;
const DimensionFilter   = @import("../usagelog/metric_registry.zig").DimensionFilter;
const AggType           = @import("../usagelog/metric_registry.zig").AggType;
const fnv1a             = @import("../usagelog/metric_registry.zig").fnv1a;

const proto = @import("../usagelog/protocol.zig");
pub const PropPair = proto.PropPair;

const Memtable    = @import("memtable.zig").Memtable;
const AggKey      = @import("memtable.zig").AggKey;
const AggValue    = @import("memtable.zig").AggValue;
const UniqueSets  = @import("unique_sets.zig").UniqueSets;
const aggregators = @import("aggregators.zig");
const watermark   = @import("watermark.zig");
const alert       = @import("alert.zig");
const AlertLog    = alert.AlertLog;

/// 30-day approximate billing period (nanoseconds).
/// Overridden per-metric by MetricSchema.period_ns when non-zero.
pub const DEFAULT_PERIOD_NS: i64 = 30 * 24 * 3600 * std.time.ns_per_s;

pub const AggWorker = struct {
    alloc:       std.mem.Allocator,
    memtable:    *Memtable,
    unique_sets: *UniqueSets,
    registry:    *const MetricRegistry,
    alert_log:   *AlertLog,
    watermark:   i64 = 0,
    /// Optional push callback invoked on each new alert crossing after it is
    /// durably appended to alert_log. Server plugs in its broadcast fan-out here.
    alert_push_fn:  ?alert.PushFn   = null,
    alert_push_ctx: ?*anyopaque     = null,

    /// Scan all .seg files in data_dir and apply events with offset >= start_offset.
    /// Returns the number of events processed.
    pub fn run_once(self: *AggWorker, data_dir: []const u8, start_offset: u64) !u64 {
        const dir_fd = try disk_io.open_dir(data_dir);
        defer disk_io.close_dir(dir_fd);

        // Collect .seg filenames into a fixed-size array.
        // 256 segments × 1M events/segment = up to 256M events per partition.
        var names: [256][32]u8 = undefined;
        var n_names: usize = 0;
        var it = disk_io.DirIter.init(dir_fd);
        while (try it.next()) |name| {
            if (!std.mem.endsWith(u8, name, ".seg")) continue;
            if (n_names >= names.len) continue;
            if (name.len >= names[n_names].len) continue;
            @memset(&names[n_names], 0);
            @memcpy(names[n_names][0..name.len], name);
            n_names += 1;
        }

        sort_seg_names(names[0..n_names]);

        var processed: u64 = 0;
        for (names[0..n_names]) |name_arr| {
            const name = std.mem.sliceTo(&name_arr, 0);
            processed += try self.process_segment(data_dir, name, start_offset);
        }
        return processed;
    }

    fn process_segment(self: *AggWorker, data_dir: []const u8, name: []const u8, start_offset: u64) !u64 {
        var path_buf: [512]u8 = undefined;
        const path = try std.fmt.bufPrint(&path_buf, "{s}/{s}", .{ data_dir, name });
        const file = try disk_io.open_ro(path);
        defer file.close();

        var event: Event = undefined;
        var processed: u64 = 0;
        while (true) {
            const n = try file.read(std.mem.asBytes(&event));
            if (n == 0) break;
            if (n != @sizeOf(Event)) return error.ShortRead;
            if (event.offset < start_offset) continue;
            try self.apply(event);
            processed += 1;
        }
        return processed;
    }

    /// Apply one event without properties (filter_hash = 0).
    pub fn apply(self: *AggWorker, event: Event) !void {
        return self.apply_with_props(event, &.{});
    }

    /// Resolved schema info cached across events with the same metric_code.
    const SchemaCache = struct {
        code:              u64,
        schema:            MetricSchema,
        agg_type:          AggType,
        period_ns:         i64,
        period_type:       u8,
        billing_cycle_day: u8,
        valid:             bool,
    };

    /// Apply a batch of events with per-event props access.
    /// `props_of(i)` returns the props slice for events[i]. Pass a no-op closure
    /// returning `&.{}` when props aren't used.
    ///
    /// Caches the schema lookup across consecutive events sharing the same
    /// metric_code — a common pattern when a single client sends a batch for
    /// one metric. Saves one HashMap lookup per event on cache hit.
    pub fn apply_batch(
        self: *AggWorker,
        events: []const Event,
        ctx: anytype,
        comptime props_of: fn (@TypeOf(ctx), usize) []const PropPair,
    ) !void {
        var cache: SchemaCache = .{
            .code = 0, .schema = undefined, .agg_type = .count,
            .period_ns = DEFAULT_PERIOD_NS, .period_type = 0, .billing_cycle_day = 1,
            .valid = false,
        };

        for (events, 0..) |event, i| {
            // Schema cache: avoid re-looking up when metric_code is unchanged.
            if (!cache.valid or cache.code != event.metric_code) {
                const schema = self.registry.get(event.metric_code) orelse continue;
                cache = .{
                    .code              = event.metric_code,
                    .schema            = schema,
                    .agg_type          = std.enums.fromInt(AggType, schema.agg_type) orelse continue,
                    .period_ns         = if (schema.period_ns > 0) @intCast(schema.period_ns) else DEFAULT_PERIOD_NS,
                    .period_type       = schema.period_type,
                    .billing_cycle_day = schema.billing_cycle_day,
                    .valid             = true,
                };
            }

            const pid = aggregators.resolve_period_id(event.timestamp, cache.period_type, @bitCast(cache.period_ns), cache.billing_cycle_day);

            // Unfiltered aggregate — always updated.
            const key = AggKey{
                .account_id  = event.account_id,
                .period_id   = pid,
                .metric_code = event.metric_code,
                .filter_hash = 0,
            };
            try self.apply_to_key(key, event, cache.agg_type, &cache.schema);

            // Per-dimension filtered aggregates (only if schema has filters).
            if (cache.schema.filter_count > 0) {
                const props = props_of(ctx, i);
                if (props.len > 0) {
                    var hashes: [5]u64 = .{0} ** 5;
                    const n = compute_per_dimension_hashes(&cache.schema, props, &hashes);
                    for (hashes[0..n]) |fh| {
                        if (fh == 0) continue;
                        const fkey = AggKey{
                            .account_id  = event.account_id,
                            .period_id   = pid,
                            .metric_code = event.metric_code,
                            .filter_hash = fh,
                        };
                        try self.apply_to_key(fkey, event, cache.agg_type, &cache.schema);
                    }
                }
            }
        }
    }

    /// Apply one event with its associated properties.
    /// Creates per-dimension filtered aggregates for each matched filter key-value pair.
    pub fn apply_with_props(self: *AggWorker, event: Event, props: []const PropPair) !void {
        const schema = self.registry.get(event.metric_code) orelse return;
        const agg_type = std.enums.fromInt(AggType, schema.agg_type) orelse return;

        const pid = aggregators.resolve_period_id(event.timestamp, schema.period_type, schema.period_ns, schema.billing_cycle_day);

        // Always update the unfiltered aggregate (filter_hash=0).
        const key_unfiltered = AggKey{
            .account_id  = event.account_id,
            .period_id   = pid,
            .metric_code = event.metric_code,
            .filter_hash = 0,
        };
        try self.apply_to_key(key_unfiltered, event, agg_type, &schema);

        // For each matched dimension filter, update a separate per-dimension aggregate.
        // This allows querying by any single dimension (e.g. provider=aws).
        if (schema.filter_count > 0 and props.len > 0) {
            var hashes: [5]u64 = .{0} ** 5;
            const n = compute_per_dimension_hashes(&schema, props, &hashes);
            for (hashes[0..n]) |fh| {
                if (fh != 0) {
                    const key_filtered = AggKey{
                        .account_id  = event.account_id,
                        .period_id   = pid,
                        .metric_code = event.metric_code,
                        .filter_hash = fh,
                    };
                    try self.apply_to_key(key_filtered, event, agg_type, &schema);
                }
            }
        }
    }

    fn apply_to_key(self: *AggWorker, key: AggKey, event: Event, agg_type: AggType, schema: *const MetricSchema) !void {
        const agg = try self.memtable.get_or_put(key);
        aggregators.update(agg, &event, agg_type);

        if (agg_type == .count_unique) {
            const op = std.enums.fromInt(OperationType, event.operation_type) orelse .none;
            try self.unique_sets.update(key, event.value, op, agg);
        }

        const current_value: u64 = switch (agg_type) {
            .count, .count_unique => agg.count,
            .sum    => if (agg.sum > std.math.maxInt(u64)) std.math.maxInt(u64)
                       else @intCast(agg.sum),
            .max    => agg.max,
            .latest => agg.last_value,
        };
        alert.update_alert_flags(agg, schema.*, current_value, event.account_id,
                                 event.timestamp, self.alert_log,
                                 self.alert_push_fn, self.alert_push_ctx);

        watermark.advance(&self.watermark, event.timestamp);
    }
};

/// Compute filter hashes for matched dimensions.
/// Returns the number of hashes written to out:
///   - Up to 4 per-dimension hashes (fnv1a(key) ^ fnv1a(value) each)
///   - If 2+ dimensions match, one combined AND hash (XOR of all per-dimension hashes)
/// The combined hash enables multi-dimension AND queries (e.g. region=us AND tier=pro).
pub fn compute_per_dimension_hashes(schema: *const MetricSchema, props: []const PropPair, out: *[5]u64) usize {
    var n: usize = 0;

    for (schema.filters[0..schema.filter_count]) |filter| {
        const filter_key = std.mem.sliceTo(&filter.key, 0);
        if (filter_key.len == 0) continue;

        for (props) |prop| {
            const prop_key = std.mem.sliceTo(&prop.key, 0);
            if (!std.mem.eql(u8, prop_key, filter_key)) continue;

            const prop_val = std.mem.sliceTo(&prop.value, 0);

            for (filter.values[0..filter.value_count]) |allowed| {
                const allowed_str = std.mem.sliceTo(&allowed, 0);
                if (std.mem.eql(u8, prop_val, allowed_str)) {
                    out[n] = fnv1a(filter_key) ^ fnv1a(prop_val);
                    n += 1;
                    break;
                }
            }
            break;
        }
    }

    // Combined AND hash: XOR all per-dimension hashes together.
    // Only added when 2+ dimensions match — single-dimension hash is already stored above.
    if (n >= 2) {
        var combined: u64 = 0;
        for (out[0..n]) |h| {
            combined ^= h;
        }
        out[n] = combined;
        n += 1;
    }

    return n;
}

fn sort_seg_names(names: [][32]u8) void {
    std.sort.pdq([32]u8, names, {}, struct {
        fn lt(_: void, a: [32]u8, b: [32]u8) bool {
            return std.mem.lessThan(u8, std.mem.sliceTo(&a, 0), std.mem.sliceTo(&b, 0));
        }
    }.lt);
}

// ---- tests ----

fn make_test_schema(code: u64, agg_type: AggType) @import("../usagelog/metric_registry.zig").MetricSchema {
    var s = std.mem.zeroes(@import("../usagelog/metric_registry.zig").MetricSchema);
    s.code     = code;
    s.agg_type = @intFromEnum(agg_type);
    s.period_ns = 24 * 3600 * std.time.ns_per_s; // 1-day periods for tests
    return s;
}

test "compute_per_dimension_hashes: single dim returns 1 hash, no combined" {
    var schema = std.mem.zeroes(MetricSchema);
    schema.filter_count = 1;
    @memcpy(schema.filters[0].key[0..6], "region");
    schema.filters[0].value_count = 2;
    @memcpy(schema.filters[0].values[0][0..2], "us");
    @memcpy(schema.filters[0].values[1][0..2], "eu");

    var prop = std.mem.zeroes(PropPair);
    @memcpy(prop.key[0..6], "region");
    @memcpy(prop.value[0..2], "us");
    const props = [_]PropPair{prop};

    var out: [5]u64 = .{0} ** 5;
    const n = compute_per_dimension_hashes(&schema, &props, &out);
    try std.testing.expectEqual(@as(usize, 1), n);
    try std.testing.expectEqual(fnv1a("region") ^ fnv1a("us"), out[0]);
}

test "compute_per_dimension_hashes: 2 dims returns 3 hashes (per-dim + combined)" {
    var schema = std.mem.zeroes(MetricSchema);
    schema.filter_count = 2;
    @memcpy(schema.filters[0].key[0..6], "region");
    schema.filters[0].value_count = 1;
    @memcpy(schema.filters[0].values[0][0..2], "us");
    @memcpy(schema.filters[1].key[0..4], "tier");
    schema.filters[1].value_count = 1;
    @memcpy(schema.filters[1].values[0][0..3], "pro");

    var p0 = std.mem.zeroes(PropPair);
    @memcpy(p0.key[0..6], "region");
    @memcpy(p0.value[0..2], "us");
    var p1 = std.mem.zeroes(PropPair);
    @memcpy(p1.key[0..4], "tier");
    @memcpy(p1.value[0..3], "pro");
    const props = [_]PropPair{ p0, p1 };

    var out: [5]u64 = .{0} ** 5;
    const n = compute_per_dimension_hashes(&schema, &props, &out);
    try std.testing.expectEqual(@as(usize, 3), n);

    const h_region = fnv1a("region") ^ fnv1a("us");
    const h_tier   = fnv1a("tier") ^ fnv1a("pro");
    try std.testing.expectEqual(h_region, out[0]);
    try std.testing.expectEqual(h_tier, out[1]);
    try std.testing.expectEqual(h_region ^ h_tier, out[2]);
}

test "compute_per_dimension_hashes: unmatched dim gives no combined" {
    var schema = std.mem.zeroes(MetricSchema);
    schema.filter_count = 2;
    @memcpy(schema.filters[0].key[0..6], "region");
    schema.filters[0].value_count = 1;
    @memcpy(schema.filters[0].values[0][0..2], "us");
    @memcpy(schema.filters[1].key[0..4], "tier");
    schema.filters[1].value_count = 1;
    @memcpy(schema.filters[1].values[0][0..3], "pro");

    // Only region prop, no tier — should get 1 hash, no combined.
    var p0 = std.mem.zeroes(PropPair);
    @memcpy(p0.key[0..6], "region");
    @memcpy(p0.value[0..2], "us");
    const props = [_]PropPair{p0};

    var out: [5]u64 = .{0} ** 5;
    const n = compute_per_dimension_hashes(&schema, &props, &out);
    try std.testing.expectEqual(@as(usize, 1), n);
}

test "worker: run_once aggregates COUNT events" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    const data_dir = "/tmp/billing_worker_count_test";
    disk_io.remove_tree(data_dir);
    try disk_io.make_path(data_dir);

    // Write a small segment directly.
    var seg_path_buf: [512]u8 = undefined;
    const seg_path = try std.fmt.bufPrint(&seg_path_buf, "{s}/00000000000000000000.seg", .{data_dir});
    const seg_file = try disk_io.open_rw(seg_path);
    defer seg_file.close();

    const metric_code: u64 = 42;
    const account_id: u64  = 1;
    const day_ns: i64      = 24 * 3600 * std.time.ns_per_s;

    for (0..5) |i| {
        const e = Event{
            .offset          = @intCast(i),
            .timestamp       = day_ns,       // all in period 1
            .idempotency_key = @intCast(i),
            .account_id      = account_id,
            .metric_code     = metric_code,
            .value           = 1_000_000,
            .operation_type  = 0,
            ._pad            = .{ 0, 0, 0 },
            .checksum        = 0,
        };
        try seg_file.write_all(std.mem.asBytes(&e));
    }

    // Set up registry.
    var reg = @import("../usagelog/metric_registry.zig").MetricRegistry.init(alloc);
    defer reg.deinit();
    try reg.put(make_test_schema(metric_code, .count));

    // Set up AggWorker.
    var mt = Memtable.init(alloc);
    defer mt.deinit();
    var us = UniqueSets.init(alloc);
    defer us.deinit();

    const alert_path = "/tmp/billing_worker_count_alert.bin";
    defer disk_io.remove(alert_path) catch {};
    var al = try AlertLog.open(alert_path);
    defer al.deinit();

    var worker = AggWorker{
        .alloc       = alloc,
        .memtable    = &mt,
        .unique_sets = &us,
        .registry    = &reg,
        .alert_log   = &al,
    };

    const n = try worker.run_once(data_dir, 0);
    try std.testing.expectEqual(@as(u64, 5), n);

    const key = AggKey{ .account_id = account_id, .period_id = 1, .metric_code = metric_code, .filter_hash = 0 };
    const v = mt.get(key).?;
    try std.testing.expectEqual(@as(u64, 5), v.count);
}

test "worker: run_once aggregates SUM events" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    const data_dir = "/tmp/billing_worker_sum_test";
    disk_io.remove_tree(data_dir);
    try disk_io.make_path(data_dir);

    var seg_path_buf: [512]u8 = undefined;
    const seg_path = try std.fmt.bufPrint(&seg_path_buf, "{s}/00000000000000000000.seg", .{data_dir});
    const seg_file = try disk_io.open_rw(seg_path);
    defer seg_file.close();

    const metric_code: u64 = 7;
    const day_ns: i64      = 24 * 3600 * std.time.ns_per_s;

    const values = [_]u64{ 1_000_000, 2_000_000, 3_000_000 };
    for (values, 0..) |val, i| {
        const e = Event{
            .offset          = @intCast(i),
            .timestamp       = day_ns,
            .idempotency_key = @intCast(i),
            .account_id      = 1,
            .metric_code     = metric_code,
            .value           = val,
            .operation_type  = 0,
            ._pad            = .{ 0, 0, 0 },
            .checksum        = 0,
        };
        try seg_file.write_all(std.mem.asBytes(&e));
    }

    var reg = @import("../usagelog/metric_registry.zig").MetricRegistry.init(alloc);
    defer reg.deinit();
    try reg.put(make_test_schema(metric_code, .sum));

    var mt = Memtable.init(alloc);
    defer mt.deinit();
    var us = UniqueSets.init(alloc);
    defer us.deinit();

    const alert_path = "/tmp/billing_worker_sum_alert.bin";
    defer disk_io.remove(alert_path) catch {};
    var al = try AlertLog.open(alert_path);
    defer al.deinit();

    var worker = AggWorker{
        .alloc       = alloc,
        .memtable    = &mt,
        .unique_sets = &us,
        .registry    = &reg,
        .alert_log   = &al,
    };

    _ = try worker.run_once(data_dir, 0);

    const key = AggKey{ .account_id = 1, .period_id = 1, .metric_code = metric_code, .filter_hash = 0 };
    const v = mt.get(key).?;
    try std.testing.expectEqual(@as(u128, 6_000_000), v.sum);
}

test "worker: start_offset skips earlier events" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    const data_dir = "/tmp/billing_worker_offset_test";
    disk_io.remove_tree(data_dir);
    try disk_io.make_path(data_dir);

    var seg_path_buf: [512]u8 = undefined;
    const seg_path = try std.fmt.bufPrint(&seg_path_buf, "{s}/00000000000000000000.seg", .{data_dir});
    const seg_file = try disk_io.open_rw(seg_path);
    defer seg_file.close();

    const metric_code: u64 = 1;
    const day_ns: i64      = 24 * 3600 * std.time.ns_per_s;

    for (0..4) |i| {
        const e = Event{
            .offset          = @intCast(i),
            .timestamp       = day_ns,
            .idempotency_key = @intCast(i),
            .account_id      = 1,
            .metric_code     = metric_code,
            .value           = 1_000_000,
            .operation_type  = 0,
            ._pad            = .{ 0, 0, 0 },
            .checksum        = 0,
        };
        try seg_file.write_all(std.mem.asBytes(&e));
    }

    var reg = @import("../usagelog/metric_registry.zig").MetricRegistry.init(alloc);
    defer reg.deinit();
    try reg.put(make_test_schema(metric_code, .count));

    var mt = Memtable.init(alloc);
    defer mt.deinit();
    var us = UniqueSets.init(alloc);
    defer us.deinit();

    const alert_path = "/tmp/billing_worker_offset_alert.bin";
    defer disk_io.remove(alert_path) catch {};
    var al = try AlertLog.open(alert_path);
    defer al.deinit();

    var worker = AggWorker{
        .alloc       = alloc,
        .memtable    = &mt,
        .unique_sets = &us,
        .registry    = &reg,
        .alert_log   = &al,
    };

    const n = try worker.run_once(data_dir, 2); // skip offsets 0 and 1
    try std.testing.expectEqual(@as(u64, 2), n);

    const key = AggKey{ .account_id = 1, .period_id = 1, .metric_code = metric_code, .filter_hash = 0 };
    const v = mt.get(key).?;
    try std.testing.expectEqual(@as(u64, 2), v.count);
}

test "worker: AND filter creates per-dim + combined AggKeys" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    const metric_code: u64 = fnv1a("test_and_filter");
    const day_ns: i64 = 24 * 3600 * std.time.ns_per_s;

    // Schema with 2 DimensionFilters: "region" (us, eu) and "tier" (pro, free).
    var schema = std.mem.zeroes(MetricSchema);
    schema.code         = metric_code;
    schema.agg_type     = @intFromEnum(AggType.sum);
    schema.period_ns    = @bitCast(day_ns);
    schema.filter_count = 2;

    var dim_region = std.mem.zeroes(DimensionFilter);
    @memcpy(dim_region.key[0..6], "region");
    dim_region.value_count = 2;
    @memcpy(dim_region.values[0][0..2], "us");
    @memcpy(dim_region.values[1][0..2], "eu");
    schema.filters[0] = dim_region;

    var dim_tier = std.mem.zeroes(DimensionFilter);
    @memcpy(dim_tier.key[0..4], "tier");
    dim_tier.value_count = 2;
    @memcpy(dim_tier.values[0][0..3], "pro");
    @memcpy(dim_tier.values[1][0..4], "free");
    schema.filters[1] = dim_tier;

    var reg = MetricRegistry.init(alloc);
    defer reg.deinit();
    try reg.put(schema);

    var mt = Memtable.init(alloc);
    defer mt.deinit();
    var us = UniqueSets.init(alloc);
    defer us.deinit();
    const alert_path = "/tmp/billing_worker_and_filter_alert.bin";
    defer disk_io.remove(alert_path) catch {};
    var al = try AlertLog.open(alert_path);
    defer al.deinit();

    var worker = AggWorker{
        .alloc       = alloc,
        .memtable    = &mt,
        .unique_sets = &us,
        .registry    = &reg,
        .alert_log   = &al,
    };

    // Event with both region=us and tier=pro props.
    const event = Event{
        .offset          = 0,
        .timestamp       = day_ns,
        .idempotency_key = 1,
        .account_id      = 1,
        .metric_code     = metric_code,
        .value           = 1_000_000,
        .operation_type  = 0,
        ._pad            = .{ 0, 0, 0 },
        .checksum        = 0,
    };

    var props: [2]PropPair = undefined;
    props[0] = std.mem.zeroes(PropPair);
    @memcpy(props[0].key[0..6], "region");
    @memcpy(props[0].value[0..2], "us");
    props[1] = std.mem.zeroes(PropPair);
    @memcpy(props[1].key[0..4], "tier");
    @memcpy(props[1].value[0..3], "pro");

    try worker.apply_with_props(event, &props);

    // Expect 4 AggKeys: unfiltered + region + tier + combined AND
    const pid: u32 = 1;
    const key_unfiltered = AggKey{ .account_id = 1, .period_id = pid, .metric_code = metric_code, .filter_hash = 0 };
    try std.testing.expect(mt.get(key_unfiltered) != null);

    // Per-dimension hashes
    const hash_region = fnv1a("region") ^ fnv1a("us");
    const hash_tier   = fnv1a("tier") ^ fnv1a("pro");
    const key_region = AggKey{ .account_id = 1, .period_id = pid, .metric_code = metric_code, .filter_hash = hash_region };
    const key_tier   = AggKey{ .account_id = 1, .period_id = pid, .metric_code = metric_code, .filter_hash = hash_tier };
    try std.testing.expect(mt.get(key_region) != null);
    try std.testing.expect(mt.get(key_tier) != null);

    // Combined AND hash = XOR of per-dimension hashes
    const hash_combined = hash_region ^ hash_tier;
    const key_combined = AggKey{ .account_id = 1, .period_id = pid, .metric_code = metric_code, .filter_hash = hash_combined };
    try std.testing.expect(mt.get(key_combined) != null);

    // All should have the same sum (one event, value=1M)
    try std.testing.expectEqual(@as(u128, 1_000_000), mt.get(key_unfiltered).?.sum);
    try std.testing.expectEqual(@as(u128, 1_000_000), mt.get(key_region).?.sum);
    try std.testing.expectEqual(@as(u128, 1_000_000), mt.get(key_tier).?.sum);
    try std.testing.expectEqual(@as(u128, 1_000_000), mt.get(key_combined).?.sum);

    // Combined hash must differ from individual hashes
    try std.testing.expect(hash_combined != hash_region);
    try std.testing.expect(hash_combined != hash_tier);
}
