//! Invariant checker for VOPR.
//!
//! Checks VSR-level and AggStore-level safety properties after each step.
//! On violation the Checker panics with the seed so the run is reproducible.
//!
//! Maps to CLAUDE.md invariants:
//!   I_A (leader uniqueness)  → precondition for invariant 1
//!   I_B (commit bound)       → sanity / precondition for all invariants
//!   I_C (monotonicity)       → invariant 3 (view/commit never go backwards)
//!   I_D (commit durability)  → invariant 4 (no committed op lost after view change)
//!   I_2 (sum consistency)    → invariant 2 (agg.sum == Σ value of committed events)
//!   I_6 (unique consistency) → invariant 6 (agg.count == |unique_set|)
//!   I_7 (filtered sum)       → invariant 7 (per-dim + AND-combined filtered sums match)
//!
//! AggRef uses resolve_period_id() for calendar-period metrics.
//! Invariant 5 (alert flags) requires AlertLog backed by real files; skipped here.

const std        = @import("std");
const cluster    = @import("cluster.zig");
const chaos_mod  = @import("chaos.zig");
const agg_mod    = @import("../aggstore/memtable.zig");
const agg_fn     = @import("../aggstore/aggregators.zig");
const uniq_mod   = @import("../aggstore/unique_sets.zig");
const worker_mod = @import("../aggstore/worker.zig");
const reg_mod    = @import("../usagelog/metric_registry.zig");

const SIM_NODES      = cluster.SIM_NODES;
const SIM_PARTITIONS = cluster.SIM_PARTITIONS;
const VirtualCluster = cluster.VirtualCluster;
const Role           = cluster.Role;

// ---- Snapshot ----

/// Per-node, per-partition state captured at a point in time.
pub const PartitionState = struct {
    view_number:   u64,
    op_number:     u64,
    commit_number: u64,
    role:          Role,
    alive:         bool,
};

/// Immutable snapshot of the whole cluster. Cheap to copy (pure value).
pub const ClusterSnapshot = struct {
    nodes: [SIM_NODES][SIM_PARTITIONS]PartitionState,

    pub fn capture(c: *const VirtualCluster) ClusterSnapshot {
        var s: ClusterSnapshot = undefined;
        for (0..SIM_NODES) |i| {
            for (0..SIM_PARTITIONS) |p| {
                s.nodes[i][p] = .{
                    .view_number   = c.nodes[i].vsr[p].view_number,
                    .op_number     = c.nodes[i].vsr[p].op_number,
                    .commit_number = c.nodes[i].vsr[p].commit_number,
                    .role          = c.nodes[i].vsr[p].role,
                    .alive         = c.nodes[i].alive,
                };
            }
        }
        return s;
    }
};

// ---- Checker ----

/// Runs invariant checks and panics with the reproduction seed on failure.
pub const Checker = struct {
    seed: u64,

    pub fn init(seed: u64) Checker {
        return .{ .seed = seed };
    }

    /// Point-in-time checks (no previous snapshot needed).
    pub fn check(self: *const Checker, c: *const VirtualCluster) void {
        self.assert(check_leader_uniqueness(c), "I_A: LeaderConflict");
        self.assert(check_commit_bounds(c),     "I_B: CommitBeyondOp");
    }

    /// Temporal checks (compares current state against the previous snapshot).
    /// Call check() first, then check_with_prev() on the same cluster pointer.
    pub fn check_with_prev(
        self: *const Checker,
        c:    *const VirtualCluster,
        prev: *const ClusterSnapshot,
    ) void {
        self.check(c);
        self.assert(check_monotonicity(c, prev),      "I_C: MonotonicityViolation");
        self.assert(check_commit_durability(c, prev), "I_D: CommitLost");
    }

    /// AggStore checks (I_2, I_6, I_7). Advances ref to process newly committed events.
    pub fn check_agg(self: *const Checker, ref: *AggRef, c: *const VirtualCluster) void {
        ref.advance(c) catch @panic("OOM in AggRef.advance");
        self.assert(check_i2(ref), "I_2: SumMismatch");
        self.assert(check_i6(ref), "I_6: UniqueCountMismatch");
        self.assert(check_i7(ref), "I_7: FilteredSumMismatch");
    }

    /// WAL durability check: recover WAL bytes and verify all committed events are present.
    /// Skipped if disk corruption chaos was applied (corrupt breaks WAL by design).
    pub fn check_wal(self: *const Checker, c: *const VirtualCluster, alloc: std.mem.Allocator) void {
        if (c.wal_corrupted) return;
        self.assert(check_wal_recovery(c, alloc), "I_WAL: WalEventsMissing");
    }

    fn assert(self: *const Checker, result: InvariantError!void, label: []const u8) void {
        result catch |err| {
            std.debug.panic(
                "VOPR invariant violated (seed={d}): {s} ({s})\n",
                .{ self.seed, label, @errorName(err) },
            );
        };
    }
};

// ---- Error type ----

pub const InvariantError = error{
    LeaderConflict,
    CommitBeyondOp,
    ViewNumberDecreased,
    CommitDecreased,
    CommitLost,
    SumMismatch,
    UniqueCountMismatch,
    FilteredSumMismatch,
    WalEventsMissing,
    WalEventsCorrupted,
};

// ---- Individual invariant functions ----

/// I_A: At most one alive node claims .leader per (partition, view_number).
///
/// Two alive nodes leading the SAME view would allow divergent commits.
/// Two alive nodes leading DIFFERENT views is safe: the old view leader cannot
/// reach quorum because replicas have already moved to the newer view.
pub fn check_leader_uniqueness(c: *const VirtualCluster) InvariantError!void {
    for (0..SIM_PARTITIONS) |p| {
        // Collect view numbers of all alive leaders for this partition.
        var views: [SIM_NODES]u64 = undefined;
        var n: usize = 0;
        for (0..SIM_NODES) |i| {
            if (!c.nodes[i].alive or c.nodes[i].vsr[p].role != .leader) continue;
            const v = c.nodes[i].vsr[p].view_number;
            for (0..n) |j| {
                if (views[j] == v) return error.LeaderConflict;
            }
            views[n] = v;
            n += 1;
        }
    }
}

/// I_B: commit_number <= op_number for all alive nodes, for all partitions.
///
/// A node cannot commit an op it has not yet prepared.
pub fn check_commit_bounds(c: *const VirtualCluster) InvariantError!void {
    for (0..SIM_NODES) |i| {
        if (!c.nodes[i].alive) continue;
        for (0..SIM_PARTITIONS) |p| {
            if (c.nodes[i].vsr[p].commit_number > c.nodes[i].vsr[p].op_number) {
                return error.CommitBeyondOp;
            }
        }
    }
}

/// I_C: view_number and commit_number only increase for nodes that were alive
/// in both the previous and current snapshots.
///
/// A node that was dead may restart with stale or reset state; that is intentional
/// and not a violation.
pub fn check_monotonicity(
    c:    *const VirtualCluster,
    prev: *const ClusterSnapshot,
) InvariantError!void {
    for (0..SIM_NODES) |i| {
        if (!c.nodes[i].alive) continue;
        for (0..SIM_PARTITIONS) |p| {
            const old = &prev.nodes[i][p];
            if (!old.alive) continue; // allowed to start fresh after restart
            if (c.nodes[i].vsr[p].view_number < old.view_number)
                return error.ViewNumberDecreased;
            if (c.nodes[i].vsr[p].commit_number < old.commit_number)
                return error.CommitDecreased;
        }
    }
}

/// I_D: The maximum commit_number seen across ALL nodes per partition never
/// decreases between snapshots.
///
/// Crashed nodes retain their last commit_number in memory (our simulator does not
/// wipe VSR state on crash). Therefore a committed op is always reflected in
/// max(all nodes). If this max decreases, an op that was durable has been lost —
/// the key safety property of invariant 4 from CLAUDE.md.
pub fn check_commit_durability(
    c:    *const VirtualCluster,
    prev: *const ClusterSnapshot,
) InvariantError!void {
    for (0..SIM_PARTITIONS) |p| {
        var prev_max: u64 = 0;
        var curr_max: u64 = 0;
        for (0..SIM_NODES) |i| {
            prev_max = @max(prev_max, prev.nodes[i][p].commit_number);
            curr_max = @max(curr_max, c.nodes[i].vsr[p].commit_number);
        }
        if (curr_max < prev_max) return error.CommitLost;
    }
}

// ---- AggRef: reference aggregator for I_2 and I_6 ----

/// Reference aggregator driven by committed_events from VirtualCluster.
///
/// Maintains:
///   - memtable:   canonical aggregate via aggregators.update()
///   - uniq_sets:  per-key unique-value sets for COUNT_UNIQUE
///   - raw_sums:   independent sum per SUM key (Σ event.value)
///   - watermarks: per-partition index of last processed event
///
/// I_2: memtable.sum == raw_sums[key] for all SUM keys.
/// I_6: memtable.count == uniq_sets.count(key) for all COUNT_UNIQUE keys.
pub const AggRef = struct {
    alloc:      std.mem.Allocator,
    registry:   reg_mod.MetricRegistry,
    memtable:   agg_mod.Memtable,
    uniq_sets:  uniq_mod.UniqueSets,
    /// Independent per-AggKey sum tracker for SUM metrics (I_2). Key filter_hash=0.
    raw_sums:   std.AutoHashMap(agg_mod.AggKey, u128),
    /// Independent per-(AggKey with filter_hash) sum tracker (I_7).
    /// Populated only for events carrying matched props.
    raw_filtered_sums: std.AutoHashMap(agg_mod.AggKey, u128),
    watermarks: [SIM_PARTITIONS]usize,

    pub fn init(alloc: std.mem.Allocator) !AggRef {
        var registry = reg_mod.MetricRegistry.init(alloc);
        errdefer registry.deinit();

        // SUM without filters.
        var sum_schema = std.mem.zeroes(reg_mod.MetricSchema);
        sum_schema.code      = chaos_mod.VOPR_METRIC_SUM;
        sum_schema.agg_type  = @intFromEnum(reg_mod.AggType.sum);
        sum_schema.period_ns = @bitCast(chaos_mod.VOPR_PERIOD_NS);
        try registry.put(sum_schema);

        // COUNT_UNIQUE without filters.
        var uniq_schema = std.mem.zeroes(reg_mod.MetricSchema);
        uniq_schema.code      = chaos_mod.VOPR_METRIC_UNIQUE;
        uniq_schema.agg_type  = @intFromEnum(reg_mod.AggType.count_unique);
        uniq_schema.period_ns = @bitCast(chaos_mod.VOPR_PERIOD_NS);
        try registry.put(uniq_schema);

        // SUM with two DimensionFilters: "region" (r0,r1,r2) + "tier" (t0,t1).
        // Tests per-dimension aggregates AND combined AND-filter hash.
        var filt_schema = std.mem.zeroes(reg_mod.MetricSchema);
        filt_schema.code         = chaos_mod.VOPR_METRIC_FILTERED;
        filt_schema.agg_type     = @intFromEnum(reg_mod.AggType.sum);
        filt_schema.period_ns    = @bitCast(chaos_mod.VOPR_PERIOD_NS);
        filt_schema.filter_count = 2;
        // Dimension 0: region
        var dim_region = std.mem.zeroes(reg_mod.DimensionFilter);
        @memcpy(dim_region.key[0..6], "region");
        dim_region.value_count = chaos_mod.VOPR_REGIONS.len;
        for (chaos_mod.VOPR_REGIONS, 0..) |r, i| {
            @memcpy(dim_region.values[i][0..r.len], r);
        }
        filt_schema.filters[0] = dim_region;
        // Dimension 1: tier
        var dim_tier = std.mem.zeroes(reg_mod.DimensionFilter);
        @memcpy(dim_tier.key[0..4], "tier");
        dim_tier.value_count = chaos_mod.VOPR_TIERS.len;
        for (chaos_mod.VOPR_TIERS, 0..) |t, i| {
            @memcpy(dim_tier.values[i][0..t.len], t);
        }
        filt_schema.filters[1] = dim_tier;
        try registry.put(filt_schema);

        // COUNT with calendar-month period (period_type=1).
        var cal_schema = std.mem.zeroes(reg_mod.MetricSchema);
        cal_schema.code        = chaos_mod.VOPR_METRIC_CALENDAR;
        cal_schema.agg_type    = @intFromEnum(reg_mod.AggType.count);
        cal_schema.period_type = @intFromEnum(reg_mod.PeriodType.calendar);
        cal_schema.billing_cycle_day = 1;
        try registry.put(cal_schema);

        var watermarks: [SIM_PARTITIONS]usize = undefined;
        for (0..SIM_PARTITIONS) |p| watermarks[p] = 0;

        return .{
            .alloc             = alloc,
            .registry          = registry,
            .memtable          = agg_mod.Memtable.init(alloc),
            .uniq_sets         = uniq_mod.UniqueSets.init(alloc),
            .raw_sums          = std.AutoHashMap(agg_mod.AggKey, u128).init(alloc),
            .raw_filtered_sums = std.AutoHashMap(agg_mod.AggKey, u128).init(alloc),
            .watermarks        = watermarks,
        };
    }

    pub fn deinit(self: *AggRef) void {
        self.registry.deinit();
        self.memtable.deinit();
        self.uniq_sets.deinit();
        self.raw_sums.deinit();
        self.raw_filtered_sums.deinit();
    }

    /// Process any newly committed events from all partitions.
    pub fn advance(self: *AggRef, c: *const VirtualCluster) !void {
        for (0..SIM_PARTITIONS) |p| {
            const events = c.committed_events[p].items;
            while (self.watermarks[p] < events.len) {
                try self.apply(&events[self.watermarks[p]]);
                self.watermarks[p] += 1;
            }
        }
    }

    fn apply(self: *AggRef, event: *const cluster.Event) !void {
        const schema = self.registry.get(event.metric_code) orelse return;
        const agg_type: reg_mod.AggType = @enumFromInt(schema.agg_type);
        const period_id = agg_fn.resolve_period_id(event.timestamp, schema.period_type, schema.period_ns, schema.billing_cycle_day);

        // Unfiltered aggregate (filter_hash=0).
        const key = agg_mod.AggKey{
            .account_id  = event.account_id,
            .period_id   = period_id,
            .metric_code = event.metric_code,
            .filter_hash = 0,
        };
        const agg = try self.memtable.get_or_put(key);
        agg_fn.update(agg, event, agg_type);

        if (agg_type == .count_unique) {
            const op: @import("../event.zig").OperationType = @enumFromInt(event.operation_type);
            try self.uniq_sets.update(key, event.value, op, agg);
        }

        if (agg_type == .sum) {
            const gop = try self.raw_sums.getOrPut(key);
            if (!gop.found_existing) gop.value_ptr.* = 0;
            gop.value_ptr.* += @as(u128, event.value);
        }

        // Per-dimension filtered aggregates.
        // Mirrors server-side apply_with_props: compute per-dim hashes and update.
        if (schema.filter_count > 0) {
            const props = chaos_mod.vopr_props_for(event);
            if (props.len > 0) {
                var hashes: [5]u64 = .{0} ** 5;
                const n = worker_mod.compute_per_dimension_hashes(&schema, props, &hashes);
                for (hashes[0..n]) |fh| {
                    if (fh == 0) continue;
                    const fkey = agg_mod.AggKey{
                        .account_id  = event.account_id,
                        .period_id   = period_id,
                        .metric_code = event.metric_code,
                        .filter_hash = fh,
                    };
                    const fagg = try self.memtable.get_or_put(fkey);
                    agg_fn.update(fagg, event, agg_type);
                    if (agg_type == .sum) {
                        const gop = try self.raw_filtered_sums.getOrPut(fkey);
                        if (!gop.found_existing) gop.value_ptr.* = 0;
                        gop.value_ptr.* += @as(u128, event.value);
                    }
                }
            }
        }
    }
};

/// I_2: For every SUM key, memtable.sum equals the independently tracked raw_sum.
pub fn check_i2(ref: *const AggRef) InvariantError!void {
    var it = ref.raw_sums.iterator();
    while (it.next()) |entry| {
        const key      = entry.key_ptr.*;
        const expected = entry.value_ptr.*;
        const agg = ref.memtable.get(key) orelse return error.SumMismatch;
        if (agg.sum != expected) return error.SumMismatch;
    }
}

/// I_6: For every COUNT_UNIQUE key, memtable.count equals the unique-set size.
pub fn check_i6(ref: *const AggRef) InvariantError!void {
    var it = ref.memtable.map.iterator();
    while (it.next()) |entry| {
        if (entry.key_ptr.*.metric_code != chaos_mod.VOPR_METRIC_UNIQUE) continue;
        const key       = entry.key_ptr.*;
        const agg_count = entry.value_ptr.*.count;
        const set_count = ref.uniq_sets.count(key);
        if (agg_count != set_count) return error.UniqueCountMismatch;
    }
}

/// I_7: For every per-dimension filtered SUM aggregate (filter_hash != 0),
/// memtable.sum equals the independently tracked raw_filtered_sums.
///
/// Verifies two properties of the filter pipeline:
///   1. compute_per_dimension_hashes produces the same hash as server-side ingest.
///   2. Only events matching the dimension contribute to the filtered aggregate.
pub fn check_i7(ref: *const AggRef) InvariantError!void {
    var it = ref.raw_filtered_sums.iterator();
    while (it.next()) |entry| {
        const key      = entry.key_ptr.*;
        const expected = entry.value_ptr.*;
        const agg = ref.memtable.get(key) orelse return error.FilteredSumMismatch;
        if (agg.sum != expected) return error.FilteredSumMismatch;
    }

    // Also check the reverse: every filtered memtable entry has a matching raw record.
    var it2 = ref.memtable.map.iterator();
    while (it2.next()) |entry| {
        if (entry.key_ptr.*.filter_hash == 0) continue;
        if (entry.key_ptr.*.metric_code != chaos_mod.VOPR_METRIC_FILTERED) continue;
        const raw = ref.raw_filtered_sums.get(entry.key_ptr.*) orelse return error.FilteredSumMismatch;
        if (entry.value_ptr.*.sum != raw) return error.FilteredSumMismatch;
    }
}

// ---- Tests ----

test "invariants: clean cluster passes all checks" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var c = try VirtualCluster.init(alloc, 0);
    defer c.deinit();

    const checker = Checker.init(0);
    checker.check(&c);

    const snap = ClusterSnapshot.capture(&c);
    checker.check_with_prev(&c, &snap);
}

test "invariants: after submit, commit_bound and leader_uniqueness hold" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var c = try VirtualCluster.init(alloc, 0);
    defer c.deinit();

    try c.submit(0, 0xABCD, 5);

    try check_leader_uniqueness(&c);
    try check_commit_bounds(&c);
}

test "invariants: commit_durability holds after crash and view change" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var c = try VirtualCluster.init(alloc, 0);
    defer c.deinit();

    // Commit one op so there is something to protect.
    try c.submit(0, 0x1234, 1);

    const snap_before = ClusterSnapshot.capture(&c);

    // Crash the leader of partition 0 and run a view change.
    c.crash(0);
    for (0..21) |_| {
        c.tick();
        c.drain();
    }

    try check_commit_durability(&c, &snap_before);
    // Commit number must still reflect the original commit across all nodes.
    var max_commit: u64 = 0;
    for (0..SIM_NODES) |i| max_commit = @max(max_commit, c.nodes[i].vsr[0].commit_number);
    try std.testing.expectEqual(@as(u64, 1), max_commit);
}

test "invariants: monotonicity holds after view change" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var c = try VirtualCluster.init(alloc, 0);
    defer c.deinit();

    try c.submit(0, 0x5678, 2);
    const snap = ClusterSnapshot.capture(&c);

    c.crash(0);
    for (0..21) |_| {
        c.tick();
        c.drain();
    }

    // view_number and commit_number must not have decreased for surviving nodes.
    try check_monotonicity(&c, &snap);
}

test "invariants: check_leader_uniqueness detects conflict" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var c = try VirtualCluster.init(alloc, 0);
    defer c.deinit();

    // Artificially force two leaders for partition 0 (simulating a bug).
    c.nodes[0].vsr[0].role = .leader;
    c.nodes[1].vsr[0].role = .leader;

    try std.testing.expectError(error.LeaderConflict, check_leader_uniqueness(&c));
}

test "invariants: check_commit_bounds detects commit beyond op" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var c = try VirtualCluster.init(alloc, 0);
    defer c.deinit();

    // Artificially set commit_number > op_number (simulating a bug).
    c.nodes[0].vsr[0].commit_number = 5;
    c.nodes[0].vsr[0].op_number     = 3;

    try std.testing.expectError(error.CommitBeyondOp, check_commit_bounds(&c));
}

test "invariants: AggRef I_2 sum consistency holds after submit_events" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var c = try VirtualCluster.init(alloc, 0);
    defer c.deinit();

    var ref = try AggRef.init(alloc);
    defer ref.deinit();

    // Partition 0, leader node0. Submit a SUM event.
    const ev = cluster.Event{
        .offset          = 0,
        .timestamp       = chaos_mod.VOPR_BASE_TS_NS,
        .idempotency_key = 1,
        .account_id      = 1,
        .metric_code     = chaos_mod.VOPR_METRIC_SUM,
        .value           = 1_000_000,
        .operation_type  = 0,
        ._pad            = .{ 0, 0, 0 },
        .checksum        = 0,
    };
    _ = try c.submit_events(0, &.{ev});

    const checker = Checker.init(0);
    checker.check_agg(&ref, &c);
    // Sum must equal the one event's value.
    try check_i2(&ref);
}

test "invariants: AggRef I_6 unique count consistency holds after submit_events" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var c = try VirtualCluster.init(alloc, 0);
    defer c.deinit();

    var ref = try AggRef.init(alloc);
    defer ref.deinit();

    // Submit two COUNT_UNIQUE add events with different values.
    const ev1 = cluster.Event{
        .offset          = 0,
        .timestamp       = chaos_mod.VOPR_BASE_TS_NS,
        .idempotency_key = 2,
        .account_id      = 1,
        .metric_code     = chaos_mod.VOPR_METRIC_UNIQUE,
        .value           = 100,
        .operation_type  = 1, // add
        ._pad            = .{ 0, 0, 0 },
        .checksum        = 0,
    };
    const ev2 = cluster.Event{
        .offset          = 1,
        .timestamp       = chaos_mod.VOPR_BASE_TS_NS,
        .idempotency_key = 3,
        .account_id      = 1,
        .metric_code     = chaos_mod.VOPR_METRIC_UNIQUE,
        .value           = 200,
        .operation_type  = 1, // add
        ._pad            = .{ 0, 0, 0 },
        .checksum        = 0,
    };
    _ = try c.submit_events(0, &.{ ev1, ev2 });

    const checker = Checker.init(0);
    checker.check_agg(&ref, &c);
    try check_i6(&ref);
}

test "invariants: AggRef I_2 detects sum mismatch" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var ref = try AggRef.init(alloc);
    defer ref.deinit();

    // Manually insert a mismatched entry into raw_sums vs memtable.
    const key = agg_mod.AggKey{ .account_id = 1, .period_id = 1, .metric_code = chaos_mod.VOPR_METRIC_SUM, .filter_hash = 0 };
    (try ref.memtable.get_or_put(key)).sum = 5_000_000;
    // raw_sums says 10_000_000 — mismatch.
    try ref.raw_sums.put(key, 10_000_000);

    try std.testing.expectError(error.SumMismatch, check_i2(&ref));
}

test "invariants: AggRef I_6 detects unique count mismatch" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var ref = try AggRef.init(alloc);
    defer ref.deinit();

    // Manually set agg.count != set size.
    const key = agg_mod.AggKey{ .account_id = 2, .period_id = 1, .metric_code = chaos_mod.VOPR_METRIC_UNIQUE, .filter_hash = 0 };
    (try ref.memtable.get_or_put(key)).count = 99; // wrong value
    // uniq_sets has no entry for this key → count = 0 → mismatch.

    try std.testing.expectError(error.UniqueCountMismatch, check_i6(&ref));
}

test "invariants: AggRef I_7 filtered sum holds after submit_events with props" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var c = try VirtualCluster.init(alloc, 0);
    defer c.deinit();

    var ref = try AggRef.init(alloc);
    defer ref.deinit();

    // Two FILTERED events: account 1 (region r1=account%3) and account 2 (r2=account%3).
    // account 1 → region "r1", account 2 → "r2", so distinct filter hashes.
    const ev1 = cluster.Event{
        .offset          = 0,
        .timestamp       = chaos_mod.VOPR_BASE_TS_NS,
        .idempotency_key = 100,
        .account_id      = 1,
        .metric_code     = chaos_mod.VOPR_METRIC_FILTERED,
        .value           = 500_000,
        .operation_type  = 0,
        ._pad            = .{ 0, 0, 0 },
        .checksum        = 0,
    };
    const ev2 = cluster.Event{
        .offset          = 1,
        .timestamp       = chaos_mod.VOPR_BASE_TS_NS,
        .idempotency_key = 101,
        .account_id      = 2,
        .metric_code     = chaos_mod.VOPR_METRIC_FILTERED,
        .value           = 300_000,
        .operation_type  = 0,
        ._pad            = .{ 0, 0, 0 },
        .checksum        = 0,
    };
    _ = try c.submit_events(0, &.{ ev1, ev2 });

    const checker = Checker.init(0);
    checker.check_agg(&ref, &c);
    try check_i7(&ref);
}

test "invariants: AggRef I_7 detects filtered sum mismatch" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var ref = try AggRef.init(alloc);
    defer ref.deinit();

    // Manually corrupt memtable filtered entry vs raw tracker.
    const fkey = agg_mod.AggKey{
        .account_id  = 1,
        .period_id   = 1,
        .metric_code = chaos_mod.VOPR_METRIC_FILTERED,
        .filter_hash = 0xDEADBEEF,
    };
    (try ref.memtable.get_or_put(fkey)).sum = 1_000_000;
    try ref.raw_filtered_sums.put(fkey, 2_000_000); // disagree

    try std.testing.expectError(error.FilteredSumMismatch, check_i7(&ref));
}

// ---- I_WAL: WAL recovery invariant ----
//
// For each partition, recover all events from the leader's WAL bytes and
// verify that every committed event is present. This catches:
//   - CRC chain corruption
//   - Truncated entries
//   - Missing events (e.g. lost during segment rotation)

const wal_mod = @import("../usagelog/wal.zig");
const fake_io = @import("../io/fake_io.zig");
const Event   = @import("../event.zig").Event;

fn check_wal_recovery(c: *const VirtualCluster, alloc: std.mem.Allocator) InvariantError!void {
    for (0..SIM_PARTITIONS) |p| {
        const committed = c.committed_events[p].items;
        if (committed.len == 0) continue;

        // Recover events from the shared SegmentedWal for this partition.
        // Uses the same recover() code path as production startup.
        var dir_buf: [32]u8 = undefined;
        const wal_dir = std.fmt.bufPrint(&dir_buf, "wal_p{d}", .{p}) catch continue;
        const storage = &@constCast(c).wal_storages.*[p];

        const entries = fake_io.FakeSegmentedWal.recover(storage, wal_dir, alloc) catch
            return error.WalEventsCorrupted;
        defer {
            for (entries) |e| alloc.free(e.payload);
            alloc.free(entries);
        }

        if (entries.len == 0 and committed.len > 0) return error.WalEventsMissing;

        var all_wal_events: std.ArrayList(Event) = .empty;
        defer all_wal_events.deinit(alloc);

        for (entries) |entry| {
            if (entry.entry_type != .commit) continue;
            if (entry.payload.len % @sizeOf(Event) != 0) continue;
            const n_events = entry.payload.len / @sizeOf(Event);
            const events_ptr: [*]const Event = @ptrCast(@alignCast(entry.payload.ptr));
            for (0..n_events) |j| {
                all_wal_events.append(alloc, events_ptr[j]) catch
                    return error.WalEventsCorrupted;
            }
        }

        // Every committed event must exist somewhere in the combined WALs.
        for (committed) |ref_ev| {
            var found = false;
            for (all_wal_events.items) |wal_ev| {
                if (wal_ev.offset == ref_ev.offset and
                    wal_ev.account_id == ref_ev.account_id and
                    wal_ev.value == ref_ev.value)
                {
                    found = true;
                    break;
                }
            }
            if (!found) return error.WalEventsMissing;
        }
    }
}
