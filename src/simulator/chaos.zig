//! Chaos engine for VOPR.
//! Each action is a standalone function: (cluster, rng) → side effects.
//! Actions are composable; the VOPR main loop picks them via weighted selection.
//!
//! VOPR metrics: SUM, COUNT_UNIQUE, FILTERED (2 dims: region+tier, tests AND hash),
//! CALENDAR (COUNT with calendar-month period).

const std       = @import("std");
const cluster   = @import("cluster.zig");
const event_mod = @import("../event.zig");
const reg_mod   = @import("../usagelog/metric_registry.zig");
const proto_mod = @import("../usagelog/protocol.zig");

pub const VirtualCluster = cluster.VirtualCluster;
pub const Event          = cluster.Event;
pub const Rng            = std.Random;
pub const PropPair       = proto_mod.PropPair;

// ---- VOPR metric codes ----

/// Billing period used for all VOPR events (1 day).
pub const VOPR_PERIOD_NS:  i64 = 86_400_000_000_000;
/// Base timestamp for generated events (2023-11-14T22:13:20 UTC).
pub const VOPR_BASE_TS_NS: i64 = 1_700_000_000_000_000_000;
/// SUM metric code: sum of event values per account per period.
pub const VOPR_METRIC_SUM:      u64 = reg_mod.fnv1a("vopr_sum");
/// COUNT_UNIQUE metric code: set of unique event values per account per period.
pub const VOPR_METRIC_UNIQUE:   u64 = reg_mod.fnv1a("vopr_unique");
/// SUM metric code with dimension filter ("region" + "tier"): tests per-dimension and AND-filter aggregates.
pub const VOPR_METRIC_FILTERED: u64 = reg_mod.fnv1a("vopr_filtered");
/// COUNT metric with calendar-month period (period_type=1, billing_cycle_day=1).
pub const VOPR_METRIC_CALENDAR: u64 = reg_mod.fnv1a("vopr_calendar");
/// Number of distinct accounts in generated events.
pub const VOPR_N_ACCOUNTS: u64 = 16;

/// Regions used by VOPR_METRIC_FILTERED events. Derived deterministically from
/// account_id so both AggRef and server-side aggregation see the same prop stream.
pub const VOPR_REGIONS = [_][]const u8{ "r0", "r1", "r2" };

/// Tiers used by VOPR_METRIC_FILTERED events (2nd dimension for AND filter testing).
pub const VOPR_TIERS = [_][]const u8{ "t0", "t1" };

/// Pre-built PropPair pairs (region + tier) for each account combination.
/// Index: region_idx * VOPR_TIERS.len + tier_idx.
/// Each entry is a 2-element array: [region_prop, tier_prop].
const VOPR_PROP_COMBOS: [VOPR_REGIONS.len * VOPR_TIERS.len][2]PropPair = blk: {
    var arr: [VOPR_REGIONS.len * VOPR_TIERS.len][2]PropPair = undefined;
    for (0..VOPR_REGIONS.len) |ri| {
        for (0..VOPR_TIERS.len) |ti| {
            const idx = ri * VOPR_TIERS.len + ti;
            // region prop
            arr[idx][0] = std.mem.zeroes(PropPair);
            @memcpy(arr[idx][0].key[0..6], "region");
            const rv = VOPR_REGIONS[ri];
            @memcpy(arr[idx][0].value[0..rv.len], rv);
            // tier prop
            arr[idx][1] = std.mem.zeroes(PropPair);
            @memcpy(arr[idx][1].key[0..4], "tier");
            const tv = VOPR_TIERS[ti];
            @memcpy(arr[idx][1].value[0..tv.len], tv);
        }
    }
    break :blk arr;
};

/// Deterministic prop slice for a FILTERED event, based on account_id.
/// Returns 2 props (region + tier) for AND filter testing.
/// Returns empty slice for non-filtered metrics (mirrors real client behaviour).
pub fn vopr_props_for(event: *const Event) []const PropPair {
    if (event.metric_code != VOPR_METRIC_FILTERED) return &.{};
    const combo_count = VOPR_REGIONS.len * VOPR_TIERS.len;
    const idx = event.account_id % combo_count;
    return &VOPR_PROP_COMBOS[idx];
}

fn gen_event(rng: Rng, offset: u64) Event {
    // 4-way split: SUM, COUNT_UNIQUE, FILTERED (with region+tier props), or CALENDAR.
    const pick = rng.uintLessThan(u8, 4);
    const metric: u64 = switch (pick) {
        0    => VOPR_METRIC_SUM,
        1    => VOPR_METRIC_UNIQUE,
        2    => VOPR_METRIC_FILTERED,
        else => VOPR_METRIC_CALENDAR,
    };
    const op: u8 = if (metric == VOPR_METRIC_UNIQUE)
        (if (rng.boolean()) 1 else 2)  // add / remove
    else
        0;
    return .{
        .offset          = offset,
        .timestamp       = VOPR_BASE_TS_NS + @as(i64, @intCast(rng.uintLessThan(u64, 1000) * 1_000_000)),
        .idempotency_key = rng.int(u128),
        .account_id      = rng.uintLessThan(u64, VOPR_N_ACCOUNTS) + 1,
        .metric_code     = metric,
        .value           = rng.uintLessThan(u64, 1_000_000) + 1,
        .operation_type  = op,
        ._pad            = .{ 0, 0, 0 },
        .checksum        = 0,
    };
}

// ---- Actions ----

/// Submit a random batch of events to a random alive partition.
/// No-op if no partition currently has a leader.
pub fn send_batch(c: *VirtualCluster, rng: Rng) !void {
    // Try each partition in random order until we find one with a leader.
    var order: [cluster.SIM_PARTITIONS]u32 = undefined;
    for (0..cluster.SIM_PARTITIONS) |i| order[i] = @intCast(i);
    rng.shuffle(u32, &order);

    for (order) |p| {
        if (c.leader_of(p) != null) {
            const n: usize = rng.uintLessThan(usize, 8) + 1;
            var events: [8]Event = undefined;
            // Use committed_events.len as the base offset for this batch,
            // so offsets are monotonically increasing within a partition.
            const base_offset = c.committed_events[p].items.len;
            for (0..n) |i| events[i] = gen_event(rng, base_offset + i);
            _ = c.submit_events(p, events[0..n]) catch |err| switch (err) {
                error.NoLeader => {},
                else           => return err,
            };
            return;
        }
    }
    // No leader available — skip without error.
}

/// Crash a random alive node.
/// No-op if all nodes are already crashed.
pub fn crash_node(c: *VirtualCluster, rng: Rng) void {
    var buf: [cluster.SIM_NODES]u8 = undefined;
    var n: usize = 0;
    for (0..cluster.SIM_NODES) |i| {
        if (c.nodes[i].alive) { buf[n] = @intCast(i); n += 1; }
    }
    if (n == 0) return;
    c.crash(buf[rng.uintLessThan(usize, n)]);
}

/// Restart a random crashed node.
/// No-op if all nodes are already alive.
pub fn restart_node(c: *VirtualCluster, rng: Rng) void {
    var buf: [cluster.SIM_NODES]u8 = undefined;
    var n: usize = 0;
    for (0..cluster.SIM_NODES) |i| {
        if (!c.nodes[i].alive) { buf[n] = @intCast(i); n += 1; }
    }
    if (n == 0) return;
    c.restart(buf[rng.uintLessThan(usize, n)]);
}

/// Enable random message dropping at `rate` probability (0.0–1.0).
/// Use heal_drops() to turn off.
pub fn drop_packets(c: *VirtualCluster, rate: f32) void {
    c.drop_probability = @max(0.0, @min(1.0, rate));
}

/// Disable random message dropping.
pub fn heal_drops(c: *VirtualCluster) void {
    c.drop_probability = 0;
}

/// Isolate one random node from the other two (split-brain).
/// Nodes 0 and 1 stay in group 0; the isolated node goes into group 1.
/// Use heal_network() to restore full connectivity.
pub fn partition_network(c: *VirtualCluster, rng: Rng) void {
    const isolated: usize = rng.uintLessThan(usize, cluster.SIM_NODES);
    for (0..cluster.SIM_NODES) |i| {
        c.groups[i] = if (i == isolated) 1 else 0;
    }
    c.partitioned = true;
}

/// Restore full network connectivity.
pub fn heal_network(c: *VirtualCluster) void {
    c.partitioned = false;
    for (&c.groups) |*g| g.* = 0;
}

/// Flip a random byte in a random WAL to simulate a torn write or bit rot.
/// Only affects alive nodes that have non-empty WALs.
pub fn corrupt_disk_write(c: *VirtualCluster, rng: Rng) void {
    const p = rng.uintLessThan(u32, cluster.SIM_PARTITIONS);
    const current = c.wals[p].current_file();
    const wal_data = current.bytes();
    if (wal_data.len == 0) return;

    const pos:  usize = rng.uintLessThan(usize, wal_data.len);
    const mask: u8    = rng.int(u8);
    current.corrupt(pos, mask);
    c.wal_corrupted = true;
}

/// Graceful shutdown of a random alive node: drain pending, crash, restart.
pub fn graceful_shutdown(c: *VirtualCluster, rng: Rng) void {
    var buf: [cluster.SIM_NODES]u8 = undefined;
    var n: usize = 0;
    for (0..cluster.SIM_NODES) |i| {
        if (c.nodes[i].alive) { buf[n] = @intCast(i); n += 1; }
    }
    if (n == 0) return;
    c.graceful_shutdown(buf[rng.uintLessThan(usize, n)]);
}

/// Rebalance partitions: shift leader assignment by 1 or 2.
pub fn rebalance(c: *VirtualCluster, rng: Rng) void {
    const shift: u8 = rng.uintLessThan(u8, 2) + 1; // 1 or 2
    c.rebalance(shift);
}

// ---- Weighted action picker ----

pub const ActionTag = enum {
    send_batch,
    crash_node,
    restart_node,
    drop_packets,
    heal_drops,
    partition_network,
    heal_network,
    corrupt_disk_write,
    graceful_shutdown,
    rebalance,
};

/// Weight table: heavier weight = more likely to be chosen.
/// Tuned so normal operations dominate; chaos events are rarer.
pub const ACTION_WEIGHTS = [_]u32{
    60, // send_batch
    5,  // crash_node
    8,  // restart_node
    4,  // drop_packets
    4,  // heal_drops
    3,  // partition_network
    5,  // heal_network
    3,  // corrupt_disk_write
    3,  // graceful_shutdown
    2,  // rebalance
};

comptime {
    std.debug.assert(ACTION_WEIGHTS.len == @typeInfo(ActionTag).@"enum".fields.len);
}

const WEIGHT_TOTAL: u32 = blk: {
    var s: u32 = 0;
    for (ACTION_WEIGHTS) |w| s += w;
    break :blk s;
};

/// Pick an action tag using the weight table.
pub fn pick_action(rng: Rng) ActionTag {
    var r = rng.uintLessThan(u32, WEIGHT_TOTAL);
    inline for (ACTION_WEIGHTS, 0..) |w, i| {
        if (r < w) return @enumFromInt(i);
        r -= w;
    }
    unreachable;
}

/// Execute one random action from the weight table.
pub fn step(c: *VirtualCluster, rng: Rng) !void {
    switch (pick_action(rng)) {
        .send_batch        => try send_batch(c, rng),
        .crash_node        => crash_node(c, rng),
        .restart_node      => restart_node(c, rng),
        .drop_packets      => drop_packets(c, 0.3),
        .heal_drops        => heal_drops(c),
        .partition_network => partition_network(c, rng),
        .heal_network      => heal_network(c),
        .corrupt_disk_write => corrupt_disk_write(c, rng),
        .graceful_shutdown  => graceful_shutdown(c, rng),
        .rebalance          => rebalance(c, rng),
    }
}

// ---- Tests ----

test "chaos: crash and restart cycle" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var c = try VirtualCluster.init(alloc, 42);
    defer c.deinit();

    var prng = std.Random.DefaultPrng.init(42);
    const rng = prng.random();

    try std.testing.expect(c.nodes[0].alive);
    crash_node(&c, rng);
    // At least one node must be alive after one crash (started with 3 alive).
    const alive_count = blk: {
        var cnt: usize = 0;
        for (c.nodes) |n| { if (n.alive) cnt += 1; }
        break :blk cnt;
    };
    try std.testing.expect(alive_count == 2);

    restart_node(&c, rng);
    const alive_after = blk: {
        var cnt: usize = 0;
        for (c.nodes) |n| { if (n.alive) cnt += 1; }
        break :blk cnt;
    };
    try std.testing.expect(alive_after == 3);
}

test "chaos: network partition drops cross-group messages" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var c = try VirtualCluster.init(alloc, 7);
    defer c.deinit();

    // Manually partition: node0 isolated, nodes 1 and 2 together.
    c.groups = .{ 1, 0, 0 };
    c.partitioned = true;

    // Submit to partition 1 (leader: node1). Replicas are node0 (isolated) and node2.
    // Partition 0 group differs from node0's group, so PREPARE to node0 is dropped.
    // node2 (same group) can still respond.
    // Because only 1 of 2 replicas can respond, and needed_ok_count=1, commit should succeed.
    try c.submit(1, 0xCAFE, 5);

    // Leader committed.
    try std.testing.expectEqual(@as(u64, 1), c.nodes[1].vsr[1].commit_number);
    // node2 (same group as leader) committed.
    try std.testing.expectEqual(@as(u64, 1), c.nodes[2].vsr[1].commit_number);
    // node0 (isolated) did NOT receive PREPARE, op_number stays 0.
    try std.testing.expectEqual(@as(u64, 0), c.nodes[0].vsr[1].op_number);

    heal_network(&c);
    try std.testing.expect(!c.partitioned);
    try std.testing.expectEqual(@as(u8, 0), c.groups[0]);
}

test "chaos: corrupt_disk_write flips a byte" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var c = try VirtualCluster.init(alloc, 1);
    defer c.deinit();

    // Write an entry to partition 0 WAL so there is data to corrupt.
    try c.wals[0].append(.commit, "DEADBEEFCAFE0000");

    const before_len = c.wals[0].current_file().bytes().len;
    try std.testing.expect(before_len > 0);

    var prng = std.Random.DefaultPrng.init(0);
    const rng = prng.random();
    corrupt_disk_write(&c, rng);

    // Length is unchanged; only content may differ.
    try std.testing.expectEqual(before_len, c.wals[0].current_file().bytes().len);
}

test "chaos: pick_action covers all tags" {
    // Run enough iterations to hit all action tags with high probability.
    var prng = std.Random.DefaultPrng.init(0);
    const rng = prng.random();

    var seen = std.mem.zeroes([@typeInfo(ActionTag).@"enum".fields.len]bool);
    var iters: usize = 0;
    while (iters < 10_000) : (iters += 1) {
        const tag = pick_action(rng);
        seen[@intFromEnum(tag)] = true;
    }
    for (seen) |s| try std.testing.expect(s);
}
