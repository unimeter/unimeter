//! VirtualCluster: deterministic 3-node cluster for VOPR.
//! Simulates a virtual message bus, tick-based time, and node crash/restart.
//! Storage uses FakeSegmentedWal — same code path as production, in-memory backend.
//!
//! Partition assignment: partition_id % SIM_NODES → leader node.
//!   Partitions 0,3,6 → node0 leads; 1,4,7 → node1; 2,5,8 → node2.

const std       = @import("std");
const vsr_mod   = @import("../cluster/vsr.zig");
const fake_io   = @import("../io/fake_io.zig");
const event_mod = @import("../event.zig");

pub const VsrPartition       = vsr_mod.VsrPartition;
pub const VsrConfig          = vsr_mod.VsrConfig;
pub const Role               = vsr_mod.Role;
pub const Event              = event_mod.Event;

pub const SIM_NODES:      u8  = 3;
pub const SIM_PARTITIONS: u32 = 9;
pub const TICK_NS:        i64 = 100_000_000; // 100ms virtual time per tick

// ---- Message envelope ----

pub const MsgPayload = union(vsr_mod.MsgType) {
    prepare:           vsr_mod.PrepareMsg,
    prepare_ok:        vsr_mod.PrepareOkMsg,
    commit:            vsr_mod.CommitMsg,
    ping:              vsr_mod.PingMsg,
    start_view_change: vsr_mod.StartViewChangeMsg,
    do_view_change:    vsr_mod.DoViewChangeMsg,
    start_view:        vsr_mod.StartViewMsg,
    redirect:          vsr_mod.MsgHeader,
};

pub const Envelope = struct {
    from:      u8,
    to:        u8,
    partition: u32,
    msg:       MsgPayload,
};

// ---- Virtual node ----

pub const VirtualNode = struct {
    id:    u8,
    alive: bool,
    vsr:   [SIM_PARTITIONS]VsrPartition,

    pub fn init(id: u8) VirtualNode {
        var self: VirtualNode = undefined;
        self.id    = id;
        self.alive = true;

        for (0..SIM_PARTITIONS) |p| {
            const pid: u32   = @intCast(p);
            const role: Role = if (pid % SIM_NODES == id) .leader else .replica;
            // replica_group[i] = (base + i) % SIM_NODES so that view 0 maps to the
            // correct leader: partition_id % SIM_NODES.
            const base: u8 = @intCast(pid % 3);
            const rg = [3]u8{
                @intCast((base + 0) % SIM_NODES),
                @intCast((base + 1) % SIM_NODES),
                @intCast((base + 2) % SIM_NODES),
            };
            self.vsr[p] = VsrPartition.init(.{
                .node_id       = id,
                .partition_id  = pid,
                .n_nodes       = SIM_NODES,
                .replica_group = rg,
            }, role);
        }
        return self;
    }

    pub fn deinit(_: *VirtualNode) void {}
};

// ---- Virtual cluster ----

pub const VirtualCluster = struct {
    alloc:             std.mem.Allocator,
    nodes:             [SIM_NODES]VirtualNode,
    inbox:             std.ArrayList(Envelope),
    now_ns:            i64,
    /// Per-partition list of successfully committed events, in commit order.
    committed_events:  [SIM_PARTITIONS]std.ArrayList(Event),
    /// Shared SegmentedWal per partition — same code path as production.
    /// Heap-allocated to avoid self-referential pointer invalidation on struct copy.
    wal_storages:      *[SIM_PARTITIONS]fake_io.FakeStorage,
    wals:              [SIM_PARTITIONS]fake_io.FakeSegmentedWal,
    /// Set to true when corrupt_disk_write chaos action fires. Disables I_WAL check.
    wal_corrupted:     bool,
    // Chaos state (all zero/false = no chaos).
    chaos_rng:         std.Random.DefaultPrng,
    drop_probability:  f32,           // 0.0–1.0 fraction of messages randomly dropped
    partitioned:       bool,          // if true, cross-group messages are dropped
    groups:            [SIM_NODES]u8, // group[node_id]: same group = can communicate

    pub fn init(alloc: std.mem.Allocator, seed: u64) !VirtualCluster {
        var nodes: [SIM_NODES]VirtualNode = undefined;
        for (0..SIM_NODES) |i| {
            nodes[i] = VirtualNode.init(@intCast(i));
        }
        var committed_events: [SIM_PARTITIONS]std.ArrayList(Event) = undefined;
        for (0..SIM_PARTITIONS) |p| committed_events[p] = .empty;
        const wal_storages = try alloc.create([SIM_PARTITIONS]fake_io.FakeStorage);
        for (0..SIM_PARTITIONS) |p| wal_storages[p] = fake_io.FakeStorage.init(alloc);

        // Tiny segment size (512B) exercises rotation frequently in VOPR.
        const seg_size: u64 = 512;
        var wals: [SIM_PARTITIONS]fake_io.FakeSegmentedWal = undefined;
        for (0..SIM_PARTITIONS) |p| {
            var dir_buf: [32]u8 = undefined;
            const d = std.fmt.bufPrint(&dir_buf, "wal_p{d}", .{p}) catch "wal";
            wals[p] = try fake_io.FakeSegmentedWal.open(&wal_storages[p], d, seg_size, alloc);
        }
        return .{
            .alloc             = alloc,
            .nodes             = nodes,
            .inbox             = .empty,
            .now_ns            = 0,
            .committed_events  = committed_events,
            .wal_storages      = wal_storages,
            .wals              = wals,
            .wal_corrupted     = false,
            .chaos_rng         = std.Random.DefaultPrng.init(seed),
            .drop_probability  = 0,
            .partitioned       = false,
            .groups            = .{ 0, 0, 0 },
        };
    }

    pub fn deinit(self: *VirtualCluster) void {
        for (&self.nodes) |*n| n.deinit();
        self.inbox.deinit(self.alloc);
        for (&self.committed_events) |*list| list.deinit(self.alloc);
        for (&self.wals) |*w| w.deinit();
        for (self.wal_storages) |*s| s.deinit();
        self.alloc.destroy(self.wal_storages);
    }

    // ---- Time ----

    /// Advance virtual clock by TICK_NS and fire leader heartbeats / replica timeouts.
    pub fn tick(self: *VirtualCluster) void {
        self.now_ns += TICK_NS;

        for (&self.nodes) |*node| {
            if (!node.alive) continue;
            for (0..SIM_PARTITIONS) |p| {
                const pid: u32 = @intCast(p);
                if (node.vsr[p].role == .leader) {
                    // Send heartbeat to replicas (skip self).
                    const ping = node.vsr[p].make_ping();
                    self.broadcast(node.id, pid, .{ .ping = ping }, node.id);
                } else {
                    // Replica: check view-change timeout.
                    const svc = node.vsr[p].tick(self.now_ns) orelse continue;
                    const new_view  = svc.header.view_number;
                    // Broadcast SVC to all other nodes.
                    for (0..SIM_NODES) |j| {
                        const jj: u8 = @intCast(j);
                        if (jj != node.id) {
                            self.enqueue(.{
                                .from      = node.id,
                                .to        = jj,
                                .partition = pid,
                                .msg       = .{ .start_view_change = svc },
                            });
                        }
                    }
                    // Inject own DVC directly into the new leader's inbox.
                    // This avoids the node sending SVC to itself and double-voting.
                    const own_dvc       = node.vsr[p].make_do_view_change(new_view);
                    const new_leader_id = node.vsr[p].leader_node(new_view);
                    self.enqueue(.{
                        .from      = node.id,
                        .to        = new_leader_id,
                        .partition = pid,
                        .msg       = .{ .do_view_change = own_dvc },
                    });
                }
            }
        }
    }

    // ---- Message delivery ----

    /// Deliver one pending message. Returns false if inbox is empty.
    /// Chaos filters (network partition, random drops) are applied before processing.
    pub fn deliver_one(self: *VirtualCluster) bool {
        if (self.inbox.items.len == 0) return false;
        const env = self.inbox.orderedRemove(0);

        // Network partition: drop messages between different groups.
        if (self.partitioned and self.groups[env.from] != self.groups[env.to]) {
            return true; // silently dropped
        }
        // Random message drop.
        if (self.drop_probability > 0) {
            if (self.chaos_rng.random().float(f32) < self.drop_probability) {
                return true; // silently dropped
            }
        }

        self.process(env);
        return true;
    }

    /// Drain all pending messages.
    pub fn drain(self: *VirtualCluster) void {
        while (self.deliver_one()) {}
    }

    fn process(self: *VirtualCluster, env: Envelope) void {
        const node = &self.nodes[env.to];
        if (!node.alive) return;
        const p = env.partition;

        switch (env.msg) {
            .prepare => |msg| {
                if (node.vsr[p].role != .replica) return;
                // Use shared WAL CRC as the wal_crc proof.
                const wal_crc = self.wals[p].last_crc_val();
                const ok = node.vsr[p].on_prepare(&msg, wal_crc) orelse return;
                node.vsr[p].last_ping_ns = self.now_ns;
                const leader_id = node.vsr[p].leader_node(msg.header.view_number);
                self.enqueue(.{
                    .from      = env.to,
                    .to        = leader_id,
                    .partition = p,
                    .msg       = .{ .prepare_ok = ok },
                });
            },
            .prepare_ok => |msg| {
                if (node.vsr[p].role != .leader) return;
                const commit = node.vsr[p].on_prepare_ok(&msg) orelse return;
                // Broadcast COMMIT to replicas (skip leader self).
                self.broadcast(env.to, p, .{ .commit = commit }, env.to);
            },
            .commit => |msg| {
                if (node.vsr[p].role != .replica) return;
                node.vsr[p].on_commit(&msg);
                node.vsr[p].last_ping_ns = self.now_ns;
            },
            .ping => |msg| {
                if (node.vsr[p].role != .replica) return;
                node.vsr[p].on_ping(&msg);
                node.vsr[p].last_ping_ns = self.now_ns;
            },
            .start_view_change => |msg| {
                const dvc           = node.vsr[p].on_start_view_change(&msg) orelse return;
                const new_leader_id = node.vsr[p].leader_node(dvc.header.view_number);
                self.enqueue(.{
                    .from      = env.to,
                    .to        = new_leader_id,
                    .partition = p,
                    .msg       = .{ .do_view_change = dvc },
                });
            },
            .do_view_change => |msg| {
                // Only the designated new leader processes DVCs.
                if (node.vsr[p].leader_node(msg.header.view_number) != node.id) return;
                const sv = node.vsr[p].on_do_view_change(&msg) orelse return;
                // Broadcast START_VIEW to all nodes.
                self.broadcast(env.to, p, .{ .start_view = sv }, null);
            },
            .start_view => |msg| {
                node.vsr[p].on_start_view(&msg, self.now_ns);
            },
            .redirect => {},
        }
    }

    // ---- Internal helpers ----

    fn enqueue(self: *VirtualCluster, env: Envelope) void {
        self.inbox.append(self.alloc, env) catch @panic("OOM");
    }

    /// Send msg from `from` to all nodes, skipping `skip` if non-null.
    fn broadcast(self: *VirtualCluster, from: u8, partition: u32, msg: MsgPayload, skip: ?u8) void {
        for (0..SIM_NODES) |j| {
            const jj: u8 = @intCast(j);
            if (skip != null and jj == skip.?) continue;
            self.enqueue(.{ .from = from, .to = jj, .partition = partition, .msg = msg });
        }
    }

    // ---- Chaos ----

    pub fn crash(self: *VirtualCluster, node_id: u8) void {
        self.nodes[node_id].alive = false;
    }

    /// Bring a crashed node back. Resets view-change timers so it has time to catch up.
    pub fn restart(self: *VirtualCluster, node_id: u8) void {
        const node = &self.nodes[node_id];
        node.alive = true;
        for (0..SIM_PARTITIONS) |p| {
            node.vsr[p].last_ping_ns = self.now_ns;
        }
    }

    // ---- Graceful shutdown ----

    /// Simulate a graceful shutdown of a node: drain pending ops, then
    /// crash + restart. Unlike crash(), pending PREPARE ops are committed
    /// (if quorum allows) before the node goes down.
    pub fn graceful_shutdown(self: *VirtualCluster, node_id: u8) void {
        const node = &self.nodes[node_id];
        if (!node.alive) return;

        // Drain all pending messages so in-flight ops can commit.
        self.drain();

        // Clear any remaining pending ops (simulates fsync + checkpoint).
        for (0..SIM_PARTITIONS) |p| {
            node.vsr[p].pending = null;
        }

        // Node goes down briefly.
        node.alive = false;

        // Immediate restart (simulates fast process restart after clean stop).
        node.alive = true;
        for (0..SIM_PARTITIONS) |p| {
            node.vsr[p].last_ping_ns = self.now_ns;
        }
    }

    // ---- Rebalance ----

    /// Reassign partition leaders across nodes. shift rotates the assignment
    /// by 1 or 2 positions: new leader = (partition_id + shift) % SIM_NODES.
    /// This simulates adding/removing a node that triggers redistribution.
    pub fn rebalance(self: *VirtualCluster, shift: u8) void {
        // Drain all messages first to settle any in-flight ops.
        self.drain();

        for (0..SIM_PARTITIONS) |p| {
            const pid: u32 = @intCast(p);
            const old_leader_idx = pid % SIM_NODES;
            const new_leader_idx = (pid + shift) % SIM_NODES;

            if (old_leader_idx == new_leader_idx) continue;

            // Compute new replica group: new_leader first, then others in order.
            const base: u8 = @intCast(new_leader_idx);
            const new_rg = [3]u8{
                @intCast((base + 0) % SIM_NODES),
                @intCast((base + 1) % SIM_NODES),
                @intCast((base + 2) % SIM_NODES),
            };

            // Determine next view number: max across all nodes + 1.
            var max_view: u64 = 0;
            for (0..SIM_NODES) |i| {
                max_view = @max(max_view, self.nodes[i].vsr[p].view_number);
            }
            const new_view = max_view + 1;

            // Update all nodes for this partition.
            for (0..SIM_NODES) |i| {
                const ii: u8 = @intCast(i);
                const new_role: Role = if (ii == @as(u8, @intCast(new_leader_idx))) .leader else .replica;

                self.nodes[i].vsr[p].role = new_role;
                self.nodes[i].vsr[p].view_number = new_view;
                self.nodes[i].vsr[p].pending = null;
                self.nodes[i].vsr[p].vc_state = null;
                self.nodes[i].vsr[p].last_ping_ns = self.now_ns;
                self.nodes[i].vsr[p].cfg.replica_group = new_rg;
            }
        }
    }

    // ---- Queries ----

    /// Returns the node_id of the current leader for a partition, or null if none alive.
    pub fn leader_of(self: *const VirtualCluster, partition: u32) ?u8 {
        for (0..SIM_NODES) |i| {
            if (self.nodes[i].alive and self.nodes[i].vsr[partition].role == .leader) {
                return @intCast(i);
            }
        }
        return null;
    }

    /// Submit an op to a partition and drive the full PREPARE → COMMIT cycle.
    /// Returns error.NoLeader if no alive leader exists for the partition.
    pub fn submit(self: *VirtualCluster, partition: u32, batch_crc: u32, n_events: u32) !void {
        const leader_id = self.leader_of(partition) orelse return error.NoLeader;
        const leader    = &self.nodes[leader_id];

        const prep = leader.vsr[partition].prepare(batch_crc, n_events);

        // Single-node fast path (n_nodes=1 would be used in unit tests).
        if (leader.vsr[partition].commit_if_no_replicas() != null) return;

        // Send PREPARE to all replicas.
        for (0..SIM_NODES) |j| {
            const jj: u8 = @intCast(j);
            if (jj == leader_id) continue;
            self.enqueue(.{
                .from      = leader_id,
                .to        = jj,
                .partition = partition,
                .msg       = .{ .prepare = prep },
            });
        }
        self.drain();
    }

    /// Submit a batch of events and track them in committed_events if the op commits.
    ///
    /// Returns true if the op was committed, false if it was lost to chaos.
    /// Clears any stuck pending op on the leader before attempting the new prepare.
    pub fn submit_events(self: *VirtualCluster, partition: u32, events: []const Event) !bool {
        const leader_id = self.leader_of(partition) orelse return error.NoLeader;
        const leader    = &self.nodes[leader_id];

        // Clear any stuck pending from a previously dropped PREPARE.
        // The op_number gap is acceptable in the simulator.
        if (leader.vsr[partition].pending != null) {
            leader.vsr[partition].pending = null;
        }

        const before = leader.vsr[partition].commit_number;

        // Compute batch CRC as XOR of each event's value and account_id.
        var batch_crc: u32 = 0;
        for (events) |e| {
            batch_crc ^= @truncate(e.value ^ e.account_id ^ e.offset);
        }

        const prep = leader.vsr[partition].prepare(batch_crc, @intCast(events.len));

        // Single-node fast path.
        if (leader.vsr[partition].commit_if_no_replicas() != null) {
            for (events) |e| try self.committed_events[partition].append(self.alloc, e);
            // Write to shared WAL for recovery invariant.
            const payload = std.mem.sliceAsBytes(events);
            try self.wals[partition].append(.commit, payload);
            return true;
        }

        // Send PREPARE to all replicas.
        for (0..SIM_NODES) |j| {
            const jj: u8 = @intCast(j);
            if (jj == leader_id) continue;
            self.enqueue(.{
                .from      = leader_id,
                .to        = jj,
                .partition = partition,
                .msg       = .{ .prepare = prep },
            });
        }
        self.drain();

        const committed = leader.vsr[partition].commit_number > before;
        if (committed) {
            for (events) |e| try self.committed_events[partition].append(self.alloc, e);
            // Write to shared WAL for recovery invariant.
            const payload = std.mem.sliceAsBytes(events);
            try self.wals[partition].append(.commit, payload);
        }
        return committed;
    }
};

// ---- Tests ----

test "cluster: submit commits on all nodes" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var cluster = try VirtualCluster.init(alloc, 0);
    defer cluster.deinit();

    // Partition 0 is led by node0 (0 % 3 == 0).
    try cluster.submit(0, 0xDEADBEEF, 10);

    // Leader committed.
    try std.testing.expectEqual(@as(u64, 1), cluster.nodes[0].vsr[0].commit_number);
    // Replicas received COMMIT.
    try std.testing.expectEqual(@as(u64, 1), cluster.nodes[1].vsr[0].commit_number);
    try std.testing.expectEqual(@as(u64, 1), cluster.nodes[2].vsr[0].commit_number);
}

test "cluster: multiple partitions are independent" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var cluster = try VirtualCluster.init(alloc, 0);
    defer cluster.deinit();

    // Submit to partition 0 (leader: node0) and partition 1 (leader: node1).
    try cluster.submit(0, 0xAAAA, 1);
    try cluster.submit(1, 0xBBBB, 1);

    // Partition 0 committed on all nodes.
    try std.testing.expectEqual(@as(u64, 1), cluster.nodes[0].vsr[0].commit_number);
    try std.testing.expectEqual(@as(u64, 1), cluster.nodes[1].vsr[0].commit_number);
    try std.testing.expectEqual(@as(u64, 1), cluster.nodes[2].vsr[0].commit_number);

    // Partition 1 committed on all nodes, independent of partition 0.
    try std.testing.expectEqual(@as(u64, 1), cluster.nodes[0].vsr[1].commit_number);
    try std.testing.expectEqual(@as(u64, 1), cluster.nodes[1].vsr[1].commit_number);
    try std.testing.expectEqual(@as(u64, 1), cluster.nodes[2].vsr[1].commit_number);

    // Partition 0 state unchanged by partition 1 ops.
    try std.testing.expectEqual(@as(u64, 1), cluster.nodes[0].vsr[0].op_number);
}

test "cluster: view change after leader crash" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var cluster = try VirtualCluster.init(alloc, 0);
    defer cluster.deinit();

    // Crash node0 (leader of partition 0) before any ticks.
    cluster.crash(0);

    // Advance time until replicas time out (timeout = 2s, tick = 100ms → 20 ticks).
    for (0..21) |_| {
        cluster.tick();
        cluster.drain();
    }

    // A new leader must exist for partition 0.
    const new_leader = cluster.leader_of(0);
    try std.testing.expect(new_leader != null);
    // New leader is node1 (view 1: 1 % 3 == 1).
    try std.testing.expectEqual(@as(u8, 1), new_leader.?);
    // New leader is in view 1.
    try std.testing.expectEqual(@as(u64, 1), cluster.nodes[1].vsr[0].view_number);
}

test "cluster: restart after crash rejoins with reset timers" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var cluster = try VirtualCluster.init(alloc, 0);
    defer cluster.deinit();

    cluster.crash(2);
    cluster.tick();
    cluster.restart(2);

    // After restart, the node's ping timers should be set to now_ns (not zero).
    try std.testing.expect(cluster.nodes[2].vsr[0].last_ping_ns > 0);
    try std.testing.expect(cluster.nodes[2].alive);
}

test "cluster: submit fails with no leader" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var cluster = try VirtualCluster.init(alloc, 0);
    defer cluster.deinit();

    // Crash the only leader for partition 0.
    cluster.crash(0);

    const result = cluster.submit(0, 0x1234, 1);
    try std.testing.expectError(error.NoLeader, result);
}
