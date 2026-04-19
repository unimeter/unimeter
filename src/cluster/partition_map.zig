//! PartitionMap: routes account_id to the leader/replica nodes for its partition.
//!
//! N_PARTITIONS is fixed at cluster init time and never changes.
//! Routing: account_id % N_PARTITIONS → partition_id → PartitionEntry.
//!
//! Persisted as a flat binary file (N_PARTITIONS × 4B = 1024B).
//! Replicated via Cluster Log on every leader change.

const std     = @import("std");
const disk_io = @import("../io/disk_io.zig");

pub const N_PARTITIONS: u32 = 256;

/// Node identifier. u8 supports up to 254 nodes; 0xFF = unassigned.
pub const NodeId = u8;
pub const NO_NODE: NodeId = 0xFF;

/// Leader and two replicas for one partition. 4 bytes.
pub const PartitionEntry = extern struct {
    leader:   NodeId,
    replicas: [2]NodeId,
    _pad:     u8,
};

comptime {
    std.debug.assert(@sizeOf(PartitionEntry) == 4);
}

pub const PartitionMap = struct {
    entries: [N_PARTITIONS]PartitionEntry,

    /// Partition index for a given account.
    pub fn partition_of(account_id: u64) u32 {
        return @intCast(account_id % N_PARTITIONS);
    }

    /// Full entry (leader + replicas) for the partition that owns account_id.
    pub fn entry_for(self: *const PartitionMap, account_id: u64) PartitionEntry {
        return self.entries[partition_of(account_id)];
    }

    /// Leader node for the partition that owns account_id.
    pub fn leader_for(self: *const PartitionMap, account_id: u64) NodeId {
        return self.entries[partition_of(account_id)].leader;
    }

    /// Build a balanced initial map for n_nodes nodes.
    /// Leader assignment: partition P → node P % n_nodes.
    /// Replicas: next two nodes in round-robin order (NO_NODE if n_nodes < 2/3).
    pub fn init_uniform(n_nodes: u8) PartitionMap {
        std.debug.assert(n_nodes >= 1);
        var map: PartitionMap = undefined;
        for (&map.entries, 0..) |*e, p| {
            e.* = .{
                .leader   = @intCast(p % n_nodes),
                .replicas = .{
                    if (n_nodes >= 2) @intCast((p + 1) % n_nodes) else NO_NODE,
                    if (n_nodes >= 3) @intCast((p + 2) % n_nodes) else NO_NODE,
                },
                ._pad = 0,
            };
        }
        return map;
    }

    /// Update the leader for partition after a View Change.
    /// Does not update replicas; caller is responsible for replica bookkeeping.
    pub fn set_leader(self: *PartitionMap, partition: u32, new_leader: NodeId) void {
        self.entries[partition].leader = new_leader;
    }

    /// Persist atomically: write to <path>.tmp, then rename.
    pub fn save(self: *const PartitionMap, path: []const u8) !void {
        var tmp_buf: [512]u8 = undefined;
        const tmp_path = try std.fmt.bufPrint(&tmp_buf, "{s}.tmp", .{path});
        {
            const f = try disk_io.open_rw(tmp_path);
            defer f.close();
            try f.write_all(std.mem.asBytes(&self.entries));
        }
        try disk_io.rename(tmp_path, path);
    }

    /// Load from file. Returns error.FileNotFound if not present.
    pub fn load(path: []const u8) !PartitionMap {
        const f = try disk_io.open_ro(path);
        defer f.close();
        var map: PartitionMap = undefined;
        const expected = @sizeOf([N_PARTITIONS]PartitionEntry);
        var total: usize = 0;
        const bytes = std.mem.asBytes(&map.entries);
        while (total < expected) {
            const n = try f.read(bytes[total..]);
            if (n == 0) break;
            total += n;
        }
        if (total != expected) return error.CorruptPartitionMap;
        return map;
    }

    /// Load from file, or build a fresh uniform map if the file does not exist.
    pub fn load_or_init(path: []const u8, n_nodes: u8) !PartitionMap {
        return PartitionMap.load(path) catch |err| {
            if (err == error.FileNotFound) return PartitionMap.init_uniform(n_nodes);
            return err;
        };
    }
};

// ----------------------------------------------------------------------------
// Tests
// ----------------------------------------------------------------------------

test "partition_of: deterministic modulo routing" {
    try std.testing.expectEqual(@as(u32, 0),   PartitionMap.partition_of(0));
    try std.testing.expectEqual(@as(u32, 1),   PartitionMap.partition_of(1));
    try std.testing.expectEqual(@as(u32, 255), PartitionMap.partition_of(255));
    try std.testing.expectEqual(@as(u32, 0),   PartitionMap.partition_of(256));
    try std.testing.expectEqual(@as(u32, 57),  PartitionMap.partition_of(12345)); // 12345 % 256 = 57
}

test "init_uniform: single node owns all partitions" {
    const map = PartitionMap.init_uniform(1);
    for (map.entries) |e| {
        try std.testing.expectEqual(@as(NodeId, 0),     e.leader);
        try std.testing.expectEqual(@as(NodeId, NO_NODE), e.replicas[0]);
        try std.testing.expectEqual(@as(NodeId, NO_NODE), e.replicas[1]);
    }
}

test "init_uniform: two nodes split partitions" {
    const map = PartitionMap.init_uniform(2);
    // Even partitions → node 0, odd → node 1.
    try std.testing.expectEqual(@as(NodeId, 0), map.entries[0].leader);
    try std.testing.expectEqual(@as(NodeId, 1), map.entries[1].leader);
    try std.testing.expectEqual(@as(NodeId, 0), map.entries[2].leader);
    // One real replica, second slot is NO_NODE.
    try std.testing.expectEqual(@as(NodeId, 1),     map.entries[0].replicas[0]);
    try std.testing.expectEqual(@as(NodeId, NO_NODE), map.entries[0].replicas[1]);
}

test "init_uniform: three nodes round-robin with two replicas" {
    const map = PartitionMap.init_uniform(3);
    // Partition 0: leader=0, replicas={1,2}
    try std.testing.expectEqual(@as(NodeId, 0), map.entries[0].leader);
    try std.testing.expectEqual(@as(NodeId, 1), map.entries[0].replicas[0]);
    try std.testing.expectEqual(@as(NodeId, 2), map.entries[0].replicas[1]);
    // Partition 1: leader=1, replicas={2,0}
    try std.testing.expectEqual(@as(NodeId, 1), map.entries[1].leader);
    try std.testing.expectEqual(@as(NodeId, 2), map.entries[1].replicas[0]);
    try std.testing.expectEqual(@as(NodeId, 0), map.entries[1].replicas[1]);
    // Partition 2: leader=2, replicas={0,1}
    try std.testing.expectEqual(@as(NodeId, 2), map.entries[2].leader);
    try std.testing.expectEqual(@as(NodeId, 0), map.entries[2].replicas[0]);
    try std.testing.expectEqual(@as(NodeId, 1), map.entries[2].replicas[1]);
}

test "init_uniform: 256 partitions distributed evenly across 8 nodes" {
    const n: u8 = 8;
    const map = PartitionMap.init_uniform(n);
    var counts = [_]u32{0} ** 8;
    for (map.entries) |e| counts[e.leader] += 1;
    // 256 / 8 = 32 partitions per node, perfectly even.
    for (counts) |c| try std.testing.expectEqual(@as(u32, 32), c);
}

test "entry_for and leader_for" {
    const map = PartitionMap.init_uniform(3);
    const e = map.entry_for(12345);
    // 12345 % 256 = 57, 57 % 3 = 0
    try std.testing.expectEqual(@as(NodeId, 0), e.leader);
    try std.testing.expectEqual(@as(NodeId, 0), map.leader_for(12345));
}

test "set_leader: view change update" {
    var map = PartitionMap.init_uniform(3);
    // Partition 0 starts with leader=0; simulate failover to node 1.
    map.set_leader(0, 1);
    try std.testing.expectEqual(@as(NodeId, 1), map.entries[0].leader);
    // Other partitions untouched.
    try std.testing.expectEqual(@as(NodeId, 1), map.entries[1].leader);
}

test "save and load: round-trip" {
    const path = "/tmp/billing_partition_map_test.bin";
    defer disk_io.remove(path) catch {};

    var map = PartitionMap.init_uniform(3);
    map.set_leader(5, 2);
    try map.save(path);

    const loaded = try PartitionMap.load(path);
    try std.testing.expectEqualSlices(
        PartitionEntry,
        &map.entries,
        &loaded.entries,
    );
}

test "load_or_init: returns uniform map when file missing" {
    const path = "/tmp/billing_partition_map_nonexistent.bin";
    const map = try PartitionMap.load_or_init(path, 3);
    // Should match a freshly built uniform map.
    const expected = PartitionMap.init_uniform(3);
    try std.testing.expectEqualSlices(PartitionEntry, &expected.entries, &map.entries);
}
