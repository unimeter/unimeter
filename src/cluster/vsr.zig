//! VSR (Viewstamped Replication) per-partition state machine.
//!
//! Covers both normal case and view change (leader election on timeout).
//!
//! Normal-case message flow:
//!   client ingest  →  leader.prepare(batch_crc, n_events)  →  PrepareMsg
//!   PrepareMsg     →  replica.on_prepare(msg, wal_crc)     →  PrepareOkMsg
//!   PrepareOkMsg   →  leader.on_prepare_ok(msg)            →  ?CommitMsg
//!   CommitMsg      →  replica.on_commit(msg)
//!
//! View-change message flow (replica detects leader timeout):
//!   replica.tick(now_ns)                  →  ?StartViewChangeMsg  (broadcast to all)
//!   replica.on_start_view_change(msg)     →  ?DoViewChangeMsg     (send to new leader)
//!   new_leader.make_do_view_change(view)  →  DoViewChangeMsg      (own state)
//!   new_leader.on_do_view_change(msg)     →  ?StartViewMsg        (broadcast when quorum)
//!   all.on_start_view(msg, now_ns)        →  void                 (transition to new view)

const std     = @import("std");
const NodeId  = @import("partition_map.zig").NodeId;
const metrics = @import("../metrics/metrics.zig");

pub const VSR_MAGIC: u32 = 0xB177_1001;

pub const MsgType = enum(u8) {
    prepare           = 1,
    prepare_ok        = 2,
    commit            = 3,
    ping              = 4,
    start_view_change = 5,
    do_view_change    = 6,
    start_view        = 7,
    redirect          = 8,
};

/// Common header for all inter-node VSR messages. 32 bytes.
///
/// checksum covers bytes [0..11]: magic + msg_type + from_node + _pad + partition_id.
/// view_number and op_number are validated by the state machine, not the checksum.
pub const MsgHeader = extern struct {
    magic:        u32,    //  4B [ 0.. 3]  VSR_MAGIC
    msg_type:     u8,     //  1B [ 4]      MsgType
    from_node:    NodeId, //  1B [ 5]      sender node_id
    _pad:         [2]u8,  //  2B [ 6.. 7]
    partition_id: u32,    //  4B [ 8..11]
    checksum:     u32,    //  4B [12..15]  CRC32 over bytes [0..11]
    view_number:  u64,    //  8B [16..23]
    op_number:    u64,    //  8B [24..31]
};

comptime {
    std.debug.assert(@sizeOf(MsgHeader)  == 32);
    std.debug.assert(@alignOf(MsgHeader) == 8);
    std.debug.assert(@offsetOf(MsgHeader, "view_number") == 16);
    std.debug.assert(@offsetOf(MsgHeader, "op_number")   == 24);
}

// ---- Normal-case messages ----

/// PREPARE: leader → replicas.
pub const PrepareMsg = extern struct {
    header:    MsgHeader, // 32B
    batch_crc: u32,       //  4B  CRC32 of the event batch (from WAL entry)
    n_events:  u32,       //  4B
};

/// PREPARE_OK: replica → leader.
pub const PrepareOkMsg = extern struct {
    header:  MsgHeader, // 32B
    wal_crc: u32,       //  4B  Wal.last_crc after appending this op
    _pad:    u32,       //  4B
};

/// COMMIT: leader → replicas. commit_number in header.op_number.
pub const CommitMsg = extern struct {
    header: MsgHeader,
};

/// PING: heartbeat from leader. commit_number piggybacked in header.op_number.
pub const PingMsg = extern struct {
    header: MsgHeader,
};

// ---- View-change messages ----

/// START_VIEW_CHANGE: any replica → all replicas. Proposes a new view.
/// Proposed view number is in header.view_number.
pub const StartViewChangeMsg = extern struct {
    header: MsgHeader,
};

/// DO_VIEW_CHANGE: replica → new leader. Carries the sender's state.
/// Sender's last prepared op in header.op_number.
pub const DoViewChangeMsg = extern struct {
    header:        MsgHeader, // 32B
    commit_number: u64,       //  8B  sender's last committed op
    _pad:          u64,       //  8B
};

/// START_VIEW: new leader → all replicas. Announces the new view.
/// New leader's op_number in header.op_number.
pub const StartViewMsg = extern struct {
    header:        MsgHeader, // 32B
    commit_number: u64,       //  8B  new leader's commit_number
    _pad:          u64,       //  8B
};

comptime {
    std.debug.assert(@sizeOf(PrepareMsg)         == 40);
    std.debug.assert(@sizeOf(PrepareOkMsg)        == 40);
    std.debug.assert(@sizeOf(CommitMsg)           == 32);
    std.debug.assert(@sizeOf(PingMsg)             == 32);
    std.debug.assert(@sizeOf(StartViewChangeMsg)  == 32);
    std.debug.assert(@sizeOf(DoViewChangeMsg)     == 48);
    std.debug.assert(@sizeOf(StartViewMsg)        == 48);
}

// ----------------------------------------------------------------------------
// State machine
// ----------------------------------------------------------------------------

pub const Role = enum { leader, replica };

/// In-flight op tracked by the leader until quorum is reached.
const PendingOp = struct {
    op_number:   u64,
    view_number: u64,
    batch_crc:   u32,
    n_events:    u32,
    ok_count:    u8,
};

/// State accumulated while waiting for DO_VIEW_CHANGE quorum.
const ViewChangeState = struct {
    new_view:       u64,
    votes_received: u8,
    best_op_number: u64,
    best_commit:    u64,
};

pub const VsrConfig = struct {
    node_id:      NodeId,
    partition_id: u32,
    /// Replication group size (1 = single-node, 3 = standard).
    n_nodes:      u8,
    /// Ordered replica group: view V → leader = replica_group[V % n_nodes].
    /// Defaults to {0,1,2}; override when partition ownership differs.
    replica_group: [3]NodeId = .{ 0, 1, 2 },
    /// Nanoseconds without a leader message before a replica initiates view change.
    view_change_timeout_ns: i64 = 2_000_000_000,
};

pub const VsrPartition = struct {
    cfg:           VsrConfig,
    role:          Role,
    view_number:   u64,
    /// Last prepared op (leader) or last received PREPARE (replica).
    op_number:     u64,
    /// Last op confirmed committed on this node.
    commit_number: u64,
    /// Leader only: in-flight op awaiting quorum.
    pending:       ?PendingOp,
    /// Replica only: nanosecond timestamp of last valid leader message.
    /// Update after every on_prepare / on_commit / on_ping call.
    last_ping_ns:  i64,
    /// Non-null while a view change is in progress.
    vc_state:      ?ViewChangeState,

    pub fn init(cfg: VsrConfig, role: Role) VsrPartition {
        return .{
            .cfg           = cfg,
            .role          = role,
            .view_number   = 0,
            .op_number     = 0,
            .commit_number = 0,
            .pending       = null,
            .last_ping_ns  = 0,
            .vc_state      = null,
        };
    }

    // ---- Quorum helpers ----

    pub fn quorum(self: *const VsrPartition) u8 {
        return self.cfg.n_nodes / 2 + 1;
    }

    pub fn needed_ok_count(self: *const VsrPartition) u8 {
        const q = self.quorum();
        return if (q > 1) q - 1 else 0;
    }

    // ---- Leader determination ----

    /// Node that should be leader for the given view number.
    pub fn leader_node(self: *const VsrPartition, view: u64) NodeId {
        return self.cfg.replica_group[@intCast(view % self.cfg.n_nodes)];
    }

    fn is_leader_for(self: *const VsrPartition, view: u64) bool {
        return self.leader_node(view) == self.cfg.node_id;
    }

    // ---- Leader: normal case ----

    pub fn prepare(self: *VsrPartition, batch_crc: u32, n_events: u32) PrepareMsg {
        std.debug.assert(self.role == .leader);
        std.debug.assert(self.pending == null);

        self.op_number += 1;
        self.pending = .{
            .op_number   = self.op_number,
            .view_number = self.view_number,
            .batch_crc   = batch_crc,
            .n_events    = n_events,
            .ok_count    = 0,
        };
        return PrepareMsg{
            .header    = self.make_header(.prepare, self.op_number),
            .batch_crc = batch_crc,
            .n_events  = n_events,
        };
    }

    /// Single-node shortcut: no replicas, commit immediately after local WAL write.
    pub fn commit_if_no_replicas(self: *VsrPartition) ?CommitMsg {
        if (self.needed_ok_count() > 0) return null;
        const p = self.pending orelse return null;
        self.commit_number = p.op_number;
        self.pending = null;
        return CommitMsg{ .header = self.make_header(.commit, self.commit_number) };
    }

    pub fn on_prepare_ok(self: *VsrPartition, msg: *const PrepareOkMsg) ?CommitMsg {
        std.debug.assert(self.role == .leader);

        if (msg.header.view_number != self.view_number) return null;
        const p = &(self.pending orelse return null);
        if (msg.header.op_number != p.op_number) return null;

        p.ok_count += 1;
        if (p.ok_count < self.needed_ok_count()) return null;

        self.commit_number = p.op_number;
        self.pending = null;
        return CommitMsg{ .header = self.make_header(.commit, self.commit_number) };
    }

    // ---- Replica: normal case ----

    pub fn on_prepare(self: *VsrPartition, msg: *const PrepareMsg, wal_crc: u32) ?PrepareOkMsg {
        std.debug.assert(self.role == .replica);

        if (msg.header.view_number != self.view_number)   return null;
        if (msg.header.op_number   != self.op_number + 1) return null;

        self.op_number = msg.header.op_number;
        return PrepareOkMsg{
            .header  = self.make_header(.prepare_ok, self.op_number),
            .wal_crc = wal_crc,
            ._pad    = 0,
        };
    }

    pub fn on_commit(self: *VsrPartition, msg: *const CommitMsg) void {
        std.debug.assert(self.role == .replica);

        if (msg.header.view_number != self.view_number) return;
        if (msg.header.op_number > self.op_number) return;
        if (msg.header.op_number > self.commit_number) {
            self.commit_number = msg.header.op_number;
        }
    }

    // ---- Ping ----

    pub fn make_ping(self: *const VsrPartition) PingMsg {
        std.debug.assert(self.role == .leader);
        return PingMsg{ .header = self.make_header(.ping, self.commit_number) };
    }

    pub fn on_ping(self: *VsrPartition, msg: *const PingMsg) void {
        std.debug.assert(self.role == .replica);

        if (msg.header.view_number != self.view_number) return;
        if (msg.header.op_number > self.commit_number and
            msg.header.op_number <= self.op_number)
        {
            self.commit_number = msg.header.op_number;
        }
    }

    // ---- View change ----

    /// Call periodically. Returns START_VIEW_CHANGE if the replica has not heard
    /// from the current leader within view_change_timeout_ns.
    /// Caller must broadcast the message and also call make_do_view_change().
    pub fn tick(self: *VsrPartition, now_ns: i64) ?StartViewChangeMsg {
        if (self.role != .replica) return null;
        if (self.vc_state != null) return null; // already running
        if (now_ns - self.last_ping_ns < self.cfg.view_change_timeout_ns) return null;

        const new_view = self.view_number + 1;
        self.vc_state = .{
            .new_view       = new_view,
            .votes_received = 0,
            .best_op_number = self.op_number,
            .best_commit    = self.commit_number,
        };
        return StartViewChangeMsg{
            .header = self.make_header_for_view(.start_view_change, new_view, self.op_number),
        };
    }

    /// Receive START_VIEW_CHANGE from another replica.
    /// Returns a DO_VIEW_CHANGE to send to the new leader (see leader_node(new_view)).
    pub fn on_start_view_change(self: *VsrPartition, msg: *const StartViewChangeMsg) ?DoViewChangeMsg {
        const proposed = msg.header.view_number;
        if (proposed <= self.view_number) return null;

        // Upgrade to the highest proposed view seen so far.
        if (self.vc_state == null or self.vc_state.?.new_view < proposed) {
            self.vc_state = .{
                .new_view       = proposed,
                .votes_received = 0,
                .best_op_number = self.op_number,
                .best_commit    = self.commit_number,
            };
        }
        return self.make_do_view_change(proposed);
    }

    /// Build the DO_VIEW_CHANGE carrying this node's own state.
    /// Send to leader_node(new_view). If self is the new leader, call on_do_view_change directly.
    pub fn make_do_view_change(self: *const VsrPartition, new_view: u64) DoViewChangeMsg {
        return DoViewChangeMsg{
            .header        = self.make_header_for_view(.do_view_change, new_view, self.op_number),
            .commit_number = self.commit_number,
            ._pad          = 0,
        };
    }

    /// New leader collects DO_VIEW_CHANGE messages.
    /// Returns START_VIEW when quorum is reached; caller broadcasts it to all nodes.
    pub fn on_do_view_change(self: *VsrPartition, msg: *const DoViewChangeMsg) ?StartViewMsg {
        const new_view = msg.header.view_number;
        if (new_view <= self.view_number) return null;
        if (!self.is_leader_for(new_view)) return null;

        // Initialize or upgrade vc_state for this view.
        if (self.vc_state == null or self.vc_state.?.new_view < new_view) {
            self.vc_state = .{
                .new_view       = new_view,
                .votes_received = 0,
                .best_op_number = self.op_number,
                .best_commit    = self.commit_number,
            };
        }
        const vc = &self.vc_state.?;
        if (vc.new_view != new_view) return null;

        // Merge: adopt the highest state seen across all DO_VIEW_CHANGE senders.
        if (msg.header.op_number > vc.best_op_number) vc.best_op_number = msg.header.op_number;
        if (msg.commit_number    > vc.best_commit)    vc.best_commit    = msg.commit_number;
        vc.votes_received += 1;

        if (vc.votes_received < self.quorum()) return null;

        // Quorum reached: become leader for new_view.
        // Use @max to preserve any commits or ops the node applied while
        // simultaneously acting as leader in the previous view during vc collection.
        self.view_number   = new_view;
        self.op_number     = @max(self.op_number,     vc.best_op_number);
        self.commit_number = @max(self.commit_number, vc.best_commit);
        self.role          = .leader;
        self.pending       = null;
        self.vc_state      = null;

        return StartViewMsg{
            .header        = self.make_header(.start_view, self.op_number),
            .commit_number = self.commit_number,
            ._pad          = 0,
        };
    }

    /// All nodes receive START_VIEW from the new leader.
    /// now_ns resets the view-change timer for replicas.
    pub fn on_start_view(self: *VsrPartition, msg: *const StartViewMsg, now_ns: i64) void {
        if (msg.header.view_number < self.view_number) return; // stale

        self.view_number   = msg.header.view_number;
        self.op_number     = @max(self.op_number,     msg.header.op_number);
        self.commit_number = @max(self.commit_number, msg.commit_number);
        self.vc_state      = null;
        self.pending       = null;

        if (self.is_leader_for(self.view_number)) {
            self.role = .leader;
            metrics.view_changes.inc();
        } else {
            self.role         = .replica;
            self.last_ping_ns = now_ns; // reset timer: new leader is live
        }
    }

    // ---- Internal ----

    fn make_header(self: *const VsrPartition, msg_type: MsgType, op: u64) MsgHeader {
        return self.make_header_for_view(msg_type, self.view_number, op);
    }

    fn make_header_for_view(self: *const VsrPartition, msg_type: MsgType, view: u64, op: u64) MsgHeader {
        var h = MsgHeader{
            .magic        = VSR_MAGIC,
            .msg_type     = @intFromEnum(msg_type),
            .from_node    = self.cfg.node_id,
            ._pad         = .{ 0, 0 },
            .partition_id = self.cfg.partition_id,
            .checksum     = 0,
            .view_number  = view,
            .op_number    = op,
        };
        h.checksum = header_checksum(&h);
        return h;
    }
};

/// CRC32 over the first 12 bytes of MsgHeader (magic..partition_id).
pub fn header_checksum(h: *const MsgHeader) u32 {
    return std.hash.Crc32.hash(std.mem.asBytes(h)[0..12]);
}

pub fn validate_header(h: *const MsgHeader) bool {
    if (h.magic != VSR_MAGIC) return false;
    if (header_checksum(h) != h.checksum) return false;
    return true;
}

// ----------------------------------------------------------------------------
// Tests
// ----------------------------------------------------------------------------

test "quorum: n_nodes=1,2,3,5" {
    const make = struct {
        fn f(n: u8) VsrPartition {
            return VsrPartition.init(.{ .node_id = 0, .partition_id = 0, .n_nodes = n }, .leader);
        }
    }.f;

    try std.testing.expectEqual(@as(u8, 1), make(1).quorum());
    try std.testing.expectEqual(@as(u8, 0), make(1).needed_ok_count());

    try std.testing.expectEqual(@as(u8, 2), make(2).quorum());
    try std.testing.expectEqual(@as(u8, 1), make(2).needed_ok_count());

    try std.testing.expectEqual(@as(u8, 2), make(3).quorum());
    try std.testing.expectEqual(@as(u8, 1), make(3).needed_ok_count());

    try std.testing.expectEqual(@as(u8, 3), make(5).quorum());
    try std.testing.expectEqual(@as(u8, 2), make(5).needed_ok_count());
}

test "header: validate_header round-trip" {
    var vsr = VsrPartition.init(
        .{ .node_id = 0, .partition_id = 7, .n_nodes = 3 },
        .leader,
    );
    const msg = vsr.prepare(0xDEAD, 10);
    try std.testing.expect(validate_header(&msg.header));
    try std.testing.expectEqual(VSR_MAGIC,   msg.header.magic);
    try std.testing.expectEqual(@as(u32, 7), msg.header.partition_id);
    try std.testing.expectEqual(@as(u8, @intFromEnum(MsgType.prepare)), msg.header.msg_type);
}

test "single-node: prepare commits immediately" {
    var leader = VsrPartition.init(
        .{ .node_id = 0, .partition_id = 0, .n_nodes = 1 },
        .leader,
    );
    const prep = leader.prepare(0xABCD, 5);
    try std.testing.expectEqual(@as(u64, 1), leader.op_number);
    try std.testing.expectEqual(@as(u64, 0), leader.commit_number);

    const commit = leader.commit_if_no_replicas();
    try std.testing.expect(commit != null);
    try std.testing.expectEqual(@as(u64, 1), leader.commit_number);
    try std.testing.expect(leader.pending == null);
    try std.testing.expect(leader.commit_if_no_replicas() == null);
    _ = prep;
}

test "three-node: full normal-case round-trip" {
    const cfg_leader   = VsrConfig{ .node_id = 0, .partition_id = 0, .n_nodes = 3 };
    const cfg_replica1 = VsrConfig{ .node_id = 1, .partition_id = 0, .n_nodes = 3 };
    const cfg_replica2 = VsrConfig{ .node_id = 2, .partition_id = 0, .n_nodes = 3 };

    var leader   = VsrPartition.init(cfg_leader,   .leader);
    var replica1 = VsrPartition.init(cfg_replica1, .replica);
    var replica2 = VsrPartition.init(cfg_replica2, .replica);

    const prep = leader.prepare(0x1234, 3);
    const ok1  = replica1.on_prepare(&prep, 0xAAAA);
    try std.testing.expect(ok1 != null);
    const ok2  = replica2.on_prepare(&prep, 0xBBBB);
    try std.testing.expect(ok2 != null);

    const commit = leader.on_prepare_ok(&ok1.?);
    try std.testing.expect(commit != null);
    try std.testing.expectEqual(@as(u64, 1), leader.commit_number);
    try std.testing.expect(leader.pending == null);

    replica1.on_commit(&commit.?);
    replica2.on_commit(&commit.?);
    try std.testing.expectEqual(@as(u64, 1), replica1.commit_number);
    try std.testing.expectEqual(@as(u64, 1), replica2.commit_number);

    // Late OK from replica2: no-op.
    try std.testing.expect(leader.on_prepare_ok(&ok2.?) == null);

    const prep2   = leader.prepare(0x5678, 7);
    const ok2b    = replica1.on_prepare(&prep2, 0xCCCC);
    const commit2 = leader.on_prepare_ok(&ok2b.?);
    try std.testing.expect(commit2 != null);
    try std.testing.expectEqual(@as(u64, 2), leader.commit_number);
    replica1.on_commit(&commit2.?);
    try std.testing.expectEqual(@as(u64, 2), replica1.commit_number);
}

test "stale view: messages from old view are ignored" {
    var leader  = VsrPartition.init(.{ .node_id = 0, .partition_id = 0, .n_nodes = 3 }, .leader);
    var replica = VsrPartition.init(.{ .node_id = 1, .partition_id = 0, .n_nodes = 3 }, .replica);
    replica.view_number = 1;
    const prep = leader.prepare(0xDEAD, 1);
    try std.testing.expect(replica.on_prepare(&prep, 0) == null);
}

test "replica: processes sequential ops correctly" {
    var leader  = VsrPartition.init(.{ .node_id = 0, .partition_id = 0, .n_nodes = 3 }, .leader);
    var replica = VsrPartition.init(.{ .node_id = 1, .partition_id = 0, .n_nodes = 3 }, .replica);

    const prep1   = leader.prepare(0x1111, 1);
    const ok1     = replica.on_prepare(&prep1, 0xA);
    const commit1 = leader.on_prepare_ok(&ok1.?);
    replica.on_commit(&commit1.?);

    const prep2 = leader.prepare(0x2222, 1);
    const ok2   = replica.on_prepare(&prep2, 0xB);
    try std.testing.expect(ok2 != null);
    try std.testing.expectEqual(@as(u64, 2), replica.op_number);

    var gap_prep = prep2;
    gap_prep.header.op_number = 4;
    try std.testing.expect(replica.on_prepare(&gap_prep, 0xC) == null);
    try std.testing.expectEqual(@as(u64, 2), replica.op_number);
}

test "ping: replica advances commit via heartbeat" {
    var leader  = VsrPartition.init(.{ .node_id = 0, .partition_id = 0, .n_nodes = 3 }, .leader);
    var replica = VsrPartition.init(.{ .node_id = 1, .partition_id = 0, .n_nodes = 3 }, .replica);

    const prep = leader.prepare(0xFFFF, 2);
    const ok   = replica.on_prepare(&prep, 0xA);
    _ = leader.on_prepare_ok(&ok.?);
    try std.testing.expectEqual(@as(u64, 0), replica.commit_number);

    const ping = leader.make_ping();
    try std.testing.expectEqual(@as(u64, 1), ping.header.op_number);
    replica.on_ping(&ping);
    try std.testing.expectEqual(@as(u64, 1), replica.commit_number);
}

// ---- View-change tests ----

test "leader_node: view → node mapping" {
    const cfg = VsrConfig{ .node_id = 0, .partition_id = 0, .n_nodes = 3,
                           .replica_group = .{ 0, 1, 2 } };
    const vsr = VsrPartition.init(cfg, .leader);
    try std.testing.expectEqual(@as(NodeId, 0), vsr.leader_node(0));
    try std.testing.expectEqual(@as(NodeId, 1), vsr.leader_node(1));
    try std.testing.expectEqual(@as(NodeId, 2), vsr.leader_node(2));
    try std.testing.expectEqual(@as(NodeId, 0), vsr.leader_node(3)); // wraps
}

test "tick: fires after timeout, silent before" {
    const cfg = VsrConfig{ .node_id = 1, .partition_id = 0, .n_nodes = 3 };
    var replica = VsrPartition.init(cfg, .replica);
    replica.last_ping_ns = 0;

    // Before timeout: no message.
    try std.testing.expect(replica.tick(1_000_000_000) == null);
    // After timeout: START_VIEW_CHANGE for view 1.
    const svc = replica.tick(2_100_000_000);
    try std.testing.expect(svc != null);
    try std.testing.expectEqual(@as(u64, 1), svc.?.header.view_number);
    try std.testing.expectEqual(@as(u8, @intFromEnum(MsgType.start_view_change)),
                                svc.?.header.msg_type);
    // Already in view change: subsequent ticks are silent.
    try std.testing.expect(replica.tick(3_000_000_000) == null);
}

test "tick: leader never fires" {
    const cfg = VsrConfig{ .node_id = 0, .partition_id = 0, .n_nodes = 3 };
    var leader = VsrPartition.init(cfg, .leader);
    leader.last_ping_ns = 0;
    try std.testing.expect(leader.tick(999_999_999_999) == null);
}

test "view change: full 3-node failover from node0 to node1" {
    // replica_group = {0,1,2}: view 0 → node0, view 1 → node1, view 2 → node2
    const cfg0 = VsrConfig{ .node_id = 0, .partition_id = 0, .n_nodes = 3 };
    const cfg1 = VsrConfig{ .node_id = 1, .partition_id = 0, .n_nodes = 3 };
    const cfg2 = VsrConfig{ .node_id = 2, .partition_id = 0, .n_nodes = 3 };

    var node0 = VsrPartition.init(cfg0, .leader);
    var node1 = VsrPartition.init(cfg1, .replica);
    var node2 = VsrPartition.init(cfg2, .replica);

    // Commit one op so nodes have non-zero state.
    const prep  = node0.prepare(0xABCD, 5);
    const ok1   = node1.on_prepare(&prep, 0x11);
    node1.last_ping_ns = 0; // will time out
    node2.last_ping_ns = 0;
    _ = node0.on_prepare_ok(&ok1.?);
    node1.on_commit(&CommitMsg{ .header = node0.make_ping().header });
    // Manually set commit_number after on_commit for the test to match
    node1.commit_number = 1;

    // --- node1 detects timeout, proposes view 1 ---
    const svc = node1.tick(2_100_000_000);
    try std.testing.expect(svc != null);
    try std.testing.expectEqual(@as(u64, 1), svc.?.header.view_number);

    // node1 is the new leader for view 1 (1 % 3 = 1)
    try std.testing.expectEqual(@as(NodeId, 1), node1.leader_node(1));

    // node2 receives START_VIEW_CHANGE → replies with DO_VIEW_CHANGE for node1
    const dvc2 = node2.on_start_view_change(&svc.?);
    try std.testing.expect(dvc2 != null);
    try std.testing.expectEqual(@as(u64, 1), dvc2.?.header.view_number);

    // node1 processes its own DVC (vote 1)
    const own_dvc = node1.make_do_view_change(1);
    const sv_early = node1.on_do_view_change(&own_dvc);
    try std.testing.expect(sv_early == null); // need 2 votes, got 1

    // node1 processes DVC from node2 (vote 2 → quorum!)
    const sv = node1.on_do_view_change(&dvc2.?);
    try std.testing.expect(sv != null);
    try std.testing.expectEqual(@as(u64, 1), node1.view_number);
    try std.testing.expectEqual(Role.leader, node1.role);

    // All nodes receive START_VIEW
    const now: i64 = 2_200_000_000;
    node0.on_start_view(&sv.?, now);
    node2.on_start_view(&sv.?, now);

    try std.testing.expectEqual(@as(u64, 1), node0.view_number);
    try std.testing.expectEqual(@as(u64, 1), node2.view_number);
    try std.testing.expectEqual(Role.replica, node0.role);
    try std.testing.expectEqual(Role.replica, node2.role);
    // Replicas' timers reset
    try std.testing.expectEqual(now, node0.last_ping_ns);
    try std.testing.expectEqual(now, node2.last_ping_ns);
}

test "view change: stale START_VIEW_CHANGE ignored" {
    const cfg = VsrConfig{ .node_id = 1, .partition_id = 0, .n_nodes = 3 };
    var node = VsrPartition.init(cfg, .replica);
    node.view_number = 2; // already at view 2

    // SVC proposing view 1 (stale) → ignored
    var svc = StartViewChangeMsg{ .header = node.make_header_for_view(
        .start_view_change, 1, 0) };
    try std.testing.expect(node.on_start_view_change(&svc) == null);

    // SVC proposing view 3 (future) → accepted
    svc.header.view_number = 3;
    try std.testing.expect(node.on_start_view_change(&svc) != null);
}

test "view change: DO_VIEW_CHANGE ignored by non-leader" {
    const cfg = VsrConfig{ .node_id = 2, .partition_id = 0, .n_nodes = 3 };
    var node = VsrPartition.init(cfg, .replica);
    // view 1 leader = node1, not node2
    const dvc = node.make_do_view_change(1);
    try std.testing.expect(node.on_do_view_change(&dvc) == null);
}

test "on_start_view: stale message is ignored" {
    const cfg = VsrConfig{ .node_id = 0, .partition_id = 0, .n_nodes = 3 };
    var node = VsrPartition.init(cfg, .replica);
    node.view_number = 5;

    const sv = StartViewMsg{
        .header        = node.make_header_for_view(.start_view, 4, 0), // view 4 < 5
        .commit_number = 0,
        ._pad          = 0,
    };
    node.on_start_view(&sv, 1000);
    try std.testing.expectEqual(@as(u64, 5), node.view_number); // unchanged
}
