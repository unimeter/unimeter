//! PeerPool: persistent TCP connections to all peer nodes in the replica group.
//!
//! Uses the server's shared io_uring ring for async connect/send/recv.
//! Messages are length-prefixed on the wire: [len: u32 LE][body: len bytes].
//!
//! Usage in the server event loop:
//!   1. if (pool.is_peer_fd(fd)) pool.on_cqe(tag, fd, result)
//!   2. pool.send(node_id, msg_bytes) — queue a VSR message for delivery
//!
//! PREPARE messages with event payloads (> MAX_MSG_BODY) require pool.send_large();
//! that path is wired up in subtask 6 (server integration).

const std   = @import("std");
const posix = std.posix;

const net_io    = @import("../io/net.zig");

const io_mod     = @import("../io/real_io.zig");
const RealIO     = io_mod.RealIO;
const OpTag      = io_mod.OpTag;
const encode     = io_mod.encode;
const decode_tag = io_mod.decode_tag;
const decode_fd  = io_mod.decode_fd;

const NodeId = @import("partition_map.zig").NodeId;

/// Offset added to the ingest port to get the replication port.
/// Node on port 7001 listens for peers on port 8001.
pub const REPL_PORT_OFFSET: u16 = 1000;

/// Maximum VSR control message body (covers all fixed-size messages ≤ 48B).
/// PREPARE with event payload uses send_large(), handled in subtask 6.
pub const MAX_MSG_BODY: usize   = 64;
pub const MAX_FRAME_LEN: usize  = @sizeOf(u32) + MAX_MSG_BODY;

/// Maximum number of peer nodes supported.
pub const MAX_PEERS: usize = 8;

/// Depth of the per-peer outbound send queue.
pub const SEND_QUEUE_DEPTH: usize = 16;

const PeerState = enum { disconnected, connecting, connected };

const SendSlot = struct {
    buf: [MAX_FRAME_LEN]u8,
    len: usize,
};

const Peer = struct {
    node_id: NodeId                = undefined,
    addr:    posix.sockaddr.in     = undefined,
    fd:      i32                   = -1,
    state:   PeerState             = .disconnected,
    in_use:  bool                  = false,

    // --- Receive state machine ---
    // recv_buf accumulates raw bytes from io_uring reads.
    // When a complete framed message is assembled, it is copied to msg_buf
    // and returned to the caller via ReceivedMsg.
    recv_buf: [MAX_FRAME_LEN]u8    = undefined,
    recv_off: usize                = 0,   // bytes valid in recv_buf so far
    msg_buf:  [MAX_MSG_BODY]u8     = undefined,
    msg_len:  usize                = 0,   // length of last fully received message

    // --- Send queue (circular buffer) ---
    send_slots: [SEND_QUEUE_DEPTH]SendSlot = undefined,
    send_head:  usize = 0, // next slot to send
    send_tail:  usize = 0, // next free slot
    send_count: usize = 0, // number of slots in queue
    send_off:   usize = 0, // bytes already sent from the current head slot
    send_busy:  bool  = false,
};

/// A fully received message from a peer.
/// `data` is valid until the next on_recv call for the same peer.
pub const ReceivedMsg = struct {
    from_node: NodeId,
    data:      []const u8,
};

pub const PeerPool = struct {
    io:      *RealIO,
    peers:   [MAX_PEERS]Peer,
    n_peers: usize,

    pub fn init(io: *RealIO) PeerPool {
        return .{
            .io      = io,
            .peers   = [_]Peer{.{}} ** MAX_PEERS,
            .n_peers = 0,
        };
    }

    /// Register a peer node. Call before connect_all().
    pub fn add_peer(self: *PeerPool, node_id: NodeId, addr: posix.sockaddr.in) void {
        std.debug.assert(self.n_peers < MAX_PEERS);
        self.peers[self.n_peers] = .{
            .node_id = node_id,
            .addr    = addr,
            .in_use  = true,
        };
        self.n_peers += 1;
    }

    /// True if fd belongs to any registered peer.
    pub fn is_peer_fd(self: *const PeerPool, fd: i32) bool {
        for (self.peers[0..self.n_peers]) |*p| {
            if (p.in_use and p.fd == fd) return true;
        }
        return false;
    }

    /// Initiate async connect to all currently-disconnected peers.
    pub fn connect_all(self: *PeerPool) !void {
        for (self.peers[0..self.n_peers]) |*p| {
            if (p.in_use and p.state == .disconnected) {
                try self.start_connect(p);
            }
        }
    }

    /// Queue a VSR control message (≤ MAX_MSG_BODY bytes) for delivery to node_id.
    /// Returns error.PeerNotConnected if the peer is not connected.
    /// Returns error.SendQueueFull if the peer's outbound queue is full.
    pub fn send(self: *PeerPool, node_id: NodeId, msg: []const u8) !void {
        std.debug.assert(msg.len <= MAX_MSG_BODY);
        const peer = self.peer_by_id(node_id) orelse return error.PeerNotConnected;
        if (peer.state != .connected) return error.PeerNotConnected;
        if (peer.send_count >= SEND_QUEUE_DEPTH) return error.SendQueueFull;

        const slot = &peer.send_slots[peer.send_tail];
        std.mem.writeInt(u32, slot.buf[0..4], @intCast(msg.len), .little);
        @memcpy(slot.buf[4..4 + msg.len], msg);
        slot.len = 4 + msg.len;

        peer.send_tail  = (peer.send_tail + 1) % SEND_QUEUE_DEPTH;
        peer.send_count += 1;

        if (!peer.send_busy) {
            try self.flush_send(peer);
        }
    }

    /// Broadcast msg to all currently-connected peers. Silently drops on error.
    pub fn send_to_all(self: *PeerPool, msg: []const u8) void {
        for (self.peers[0..self.n_peers]) |*p| {
            if (p.in_use and p.state == .connected) {
                self.send(p.node_id, msg) catch {};
            }
        }
    }

    /// Dispatch a completed CQE to the appropriate peer handler.
    /// Returns a ReceivedMsg if a complete message was assembled, null otherwise.
    pub fn on_cqe(
        self:   *PeerPool,
        tag:    OpTag,
        fd:     i32,
        result: i32,
    ) !?ReceivedMsg {
        return switch (tag) {
            .connect => { try self.on_connect(fd, result); return null; },
            .recv    => self.on_recv(fd, result),
            .send    => { try self.on_send(fd, result); return null; },
            else     => null,
        };
    }

    // ---- Internal ----

    fn peer_by_id(self: *PeerPool, node_id: NodeId) ?*Peer {
        for (self.peers[0..self.n_peers]) |*p| {
            if (p.in_use and p.node_id == node_id) return p;
        }
        return null;
    }

    fn peer_by_fd(self: *PeerPool, fd: i32) ?*Peer {
        for (self.peers[0..self.n_peers]) |*p| {
            if (p.in_use and p.fd == fd) return p;
        }
        return null;
    }

    fn start_connect(self: *PeerPool, peer: *Peer) !void {
        const sock = try net_io.socket(posix.AF.INET, posix.SOCK.STREAM | posix.SOCK.NONBLOCK, 0);
        peer.fd    = sock;
        peer.state = .connecting;
        try self.io.queue_connect(
            encode(.connect, sock),
            sock,
            @ptrCast(&peer.addr),
            @sizeOf(posix.sockaddr.in),
        );
    }

    fn on_connect(self: *PeerPool, fd: i32, result: i32) !void {
        const peer = self.peer_by_fd(fd) orelse return;
        if (result < 0) {
            // Connection refused or timed out — mark disconnected for retry.
            net_io.close(fd);
            peer.fd    = -1;
            peer.state = .disconnected;
            std.log.warn("peer {d}: connect failed ({})", .{ peer.node_id, result });
            return;
        }
        peer.state   = .connected;
        peer.recv_off = 0;
        peer.send_busy  = false;
        peer.send_head  = 0;
        peer.send_tail  = 0;
        peer.send_count = 0;
        std.log.info("peer {d}: connected", .{peer.node_id});
        // Start listening for incoming VSR messages.
        try self.queue_recv(peer);
    }

    fn on_recv(self: *PeerPool, fd: i32, result: i32) !?ReceivedMsg {
        const peer = self.peer_by_fd(fd) orelse return null;

        if (result <= 0) {
            self.disconnect(peer);
            return null;
        }

        peer.recv_off += @intCast(result);

        // Need at least 4 bytes for the length prefix.
        if (peer.recv_off < 4) {
            try self.queue_recv(peer);
            return null;
        }

        const msg_len = std.mem.readInt(u32, peer.recv_buf[0..4], .little);

        if (msg_len > MAX_MSG_BODY) {
            std.log.err("peer {d}: oversized message ({d} > {d}), disconnecting",
                .{ peer.node_id, msg_len, MAX_MSG_BODY });
            self.disconnect(peer);
            return null;
        }

        // Need 4 + msg_len bytes total.
        if (peer.recv_off < 4 + msg_len) {
            try self.queue_recv(peer);
            return null;
        }

        // Complete message: copy to msg_buf before shifting recv_buf.
        @memcpy(peer.msg_buf[0..msg_len], peer.recv_buf[4..4 + msg_len]);
        peer.msg_len = msg_len;

        // Shift any leftover bytes (start of next message) to front.
        const consumed  = 4 + msg_len;
        const remaining = peer.recv_off - consumed;
        if (remaining > 0) {
            std.mem.copyForwards(u8, peer.recv_buf[0..remaining],
                                 peer.recv_buf[consumed..consumed + remaining]);
        }
        peer.recv_off = remaining;

        // Queue recv for the next message (or continuation of remaining bytes).
        try self.queue_recv(peer);

        return ReceivedMsg{
            .from_node = peer.node_id,
            .data      = peer.msg_buf[0..msg_len],
        };
    }

    fn on_send(self: *PeerPool, fd: i32, result: i32) !void {
        const peer = self.peer_by_fd(fd) orelse return;

        if (result <= 0) {
            self.disconnect(peer);
            return;
        }

        peer.send_off += @intCast(result);
        const slot = &peer.send_slots[peer.send_head];

        if (peer.send_off < slot.len) {
            // Partial send: queue remainder of the same slot.
            try self.io.queue_send(
                encode(.send, peer.fd),
                peer.fd,
                slot.buf[peer.send_off..slot.len],
            );
            return;
        }

        // Slot fully sent: dequeue.
        peer.send_head  = (peer.send_head + 1) % SEND_QUEUE_DEPTH;
        peer.send_count -= 1;
        peer.send_off   = 0;

        if (peer.send_count > 0) {
            try self.flush_send(peer);
        } else {
            peer.send_busy = false;
        }
    }

    fn flush_send(self: *PeerPool, peer: *Peer) !void {
        peer.send_busy = true;
        peer.send_off  = 0;
        const slot = &peer.send_slots[peer.send_head];
        try self.io.queue_send(encode(.send, peer.fd), peer.fd, slot.buf[0..slot.len]);
    }

    fn queue_recv(self: *PeerPool, peer: *Peer) !void {
        try self.io.queue_recv(
            encode(.recv, peer.fd),
            peer.fd,
            peer.recv_buf[peer.recv_off..],
        );
    }

    fn disconnect(self: *PeerPool, peer: *Peer) void {
        _ = self;
        std.log.warn("peer {d}: disconnected", .{peer.node_id});
        net_io.close(peer.fd);
        peer.fd        = -1;
        peer.state     = .disconnected;
        peer.recv_off  = 0;
        peer.send_busy = false;
        peer.send_head = 0;
        peer.send_tail = 0;
        peer.send_count = 0;
    }
};

// ---- Message framing helpers (used by tests and server) ----

/// Encode a message into a length-prefixed frame. `out` must be ≥ 4 + msg.len.
pub fn encode_frame(out: []u8, msg: []const u8) void {
    std.debug.assert(out.len >= 4 + msg.len);
    std.mem.writeInt(u32, out[0..4], @intCast(msg.len), .little);
    @memcpy(out[4..4 + msg.len], msg);
}

/// Decode a length-prefixed frame from `buf`. Returns the message slice, or null
/// if `buf` does not yet contain a complete frame.
pub fn decode_frame(buf: []const u8) ?[]const u8 {
    if (buf.len < 4) return null;
    const msg_len = std.mem.readInt(u32, buf[0..4], .little);
    if (buf.len < 4 + msg_len) return null;
    return buf[4..4 + msg_len];
}

// ----------------------------------------------------------------------------
// Tests
// ----------------------------------------------------------------------------

test "encode_frame / decode_frame: round-trip" {
    const msg = "hello VSR";
    var frame: [4 + 9]u8 = undefined;
    encode_frame(&frame, msg);

    // Check length prefix.
    try std.testing.expectEqual(@as(u32, 9), std.mem.readInt(u32, frame[0..4], .little));

    // decode_frame: partial buffer → null.
    try std.testing.expect(decode_frame(frame[0..3]) == null);

    // decode_frame: exact buffer → body.
    const body = decode_frame(&frame);
    try std.testing.expect(body != null);
    try std.testing.expectEqualSlices(u8, msg, body.?);
}

test "encode_frame: zero-length message" {
    var frame: [4]u8 = undefined;
    encode_frame(&frame, &.{});
    try std.testing.expectEqual(@as(u32, 0), std.mem.readInt(u32, frame[0..4], .little));
    const body = decode_frame(&frame);
    try std.testing.expect(body != null);
    try std.testing.expectEqual(@as(usize, 0), body.?.len);
}

test "peer_pool: init and add_peer" {
    var io = try RealIO.init(64);
    defer io.deinit();

    var pool = PeerPool.init(&io);

    const addr = posix.sockaddr.in{
        .family = posix.AF.INET,
        .port   = std.mem.nativeToBig(u16, 8001),
        .addr   = 0x0100007f, // 127.0.0.1 big-endian
        .zero   = [_]u8{0} ** 8,
    };
    pool.add_peer(1, addr);
    pool.add_peer(2, addr);

    try std.testing.expectEqual(@as(usize, 2), pool.n_peers);
    try std.testing.expectEqual(@as(NodeId, 1), pool.peers[0].node_id);
    try std.testing.expectEqual(@as(NodeId, 2), pool.peers[1].node_id);
}

test "peer_pool: is_peer_fd false for unknown fd" {
    var io = try RealIO.init(64);
    defer io.deinit();
    var pool = PeerPool.init(&io);
    try std.testing.expect(!pool.is_peer_fd(42));
}

test "peer_pool: send returns error when not connected" {
    var io = try RealIO.init(64);
    defer io.deinit();

    var pool = PeerPool.init(&io);
    const addr = posix.sockaddr.in{
        .family = posix.AF.INET,
        .port   = std.mem.nativeToBig(u16, 8001),
        .addr   = 0x0100007f,
        .zero   = [_]u8{0} ** 8,
    };
    pool.add_peer(1, addr);

    const err = pool.send(1, "test");
    try std.testing.expectError(error.PeerNotConnected, err);
}

test "peer_pool: send enqueues framed message in send slot" {
    // Verify that pool.send() correctly encodes the frame into the circular send queue
    // without submitting to io_uring (avoids blocking on CQE in test environments).
    var io = try RealIO.init(64);
    defer io.deinit();

    var pool = PeerPool.init(&io);
    pool.add_peer(1, std.mem.zeroes(posix.sockaddr.in));
    pool.peers[0].fd         = 99; // synthetic fd; no real socket needed
    pool.peers[0].state      = .connected;
    pool.peers[0].send_busy  = false;
    pool.peers[0].send_head  = 0;
    pool.peers[0].send_tail  = 0;
    pool.peers[0].send_count = 0;

    const payload = "ping msg";
    // pool.send() will call flush_send() → io.queue_send(), which only adds to the SQ
    // ring without blocking. We never call io.submit(), so no CQE is expected.
    try pool.send(1, payload);

    // The frame must be in send_slots[0].
    const slot = &pool.peers[0].send_slots[0];
    try std.testing.expectEqual(@as(usize, 4 + payload.len), slot.len);
    // Verify length prefix.
    const len_in_frame = std.mem.readInt(u32, slot.buf[0..4], .little);
    try std.testing.expectEqual(@as(u32, payload.len), len_in_frame);
    // Verify payload bytes.
    try std.testing.expectEqualSlices(u8, payload, slot.buf[4..4 + payload.len]);

    // send_busy must be true (flush_send set it), send_count must be 1.
    try std.testing.expect(pool.peers[0].send_busy);
    try std.testing.expectEqual(@as(usize, 1), pool.peers[0].send_count);
}

test "peer_pool: send queue full returns error" {
    var io = try RealIO.init(64);
    defer io.deinit();

    var pool = PeerPool.init(&io);
    pool.add_peer(1, std.mem.zeroes(posix.sockaddr.in));
    pool.peers[0].fd         = 99;
    pool.peers[0].state      = .connected;
    pool.peers[0].send_busy  = true;  // simulate in-flight send
    pool.peers[0].send_head  = 0;
    pool.peers[0].send_tail  = 0;
    pool.peers[0].send_count = SEND_QUEUE_DEPTH; // queue full

    const err = pool.send(1, "overflow");
    try std.testing.expectError(error.SendQueueFull, err);
}
