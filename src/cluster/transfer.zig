//! State transfer for partition rebalancing.
//!
//! When partitions move between nodes, this module handles scanning local
//! segment files for events belonging to a specific partition, and streaming
//! them to a destination node via a direct TCP connection using the ingest
//! protocol.
//!
//! The transfer is synchronous and blocking — it runs in the rebalance handler
//! after the event loop has paused accepting new ingest traffic for the
//! affected partitions.

const std     = @import("std");
const disk_io = @import("../io/disk_io.zig");
const net_io  = @import("../io/net.zig");
const Event   = @import("../event.zig").Event;
const proto   = @import("../usagelog/protocol.zig");

const cluster_mod = @import("partition_map.zig");
const N_PARTITIONS = cluster_mod.N_PARTITIONS;
const NodeId       = cluster_mod.NodeId;
const PartitionMap = cluster_mod.PartitionMap;

const log = std.log.scoped(.transfer);

/// A partition move: from old_leader to new_leader.
pub const PartitionMove = struct {
    partition_id: u16,
    old_leader:   NodeId,
    new_leader:   NodeId,
};

pub const MoveSet = struct {
    moves: [N_PARTITIONS]PartitionMove,
    count: u32,
};

/// Compute which partitions change leader when going from old_map to new_map.
/// Returns a static buffer of moves (max 256).
pub fn compute_moves(
    old_map: *const PartitionMap,
    new_map: *const PartitionMap,
) MoveSet {
    var result = MoveSet{
        .moves = undefined,
        .count = 0,
    };
    for (0..N_PARTITIONS) |p| {
        const old_leader = old_map.entries[p].leader;
        const new_leader = new_map.entries[p].leader;
        if (old_leader != new_leader) {
            result.moves[result.count] = .{
                .partition_id = @intCast(p),
                .old_leader   = old_leader,
                .new_leader   = new_leader,
            };
            result.count += 1;
        }
    }
    return result;
}

/// Scan all segment files in data_dir and count events belonging to a partition.
pub fn count_partition_events(
    data_dir: []const u8,
    partition_id: u16,
) u64 {
    var total: u64 = 0;
    const dir_fd = disk_io.open_dir(data_dir) catch return 0;
    defer disk_io.close_dir(dir_fd);

    var it = disk_io.DirIter.init(dir_fd);
    while (it.next() catch null) |name| {
        if (name.len < 4) continue;
        if (!std.mem.eql(u8, name[name.len - 4 ..], ".seg")) continue;

        var path_buf: [512]u8 = undefined;
        const path = std.fmt.bufPrint(&path_buf, "{s}/{s}", .{ data_dir, name }) catch continue;
        const f = disk_io.open_ro(path) catch continue;
        defer f.close();

        const sz = f.size() catch continue;
        const n_events = sz / @sizeOf(Event);

        var i: u64 = 0;
        while (i < n_events) : (i += 1) {
            var evt: Event = undefined;
            const n = f.read(std.mem.asBytes(&evt)) catch break;
            if (n != @sizeOf(Event)) break;
            if (evt.account_id % N_PARTITIONS == partition_id) {
                total += 1;
            }
        }
    }
    return total;
}

/// Transfer events for a partition from local segments to a remote node.
/// Connects to dest_addr (host:port string), sends events via the ingest
/// protocol in batches.
///
/// Returns number of events transferred.
pub fn transfer_partition(
    data_dir: []const u8,
    partition_id: u16,
    dest_addr: []const u8,
) !u64 {
    // Parse destination address.
    var addr_parts = std.mem.splitScalar(u8, dest_addr, ':');
    const host = addr_parts.next() orelse return error.BadAddress;
    const port_str = addr_parts.next() orelse return error.BadAddress;
    const port = try std.fmt.parseInt(u16, port_str, 10);

    // Resolve host to IPv4.
    const ip_be = try parse_ipv4_be(host);

    // Connect to destination node.
    const sock = try net_io.socket(std.posix.AF.INET, std.posix.SOCK.STREAM, 0);
    errdefer net_io.close(sock);

    const addr = std.posix.sockaddr.in{
        .family = std.posix.AF.INET,
        .port   = std.mem.nativeToBig(u16, port),
        .addr   = ip_be,
        .zero   = [_]u8{0} ** 8,
    };
    try net_io.connect(sock, @ptrCast(&addr), @sizeOf(@TypeOf(addr)));

    // Scan segments and send events in batches.
    const BATCH_SIZE = 256;
    var batch: [BATCH_SIZE]Event = undefined;
    var batch_count: u32 = 0;
    var total_sent: u64 = 0;

    const dir_fd = try disk_io.open_dir(data_dir);
    defer disk_io.close_dir(dir_fd);

    var it = disk_io.DirIter.init(dir_fd);
    while (it.next() catch null) |name| {
        if (name.len < 4) continue;
        if (!std.mem.eql(u8, name[name.len - 4 ..], ".seg")) continue;

        var path_buf: [512]u8 = undefined;
        const path = std.fmt.bufPrint(&path_buf, "{s}/{s}", .{ data_dir, name }) catch continue;
        const f = disk_io.open_ro(path) catch continue;
        defer f.close();

        const sz = f.size() catch continue;
        const n_events = sz / @sizeOf(Event);

        var i: u64 = 0;
        while (i < n_events) : (i += 1) {
            var evt: Event = undefined;
            const n = f.read(std.mem.asBytes(&evt)) catch break;
            if (n != @sizeOf(Event)) break;

            if (evt.account_id % N_PARTITIONS == partition_id) {
                batch[batch_count] = evt;
                batch_count += 1;

                if (batch_count >= BATCH_SIZE) {
                    try send_event_batch(sock, batch[0..batch_count], partition_id);
                    total_sent += batch_count;
                    batch_count = 0;
                }
            }
        }
    }

    // Flush remaining batch.
    if (batch_count > 0) {
        try send_event_batch(sock, batch[0..batch_count], partition_id);
        total_sent += batch_count;
    }

    net_io.close(sock);
    log.info("transfer: partition {d} → {s}: {d} events", .{ partition_id, dest_addr, total_sent });
    return total_sent;
}

/// Send a batch of raw Events to the destination via the ingest protocol.
fn send_event_batch(sock: i32, events: []const Event, partition_id: u16) !void {
    // Build wire events from Events.
    const event_count: u32 = @intCast(events.len);
    const wire_event_size = @sizeOf(proto.WireEvent);
    const payload_len: u32 = @intCast(@sizeOf(proto.IngestPayloadHeader) +
        event_count * wire_event_size);

    // Send request header.
    const hdr = proto.RequestHeader{
        .magic       = proto.MAGIC,
        .version     = proto.VERSION,
        .packet_type = @intFromEnum(proto.PacketType.ingest_sync),
        .partition   = partition_id,
        .payload_len = payload_len,
        .request_id  = 0,
    };
    try send_all(sock, std.mem.asBytes(&hdr));

    // Send ingest payload header.
    const iph = proto.IngestPayloadHeader{
        .event_count = event_count,
        .props_count = 0,
    };
    try send_all(sock, std.mem.asBytes(&iph));

    // Send wire events.
    for (events) |*evt| {
        var we: proto.WireEvent = std.mem.zeroes(proto.WireEvent);
        // metric_code is already an FNV hash in the Event; we need the raw code.
        // For transfer, we set it as the hash bytes since the destination will
        // re-use it directly.
        std.mem.writeInt(u64, we.metric_code_str[0..8], evt.metric_code, .little);
        we.account_id     = evt.account_id;
        we.timestamp      = evt.timestamp;
        we.value          = evt.value;
        we.operation_type = evt.operation_type;
        we.props_count    = 0;
        try send_all(sock, std.mem.asBytes(&we));
    }

    // Read response header (12B).
    var resp_buf: [12]u8 = undefined;
    try recv_all(sock, &resp_buf);
    // Ignore response body if any (read payload_len bytes).
    const resp_hdr = std.mem.bytesAsValue(proto.ResponseHeader, &resp_buf);
    if (resp_hdr.payload_len > 0) {
        var discard: [256]u8 = undefined;
        var remaining = resp_hdr.payload_len;
        while (remaining > 0) {
            const to_read = @min(remaining, discard.len);
            const n = net_io.recv(sock, discard[0..to_read], 0) catch break;
            if (n == 0) break;
            remaining -= @intCast(n);
        }
    }
}

fn send_all(sock: i32, data: []const u8) !void {
    var sent: usize = 0;
    while (sent < data.len) {
        const n = try net_io.send(sock, data[sent..], 0);
        sent += n;
    }
}

fn recv_all(sock: i32, buf: []u8) !void {
    var got: usize = 0;
    while (got < buf.len) {
        const n = try net_io.recv(sock, buf[got..], 0);
        if (n == 0) return error.ConnectionClosed;
        got += n;
    }
}

fn parse_ipv4_be(ip: []const u8) !u32 {
    var it = std.mem.splitScalar(u8, ip, '.');
    var result: u32 = 0;
    for (0..4) |_| {
        const octet_str = it.next() orelse return error.BadIPv4;
        const octet = try std.fmt.parseInt(u8, octet_str, 10);
        result = (result << 8) | octet;
    }
    return result;
}

// ---- Tests ----

test "compute_moves: no changes" {
    const map = PartitionMap.init_uniform(3);
    const result = compute_moves(&map, &map);
    try std.testing.expectEqual(@as(u32, 0), result.count);
}

test "compute_moves: scale from 2 to 3 nodes" {
    const old = PartitionMap.init_uniform(2);
    const new = PartitionMap.init_uniform(3);
    const result = compute_moves(&old, &new);
    // Many partitions should move. At minimum partition 2 moves: was node 0, now node 2.
    try std.testing.expect(result.count > 0);
    // Verify a known move: partition 2 (2%2=0 → 2%3=2).
    var found: bool = false;
    for (result.moves[0..result.count]) |m| {
        if (m.partition_id == 2) {
            try std.testing.expectEqual(@as(NodeId, 0), m.old_leader);
            try std.testing.expectEqual(@as(NodeId, 2), m.new_leader);
            found = true;
        }
    }
    try std.testing.expect(found);
}
