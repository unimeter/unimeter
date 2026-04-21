//! billing — usage metering server.
//!
//! Usage:
//!   billing [options]
//!
//! Options:
//!   --node-id=N        This node's ID (0-based, default 0)
//!   --peers=LIST       Comma-separated peer replication addresses: id:ip:port
//!                      Example: --peers=1:127.0.0.1:8002,2:127.0.0.1:8003
//!   --port=N           Ingest TCP port (default 7001)
//!   --http-port=N      HTTP port for /metrics, /health, /v1/events (default 9090)
//!   --data-dir=PATH    Data directory (default "data")

const std    = @import("std");
const server = @import("usagelog/server.zig");

pub fn main(init: std.process.Init) !void {
    const alloc = init.gpa;

    var cfg = server.ServerConfig{};
    cfg.io = init.io;

    // Static peer buffer: supports up to 8 peer nodes.
    var peer_buf: [8]server.PeerEntry = undefined;
    var n_peers: usize = 0;

    // Environment variables (CLI args below take precedence).
    if (init.environ_map.get("METRICS_PORT")) |p| {
        cfg.http_port = try std.fmt.parseInt(u16, p, 10);
    }
    if (init.environ_map.get("MY_ADDR")) |addr| {
        const n = @min(addr.len, server.ADDR_LEN - 1);
        @memcpy(cfg.node_addrs[cfg.node_id][0..n], addr[0..n]);
    }
    if (init.environ_map.get("RETENTION_DAYS")) |d| {
        cfg.retention_days = std.fmt.parseInt(u32, d, 10) catch 90;
    }

    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.next(); // skip argv[0]

    while (args.next()) |arg| {
        if (std.mem.startsWith(u8, arg, "--node-id=")) {
            cfg.node_id = try std.fmt.parseInt(u8, arg["--node-id=".len..], 10);
        } else if (std.mem.startsWith(u8, arg, "--port=")) {
            cfg.port = try std.fmt.parseInt(u16, arg["--port=".len..], 10);
        } else if (std.mem.startsWith(u8, arg, "--http-port=")) {
            cfg.http_port = try std.fmt.parseInt(u16, arg["--http-port=".len..], 10);
        } else if (std.mem.startsWith(u8, arg, "--data-dir=")) {
            cfg.data_dir = arg["--data-dir=".len..];
        } else if (std.mem.startsWith(u8, arg, "--retention-days=")) {
            cfg.retention_days = try std.fmt.parseInt(u32, arg["--retention-days=".len..], 10);
        } else if (std.mem.startsWith(u8, arg, "--peers=")) {
            const list = arg["--peers=".len..];
            var it = std.mem.splitScalar(u8, list, ',');
            while (it.next()) |peer_str| {
                // Format: node_id:ip:repl_port  e.g.  1:127.0.0.1:8002
                var parts = std.mem.splitScalar(u8, peer_str, ':');
                const id_str   = parts.next() orelse return error.BadPeerArg;
                const ip_str   = parts.next() orelse return error.BadPeerArg;
                const port_str = parts.next() orelse return error.BadPeerArg;

                if (n_peers >= peer_buf.len) return error.TooManyPeers;
                peer_buf[n_peers] = .{
                    .node_id   = try std.fmt.parseInt(u8, id_str, 10),
                    .repl_port = try std.fmt.parseInt(u16, port_str, 10),
                    .addr_be   = try parse_ipv4_be(ip_str),
                };
                n_peers += 1;
            }
        } else {
            std.log.warn("unknown argument: {s}", .{arg});
        }
    }

    cfg.peers = peer_buf[0..n_peers];

    std.log.info("starting node {d} on port {d}, http_port={d}, data_dir={s}, peers={d}",
        .{ cfg.node_id, cfg.port, cfg.http_port, cfg.data_dir, n_peers });

    try server.run(alloc, cfg);
}

/// Parse a dotted-decimal IPv4 string ("a.b.c.d") into big-endian u32.
fn parse_ipv4_be(ip: []const u8) !u32 {
    var it = std.mem.splitScalar(u8, ip, '.');
    var result: u32 = 0;
    for (0..4) |_| {
        const octet_str = it.next() orelse return error.BadIPv4;
        const octet     = try std.fmt.parseInt(u8, octet_str, 10);
        result = (result << 8) | octet;
    }
    return result;
}
