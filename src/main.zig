//! billing — usage metering server.
//!
//! Usage:
//!   billing [options]
//!
//! Options:
//!   --node-id=N        This node's ID (0-based, default 0)
//!   --bind=ADDR        This node's advertised address (host:port or host)
//!                      If port is omitted, --port value is appended.
//!                      Overrides MY_ADDR env var.
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

    // Environment variables — stashed and applied after CLI args so that
    // MY_ADDR lands in the correct node_addrs[node_id] slot.
    const env_metrics_port = init.environ_map.get("METRICS_PORT");
    const env_my_addr      = init.environ_map.get("MY_ADDR");
    const env_retention    = init.environ_map.get("RETENTION_DAYS");

    var bind_addr: ?[]const u8 = null;

    var args = std.process.Args.Iterator.init(init.minimal.args);
    _ = args.next(); // skip argv[0]

    while (args.next()) |arg| {
        if (std.mem.startsWith(u8, arg, "--node-id=")) {
            cfg.node_id = try std.fmt.parseInt(u8, arg["--node-id=".len..], 10);
        } else if (std.mem.startsWith(u8, arg, "--port=")) {
            cfg.port = try std.fmt.parseInt(u16, arg["--port=".len..], 10);
        } else if (std.mem.startsWith(u8, arg, "--http-port=")) {
            cfg.http_port = try std.fmt.parseInt(u16, arg["--http-port=".len..], 10);
        } else if (std.mem.startsWith(u8, arg, "--bind=")) {
            bind_addr = arg["--bind=".len..];
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

    // Apply env vars after CLI args so node_id is known.
    if (env_metrics_port) |p| {
        cfg.http_port = try std.fmt.parseInt(u16, p, 10);
    }
    // --bind takes precedence over MY_ADDR env.
    const my_addr = bind_addr orelse env_my_addr;
    if (my_addr) |addr| {
        writeNodeAddr(&cfg, cfg.node_id, addr);
    }
    if (env_retention) |d| {
        cfg.retention_days = std.fmt.parseInt(u32, d, 10) catch 90;
    }

    std.log.info("starting node {d} on port {d}, http_port={d}, data_dir={s}, peers={d}",
        .{ cfg.node_id, cfg.port, cfg.http_port, cfg.data_dir, n_peers });

    try server.run(alloc, cfg);
}

/// Write addr into cfg.node_addrs[id]. If addr has no port (no ':'),
/// the configured ingest port is appended automatically.
fn writeNodeAddr(cfg: *server.ServerConfig, id: u8, addr: []const u8) void {
    @memset(&cfg.node_addrs[id], 0);
    if (std.mem.indexOfScalar(u8, addr, ':') != null) {
        const n = @min(addr.len, server.ADDR_LEN - 1);
        @memcpy(cfg.node_addrs[id][0..n], addr[0..n]);
    } else {
        _ = std.fmt.bufPrint(&cfg.node_addrs[id],
            "{s}:{d}", .{ addr, cfg.port }) catch {};
    }
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
