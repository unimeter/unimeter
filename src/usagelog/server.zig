//! Ingest TCP server. All network I/O goes through io_uring.
//!
//! Per-connection state machine:
//!   reading_header
//!     → reading_body            (accumulate payload_len bytes)
//!       → writing_wal           (disk write queued — ingest only)
//!         → fsyncing_wal  (SYNC)
//!         → writing_seg  (ASYNC)
//!       → writing_seg
//!         → fsyncing_seg (SYNC)
//!         → sending_resp (ASYNC)
//!       → sending_resp
//!   Non-ingest packets dispatch directly after body is received.

const std   = @import("std");
const posix = std.posix;
const net_io = @import("../io/net.zig");
const linux = std.os.linux;

const time_util = @import("../util/time.zig");

const io_mod     = @import("../io/real_io.zig");
const RealIO     = io_mod.RealIO;
const OpTag      = io_mod.OpTag;
const encode     = io_mod.encode;
const decode_tag = io_mod.decode_tag;
const decode_fd  = io_mod.decode_fd;

const proto          = @import("protocol.zig");
const RequestHeader  = proto.RequestHeader;
const WireEvent      = proto.WireEvent;

const metric_registry = @import("metric_registry.zig");
const MetricRegistry  = metric_registry.MetricRegistry;
const MetricSchema    = metric_registry.MetricSchema;
const DimensionFilter = metric_registry.DimensionFilter;

const metrics = @import("../metrics/metrics.zig");
const handler = @import("../http/handler.zig");
const HttpConn      = handler.HttpConn;
const HttpConnState = handler.HttpConnState;

const Event      = @import("../event.zig").Event;
const UsageLog   = @import("usagelog.zig").UsageLog;
const PreparedBatch = @import("usagelog.zig").PreparedBatch;

const cluster_mod  = @import("../cluster/partition_map.zig");
const transfer_mod = @import("../cluster/transfer.zig");
const PartitionMap = cluster_mod.PartitionMap;
const NodeId       = cluster_mod.NodeId;

const peer_pool_mod = @import("../cluster/peer_pool.zig");
const PeerPool         = peer_pool_mod.PeerPool;
const encode_peer      = peer_pool_mod.encode_frame;
const REPL_PORT_OFFSET = peer_pool_mod.REPL_PORT_OFFSET;
const MAX_MSG_BODY     = peer_pool_mod.MAX_MSG_BODY;

const vsr_mod      = @import("../cluster/vsr.zig");
const VsrPartition = vsr_mod.VsrPartition;
const VsrConfig    = vsr_mod.VsrConfig;
const Role         = vsr_mod.Role;
const MsgType      = vsr_mod.MsgType;
const PrepareMsg         = vsr_mod.PrepareMsg;
const PrepareOkMsg       = vsr_mod.PrepareOkMsg;
const CommitMsg          = vsr_mod.CommitMsg;
const PingMsg            = vsr_mod.PingMsg;
const StartViewChangeMsg = vsr_mod.StartViewChangeMsg;
const DoViewChangeMsg    = vsr_mod.DoViewChangeMsg;
const StartViewMsg       = vsr_mod.StartViewMsg;
const MsgHeader          = vsr_mod.MsgHeader;
const VSR_MAGIC          = vsr_mod.VSR_MAGIC;

const startup_check = @import("startup_check.zig");
const checkpoint   = @import("../aggstore/checkpoint.zig");
const query_mod    = @import("../aggstore/query.zig");
const alert_mod    = @import("../aggstore/alert.zig");
const AlertEntry   = alert_mod.AlertEntry;
const worker_mod   = @import("../aggstore/worker.zig");
const AggWorker    = worker_mod.AggWorker;
const memtable_mod = @import("../aggstore/memtable.zig");
const Memtable     = memtable_mod.Memtable;
const AggKey       = memtable_mod.AggKey;
const AggValue     = memtable_mod.AggValue;
const unique_mod   = @import("../aggstore/unique_sets.zig");
const UniqueSets   = unique_mod.UniqueSets;
const aggregators  = @import("../aggstore/aggregators.zig");

const N_PARTITIONS: u32 = cluster_mod.N_PARTITIONS;

pub const MAX_EVENTS_PER_BATCH: usize = 1024;
pub const MAX_PROPS_PER_BATCH:  usize = 1024;

// Receive buffer must fit: IngestPayloadHeader + events + props.
pub const MAX_BODY_LEN: usize =
    @sizeOf(proto.IngestPayloadHeader) +
    MAX_EVENTS_PER_BATCH * @sizeOf(WireEvent) +
    MAX_PROPS_PER_BATCH  * proto.PROP_PAIR_SIZE;

// WAL write buffer: EntryHeader(16B) + Event payload (up to 1024 × 64B).
const WAL_BUF_LEN: usize = @sizeOf(@import("wal.zig").EntryHeader) +
                            MAX_EVENTS_PER_BATCH * @sizeOf(Event);

// Response header size (new 12-byte format).
pub const RESP_HDR_SIZE: usize = 12;

pub const MAX_CONNS:      usize = 256;
pub const QUEUE_DEPTH:    u16   = 512;
pub const PORT:           u16   = 7001;
const MAX_HTTP_CONNS:     usize = 64;

const ConnState = enum {
    reading_header,
    reading_body,
    writing_wal,
    fsyncing_wal,
    writing_seg,
    fsyncing_seg,
    sending_resp,
};

/// Per-connection state.
/// body[] is dual-purpose: receive buffer for requests, then send buffer for responses.
const Conn = struct {
    fd:           i32              = -1,
    in_use:       bool             = false,
    state:        ConnState        = .reading_header,
    bytes_got:    usize            = 0,
    packet_type:  proto.PacketType = .ingest_async,
    request_id:   u32              = 0,
    sync_mode:    bool             = false,
    partition_id: u16              = 0,
    /// Client subscribed to live alert push (see ALERT_PUSH_ENABLE).
    /// One bit of state per connection — no subscription tables kept server-side.
    /// Reset implicitly when the connection is closed (whole Conn reinitialised
    /// on next alloc), so no explicit cleanup is needed.
    wants_alerts: bool             = false,

    // Receive buffer for request payload; reused as response send buffer after parsing.
    body: [MAX_BODY_LEN]u8 align(@alignOf(WireEvent)) = undefined,

    // WAL write buffer: [EntryHeader][Event × n]. Filled by log.prepare().
    wal_buf: [WAL_BUF_LEN]u8 = undefined,

    // Assembled Events. Filled by log.prepare().
    events_buf: [MAX_EVENTS_PER_BATCH]Event = undefined,

    // Props extracted from ingest payload, indexed by wire_event position.
    // wire_props_offsets[i] = starting index in props_buf for wire_event[i].
    // wire_props_counts[i]  = number of props for wire_event[i].
    props_buf:           [MAX_PROPS_PER_BATCH]proto.PropPair = undefined,
    wire_props_offsets:  [MAX_EVENTS_PER_BATCH]u16           = undefined,
    wire_props_counts:   [MAX_EVENTS_PER_BATCH]u8            = undefined,

    // Mapping from events_buf[i] → wire_event index (set by prepare).
    wire_indices:        [MAX_EVENTS_PER_BATCH]u16           = undefined,

    hdr:   RequestHeader = undefined,
    batch: PreparedBatch = undefined,

    // Response tracking (body[0..resp_total_len] is sent as the response).
    resp_total_len: usize = 0,
    resp_sent:      usize = 0,

    start_ns:       i64 = 0,
    fsync_start_ns: i64 = 0,
};

// Static pool: allocated once at process start, never freed.
var pool: [MAX_CONNS]Conn = [_]Conn{.{}} ** MAX_CONNS;

// ---- HTTP connection pool ----

var http_pool: [MAX_HTTP_CONNS]HttpConn = [_]HttpConn{.{}} ** MAX_HTTP_CONNS;
/// Listening fd for the HTTP server (serves /metrics, /health, /v1/events).
var g_http_listen_fd: posix.fd_t = -1;
/// Server start time (nanoseconds) for /health uptime calculation.
var g_start_ns: i64 = 0;

fn http_conn_alloc(fd: i32) ?*HttpConn {
    for (&http_pool) |*c| {
        if (!c.in_use) { c.* = .{ .fd = fd, .in_use = true }; return c; }
    }
    return null;
}

fn http_conn_get(fd: i32) ?*HttpConn {
    for (&http_pool) |*c| { if (c.in_use and c.fd == fd) return c; }
    return null;
}

fn http_conn_free(fd: i32) void {
    for (&http_pool) |*c| {
        if (c.in_use and c.fd == fd) { c.in_use = false; return; }
    }
}

fn http_conn_close(fd: i32) void {
    metrics.http_connections_active.dec();
    http_conn_free(fd);
    net_io.close(fd);
}

fn is_http_conn_fd(fd: i32) bool {
    return http_conn_get(fd) != null;
}

// ---- Inbound replication connections ----

const MAX_REPL_CONNS: usize = 8;
const REPL_BUF_LEN: usize = 4 + MAX_MSG_BODY;

const ReplConnState = enum { reading_hdr, reading_body };

const ReplConn = struct {
    fd:        i32           = -1,
    in_use:    bool          = false,
    state:     ReplConnState = .reading_hdr,
    bytes_got: usize         = 0,
    frame_len: u32           = 0,
    buf:       [REPL_BUF_LEN]u8 = undefined,
};

var repl_pool: [MAX_REPL_CONNS]ReplConn = [_]ReplConn{.{}} ** MAX_REPL_CONNS;
var g_repl_listen_fd: posix.fd_t = -1;

fn alloc_repl_conn(fd: i32) ?*ReplConn {
    for (&repl_pool) |*c| {
        if (!c.in_use) { c.* = .{ .fd = fd, .in_use = true }; return c; }
    }
    return null;
}

fn get_repl_conn(fd: i32) ?*ReplConn {
    for (&repl_pool) |*c| { if (c.in_use and c.fd == fd) return c; }
    return null;
}

fn free_repl_conn(fd: i32) void {
    for (&repl_pool) |*c| { if (c.in_use and c.fd == fd) { c.in_use = false; return; } }
}

fn is_repl_fd(fd: i32) bool {
    return get_repl_conn(fd) != null;
}

fn conn_alloc(fd: i32) ?*Conn {
    for (&pool) |*c| {
        if (!c.in_use) {
            c.* = .{}; c.fd = fd; c.in_use = true;
            metrics.connections_active.inc();
            return c;
        }
    }
    return null;
}

fn conn_get(fd: i32) ?*Conn {
    for (&pool) |*c| { if (c.in_use and c.fd == fd) return c; }
    return null;
}

fn conn_free(fd: i32) void {
    for (&pool) |*c| {
        if (c.in_use and c.fd == fd) {
            c.in_use = false;
            if (c.wants_alerts) {
                c.wants_alerts = false;
                metrics.alert_subscribers.dec();
            }
            metrics.connections_active.dec();
            return;
        }
    }
}

/// Peer node for outbound replication.
pub const PeerEntry = struct {
    node_id:   NodeId,
    repl_port: u16,
    addr_be:   u32, // IPv4, big-endian
};

pub const ADDR_LEN: usize = proto.ADDR_LEN;

pub const ServerConfig = struct {
    port:           u16                   = PORT,
    http_port:      u16                   = 9090,
    data_dir:       []const u8            = "data",
    node_id:        NodeId                = 0,
    retention_days: u32                   = 90,
    peers:          []const PeerEntry     = &.{},
    /// On-wire "host:port" addresses indexed by NodeId.
    /// Populated from MY_ADDR env var for self, and derived from peer configs.
    node_addrs: [256][ADDR_LEN]u8         = std.mem.zeroes([256][ADDR_LEN]u8),
    /// Io handle (std.Io). Required for file operations (WAL, segment, props, alert_log, checkpoint).
    io:        std.Io                     = undefined,
};

// ---- Graceful shutdown state ----

var g_shutting_down: bool = false;
var g_inflight_count: u32 = 0;
/// signalfd file descriptor (-1 if not set up).
var g_signal_fd: i32 = -1;
/// Buffer for signalfd reads (signalfd_siginfo is 128 bytes).
var g_signal_buf: [128]u8 = undefined;

/// Set up signal handling for SIGTERM/SIGINT via signalfd. Block the signals
/// first (required for signalfd to receive them), then create the fd and queue
/// an io_uring read on it.
fn setup_signal_handler(io: *RealIO) !void {
    // Block SIGTERM and SIGINT so they go to signalfd instead of default handler.
    var mask = std.mem.zeroes(linux.sigset_t);
    linux.sigaddset(&mask, linux.SIG.TERM);
    linux.sigaddset(&mask, linux.SIG.INT);
    const SIG_BLOCK: u32 = 0;
    const rc = linux.sigprocmask(SIG_BLOCK, &mask, null);
    const signed: i64 = @bitCast(@as(u64, rc));
    if (signed < 0) return error.SignalSetupFailed;

    // Create signalfd.
    const sfd_rc = linux.signalfd(-1, &mask, 0);
    const sfd_signed: i64 = @bitCast(@as(u64, sfd_rc));
    if (sfd_signed < 0) return error.SignalSetupFailed;
    g_signal_fd = @intCast(sfd_rc);

    // Queue a read on the signalfd via io_uring.
    try io.queue_recv(encode(.signal, g_signal_fd), g_signal_fd, &g_signal_buf);
}

fn begin_shutdown(_: *RealIO, ingest_fd: i32) void {
    std.log.info("shutdown: signal received, draining in-flight requests...", .{});
    g_shutting_down = true;

    // Close listen fds to stop accepting new connections.
    net_io.close(ingest_fd);
    if (g_repl_listen_fd >= 0) { net_io.close(g_repl_listen_fd); g_repl_listen_fd = -1; }
    if (g_http_listen_fd >= 0) { net_io.close(g_http_listen_fd); g_http_listen_fd = -1; }

    // Close idle client connections (reading_header/reading_body).
    for (&pool) |*c| {
        if (c.in_use and (c.state == .reading_header or c.state == .reading_body)) {
            conn_free(c.fd);
            net_io.close(c.fd);
        }
    }

    // In-flight connections (writing_wal, fsyncing_*, sending_resp) will drain naturally.
    if (g_inflight_count == 0) {
        std.log.info("shutdown: no in-flight requests, exiting immediately", .{});
    } else {
        std.log.info("shutdown: waiting for {d} in-flight requests", .{g_inflight_count});
    }

    // Close signalfd.
    if (g_signal_fd >= 0) { net_io.close(g_signal_fd); g_signal_fd = -1; }
}

// ---- Disk space protection ----

const disk_io = @import("../io/disk_io.zig");

/// Minimum free disk space (256 MB). Below this, ingest returns backpressure.
const MIN_FREE_SPACE: u64 = 256 * 1024 * 1024;
/// How often to re-check free space (5 seconds in nanoseconds).
const SPACE_CHECK_INTERVAL_NS: i64 = 5 * std.time.ns_per_s;

var g_free_space: u64 = std.math.maxInt(u64);
var g_space_check_ns: i64 = 0;

fn check_disk_space() void {
    const now = @as(i64, @truncate(time_util.wallNanos()));
    if (now - g_space_check_ns < SPACE_CHECK_INTERVAL_NS) return;
    g_space_check_ns = now;
    g_free_space = disk_io.free_space(g_data_dir) catch MIN_FREE_SPACE;
}

fn disk_has_space() bool {
    check_disk_space();
    return g_free_space >= MIN_FREE_SPACE;
}

// ---- Cluster-level state (static) ----

var g_partition_map: PartitionMap = PartitionMap.init_uniform(1);
var g_peer_pool: PeerPool = undefined;
var g_peer_pool_init: bool = false;
var g_vsr: [N_PARTITIONS]VsrPartition = undefined;
var g_vsr_init: bool = false;
var g_pending_client: [N_PARTITIONS]i32 = [_]i32{-1} ** N_PARTITIONS;
var g_node_id: NodeId = 0;

// Saved server config for handlers that need node_addrs / data_dir.
var g_cfg: ServerConfig = .{};
// Data directory path (points into g_cfg.data_dir slice).
var g_data_dir: []const u8 = "data";
// Allocator saved from run() for handlers that need it.
var g_alloc: std.mem.Allocator = undefined;

// ---- AggStore in-memory state ----
var g_memtable: Memtable = undefined;
var g_memtable_init: bool = false;
var g_unique_sets: UniqueSets = undefined;
var g_alert_log: alert_mod.AlertLog = undefined;
var g_agg_worker: AggWorker = undefined;
/// Highest event offset that has been applied to aggregates.
/// Used by retention sweep to avoid deleting unprocessed segments.
var g_last_agg_offset: u64 = 0;

fn make_listen_fd(port: u16) !posix.fd_t {
    const fd = try net_io.socket(posix.AF.INET, posix.SOCK.STREAM, 0);
    try posix.setsockopt(fd, posix.SOL.SOCKET, posix.SO.REUSEADDR,
        &std.mem.toBytes(@as(c_int, 1)));
    const addr = posix.sockaddr.in{
        .family = posix.AF.INET,
        .port   = std.mem.nativeToBig(u16, port),
        .addr   = 0,
        .zero   = [_]u8{0} ** 8,
    };
    try net_io.bind(fd, @ptrCast(&addr), @sizeOf(@TypeOf(addr)));
    try net_io.listen(fd, 128);
    return fd;
}

pub fn run(alloc: std.mem.Allocator, cfg: ServerConfig) !void {
    g_start_ns = @truncate(time_util.wallNanos());
    g_cfg      = cfg;
    g_data_dir = cfg.data_dir;
    g_alloc    = alloc;

    var io = try RealIO.init(QUEUE_DEPTH);
    defer io.deinit();

    // Startup validation: check data dir, WAL, checkpoint, segments before accepting traffic.
    const check = startup_check.validate(alloc, cfg.data_dir) catch |err| {
        std.log.err("startup validation failed: {}", .{err});
        return err;
    };
    if (check.segments_bad > 0) {
        std.log.warn("startup: {d} segment files have inconsistent sizes", .{check.segments_bad});
    }

    var log = try UsageLog.init(alloc, .{ .data_dir = cfg.data_dir });
    defer log.deinit();

    // Initialize AggStore in-memory state.
    // Pre-allocate for ~64K aggregate keys: avoids rehash churn during bursts.
    // Size chosen based on expected working set: 1K accounts × ~16 metrics × few filter dims.
    g_memtable = Memtable.init_capacity(alloc, 65536) catch Memtable.init(alloc);
    g_unique_sets = UniqueSets.init(alloc);

    // Build alert_log path: <data_dir>/alert_log.bin
    var alert_path_buf: [256]u8 = undefined;
    const alert_path = std.fmt.bufPrint(&alert_path_buf, "{s}/alert_log.bin", .{cfg.data_dir}) catch "data/alert_log.bin";
    g_alert_log = alert_mod.AlertLog.open(alert_path) catch blk: {
        std.log.warn("failed to open alert_log, alerts disabled", .{});
        break :blk undefined;
    };

    g_agg_worker = AggWorker{
        .alloc          = alloc,
        .memtable       = &g_memtable,
        .unique_sets    = &g_unique_sets,
        .registry       = &log.registry,
        .alert_log      = &g_alert_log,
        .alert_push_fn  = on_alert_crossing,
        .alert_push_ctx = &io,
    };
    g_memtable_init = true;
    g_last_agg_offset = log.offset; // all existing events are aggregated at startup

    g_node_id = cfg.node_id;
    const n_nodes: u8 = if (cfg.peers.len == 0) 1 else @intCast(cfg.peers.len + 1);
    for (0..N_PARTITIONS) |p| {
        const role: Role = if (p % n_nodes == cfg.node_id) .leader else .replica;
        g_vsr[p] = VsrPartition.init(.{
            .node_id      = cfg.node_id,
            .partition_id = @intCast(p),
            .n_nodes      = n_nodes,
        }, role);
    }
    g_vsr_init = true;
    g_partition_map = PartitionMap.init_uniform(n_nodes);

    // Populate node_addrs for peers (derive ingest port from repl_port).
    for (cfg.peers) |p| {
        const ingest_port = p.repl_port - REPL_PORT_OFFSET;
        const a: u8 = @intCast((p.addr_be >> 24) & 0xFF);
        const b: u8 = @intCast((p.addr_be >> 16) & 0xFF);
        const c: u8 = @intCast((p.addr_be >> 8)  & 0xFF);
        const d: u8 = @intCast(p.addr_be & 0xFF);
        @memset(&g_cfg.node_addrs[p.node_id], 0);
        _ = std.fmt.bufPrint(&g_cfg.node_addrs[p.node_id],
            "{d}.{d}.{d}.{d}:{d}", .{ a, b, c, d, ingest_port }) catch {};
    }

    if (cfg.peers.len > 0) {
        g_peer_pool = PeerPool.init(&io);
        g_peer_pool_init = true;

        for (cfg.peers) |p| {
            const repl_addr = posix.sockaddr.in{
                .family = posix.AF.INET,
                .port   = std.mem.nativeToBig(u16, p.repl_port),
                .addr   = p.addr_be,
                .zero   = [_]u8{0} ** 8,
            };
            g_peer_pool.add_peer(p.node_id, repl_addr);
        }
        try g_peer_pool.connect_all();
    }

    const ingest_fd = try make_listen_fd(cfg.port);
    // ingest_fd is closed in begin_shutdown(); defer only if no shutdown occurred.
    defer if (!g_shutting_down) net_io.close(ingest_fd);
    try io.queue_accept(encode(.accept, ingest_fd), ingest_fd);
    if (g_vsr_init) try queue_tick_timeout(&io);

    if (cfg.peers.len > 0) {
        g_repl_listen_fd = try make_listen_fd(cfg.port + REPL_PORT_OFFSET);
        try io.queue_accept(encode(.accept, g_repl_listen_fd), g_repl_listen_fd);
        std.log.info("replication listener on port {d}", .{cfg.port + REPL_PORT_OFFSET});
    }

    if (cfg.http_port > 0) {
        g_http_listen_fd = try make_listen_fd(cfg.http_port);
        try io.queue_accept(encode(.accept, g_http_listen_fd), g_http_listen_fd);
        std.log.info("HTTP listener on port {d}", .{cfg.http_port});
    }
    defer if (!g_shutting_down and g_http_listen_fd >= 0) net_io.close(g_http_listen_fd);

    // Set up signalfd for graceful shutdown (SIGTERM/SIGINT).
    setup_signal_handler(&io) catch |err| {
        std.log.warn("signal handler setup failed: {}, graceful shutdown unavailable", .{err});
    };

    _ = try io.submit();

    std.log.info("ingest listener on port {d}", .{cfg.port});

    while (!g_shutting_down or g_inflight_count > 0) {
        const cqe  = try io.wait_cqe();
        const tag  = decode_tag(cqe.user_data);
        const fd   = decode_fd(cqe.user_data);

        if (g_peer_pool_init and g_peer_pool.is_peer_fd(fd)) {
            if (try g_peer_pool.on_cqe(tag, fd, cqe.res)) |msg| {
                handle_peer_msg(msg.from_node, msg.data) catch |e| {
                    std.log.err("peer msg error: {}", .{e});
                };
            }
            _ = try io.submit();
            continue;
        }

        if (is_repl_fd(fd)) {
            try handle_repl_recv(&io, fd, cqe.res);
            _ = try io.submit();
            continue;
        }

        if (is_http_conn_fd(fd)) {
            try handle_http_cqe(&io, &log, tag, fd, cqe.res);
            _ = try io.submit();
            continue;
        }

        switch (tag) {
            .accept  => {
                if (!g_shutting_down) try handle_accept(&io, fd, cqe.res);
            },
            .recv    => try handle_recv(&io, &log, fd, cqe.res),
            .write   => try handle_write(&io, &log, fd, cqe.res),
            .fsync   => try handle_fsync(&io, &log, fd),
            .send    => try handle_send(&io, fd, cqe.res),
            .alert_push => {}, // fire-and-forget; completion must not touch conn state
            .connect => {},
            .signal  => {
                if (cqe.res > 0) {
                    begin_shutdown(&io, ingest_fd);
                } else {
                    // Spurious wakeup or error — re-queue the read.
                    if (g_signal_fd >= 0) {
                        io.queue_recv(encode(.signal, g_signal_fd), g_signal_fd, &g_signal_buf) catch {};
                    }
                }
            },
            .timeout => {
                if (!g_shutting_down) {
                    tick_all_partitions();
                    maybe_peer_reconnect();
                    maybe_retention_sweep(alloc);
                    queue_tick_timeout(&io) catch {};
                }
            },
        }
        _ = try io.submit();
    }

    // Post-loop: save final checkpoint and flush WAL.
    if (g_memtable_init) {
        var ckpt_buf: [512]u8 = undefined;
        const ckpt = std.fmt.bufPrint(&ckpt_buf, "{s}/checkpoint.bin", .{cfg.data_dir}) catch "data/checkpoint.bin";
        checkpoint.save(alloc, &g_memtable, log.offset, ckpt) catch |err| {
            std.log.err("shutdown: failed to save checkpoint: {}", .{err});
        };
        std.log.info("shutdown: checkpoint saved at offset {d}", .{log.offset});
    }
    log.wal.sync() catch {};
    std.log.info("graceful shutdown complete", .{});
}

// ---- accept ----

fn handle_accept(io: *RealIO, server_fd: i32, res: i32) !void {
    try io.queue_accept(encode(.accept, server_fd), server_fd);
    if (res < 0) return;

    if (server_fd == g_repl_listen_fd) {
        try handle_repl_accept(io, res);
        return;
    }

    if (server_fd == g_http_listen_fd) {
        try handle_http_accept(io, res);
        return;
    }

    const client_fd = res;
    if (conn_alloc(client_fd)) |conn| {
        try io.queue_recv(encode(.recv, client_fd), client_fd,
            conn.body[0..@sizeOf(RequestHeader)]);
    } else {
        net_io.close(client_fd);
    }
}

fn handle_repl_accept(io: *RealIO, peer_fd: i32) !void {
    if (alloc_repl_conn(peer_fd)) |conn| {
        conn.state     = .reading_hdr;
        conn.bytes_got = 0;
        try io.queue_recv(encode(.recv, peer_fd), peer_fd, conn.buf[0..4]);
    } else {
        net_io.close(peer_fd);
    }
}

fn handle_repl_recv(io: *RealIO, fd: i32, res: i32) !void {
    if (res <= 0) { free_repl_conn(fd); net_io.close(fd); return; }
    const conn = get_repl_conn(fd) orelse return;
    conn.bytes_got += @intCast(res);

    switch (conn.state) {
        .reading_hdr => {
            if (conn.bytes_got < 4) {
                const off = conn.bytes_got;
                try io.queue_recv(encode(.recv, fd), fd, conn.buf[off..4]);
                return;
            }
            conn.frame_len = std.mem.readInt(u32, conn.buf[0..4], .little);
            if (conn.frame_len == 0 or conn.frame_len > MAX_MSG_BODY) {
                std.log.warn("repl conn {d}: invalid frame len {d}, closing", .{ fd, conn.frame_len });
                free_repl_conn(fd);
                net_io.close(fd);
                return;
            }
            conn.state     = .reading_body;
            conn.bytes_got = 0;
            try io.queue_recv(encode(.recv, fd), fd, conn.buf[4..4 + conn.frame_len]);
        },
        .reading_body => {
            if (conn.bytes_got < conn.frame_len) {
                const off = conn.bytes_got;
                try io.queue_recv(encode(.recv, fd), fd,
                    conn.buf[4 + off..4 + conn.frame_len]);
                return;
            }
            const data = conn.buf[4..4 + conn.frame_len];
            if (data.len >= @sizeOf(MsgHeader)) {
                var hdr: MsgHeader = undefined;
                @memcpy(std.mem.asBytes(&hdr), data[0..@sizeOf(MsgHeader)]);
                handle_peer_msg(hdr.from_node, data) catch |e| {
                    std.log.warn("repl peer msg: {}", .{e});
                };
            }
            conn.state     = .reading_hdr;
            conn.bytes_got = 0;
            try io.queue_recv(encode(.recv, fd), fd, conn.buf[0..4]);
        },
    }
}

// ---- peer message handler ----

fn handle_peer_msg(from_node: NodeId, data: []const u8) !void {
    if (data.len < @sizeOf(MsgHeader)) return;
    const hdr = std.mem.bytesAsValue(MsgHeader, data[0..@sizeOf(MsgHeader)]);
    if (hdr.magic != VSR_MAGIC) return;

    const p: u16 = @truncate(hdr.partition_id);
    if (p >= N_PARTITIONS) return;

    const msg_type: vsr_mod.MsgType = @enumFromInt(hdr.msg_type);
    switch (msg_type) {
        .prepare_ok => {
            if (data.len < @sizeOf(PrepareOkMsg)) return;
            var pok: PrepareOkMsg = undefined;
            @memcpy(std.mem.asBytes(&pok), data[0..@sizeOf(PrepareOkMsg)]);
            if (g_vsr[p].on_prepare_ok(&pok)) |_| {
                g_pending_client[p] = -1;
            }
        },
        .prepare => {
            if (data.len < @sizeOf(PrepareMsg)) return;
            if (g_peer_pool_init) {
                var pmsg: PrepareMsg = undefined;
                @memcpy(std.mem.asBytes(&pmsg), data[0..@sizeOf(PrepareMsg)]);
                if (g_vsr[p].on_prepare(&pmsg, 0)) |ok_msg| {
                    var ok_buf: [@sizeOf(PrepareOkMsg)]u8 = undefined;
                    @memcpy(&ok_buf, std.mem.asBytes(&ok_msg));
                    g_peer_pool.send(from_node, &ok_buf) catch |e| {
                        std.log.warn("replica: send PREPARE_OK to {d}: {}", .{ from_node, e });
                    };
                }
            }
        },
        .ping => {
            if (data.len < @sizeOf(PingMsg)) return;
            if (g_vsr[p].role != .replica) return;
            var ping: PingMsg = undefined;
            @memcpy(std.mem.asBytes(&ping), data[0..@sizeOf(PingMsg)]);
            g_vsr[p].on_ping(&ping);
        },
        .start_view_change => {
            if (data.len < @sizeOf(StartViewChangeMsg)) return;
            var svc: StartViewChangeMsg = undefined;
            @memcpy(std.mem.asBytes(&svc), data[0..@sizeOf(StartViewChangeMsg)]);
            if (g_vsr[p].on_start_view_change(&svc)) |dvc_msg| {
                const new_leader = g_vsr[p].leader_node(dvc_msg.header.view_number);
                var dvc_buf: [@sizeOf(DoViewChangeMsg)]u8 = undefined;
                @memcpy(&dvc_buf, std.mem.asBytes(&dvc_msg));
                if (new_leader == g_node_id) {
                    if (g_vsr[p].on_do_view_change(&dvc_msg)) |sv_msg| {
                        broadcast_start_view(p, &sv_msg);
                    }
                } else if (g_peer_pool_init) {
                    g_peer_pool.send(new_leader, &dvc_buf) catch {};
                }
            }
        },
        .do_view_change => {
            if (data.len < @sizeOf(DoViewChangeMsg)) return;
            var dvc: DoViewChangeMsg = undefined;
            @memcpy(std.mem.asBytes(&dvc), data[0..@sizeOf(DoViewChangeMsg)]);
            if (g_vsr[p].on_do_view_change(&dvc)) |sv_msg| {
                broadcast_start_view(p, &sv_msg);
            }
        },
        .start_view => {
            if (data.len < @sizeOf(StartViewMsg)) return;
            var sv: StartViewMsg = undefined;
            @memcpy(std.mem.asBytes(&sv), data[0..@sizeOf(StartViewMsg)]);
            g_vsr[p].on_start_view(&sv, @truncate(time_util.wallNanos()));
            g_partition_map.set_leader(p, sv.header.from_node);
            std.log.info("partition {d}: START_VIEW from node {d} (view {})",
                .{ p, sv.header.from_node, sv.header.view_number });
        },
        else => {},
    }
}

// ---- recv ----

fn handle_recv(io: *RealIO, log: *UsageLog, fd: i32, res: i32) !void {
    if (res <= 0) { conn_free(fd); net_io.close(fd); return; }
    const conn = conn_get(fd) orelse return;
    conn.bytes_got += @intCast(res);

    switch (conn.state) {
        .reading_header => {
            if (conn.bytes_got < @sizeOf(RequestHeader)) {
                const off = conn.bytes_got;
                try io.queue_recv(encode(.recv, fd), fd,
                    conn.body[off..@sizeOf(RequestHeader)]);
                return;
            }
            @memcpy(std.mem.asBytes(&conn.hdr), conn.body[0..@sizeOf(RequestHeader)]);

            if (!proto.validate_header(&conn.hdr)) {
                return send_error(io, conn);
            }

            conn.packet_type = @enumFromInt(conn.hdr.packet_type);
            conn.request_id  = conn.hdr.request_id;

            // Resolve partition for ingest: check leadership and possibly redirect.
            const partition_id: u16 = if (conn.hdr.partition == 0xFFFF) 0
                else conn.hdr.partition;
            conn.partition_id = partition_id;

            const is_ingest = (conn.packet_type == .ingest_async or
                               conn.packet_type == .ingest_sync);
            if (is_ingest and g_vsr_init) {
                const leader = g_partition_map.leader_for(partition_id);
                if (leader != g_node_id) {
                    return send_redirect(io, conn, leader);
                }
            }

            if (conn.hdr.payload_len == 0) {
                // No body to read; dispatch immediately.
                conn.start_ns = @truncate(time_util.wallNanos());
                return dispatch_packet(io, log, conn);
            }
            if (conn.hdr.payload_len > MAX_BODY_LEN) {
                return send_error(io, conn);
            }

            conn.state     = .reading_body;
            conn.bytes_got = 0;
            conn.start_ns  = @truncate(time_util.wallNanos());
            try io.queue_recv(encode(.recv, fd), fd,
                conn.body[0..conn.hdr.payload_len]);
        },

        .reading_body => {
            if (conn.bytes_got < conn.hdr.payload_len) {
                const off = conn.bytes_got;
                try io.queue_recv(encode(.recv, fd), fd,
                    conn.body[off..conn.hdr.payload_len]);
                return;
            }
            try dispatch_packet(io, log, conn);
        },

        else => {},
    }
}

/// Dispatch a fully-received packet to the appropriate handler.
fn dispatch_packet(io: *RealIO, log: *UsageLog, conn: *Conn) !void {
    switch (conn.packet_type) {
        .ingest_async, .ingest_sync => try handle_ingest(io, log, conn),
        .get_partition_map           => try handle_get_partition_map(io, conn),
        .metric_put                  => try handle_metric_put(io, log, conn),
        .metric_delete               => try handle_metric_delete(io, log, conn),
        .metric_list                 => try handle_metric_list(io, log, conn),
        .usage_query                 => try handle_usage_query(io, conn),
        .usage_realtime              => try handle_usage_realtime(io, conn),
        .events_list                 => try handle_events_list(io, log, conn),
        .alerts_list                 => try handle_alerts_list(io, log, conn),
        .alert_push_enable           => try handle_alert_push_enable(io, conn),
        .cluster_status              => try handle_cluster_status(io, conn),
        .cluster_rebalance           => try handle_cluster_rebalance(io, conn),
        else                         => try send_error(io, conn),
    }
}

fn handle_alert_push_enable(io: *RealIO, conn: *Conn) !void {
    if (!conn.wants_alerts) {
        conn.wants_alerts = true;
        metrics.alert_subscribers.inc();
    }
    write_resp_hdr(conn, .ok, 0);
    try queue_send_resp(io, conn);
}

fn handle_cluster_status(io: *RealIO, conn: *Conn) !void {
    const payload_size = proto.CLUSTER_STATUS_PAYLOAD_SIZE;
    const payload = conn.body[RESP_HDR_SIZE .. RESP_HDR_SIZE + payload_size];

    // Fill header.
    const n_nodes: u8 = if (g_cfg.peers.len == 0) 1 else @intCast(g_cfg.peers.len + 1);
    const now_ns: i64 = @truncate(time_util.wallNanos());

    // Count segment files.
    var seg_count: u64 = 0;
    const dir_fd = disk_io.open_dir(g_data_dir) catch 0;
    if (dir_fd > 0) {
        var it = disk_io.DirIter.init(dir_fd);
        while (it.next() catch null) |name| {
            if (name.len >= 4 and std.mem.eql(u8, name[name.len - 4 ..], ".seg")) {
                seg_count += 1;
            }
        }
        disk_io.close_dir(dir_fd);
    }

    const hdr = proto.ClusterStatusHeader{
        .node_id          = g_node_id,
        .n_nodes          = n_nodes,
        ._pad             = .{0} ** 6,
        .wal_offset       = if (g_memtable_init) 0 else 0, // filled below after we have log access
        .segment_count    = seg_count,
        .memtable_entries = if (g_memtable_init) g_memtable.count() else 0,
        .uptime_ns        = now_ns - g_start_ns,
    };
    @memcpy(payload[0..@sizeOf(proto.ClusterStatusHeader)], std.mem.asBytes(&hdr));

    // Fill per-partition entries.
    const entries_start = @sizeOf(proto.ClusterStatusHeader);
    for (0..N_PARTITIONS) |p| {
        const entry = proto.PartitionStatusEntry{
            .view_number   = if (g_vsr_init) @truncate(g_vsr[p].view_number) else 0,
            .commit_number = if (g_vsr_init) @truncate(g_vsr[p].commit_number) else 0,
            .role          = if (g_vsr_init) @intFromEnum(g_vsr[p].role) else 0,
            .leader        = if (g_vsr_init) g_partition_map.entries[p].leader else 0,
        };
        const off = entries_start + p * @sizeOf(proto.PartitionStatusEntry);
        @memcpy(payload[off .. off + @sizeOf(proto.PartitionStatusEntry)],
            std.mem.asBytes(&entry));
    }

    write_resp_hdr(conn, .ok, @intCast(payload_size));
    try queue_send_resp(io, conn);
}

fn handle_cluster_rebalance(io: *RealIO, conn: *Conn) !void {
    // Payload: 1 byte = new_node_count.
    if (conn.hdr.payload_len < 1) return send_error(io, conn);
    const new_node_count = conn.body[0];
    if (new_node_count < 1) return send_error(io, conn);

    std.log.info("rebalance: requested for {d} nodes", .{new_node_count});

    // Compute new partition map and moves.
    const new_map = PartitionMap.init_uniform(new_node_count);
    const moves = transfer_mod.compute_moves(&g_partition_map, &new_map);

    std.log.info("rebalance: {d} partition moves required", .{moves.count});

    // Transfer partitions this node was leader for that are moving away.
    var transferred: u32 = 0;
    for (moves.moves[0..moves.count]) |m| {
        if (m.old_leader != g_node_id) continue;

        // Get destination address.
        const dest_addr_bytes = &g_cfg.node_addrs[m.new_leader];
        var addr_len: usize = 0;
        while (addr_len < dest_addr_bytes.len and dest_addr_bytes[addr_len] != 0) {
            addr_len += 1;
        }
        if (addr_len == 0) {
            std.log.warn("rebalance: no address for node {d}, skipping partition {d}", .{
                m.new_leader, m.partition_id,
            });
            continue;
        }
        const dest_addr = dest_addr_bytes[0..addr_len];

        _ = transfer_mod.transfer_partition(g_data_dir, m.partition_id, dest_addr) catch |err| {
            std.log.err("rebalance: transfer partition {d} → node {d} failed: {}", .{
                m.partition_id, m.new_leader, err,
            });
            continue;
        };
        transferred += 1;
    }

    // Apply new partition map.
    g_partition_map = new_map;

    // Update VSR roles.
    if (g_vsr_init) {
        for (0..N_PARTITIONS) |p| {
            const new_role: vsr_mod.Role = if (p % new_node_count == g_node_id) .leader else .replica;
            g_vsr[p].role = new_role;
        }
    }

    // Rebuild memtable for newly acquired partitions.
    for (moves.moves[0..moves.count]) |m| {
        if (m.new_leader == g_node_id and m.old_leader != g_node_id) {
            // This node is the new owner. The transferred events are now in our
            // segments (ingested via the sync ingest path). The agg_worker will
            // pick them up on the next checkpoint cycle.
            std.log.info("rebalance: acquired partition {d}", .{m.partition_id});
        }
    }

    // Save and broadcast new partition map.
    var map_path_buf: [512]u8 = undefined;
    const map_path = std.fmt.bufPrint(&map_path_buf, "{s}/partition_map.bin", .{g_data_dir}) catch "data/partition_map.bin";
    g_partition_map.save(map_path) catch |err| {
        std.log.err("rebalance: save partition map failed: {}", .{err});
    };

    // Broadcast updated partition map to connected clients.
    broadcast_partition_map(io);

    std.log.info("rebalance: complete. {d} partitions transferred from this node", .{transferred});

    write_resp_hdr(conn, .ok, 0);
    try queue_send_resp(io, conn);
}

// ---- ingest handler ----

fn handle_ingest(io: *RealIO, log: *UsageLog, conn: *Conn) !void {
    if (!disk_has_space()) {
        write_resp_hdr(conn, .backpressure, 0);
        return queue_send_resp(io, conn);
    }

    const payload = conn.body[0..conn.hdr.payload_len];
    if (payload.len < @sizeOf(proto.IngestPayloadHeader)) {
        return send_error(io, conn);
    }

    const ingest_hdr = std.mem.bytesAsValue(proto.IngestPayloadHeader,
        payload[0..@sizeOf(proto.IngestPayloadHeader)]);
    const event_count = ingest_hdr.event_count;
    const props_count = ingest_hdr.props_count;
    const events_offset = @sizeOf(proto.IngestPayloadHeader); // 8B
    const events_len    = event_count * @sizeOf(WireEvent);

    if (payload.len < events_offset + events_len) {
        return send_error(io, conn);
    }

    const wire_events = @as([*]const WireEvent,
        @ptrCast(@alignCast(&payload[events_offset])))[0..event_count];

    // Extract props from payload (after events section).
    const props_start = events_offset + events_len;
    const props_len   = props_count * proto.PROP_PAIR_SIZE;
    if (props_count > 0 and payload.len >= props_start + props_len) {
        const props_bytes = payload[props_start..props_start + props_len];
        const wire_props = @as([*]const proto.PropPair,
            @ptrCast(@alignCast(props_bytes.ptr)))[0..props_count];
        const n = @min(props_count, MAX_PROPS_PER_BATCH);
        @memcpy(conn.props_buf[0..n], wire_props[0..n]);

        // Build per-wire-event props index using WireEvent.props_count.
        var prop_cursor: u16 = 0;
        for (wire_events, 0..) |*we, i| {
            if (i >= MAX_EVENTS_PER_BATCH) break;
            conn.wire_props_offsets[i] = prop_cursor;
            conn.wire_props_counts[i]  = we.props_count;
            prop_cursor += we.props_count;
        }
    } else {
        // No props: zero out counts.
        for (0..@min(event_count, MAX_EVENTS_PER_BATCH)) |i| {
            conn.wire_props_counts[i] = 0;
        }
    }

    conn.sync_mode = conn.packet_type == .ingest_sync;

    conn.batch = log.prepare(wire_events, &conn.wal_buf, &conn.events_buf, &conn.wire_indices) catch {
        return send_error(io, conn);
    };

    if (conn.batch.n_events == 0) {
        const status: proto.StatusCode = if (conn.batch.result.n_duplicates > 0)
            .duplicate else .ok;
        build_ingest_resp(conn, status);
        return queue_send_resp(io, conn);
    }

    conn.state = .writing_wal;
    g_inflight_count += 1;
    try io.queue_write(
        encode(.write, conn.fd),
        log.wal.file.fd(),
        conn.wal_buf[0..conn.batch.wal_entry_len],
        log.wal.write_offset,
    );
}

// ---- write CQE ----

fn handle_write(io: *RealIO, log: *UsageLog, fd: i32, res: i32) !void {
    if (res < 0) {
        const conn = conn_get(fd) orelse return;
        return send_error(io, conn);
    }
    const conn = conn_get(fd) orelse return;

    switch (conn.state) {
        .writing_wal => {
            log.wal.advance(conn.batch.wal_entry_len, conn.batch.wal_new_crc);
            metrics.wal_writes.inc();
            metrics.wal_offset_bytes.set(@intCast(log.wal.write_offset));

            if (conn.sync_mode) {
                conn.state          = .fsyncing_wal;
                conn.fsync_start_ns = @truncate(time_util.wallNanos());
                try io.queue_fsync(encode(.fsync, fd), log.wal.file.fd());
            } else {
                try queue_seg_write(io, log, conn);
            }
        },

        .writing_seg => {
            log.segment.advance(conn.batch.n_events);

            if (conn.sync_mode) {
                conn.state          = .fsyncing_seg;
                conn.fsync_start_ns = @truncate(time_util.wallNanos());
                try io.queue_fsync(encode(.fsync, fd), log.segment.file.fd());
            } else {
                finish_ingest_batch(conn);
                try queue_send_resp(io, conn);
            }
        },

        else => {},
    }
}

// ---- fsync CQE ----

fn handle_fsync(io: *RealIO, log: *UsageLog, fd: i32) !void {
    const conn = conn_get(fd) orelse return;

    switch (conn.state) {
        .fsyncing_wal => try queue_seg_write(io, log, conn),
        .fsyncing_seg => {
            const now: i64 = @truncate(time_util.wallNanos());
            const fsync_dur: u64 = @intCast(@max(0, now - conn.fsync_start_ns));
            metrics.wal_syncs.inc();
            metrics.wal_sync_duration.observe(fsync_dur);

            finish_ingest_batch(conn);
            send_prepare_to_replicas(conn) catch |e| {
                std.log.warn("send_prepare failed: {}", .{e});
            };
            try queue_send_resp(io, conn);
        },
        else => {},
    }
}

fn send_prepare_to_replicas(conn: *const Conn) !void {
    if (!g_peer_pool_init) return;
    if (g_vsr[conn.partition_id].role != .leader) return;

    const prepare = g_vsr[conn.partition_id].prepare(
        conn.batch.wal_new_crc,
        @intCast(conn.batch.n_events),
    );

    const entry = g_partition_map.entries[conn.partition_id];
    for (entry.replicas) |replica_id| {
        if (replica_id == cluster_mod.NO_NODE) continue;
        g_peer_pool.send(replica_id, std.mem.asBytes(&prepare)) catch |e| {
            std.log.warn("send PREPARE to node {d}: {}", .{ replica_id, e });
        };
    }
}

// ---- send CQE ----

fn handle_send(io: *RealIO, fd: i32, res: i32) !void {
    if (res <= 0) {
        // Connection failed; decrement inflight if it was an ingest.
        if (conn_get(fd)) |c| {
            if ((c.packet_type == .ingest_async or c.packet_type == .ingest_sync) and g_inflight_count > 0) {
                g_inflight_count -= 1;
            }
        }
        conn_free(fd); net_io.close(fd); return;
    }
    const conn = conn_get(fd) orelse return;
    conn.resp_sent += @intCast(res);

    if (conn.resp_sent < conn.resp_total_len) {
        const off = conn.resp_sent;
        try io.queue_send(encode(.send, fd), fd,
            conn.body[off..conn.resp_total_len]);
        return;
    }

    // Response fully sent. Decrement inflight if this was an ingest write path.
    if (conn.packet_type == .ingest_async or conn.packet_type == .ingest_sync) {
        if (g_inflight_count > 0) g_inflight_count -= 1;
    }

    // Reset for next request (or close if shutting down).
    if (g_shutting_down) {
        conn_free(fd);
        net_io.close(fd);
        return;
    }
    conn.state         = .reading_header;
    conn.bytes_got     = 0;
    conn.resp_sent     = 0;
    conn.resp_total_len = 0;
    try io.queue_recv(encode(.recv, fd), fd,
        conn.body[0..@sizeOf(RequestHeader)]);
}

// ---- packet handlers (non-ingest) ----

fn handle_get_partition_map(io: *RealIO, conn: *Conn) !void {
    // Payload: 256 × 96B partition entries (LeaderAddr + Replica0 + Replica1 + _pad).
    const ENTRY_SIZE:   usize = proto.ADDR_LEN * 4; // 96B per entry
    const PAYLOAD_SIZE: usize = N_PARTITIONS * ENTRY_SIZE; // 24576B

    // Write entries into body[RESP_HDR_SIZE..].
    const payload = conn.body[RESP_HDR_SIZE..RESP_HDR_SIZE + PAYLOAD_SIZE];
    @memset(payload, 0);

    for (0..N_PARTITIONS) |p| {
        const slot = payload[p * ENTRY_SIZE .. (p + 1) * ENTRY_SIZE];
        const entry = g_partition_map.entries[p];

        if (entry.leader < g_cfg.node_addrs.len) {
            @memcpy(slot[0..proto.ADDR_LEN], &g_cfg.node_addrs[entry.leader]);
        }
        if (entry.replicas[0] != cluster_mod.NO_NODE) {
            @memcpy(slot[proto.ADDR_LEN..2 * proto.ADDR_LEN],
                    &g_cfg.node_addrs[entry.replicas[0]]);
        }
        if (entry.replicas[1] != cluster_mod.NO_NODE) {
            @memcpy(slot[2 * proto.ADDR_LEN..3 * proto.ADDR_LEN],
                    &g_cfg.node_addrs[entry.replicas[1]]);
        }
        // slot[3*ADDR_LEN .. 4*ADDR_LEN] = _pad, already zeroed
    }

    write_resp_hdr(conn, .ok, @intCast(PAYLOAD_SIZE));
    try queue_send_resp(io, conn);
}

fn handle_metric_put(io: *RealIO, log: *UsageLog, conn: *Conn) !void {
    const MetricSchemaWireBaseSize = 132;
    const DimFilterWireSize        = 292;

    const payload = conn.body[0..conn.hdr.payload_len];
    if (payload.len < MetricSchemaWireBaseSize) return send_error(io, conn);

    var schema = std.mem.zeroes(MetricSchema);

    // code_str [64]
    @memcpy(&schema.code_str, payload[0..64]);
    schema.code = metric_registry.fnv1a(schema.code_str[0..]);
    // agg_type:u8, recurring:u8, filter_count:u8, _pad:u8
    schema.agg_type      = payload[64];
    schema.recurring     = payload[65] != 0;
    const wire_fc: u8    = payload[66];
    // payload[67] = _pad
    // field_name [64]
    @memcpy(&schema.field_name, payload[68..132]);

    // Parse DimensionFilter entries.
    const n_filters = @min(wire_fc, metric_registry.MAX_FILTERS);
    var off: usize = MetricSchemaWireBaseSize;
    for (0..n_filters) |i| {
        if (off + DimFilterWireSize > payload.len) break;
        @memcpy(&schema.filters[i].key, payload[off..off + 32]);
        off += 32;
        schema.filters[i].value_count = payload[off];
        off += 4; // 1 byte ValuesCount + 3 bytes pad
        for (0..8) |vi| {
            @memcpy(&schema.filters[i].values[vi], payload[off..off + 32]);
            off += 32;
        }
    }
    schema.filter_count = @intCast(n_filters);

    // Optional alert thresholds section: [alert_count:u8][7B pad][N × 48B AlertThresholdWire].
    // Present only if the payload extends past the filters section. Older clients
    // that don't know about thresholds simply stop here and thresholds stay zero.
    const AlertThresholdWireSize: usize = 48;
    if (off + 8 <= payload.len) {
        const alert_count: u8 = payload[off];
        off += 8; // 1 byte count + 7 bytes pad
        const n_thresholds = @min(alert_count, @as(u8, 8));
        for (0..n_thresholds) |i| {
            if (off + AlertThresholdWireSize > payload.len) break;
            @memcpy(&schema.alert_thresholds[i].code, payload[off..off + 32]);
            off += 32;
            schema.alert_thresholds[i].value = std.mem.readInt(u64, payload[off..][0..8], .little);
            off += 8;
            schema.alert_thresholds[i].recurring = payload[off] != 0;
            off += 8; // 1 byte + 7 pad
        }
        schema.alert_count = @intCast(n_thresholds);
    }

    // Optional period config section: [period_type:u8][billing_cycle_day:u8][6B pad].
    // Present only if the payload extends past the thresholds section.
    if (off + 8 <= payload.len) {
        schema.period_type       = payload[off];
        schema.billing_cycle_day = payload[off + 1];
        off += 8;
    }

    log.registry.put(schema) catch { return send_error(io, conn); };
    save_registry(&log.registry) catch {};

    write_resp_hdr(conn, .ok, 0);
    try queue_send_resp(io, conn);
}

fn handle_metric_delete(io: *RealIO, log: *UsageLog, conn: *Conn) !void {
    const payload = conn.body[0..conn.hdr.payload_len];
    if (payload.len < 64) return send_error(io, conn);

    const code_hash = metric_registry.fnv1a(payload[0..64]);
    log.registry.remove(code_hash);
    save_registry(&log.registry) catch {};

    write_resp_hdr(conn, .ok, 0);
    try queue_send_resp(io, conn);
}

fn handle_metric_list(io: *RealIO, log: *UsageLog, conn: *Conn) !void {
    const MetricSchemaWireBaseSize = 132;
    const DimFilterWireSize        = 292;
    // Response: 8B count-header + N × (132B base + filter_count × 292B)
    // Use body[RESP_HDR_SIZE + 8 ..] for schema data; body[RESP_HDR_SIZE..] for count header.
    const hdr_off:   usize = RESP_HDR_SIZE;
    const count_off: usize = hdr_off;     // 8B count+pad header starts here in payload
    const data_off:  usize = hdr_off + 8; // schema data starts here
    const body_end:  usize = MAX_BODY_LEN;

    var out: usize = data_off;
    var count: u32 = 0;

    const AlertThresholdWireSize: usize = 48;

    var it = log.registry.schemas.valueIterator();
    while (it.next()) |schema| {
        const fc   = @min(schema.filter_count, metric_registry.MAX_FILTERS);
        const ac   = @min(schema.alert_count, 8);
        const need = MetricSchemaWireBaseSize + @as(usize, fc) * DimFilterWireSize + 8 + @as(usize, ac) * AlertThresholdWireSize + 8;
        if (out + need > body_end) break; // truncate if needed

        // MetricSchemaWireBase (132B): code_str[64] + agg_type + recurring + filter_count + _pad + field_name[64]
        @memcpy(conn.body[out..out + 64], &schema.code_str); out += 64;
        conn.body[out]     = schema.agg_type;
        conn.body[out + 1] = @intFromBool(schema.recurring);
        conn.body[out + 2] = fc;
        conn.body[out + 3] = 0; // _pad
        out += 4;
        @memcpy(conn.body[out..out + 64], &schema.field_name); out += 64;

        // DimensionFilter entries
        for (schema.filters[0..fc]) |f| {
            @memcpy(conn.body[out..out + 32], &f.key); out += 32;
            conn.body[out]     = f.value_count;
            conn.body[out + 1] = 0;
            conn.body[out + 2] = 0;
            conn.body[out + 3] = 0;
            out += 4;
            for (f.values) |v| {
                @memcpy(conn.body[out..out + 32], &v); out += 32;
            }
        }

        // Alert thresholds section: [alert_count:u8][7B pad][N × 48B]
        conn.body[out] = ac;
        @memset(conn.body[out + 1..out + 8], 0);
        out += 8;
        for (schema.alert_thresholds[0..ac]) |t| {
            @memcpy(conn.body[out..out + 32], &t.code); out += 32;
            std.mem.writeInt(u64, conn.body[out..][0..8], t.value, .little); out += 8;
            conn.body[out] = @intFromBool(t.recurring);
            @memset(conn.body[out + 1..out + 8], 0);
            out += 8;
        }

        // Period config section: [period_type:u8][billing_cycle_day:u8][6B pad]
        conn.body[out]     = schema.period_type;
        conn.body[out + 1] = schema.billing_cycle_day;
        @memset(conn.body[out + 2..out + 8], 0);
        out += 8;

        count += 1;
    }

    // Write 8B count header.
    std.mem.writeInt(u32, conn.body[count_off..][0..4], count, .little);
    std.mem.writeInt(u32, conn.body[count_off + 4..][0..4], 0, .little);

    const payload_len: u32 = @intCast(out - hdr_off);
    write_resp_hdr(conn, .ok, payload_len);
    try queue_send_resp(io, conn);
}

fn handle_usage_realtime(io: *RealIO, conn: *Conn) !void {
    // Payload: account_id:u64 (8B) + metric_code:[64]u8 (64B) = 72B minimum.
    if (conn.hdr.payload_len < 72) return send_error(io, conn);

    const payload = conn.body[0..conn.hdr.payload_len];
    const account_id = std.mem.readInt(u64, payload[0..8], .little);
    const metric_code_str = payload[8..72];
    const metric_code = metric_registry.fnv1a(std.mem.sliceTo(metric_code_str, 0));

    // Look up current period in memtable.
    var agg = AggValue{};
    if (g_memtable_init) {
        const now_ns: i64 = @truncate(time_util.wallNanos());
        const schema = g_agg_worker.registry.get(metric_code);
        const pid = if (schema) |s| aggregators.resolve_period_id(now_ns, s.period_type, s.period_ns, s.billing_cycle_day) else aggregators.period_id_of(now_ns, worker_mod.DEFAULT_PERIOD_NS);
        const key = AggKey{
            .account_id  = account_id,
            .period_id   = pid,
            .metric_code = metric_code,
            .filter_hash = 0,
        };
        if (g_memtable.get(key)) |v| { agg = v; }
    }

    write_agg_value_wire(conn, &agg);
    write_resp_hdr(conn, .ok, proto.AGG_VALUE_WIRE_SIZE);
    try queue_send_resp(io, conn);
}

fn handle_usage_query(io: *RealIO, conn: *Conn) !void {
    // Payload: account_id:u64 + period_start:i64 + period_end:i64 + metric_code:[64]u8 + filters_count:u8 + _pad:[7]u8 = 96B base.
    const BASE_SIZE = 96;
    if (conn.hdr.payload_len < 88) return send_error(io, conn);

    const payload = conn.body[0..conn.hdr.payload_len];
    const account_id    = std.mem.readInt(u64, payload[0..8], .little);
    const period_start  = std.mem.readInt(i64, payload[8..16], .little);
    const period_end    = std.mem.readInt(i64, payload[16..24], .little);
    const metric_code_str = payload[24..88];
    const metric_code = metric_registry.fnv1a(std.mem.sliceTo(metric_code_str, 0));
    const filters_count: u8 = if (payload.len >= 89) payload[88] else 0;

    // Compute filter_hash from query filters (same algorithm as ingest).
    var filter_hash: u64 = 0;
    if (filters_count > 0 and payload.len >= BASE_SIZE + @as(usize, filters_count) * @sizeOf(DimensionFilter)) {
        filter_hash = compute_query_filter_hash(payload[BASE_SIZE..], filters_count);
    }

    var merged = AggValue{};
    if (g_memtable_init) {
        const schema = g_agg_worker.registry.get(metric_code);
        const pid_start = if (schema) |s| aggregators.resolve_period_id(period_start, s.period_type, s.period_ns, s.billing_cycle_day) else aggregators.period_id_of(period_start, worker_mod.DEFAULT_PERIOD_NS);
        const pid_end = if (schema) |s| aggregators.resolve_period_id(period_end, s.period_type, s.period_ns, s.billing_cycle_day) else aggregators.period_id_of(period_end, worker_mod.DEFAULT_PERIOD_NS);

        var pid: u32 = pid_start;
        while (pid <= pid_end) : (pid += 1) {
            const key = AggKey{
                .account_id  = account_id,
                .period_id   = pid,
                .metric_code = metric_code,
                .filter_hash = filter_hash,
            };
            if (g_memtable.get(key)) |v| {
                merged.sum   += v.sum;
                merged.count += v.count;
                merged.max    = @max(merged.max, v.max);
                if (v.last_timestamp > merged.last_timestamp) {
                    merged.last_value     = v.last_value;
                    merged.last_timestamp = v.last_timestamp;
                }
                merged.alert_flags |= v.alert_flags;
            }
        }
    }

    write_agg_value_wire(conn, &merged);
    write_resp_hdr(conn, .ok, proto.AGG_VALUE_WIRE_SIZE);
    try queue_send_resp(io, conn);
}

/// Compute filter_hash from DimensionFilterWire entries in the query payload.
/// Uses the same XOR(fnv1a(key), fnv1a(value)) scheme as ingest-time filter_hash.
fn compute_query_filter_hash(filter_data: []const u8, count: u8) u64 {
    const DimFilterWireSize = @sizeOf(DimensionFilter); // 292B
    var hash: u64 = 0;
    for (0..count) |i| {
        const off = i * DimFilterWireSize;
        if (off + DimFilterWireSize > filter_data.len) break;
        const f = std.mem.bytesAsValue(DimensionFilter, filter_data[off..][0..DimFilterWireSize]);
        const key_str = std.mem.sliceTo(&f.key, 0);
        if (key_str.len == 0 or f.value_count == 0) continue;
        // Use first value for hash (query typically specifies exactly one value per dimension).
        const val_str = std.mem.sliceTo(&f.values[0], 0);
        hash ^= metric_registry.fnv1a(key_str) ^ metric_registry.fnv1a(val_str);
    }
    return hash;
}

fn handle_events_list(io: *RealIO, log: *UsageLog, conn: *Conn) !void {
    // Payload: account_id:u64 + since_ns:i64 + until_ns:i64 (+ optional _pad:u64) = 24B min.
    const payload = conn.body[0..conn.hdr.payload_len];
    if (payload.len < 24) return send_error(io, conn);

    const account_id = std.mem.readInt(u64, payload[0..8], .little);
    const since_ns   = std.mem.readInt(i64, payload[8..16], .little);
    const until_ns   = std.mem.readInt(i64, payload[16..24], .little);

    // Use conn.events_buf as a temporary buffer for scanned events.
    const EventRecordWireSize = 104;
    const max_events = @min(
        (MAX_BODY_LEN - RESP_HDR_SIZE - 8) / EventRecordWireSize,
        MAX_EVENTS_PER_BATCH,
    );

    const events = query_mod.list_events(
        g_data_dir, account_id, since_ns, until_ns, conn.events_buf[0..max_events],
    ) catch &[_]Event{};

    // Serialize into body[RESP_HDR_SIZE + 8 ..].
    var out: usize = RESP_HDR_SIZE + 8;
    for (events) |e| {
        if (out + EventRecordWireSize > MAX_BODY_LEN) break;
        std.mem.writeInt(u64, conn.body[out..][0..8], e.offset,     .little); out += 8;
        std.mem.writeInt(i64, conn.body[out..][0..8], e.timestamp,   .little); out += 8;
        std.mem.writeInt(u64, conn.body[out..][0..8], e.account_id,  .little); out += 8;
        // MetricCode as string (64B): reverse-lookup in registry.
        @memset(conn.body[out..out + 64], 0);
        if (log.registry.get(e.metric_code)) |schema| {
            @memcpy(conn.body[out..out + 64], &schema.code_str);
        }
        out += 64;
        std.mem.writeInt(u64, conn.body[out..][0..8], e.value,       .little); out += 8;
        conn.body[out] = e.operation_type; out += 1;
        @memset(conn.body[out..out + 7], 0); out += 7; // _pad
    }

    // Write 8B count header.
    std.mem.writeInt(u32, conn.body[RESP_HDR_SIZE..][0..4],     @intCast(events.len), .little);
    std.mem.writeInt(u32, conn.body[RESP_HDR_SIZE + 4..][0..4], 0, .little);

    write_resp_hdr(conn, .ok, @intCast(out - RESP_HDR_SIZE));
    try queue_send_resp(io, conn);
}

fn handle_alerts_list(io: *RealIO, log: *UsageLog, conn: *Conn) !void {
    // Payload: account_id:u64 + since_offset:u64 = 16B.
    const payload = conn.body[0..conn.hdr.payload_len];
    if (payload.len < 16) return send_error(io, conn);

    const account_id  = std.mem.readInt(u64, payload[0..8], .little);
    const since_offset = std.mem.readInt(u64, payload[8..16], .little);

    var path_buf: [512]u8 = undefined;
    const alert_path = std.fmt.bufPrint(&path_buf, "{s}/alert_log.bin", .{g_data_dir})
        catch return send_error(io, conn);

    const AlertRecordWireSize = 136;
    const max_alerts = (MAX_BODY_LEN - RESP_HDR_SIZE - 8) / AlertRecordWireSize;

    var alert_buf: [256]AlertEntry = undefined;
    const out_count = @min(max_alerts, alert_buf.len);
    const entries = query_mod.get_alerts(alert_path, account_id, since_offset, alert_buf[0..out_count])
        catch &[_]AlertEntry{};

    var out: usize = RESP_HDR_SIZE + 8;
    for (entries) |e| {
        if (out + AlertRecordWireSize > MAX_BODY_LEN) break;
        std.mem.writeInt(u64, conn.body[out..][0..8], 0,            .little); out += 8; // Offset (TODO)
        std.mem.writeInt(u64, conn.body[out..][0..8], e.account_id, .little); out += 8;
        // MetricCode as string (64B).
        @memset(conn.body[out..out + 64], 0);
        if (log.registry.get(e.metric_code)) |schema| {
            @memcpy(conn.body[out..out + 64], &schema.code_str);
        }
        out += 64;
        // ThresholdCode (32B).
        @memset(conn.body[out..out + 32], 0);
        if (log.registry.get(e.metric_code)) |schema| {
            if (e.threshold_index < schema.alert_count) {
                @memcpy(conn.body[out..out + 32],
                        &schema.alert_thresholds[e.threshold_index].code);
            }
        }
        out += 32;
        std.mem.writeInt(u64, conn.body[out..][0..8], e.value_at_cross,  .little); out += 8;
        std.mem.writeInt(i64, conn.body[out..][0..8], e.event_timestamp, .little); out += 8;
        @memset(conn.body[out..out + 8], 0); out += 8; // _pad
    }

    std.mem.writeInt(u32, conn.body[RESP_HDR_SIZE..][0..4],     @intCast(entries.len), .little);
    std.mem.writeInt(u32, conn.body[RESP_HDR_SIZE + 4..][0..4], 0, .little);

    write_resp_hdr(conn, .ok, @intCast(out - RESP_HDR_SIZE));
    try queue_send_resp(io, conn);
}

// ---- helpers ----

/// Write a 12-byte response header at body[0..12] and set resp_total_len.
/// Caller must have written any response payload to body[12..12+payload_len].
fn write_resp_hdr(conn: *Conn, status: proto.StatusCode, payload_len: u32) void {
    conn.body[0] = @intFromEnum(status);
    conn.body[1] = 0;
    conn.body[2] = 0;
    conn.body[3] = 0;
    std.mem.writeInt(u32, conn.body[4..8],  conn.request_id, .little);
    std.mem.writeInt(u32, conn.body[8..12], payload_len,     .little);
    conn.resp_total_len = RESP_HDR_SIZE + payload_len;
}

// ---- Alert push fan-out ----

/// AggWorker invokes this (via alert.PushFn) whenever a threshold crossing is
/// durably appended to alert_log. We fan the push out to every connection that
/// has `wants_alerts` set, but only when this node is the current leader of
/// the event's partition. Replicas persist the entry and stay silent.
fn on_alert_crossing(ctx: *anyopaque, entry: alert_mod.AlertEntry, log_index: u64) void {
    const io: *RealIO = @ptrCast(@alignCast(ctx));

    // Every crossing is durable by the time this callback fires.
    metrics.alerts_recorded.inc();

    // Only the current leader of the event's partition pushes live alerts.
    const partition: u16 = @intCast(entry.account_id % N_PARTITIONS);
    if (!g_vsr_init) return;
    if (g_partition_map.leader_for(partition) != g_node_id) return;

    // Build the push payload once.
    var payload: proto.AlertPushPayload = std.mem.zeroes(proto.AlertPushPayload);
    payload.record.offset       = log_index;
    payload.record.account_id   = entry.account_id;
    if (g_agg_worker.registry.get(entry.metric_code)) |schema| {
        @memcpy(&payload.record.metric_code_str, &schema.code_str);
        if (entry.threshold_index < schema.alert_count) {
            @memcpy(&payload.record.threshold_code,
                    &schema.alert_thresholds[entry.threshold_index].code);
        }
    }
    payload.record.value_at_cross  = entry.value_at_cross;
    payload.record.event_timestamp = entry.event_timestamp;
    payload.node_id                = g_node_id;

    broadcast_alert_push(io, &payload);
}

/// Queue an ALERT_PUSH send on every alert-subscribed client connection.
/// Asynchronous via io_uring — fire-and-forget; TCP backpressure handles slow consumers.
/// Broadcast updated partition map to all connected clients.
/// Uses a simplified approach: sets a flag so the next request from each client
/// triggers a partition map refresh (via REDIRECT if needed).
/// Full broadcast of the 24KB payload is deferred to avoid saturating connections.
fn broadcast_partition_map(_: *RealIO) void {
    // In the current architecture, clients discover the new partition map
    // through REDIRECT responses when they send to the wrong leader.
    // A full partition_map_upd broadcast would require 24KB per connection.
    // For now we log the change; clients will self-correct within one request.
    std.log.info("partition map updated, clients will discover via redirects", .{});
}

fn broadcast_alert_push(io: *RealIO, payload: *const proto.AlertPushPayload) void {
    for (&pool) |*conn| {
        if (!conn.in_use or !conn.wants_alerts) continue;
        push_to_conn(io, conn, payload) catch continue;
        metrics.alerts_pushed.inc();
    }
}

const PUSH_FRAME_LEN: usize = RESP_HDR_SIZE + proto.ALERT_PUSH_PAYLOAD_SIZE;

fn push_to_conn(io: *RealIO, conn: *Conn, payload: *const proto.AlertPushPayload) !void {
    // Use the tail of conn.body reserved for async server-initiated sends.
    // Broadcast frame reuses the ResponseHeader layout but overloads the status
    // byte as a packet-type tag (request_id == 0 signals broadcast; the status
    // byte then tells the client which kind of broadcast this is — e.g.
    // alert_push (0x35) vs partition_map_upd (0x11)).
    const frame_start = MAX_BODY_LEN - PUSH_FRAME_LEN;
    const hdr = frame_start;
    conn.body[hdr]     = @intFromEnum(proto.PacketType.alert_push);
    conn.body[hdr + 1] = 0;
    conn.body[hdr + 2] = 0;
    conn.body[hdr + 3] = 0;
    std.mem.writeInt(u32, conn.body[hdr + 4..][0..4], 0, .little); // request_id = 0
    std.mem.writeInt(u32, conn.body[hdr + 8..][0..4], @intCast(proto.ALERT_PUSH_PAYLOAD_SIZE), .little);

    // Copy the payload right after the header.
    const pay = frame_start + RESP_HDR_SIZE;
    @memcpy(conn.body[pay..pay + proto.ALERT_PUSH_PAYLOAD_SIZE],
            std.mem.asBytes(payload));

    try io.queue_send(encode(.alert_push, conn.fd),
        conn.fd, conn.body[frame_start..frame_start + PUSH_FRAME_LEN]);
}

/// Serialize AggValue into body[RESP_HDR_SIZE..RESP_HDR_SIZE+56] as AggValueWire.
fn write_agg_value_wire(conn: *Conn, agg: *const AggValue) void {
    const off = RESP_HDR_SIZE;
    std.mem.writeInt(u64, conn.body[off..][0..8],      @truncate(agg.sum), .little);          // SumLo
    std.mem.writeInt(u64, conn.body[off + 8..][0..8],  @truncate(agg.sum >> 64), .little);    // SumHi
    std.mem.writeInt(u64, conn.body[off + 16..][0..8], agg.count, .little);
    std.mem.writeInt(u64, conn.body[off + 24..][0..8], agg.max, .little);
    std.mem.writeInt(u64, conn.body[off + 32..][0..8], agg.last_value, .little);
    std.mem.writeInt(i64, conn.body[off + 40..][0..8], agg.last_timestamp, .little);
    std.mem.writeInt(u64, conn.body[off + 48..][0..8], agg.alert_flags, .little);
}

fn build_ingest_resp(conn: *Conn, status: proto.StatusCode) void {
    const b = &conn.batch.result;
    std.mem.writeInt(u32, conn.body[RESP_HDR_SIZE..][0..4],  b.n_stored,    .little);
    std.mem.writeInt(u32, conn.body[RESP_HDR_SIZE + 4..][0..4], b.n_duplicates, .little);
    std.mem.writeInt(u64, conn.body[RESP_HDR_SIZE + 8..][0..8], b.last_offset,  .little);
    write_resp_hdr(conn, status, @intCast(proto.INGEST_RESPONSE_SIZE));
}

/// Props accessor for worker.apply_batch. Looks up props for events_buf[i]
/// via wire_indices and the per-wire-event props index stored in conn.
fn conn_props_of(conn: *Conn, i: usize) []const proto.PropPair {
    const wi = conn.wire_indices[i];
    const pc = conn.wire_props_counts[wi];
    if (pc == 0) return &.{};
    const po = conn.wire_props_offsets[wi];
    return conn.props_buf[po..po + pc];
}

fn finish_ingest_batch(conn: *Conn) void {
    const b = &conn.batch;
    const status: proto.StatusCode =
        if (b.result.n_stored == 0 and b.result.n_duplicates > 0)
            .duplicate
        else if (b.result.unknown_metrics)
            .unknown_metric
        else
            .ok;
    build_ingest_resp(conn, status);

    if (b.result.n_stored > 0) {
        if (conn.sync_mode) {
            metrics.events_ingested_sync.add(b.result.n_stored);
        } else {
            metrics.events_ingested_async.add(b.result.n_stored);
        }

        // Apply committed events to in-memory aggregates with props.
        // apply_batch caches schema lookups across consecutive events sharing
        // the same metric_code — a common pattern in real workloads.
        if (g_memtable_init) {
            g_agg_worker.apply_batch(
                conn.events_buf[0..b.n_events],
                conn,
                conn_props_of,
            ) catch {};
            // Track highest offset applied to aggregates for retention safety.
            const last_evt = conn.events_buf[b.n_events - 1];
            if (last_evt.offset >= g_last_agg_offset) {
                g_last_agg_offset = last_evt.offset + 1;
            }
        }
    }
    if (b.result.n_duplicates > 0) metrics.events_duplicate.add(b.result.n_duplicates);

    const now: i64 = @truncate(time_util.wallNanos());
    const dur: u64 = @intCast(@max(0, now - conn.start_ns));
    if (conn.sync_mode) {
        metrics.ingest_sync_duration.observe(dur);
    } else {
        metrics.ingest_async_duration.observe(dur);
    }
}

fn queue_seg_write(io: *RealIO, log: *UsageLog, conn: *Conn) !void {
    conn.state = .writing_seg;
    const events_bytes = std.mem.sliceAsBytes(conn.events_buf[0..conn.batch.n_events]);
    try io.queue_write(
        encode(.write, conn.fd),
        log.segment.file.fd(),
        events_bytes,
        log.segment.next_write_offset(),
    );
}

fn queue_send_resp(io: *RealIO, conn: *Conn) !void {
    conn.state     = .sending_resp;
    conn.resp_sent = 0;
    try io.queue_send(encode(.send, conn.fd), conn.fd, conn.body[0..conn.resp_total_len]);
}

fn send_error(io: *RealIO, conn: *Conn) !void {
    write_resp_hdr(conn, .err, 0);
    try queue_send_resp(io, conn);
}

fn send_redirect(io: *RealIO, conn: *Conn, leader_id: NodeId) !void {
    // Payload: leader's ingest address (ADDR_LEN bytes).
    @memcpy(conn.body[RESP_HDR_SIZE..RESP_HDR_SIZE + proto.ADDR_LEN],
            &g_cfg.node_addrs[leader_id]);
    write_resp_hdr(conn, .redirect, proto.ADDR_LEN);
    try queue_send_resp(io, conn);
}

fn save_registry(registry: *const MetricRegistry) !void {
    var path_buf: [512]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "{s}/metric_registry.bin", .{g_data_dir});
    try registry.save(path);
}

// ---- HTTP server ----

fn handle_http_accept(io: *RealIO, client_fd: i32) !void {
    if (http_conn_alloc(client_fd)) |conn| {
        metrics.http_connections_active.inc();
        try io.queue_recv(encode(.recv, client_fd), client_fd,
            conn.req_buf[0..]);
    } else {
        net_io.close(client_fd);
    }
}

fn handle_http_cqe(io: *RealIO, log: *UsageLog, tag: OpTag, fd: i32, res: i32) !void {
    switch (tag) {
        .recv => try handle_http_recv(io, log, fd, res),
        .send => try handle_http_send(io, fd, res),
        else  => {},
    }
}

fn handle_http_recv(io: *RealIO, log: *UsageLog, fd: i32, res: i32) !void {
    if (res <= 0) { http_conn_close(fd); return; }
    const conn = http_conn_get(fd) orelse return;
    conn.bytes_got += @intCast(res);

    switch (conn.state) {
        .reading_headers => {
            const buf = conn.req_buf[0..conn.bytes_got];
            if (std.mem.indexOf(u8, buf, "\r\n\r\n")) |i| {
                conn.headers_end    = i + 4;
                conn.content_length = parse_content_length(buf[0..conn.headers_end]);

                const body_received = conn.bytes_got - conn.headers_end;
                if (conn.content_length == 0 or body_received >= conn.content_length) {
                    handler.dispatch(conn, log, g_start_ns);
                    return try queue_http_send(io, conn);
                }
                conn.state = .reading_body;
                if (conn.bytes_got < conn.req_buf.len) {
                    try io.queue_recv(encode(.recv, fd), fd,
                        conn.req_buf[conn.bytes_got..]);
                } else {
                    http_error_resp(conn, "413 Content Too Large");
                    try queue_http_send(io, conn);
                }
            } else {
                if (conn.bytes_got < conn.req_buf.len) {
                    try io.queue_recv(encode(.recv, fd), fd,
                        conn.req_buf[conn.bytes_got..]);
                } else {
                    http_error_resp(conn, "431 Request Header Fields Too Large");
                    try queue_http_send(io, conn);
                }
            }
        },
        .reading_body => {
            const body_received = conn.bytes_got - conn.headers_end;
            if (body_received >= conn.content_length) {
                handler.dispatch(conn, log, g_start_ns);
                return try queue_http_send(io, conn);
            }
            if (conn.bytes_got < conn.req_buf.len) {
                try io.queue_recv(encode(.recv, fd), fd,
                    conn.req_buf[conn.bytes_got..]);
            } else {
                http_error_resp(conn, "413 Content Too Large");
                try queue_http_send(io, conn);
            }
        },
        .sending_response => {},
    }
}

fn handle_http_send(io: *RealIO, fd: i32, res: i32) !void {
    if (res <= 0) { http_conn_close(fd); return; }
    const conn = http_conn_get(fd) orelse return;
    conn.resp_sent += @intCast(res);

    if (conn.resp_sent < conn.resp_len) {
        try io.queue_send(encode(.send, fd), fd,
            conn.resp_buf[conn.resp_sent..conn.resp_len]);
        return;
    }
    http_conn_close(fd);
}

fn queue_http_send(io: *RealIO, conn: *HttpConn) !void {
    conn.state     = .sending_response;
    conn.resp_sent = 0;
    try io.queue_send(encode(.send, conn.fd), conn.fd,
        conn.resp_buf[0..conn.resp_len]);
}

fn http_error_resp(conn: *HttpConn, status: []const u8) void {
    const body = "{\"error\":\"request too large\"}";
    const full = std.fmt.bufPrint(&conn.resp_buf,
        "HTTP/1.1 {s}\r\nContent-Type: application/json\r\n" ++
        "Content-Length: {d}\r\nConnection: close\r\n\r\n{s}",
        .{ status, body.len, body }) catch return;
    conn.resp_len = full.len;
}

fn parse_content_length(headers: []const u8) usize {
    const needle = "content-length:";
    var pos: usize = 0;
    while (pos < headers.len) {
        const line_end = std.mem.indexOfPos(u8, headers, pos, "\r\n") orelse break;
        const line = headers[pos..line_end];
        if (line.len > needle.len) {
            var match = true;
            for (needle, 0..) |c, i| {
                if (std.ascii.toLower(line[i]) != c) { match = false; break; }
            }
            if (match) {
                const val = std.mem.trim(u8, line[needle.len..], " \t");
                return std.fmt.parseInt(usize, val, 10) catch 0;
            }
        }
        pos = line_end + 2;
    }
    return 0;
}

// ---- View change tick ----

var g_tick_ts: linux.kernel_timespec = .{ .sec = 0, .nsec = 100_000_000 };

fn queue_tick_timeout(io: *RealIO) !void {
    try io.queue_timeout(encode(.timeout, 0), &g_tick_ts);
}

// ---- Peer reconnect ----

/// Tick counter for peer reconnect. At 100ms per tick, 50 = 5 seconds.
var g_reconnect_tick: u32 = 0;
const RECONNECT_TICKS: u32 = 50;

fn maybe_peer_reconnect() void {
    if (!g_vsr_init) return;
    g_reconnect_tick += 1;
    if (g_reconnect_tick < RECONNECT_TICKS) return;
    g_reconnect_tick = 0;
    g_peer_pool.connect_all() catch {};
}

// ---- Data retention ----

/// Tick counter for the retention sweep. At 100ms per tick, 36000 = 1 hour.
var g_retention_tick: u32 = 0;
const RETENTION_SWEEP_TICKS: u32 = 36000;

fn maybe_retention_sweep(alloc: std.mem.Allocator) void {
    g_retention_tick += 1;
    if (g_retention_tick < RETENTION_SWEEP_TICKS) return;
    g_retention_tick = 0;

    const retention_ns: i64 = @as(i64, @intCast(g_cfg.retention_days)) * 24 * 3600 * std.time.ns_per_s;
    const cutoff_ns = @as(i64, @truncate(time_util.wallNanos())) - retention_ns;
    if (cutoff_ns <= 0) return;

    // Scan segment files and delete old ones.
    const dir_fd = disk_io.open_dir(g_data_dir) catch return;
    defer disk_io.close_dir(dir_fd);

    var it = disk_io.DirIter.init(dir_fd);
    var deleted: u32 = 0;
    while (it.next() catch null) |name| {
        if (name.len < 4) continue;
        if (!std.mem.eql(u8, name[name.len - 4 ..], ".seg")) continue;

        var seg_path_buf: [512]u8 = undefined;
        const seg_path = std.fmt.bufPrint(&seg_path_buf, "{s}/{s}", .{ g_data_dir, name }) catch continue;
        const base = name[0 .. name.len - 4]; // strip ".seg"

        // Safety guard: never delete segments that AggWorker hasn't processed.
        const seg_base_offset = std.fmt.parseInt(u64, base, 10) catch continue;

        const f = disk_io.open_ro(seg_path) catch continue;
        const file_size = f.size() catch { f.close(); continue; };
        var evt: Event = undefined;
        const n = f.read(std.mem.asBytes(&evt)) catch { f.close(); continue; };
        f.close();
        if (n != @sizeOf(Event)) continue;

        // Only delete if all events in this segment have been aggregated.
        const seg_end_offset = seg_base_offset + file_size / @sizeOf(Event);
        if (seg_end_offset > g_last_agg_offset) continue;

        // Check age: first event timestamp must be older than cutoff.
        if (evt.timestamp >= cutoff_ns) continue;

        {
            disk_io.remove(seg_path) catch {};
            var idx_buf: [512]u8 = undefined;
            var props_buf: [512]u8 = undefined;
            const idx_path = std.fmt.bufPrint(&idx_buf, "{s}/{s}.idx", .{ g_data_dir, base }) catch continue;
            const props_path = std.fmt.bufPrint(&props_buf, "{s}/{s}.props", .{ g_data_dir, base }) catch continue;
            disk_io.remove(idx_path) catch {};
            disk_io.remove(props_path) catch {};
            deleted += 1;
        }
    }

    if (deleted > 0) {
        std.log.info("retention: deleted {d} expired segments (cutoff: {d}d)", .{
            deleted, g_cfg.retention_days,
        });
    }

    // Evict closed periods from memtable.
    if (g_memtable_init) {
        // Evict using the smaller cutoff to be safe across fixed and calendar metrics.
        const period_ns: i64 = 30 * 24 * 3600 * std.time.ns_per_s;
        const cutoff_fixed: u32 = @intCast(@divFloor(cutoff_ns, period_ns));
        const cutoff_cal: u32 = aggregators.calendar_period_id_of(cutoff_ns, 1);
        const cutoff_period: u32 = @min(cutoff_fixed, cutoff_cal);
        checkpoint.evict_closed_periods(&g_memtable, &g_unique_sets, cutoff_period);

        // Save checkpoint after eviction with correct offset for recovery.
        var ckpt_buf: [512]u8 = undefined;
        const ckpt = std.fmt.bufPrint(&ckpt_buf, "{s}/checkpoint.bin", .{g_data_dir}) catch return;
        checkpoint.save(alloc, &g_memtable, g_last_agg_offset, ckpt) catch |err| {
            std.log.err("retention: checkpoint save failed: {}", .{err});
        };
    }
}

fn tick_all_partitions() void {
    if (!g_vsr_init) return;
    if (!g_peer_pool_init) return;

    const now: i64 = @truncate(time_util.wallNanos());
    for (0..N_PARTITIONS) |p| {
        const pi: u16 = @intCast(p);
        if (g_vsr[p].role == .leader) {
            const ping = g_vsr[p].make_ping();
            var ping_buf: [@sizeOf(PingMsg)]u8 = undefined;
            @memcpy(&ping_buf, std.mem.asBytes(&ping));
            for (g_partition_map.entries[p].replicas) |r| {
                if (r == cluster_mod.NO_NODE) continue;
                g_peer_pool.send(r, &ping_buf) catch {};
            }
        } else {
            if (g_vsr[p].tick(now)) |svc_msg| {
                var svc_buf: [@sizeOf(StartViewChangeMsg)]u8 = undefined;
                @memcpy(&svc_buf, std.mem.asBytes(&svc_msg));
                g_peer_pool.send_to_all(&svc_buf);
                process_start_view_change(pi, &svc_msg);
            }
        }
    }
}

fn process_start_view_change(p: u16, svc_msg: *const StartViewChangeMsg) void {
    const dvc = g_vsr[p].make_do_view_change(svc_msg.header.view_number);
    const new_leader = g_vsr[p].leader_node(dvc.header.view_number);
    var dvc_buf: [@sizeOf(DoViewChangeMsg)]u8 = undefined;
    @memcpy(&dvc_buf, std.mem.asBytes(&dvc));
    if (new_leader == g_node_id) {
        if (g_vsr[p].on_do_view_change(&dvc)) |sv_msg| {
            broadcast_start_view(p, &sv_msg);
        }
    } else {
        g_peer_pool.send(new_leader, &dvc_buf) catch {};
    }
}

fn broadcast_start_view(p: u16, sv_msg: *const StartViewMsg) void {
    var sv_buf: [@sizeOf(StartViewMsg)]u8 = undefined;
    @memcpy(&sv_buf, std.mem.asBytes(sv_msg));
    g_peer_pool.send_to_all(&sv_buf);
    g_vsr[p].on_start_view(sv_msg, @truncate(time_util.wallNanos()));
    g_partition_map.set_leader(p, sv_msg.header.from_node);
    std.log.info("partition {d}: new leader node {d} (view {})",
        .{ p, sv_msg.header.from_node, sv_msg.header.view_number });
}
