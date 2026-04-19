//! Benchmark tool. Measures latency and throughput of the ingest server.
//! Requires a running server on SERVER_PORT.
//! Run with: just bench-run

const std   = @import("std");
const posix = std.posix;
const net_io = @import("../io/net.zig");

const time_util = @import("../util/time.zig");

const protocol      = @import("../usagelog/protocol.zig");
const RequestHeader = protocol.RequestHeader;
const ResponseHeader = protocol.ResponseHeader;
const WireEvent     = protocol.WireEvent;
const PacketType    = protocol.PacketType;
const IngestPayloadHeader = protocol.IngestPayloadHeader;

const SERVER_HOST: u32 = 0x7F000001; // 127.0.0.1
const SERVER_PORT: u16 = 7001;

const WARMUP_ROUNDS:  u32 = 200;
const LATENCY_ROUNDS: u32 = 10_000;
const THROUGHPUT_N:   u32 = 50_000;
const BATCH_SIZE:     u32 = 1024; // must not exceed server MAX_EVENTS_PER_BATCH

// Metric codes used in the benchmark.
const METRICS = [_][64]u8{
    str64("api_calls"),
    str64("bytes_in"),
    str64("bytes_out"),
    str64("errors"),
};

pub fn main(init: std.process.Init) !void {
    const alloc = init.gpa;

    // --quick: skip the slow latency test (useful for cluster comparisons).
    var quick = false;
    var args_it = std.process.Args.Iterator.init(init.minimal.args);
    _ = args_it.next(); // skip argv[0]
    while (args_it.next()) |arg| {
        if (std.mem.eql(u8, arg, "--quick")) quick = true;
    }

    var out_buf: [4096]u8 = undefined;
    var fw = std.Io.File.stdout().writer(init.io, &out_buf);
    defer fw.interface.flush() catch {};

    if (!quick) {
        try bench_latency(&fw.interface);
        try fw.interface.flush();
    }
    try bench_throughput(alloc, &fw.interface);
    try fw.interface.flush();
    try bench_cluster(alloc, &fw.interface);
}

// ---------- latency ----------

fn bench_latency(w: *std.Io.Writer) !void {
    const fd = try tcp_connect();
    defer net_io.close(fd);

    var latencies: [LATENCY_ROUNDS]u64 = undefined;
    var prng = std.Random.DefaultPrng.init(42);
    const rng = prng.random();

    // Warmup: SYNC mode, 1 event per round.
    for (0..WARMUP_ROUNDS) |_| {
        try send_batch(fd, &.{make_wire_event(rng, 0)}, .ingest_sync);
    }

    for (&latencies) |*lat| {
        const t0: u64 = @intCast(time_util.wallNanos());
        try send_batch(fd, &.{make_wire_event(rng, 0)}, .ingest_sync);
        const t1: u64 = @intCast(time_util.wallNanos());
        lat.* = t1 - t0;
    }

    std.mem.sort(u64, &latencies, {}, std.sort.asc(u64));
    const p50  = latencies[LATENCY_ROUNDS * 50  / 100];
    const p99  = latencies[LATENCY_ROUNDS * 99  / 100];
    const p999 = latencies[LATENCY_ROUNDS * 999 / 1000];
    const max  = latencies[LATENCY_ROUNDS - 1];

    try w.print(
        \\benchmark: latency (sync, 1 event/roundtrip, {d} rounds)
        \\  p50:   {d} µs
        \\  p99:   {d} µs
        \\  p999:  {d} µs
        \\  max:   {d} µs
        \\
    , .{
        LATENCY_ROUNDS,
        p50  / 1000,
        p99  / 1000,
        p999 / 1000,
        max  / 1000,
    });
}

// ---------- throughput ----------

fn bench_throughput(alloc: std.mem.Allocator, w: *std.Io.Writer) !void {
    const fd = try tcp_connect();
    defer net_io.close(fd);

    // Pre-generate all events.
    var prng = std.Random.DefaultPrng.init(123);
    const rng = prng.random();

    const events = try alloc.alloc(WireEvent, THROUGHPUT_N);
    defer alloc.free(events);
    for (events, 0..) |*e, i| e.* = make_wire_event(rng, i);

    const t0: u64 = @intCast(time_util.wallNanos());

    // Send in batches; server limits body to BATCH_SIZE events.
    var sent: u32 = 0;
    while (sent < THROUGHPUT_N) {
        const end = @min(sent + BATCH_SIZE, THROUGHPUT_N);
        try send_batch(fd, events[sent..end], .ingest_async);
        sent = end;
    }

    const t1: u64 = @intCast(time_util.wallNanos());
    const elapsed_s = @as(f64, @floatFromInt(t1 - t0)) / 1e9;
    const events_per_sec: u64 = @intFromFloat(@as(f64, THROUGHPUT_N) / elapsed_s);

    try w.print(
        \\benchmark: throughput (async, {d} events, batch={d})
        \\  events/sec:  {d}
        \\  elapsed:     {d} ms
        \\
    , .{
        THROUGHPUT_N,
        BATCH_SIZE,
        events_per_sec,
        (t1 - t0) / 1_000_000,
    });
}

// ---------- cluster throughput ----------
//
// Spawns one thread per node. Each thread sends THROUGHPUT_N events with
// account_ids routed to that specific node (no REDIRECT overhead).
// Aggregate events/sec = (N_NODES × THROUGHPUT_N) / wall-clock time of
// the slowest thread — the true parallel throughput of the cluster.

const N_NODES:          usize = 3;
const N_NODES_U8:       u8    = N_NODES;
const CLUSTER_PORTS = [N_NODES]u16{ 7001, 7002, 7003 };
// Distinct account_ids per node s.t. (account_id % 256) % N_NODES == node_id.
const ACCOUNTS_PER_NODE: usize = 256 / N_NODES; // 85

const NodeCtx = struct {
    port:       u16,
    node_id:    u8,
    events:     []const WireEvent, // pre-allocated by caller
    eps:        u64  = 0,
    elapsed_ms: u64  = 0,
    failed:     bool = false,
};

fn node_bench_thread(ctx: *NodeCtx) void {
    const fd = tcp_connect_port(ctx.port) catch { ctx.failed = true; return; };
    defer net_io.close(fd);

    const t0: u64 = @intCast(time_util.wallNanos());

    var sent: u32 = 0;
    const total: u32 = @intCast(ctx.events.len);
    while (sent < total) {
        const end = @min(sent + BATCH_SIZE, total);
        send_batch(fd, ctx.events[sent..end], .ingest_async) catch { ctx.failed = true; return; };
        sent = end;
    }

    const t1: u64 = @intCast(time_util.wallNanos());
    const elapsed_s = @as(f64, @floatFromInt(t1 - t0)) / 1e9;
    ctx.elapsed_ms = (t1 - t0) / 1_000_000;
    ctx.eps = @intFromFloat(@as(f64, @floatFromInt(ctx.events.len)) / elapsed_s);
}

fn bench_cluster(alloc: std.mem.Allocator, w: *std.Io.Writer) !void {
    // Check which nodes are reachable.
    var n_active: u8 = 0;
    for (CLUSTER_PORTS) |port| {
        const fd = tcp_connect_port(port) catch break;
        net_io.close(fd);
        n_active += 1;
    }
    if (n_active < 2) {
        try w.print(
            "benchmark: cluster ({d}/3 nodes reachable — skipped; run `just cluster-up-d` first)\n\n",
            .{n_active});
        return;
    }

    // Pre-generate events for each node.
    var node_events: [N_NODES][]WireEvent = undefined;
    for (0..N_NODES) |i| {
        node_events[i] = try alloc.alloc(WireEvent, THROUGHPUT_N);
        var prng = std.Random.DefaultPrng.init(@intCast(42 + i * 7));
        const rng = prng.random();
        for (node_events[i], 0..) |*e, j| e.* = make_wire_event_node(rng, j, @intCast(i));
    }
    defer for (0..N_NODES) |i| alloc.free(node_events[i]);

    // Spawn one thread per node.
    var ctxs: [N_NODES]NodeCtx = undefined;
    var threads: [N_NODES]std.Thread = undefined;
    for (0..N_NODES) |i| {
        ctxs[i] = .{ .port = CLUSTER_PORTS[i], .node_id = @intCast(i), .events = node_events[i] };
        threads[i] = try std.Thread.spawn(.{}, node_bench_thread, .{&ctxs[i]});
    }
    for (0..N_NODES) |i| threads[i].join();

    // Report per-node then aggregate.
    var total_events: u64 = 0;
    var max_elapsed_ms: u64 = 0;
    var all_ok = true;
    for (0..N_NODES) |i| {
        if (ctxs[i].failed) { all_ok = false; continue; }
        total_events    += ctxs[i].events.len;
        max_elapsed_ms   = @max(max_elapsed_ms, ctxs[i].elapsed_ms);
    }

    try w.print(
        "benchmark: cluster throughput (async, {d} events × {d} nodes, batch={d})\n",
        .{ THROUGHPUT_N, n_active, BATCH_SIZE });
    for (0..N_NODES) |i| {
        if (ctxs[i].failed) {
            try w.print("  node {d} (:{d}):  FAILED\n", .{ i, ctxs[i].port });
        } else {
            try w.print("  node {d} (:{d}):  {d} events/sec  ({d} ms)\n",
                .{ i, ctxs[i].port, ctxs[i].eps, ctxs[i].elapsed_ms });
        }
    }
    if (all_ok and max_elapsed_ms > 0) {
        const agg: u64 = @intFromFloat(
            @as(f64, @floatFromInt(total_events)) /
            (@as(f64, @floatFromInt(max_elapsed_ms)) / 1000.0));
        try w.print("  aggregate:       {d} events/sec  ({d} total events)\n", .{ agg, total_events });
    }
    try w.print("\n", .{});
}

fn make_wire_event_node(rng: std.Random, idx: usize, node_id: u8) WireEvent {
    var e = std.mem.zeroes(WireEvent);
    // (account_id % 256) % N_NODES == node_id — all values < 256, no mod-wrap ambiguity.
    const step: u8 = @intCast(idx % ACCOUNTS_PER_NODE);
    e.account_id      = @as(u64, node_id) + @as(u64, step) * N_NODES;
    e.metric_code_str = METRICS[idx % METRICS.len];
    e.timestamp       = @intCast(time_util.wallNanos());
    e.value           = rng.uintLessThan(u64, 1_000_000_000);
    return e;
}

// ---------- protocol helpers ----------

fn send_batch(fd: posix.fd_t, events: []const WireEvent, ptype: PacketType) !void {
    const events_bytes = std.mem.sliceAsBytes(events);
    const payload_len: u32 = @intCast(@sizeOf(IngestPayloadHeader) + events_bytes.len);

    const hdr = RequestHeader{
        .magic       = protocol.MAGIC,
        .version     = protocol.VERSION,
        .packet_type = @intFromEnum(ptype),
        .partition   = 0xFFFF,
        .payload_len = payload_len,
        .request_id  = 0,
    };
    const ingest_hdr = IngestPayloadHeader{
        .event_count = @intCast(events.len),
        .props_count = 0,
    };

    try send_all(fd, std.mem.asBytes(&hdr));
    try send_all(fd, std.mem.asBytes(&ingest_hdr));
    try send_all(fd, events_bytes);

    var resp: ResponseHeader = undefined;
    try recv_all(fd, std.mem.asBytes(&resp));
    // Drain response payload (ingest response is 16B).
    if (resp.payload_len > 0) {
        var drain: [64]u8 = undefined;
        const to_read = @min(resp.payload_len, drain.len);
        try recv_all(fd, drain[0..to_read]);
    }
}

fn make_wire_event(rng: std.Random, idx: usize) WireEvent {
    var e = std.mem.zeroes(WireEvent);
    e.metric_code_str = METRICS[idx % METRICS.len];
    e.account_id      = rng.uintLessThan(u64, 1000);
    e.timestamp       = @intCast(time_util.wallNanos());
    e.value           = rng.uintLessThan(u64, 1_000_000_000);
    e.operation_type  = 0;
    return e;
}

// ---------- TCP helpers ----------

fn tcp_connect() !posix.fd_t { return tcp_connect_port(SERVER_PORT); }

fn tcp_connect_port(port: u16) !posix.fd_t {
    const fd = try net_io.socket(posix.AF.INET, posix.SOCK.STREAM, 0);
    errdefer net_io.close(fd);
    const addr = posix.sockaddr.in{
        .family = posix.AF.INET,
        .port   = std.mem.nativeToBig(u16, port),
        .addr   = std.mem.nativeToBig(u32, SERVER_HOST),
        .zero   = [_]u8{0} ** 8,
    };
    try net_io.connect(fd, @ptrCast(&addr), @sizeOf(@TypeOf(addr)));
    return fd;
}

fn send_all(fd: posix.fd_t, buf: []const u8) !void {
    var sent: usize = 0;
    while (sent < buf.len) sent += try net_io.send(fd, buf[sent..], 0);
}

fn recv_all(fd: posix.fd_t, buf: []u8) !void {
    var got: usize = 0;
    while (got < buf.len) got += try net_io.recv(fd, buf[got..], 0);
}

// ---------- util ----------

/// Build a null-padded [64]u8 from a string literal at compile time.
fn str64(comptime s: []const u8) [64]u8 {
    comptime {
        std.debug.assert(s.len <= 63);
    }
    var buf = [_]u8{0} ** 64;
    @memcpy(buf[0..s.len], s);
    return buf;
}
