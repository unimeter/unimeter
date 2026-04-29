//! HTTP route handlers and HttpConn state type.
//! Handles: GET /health, GET /metrics, POST /v1/events, * → 404.
//! No keep-alive — each connection is closed after one response.

const std       = @import("std");
const time_util = @import("../util/time.zig");
const UsageLog  = @import("../usagelog/usagelog.zig").UsageLog;
const metrics   = @import("../metrics/metrics.zig");
const exporter  = @import("../metrics/exporter.zig");
const json_mod  = @import("json.zig");
const WireEvent = @import("../usagelog/protocol.zig").WireEvent;

pub const HTTP_REQ_BUF:  usize = 16384;
pub const HTTP_RESP_BUF: usize = 16384;
/// Bytes reserved at the start of resp_buf for HTTP headers before the body.
const RESP_HEADER_RESERVE: usize = 512;

pub const HttpConnState = enum { reading_headers, reading_body, sending_response };

pub const HttpConn = struct {
    fd:             i32           = -1,
    in_use:         bool          = false,
    state:          HttpConnState = .reading_headers,
    bytes_got:      usize         = 0,
    content_length: usize         = 0,
    headers_end:    usize         = 0,
    req_buf:        [HTTP_REQ_BUF]u8  = undefined,
    resp_buf:       [HTTP_RESP_BUF]u8 = undefined,
    resp_len:       usize         = 0,
    resp_sent:      usize         = 0,
};

/// Dispatch a fully-received HTTP request. Fills conn.resp_buf and sets conn.resp_len.
/// `start_ns` is the server start time for /health uptime calculation.
pub fn dispatch(conn: *HttpConn, log: *UsageLog, start_ns: i64) void {
    const headers = conn.req_buf[0..conn.headers_end];
    const body    = conn.req_buf[conn.headers_end..conn.bytes_got];

    // Parse request line: "METHOD PATH HTTP/1.x\r\n..."
    const line_end = std.mem.indexOf(u8, headers, "\r\n") orelse 0;
    const line     = headers[0..line_end];
    const sp1      = std.mem.indexOf(u8, line, " ") orelse 0;
    const method   = line[0..sp1];
    const rest     = if (sp1 < line.len) line[sp1 + 1..] else "";
    const sp2      = std.mem.indexOf(u8, rest, " ") orelse rest.len;
    const path     = rest[0..sp2];

    if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/health")) {
        respond_health(conn, start_ns);
    } else if (std.mem.eql(u8, method, "GET") and std.mem.eql(u8, path, "/metrics")) {
        respond_metrics(conn);
    } else if (std.mem.eql(u8, method, "POST") and std.mem.eql(u8, path, "/v1/events")) {
        respond_events(conn, log, body);
    } else {
        respond_not_found(conn);
    }
}

// ---- Route handlers ----

fn respond_health(conn: *HttpConn, start_ns: i64) void {
    const now: i64 = @truncate(time_util.wallNanos());
    const uptime_s = @divTrunc(@max(0, now - start_ns), 1_000_000_000);

    var body_buf: [64]u8 = undefined;
    const body = std.fmt.bufPrint(&body_buf,
        "{{\"status\":\"ok\",\"uptime_seconds\":{d}}}", .{uptime_s})
        catch "{\"status\":\"ok\"}";

    build_response(conn, "200 OK", "application/json", body);
}

fn respond_metrics(conn: *HttpConn) void {
    // Render Prometheus body into the reserved area after the header space.
    const body_area = conn.resp_buf[RESP_HEADER_RESERVE..];
    const body = exporter.render(body_area) catch {
        respond_error(conn, "500 Internal Server Error", "metrics render failed");
        return;
    };
    build_response_with_body_in_place(conn,
        "200 OK", "text/plain; version=0.0.4; charset=utf-8", body.len);
}

fn respond_events(conn: *HttpConn, log: *UsageLog, body: []const u8) void {
    if (body.len == 0) {
        build_response(conn, "400 Bad Request", "application/json",
            "{\"error\":\"empty body\"}");
        return;
    }

    var parsed: [1024]json_mod.ParsedEvent = undefined;
    const n = json_mod.parse_events(body, &parsed) catch {
        build_response(conn, "400 Bad Request", "application/json",
            "{\"error\":\"invalid json\"}");
        return;
    };

    if (n == 0) {
        build_response(conn, "200 OK", "application/json", "{\"ok\":true,\"count\":0}");
        return;
    }

    // Convert ParsedEvent → WireEvent slice.
    var wire_buf: [1024]WireEvent = undefined;
    var sync_mode = false;
    for (parsed[0..n], 0..) |*pe, i| {
        wire_buf[i] = pe.wire;
        if (pe.sync_mode) sync_mode = true;
    }
    const wire_events = wire_buf[0..n];

    const t0 = time_util.wallNanos();
    const result = log.ingest(wire_events, sync_mode) catch {
        respond_error(conn, "500 Internal Server Error", "ingest failed");
        return;
    };
    const dur: u64 = @intCast(@max(0, time_util.wallNanos() - t0));

    // Update metrics.
    if (result.n_stored > 0) {
        metrics.wal_writes.inc();
        metrics.wal_offset_bytes.set(@intCast(log.wal.global_offset));
        if (sync_mode) {
            metrics.wal_syncs.inc();
            metrics.wal_sync_duration.observe(dur);
            metrics.events_ingested_sync.add(result.n_stored);
            metrics.ingest_sync_duration.observe(dur);
        } else {
            metrics.events_ingested_async.add(result.n_stored);
            metrics.ingest_async_duration.observe(dur);
        }
    }
    if (result.n_duplicates > 0) metrics.events_duplicate.add(result.n_duplicates);

    var resp_body_buf: [64]u8 = undefined;
    const resp_body = std.fmt.bufPrint(&resp_body_buf,
        "{{\"ok\":true,\"count\":{d}}}", .{result.n_stored})
        catch "{\"ok\":true}";
    build_response(conn, "200 OK", "application/json", resp_body);
}

fn respond_not_found(conn: *HttpConn) void {
    build_response(conn, "404 Not Found", "application/json",
        "{\"error\":\"not found\"}");
}

fn respond_error(conn: *HttpConn, status: []const u8, msg: []const u8) void {
    var body_buf: [128]u8 = undefined;
    const body = std.fmt.bufPrint(&body_buf,
        "{{\"error\":\"{s}\"}}", .{msg}) catch "{\"error\":\"error\"}";
    build_response(conn, status, "application/json", body);
}

// ---- Response builders ----

/// Build a complete HTTP response for a small body (fits in resp_buf).
fn build_response(
    conn:         *HttpConn,
    status:       []const u8,
    content_type: []const u8,
    body:         []const u8,
) void {
    const full = std.fmt.bufPrint(&conn.resp_buf,
        "HTTP/1.1 {s}\r\nContent-Type: {s}\r\n" ++
        "Content-Length: {d}\r\nConnection: close\r\n\r\n{s}",
        .{ status, content_type, body.len, body })
        catch {
            // Buffer too small: send a minimal error.
            const fallback = "HTTP/1.1 500 Internal Server Error\r\n" ++
                "Content-Length: 0\r\nConnection: close\r\n\r\n";
            @memcpy(conn.resp_buf[0..fallback.len], fallback);
            conn.resp_len = fallback.len;
            return;
        };
    conn.resp_len = full.len;
}

/// Build headers for a body already rendered into resp_buf[RESP_HEADER_RESERVE..].
/// Moves the body immediately after the headers.
fn build_response_with_body_in_place(
    conn:         *HttpConn,
    status:       []const u8,
    content_type: []const u8,
    body_len:     usize,
) void {
    const hdrs = std.fmt.bufPrint(conn.resp_buf[0..RESP_HEADER_RESERVE],
        "HTTP/1.1 {s}\r\nContent-Type: {s}\r\n" ++
        "Content-Length: {d}\r\nConnection: close\r\n\r\n",
        .{ status, content_type, body_len })
        catch {
            const fallback = "HTTP/1.1 500 Internal Server Error\r\n" ++
                "Content-Length: 0\r\nConnection: close\r\n\r\n";
            @memcpy(conn.resp_buf[0..fallback.len], fallback);
            conn.resp_len = fallback.len;
            return;
        };
    const header_len = hdrs.len;
    // Move body from offset RESP_HEADER_RESERVE to offset header_len.
    // Source and dest may overlap, but dest starts before source, so copyForwards is safe.
    std.mem.copyForwards(u8,
        conn.resp_buf[header_len..header_len + body_len],
        conn.resp_buf[RESP_HEADER_RESERVE..RESP_HEADER_RESERVE + body_len]);
    conn.resp_len = header_len + body_len;
}

// ---- Tests ----

test "handler: GET /health sets resp_len > 0" {
    var conn = HttpConn{};
    // Simulate parsed GET /health request
    const req = "GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n";
    @memcpy(conn.req_buf[0..req.len], req);
    conn.headers_end = req.len;
    conn.bytes_got   = req.len;

    // No log needed for /health
    dispatch(&conn, undefined, 0);
    try std.testing.expect(conn.resp_len > 0);
    try std.testing.expect(std.mem.indexOf(u8,
        conn.resp_buf[0..conn.resp_len], "200 OK") != null);
    try std.testing.expect(std.mem.indexOf(u8,
        conn.resp_buf[0..conn.resp_len], "uptime_seconds") != null);
}

test "handler: unknown route returns 404" {
    var conn = HttpConn{};
    const req = "GET /nonexistent HTTP/1.1\r\n\r\n";
    @memcpy(conn.req_buf[0..req.len], req);
    conn.headers_end = req.len;
    conn.bytes_got   = req.len;

    dispatch(&conn, undefined, 0);
    try std.testing.expect(std.mem.indexOf(u8,
        conn.resp_buf[0..conn.resp_len], "404") != null);
}

test "handler: GET /metrics returns Prometheus text" {
    var conn = HttpConn{};
    const req = "GET /metrics HTTP/1.1\r\n\r\n";
    @memcpy(conn.req_buf[0..req.len], req);
    conn.headers_end = req.len;
    conn.bytes_got   = req.len;

    dispatch(&conn, undefined, 0);
    try std.testing.expect(conn.resp_len > 0);
    const resp = conn.resp_buf[0..conn.resp_len];
    try std.testing.expect(std.mem.indexOf(u8, resp, "# TYPE") != null);
    try std.testing.expect(std.mem.indexOf(u8, resp, "billing_") != null);
}
