//! Prometheus text format renderer.
//! Renders all global metrics into a caller-provided buffer.
//! Returns the written slice or error.NoSpaceLeft if the buffer is too small.
//! A 16384-byte buffer comfortably fits all ~30 current metrics.

const std     = @import("std");
const metrics = @import("metrics.zig");
const reg     = @import("registry.zig");

/// Prometheus `le` label strings matching BUCKET_NS boundaries.
const BUCKET_LE = [reg.N_BUCKETS][]const u8{
    "0.0005", "0.001", "0.005", "0.01", "0.025",
    "0.05",   "0.1",   "0.25",  "0.5",  "1.0",
    "2.5",    "+Inf",
};

/// Render all global metrics into `buf` in Prometheus exposition format.
/// Returns the populated slice of `buf`.
pub fn render(buf: []u8) ![]u8 {
    var pos: usize = 0;

    // ---- Counters ----

    try wf(buf, &pos,
        "# HELP billing_events_ingested_total Total events successfully ingested\n" ++
        "# TYPE billing_events_ingested_total counter\n", .{});
    try wf(buf, &pos, "billing_events_ingested_total{{mode=\"async\"}} {d}\n",
        .{metrics.events_ingested_async.get()});
    try wf(buf, &pos, "billing_events_ingested_total{{mode=\"sync\"}} {d}\n",
        .{metrics.events_ingested_sync.get()});

    try wf(buf, &pos,
        "# HELP billing_events_duplicate_total Events rejected by dedup ring\n" ++
        "# TYPE billing_events_duplicate_total counter\n", .{});
    try wf(buf, &pos, "billing_events_duplicate_total {d}\n",
        .{metrics.events_duplicate.get()});

    try wf(buf, &pos,
        "# HELP billing_wal_writes_total WAL write completions\n" ++
        "# TYPE billing_wal_writes_total counter\n", .{});
    try wf(buf, &pos, "billing_wal_writes_total {d}\n",
        .{metrics.wal_writes.get()});

    try wf(buf, &pos,
        "# HELP billing_wal_syncs_total WAL fsync completions\n" ++
        "# TYPE billing_wal_syncs_total counter\n", .{});
    try wf(buf, &pos, "billing_wal_syncs_total {d}\n",
        .{metrics.wal_syncs.get()});

    try wf(buf, &pos,
        "# HELP billing_view_changes_total VSR leadership transitions\n" ++
        "# TYPE billing_view_changes_total counter\n", .{});
    try wf(buf, &pos, "billing_view_changes_total {d}\n",
        .{metrics.view_changes.get()});

    try wf(buf, &pos,
        "# HELP billing_alerts_recorded_total Threshold crossings durably appended to alert_log\n" ++
        "# TYPE billing_alerts_recorded_total counter\n", .{});
    try wf(buf, &pos, "billing_alerts_recorded_total {d}\n",
        .{metrics.alerts_recorded.get()});

    try wf(buf, &pos,
        "# HELP billing_alerts_pushed_total Alert frames queued for live subscribers by the leader\n" ++
        "# TYPE billing_alerts_pushed_total counter\n", .{});
    try wf(buf, &pos, "billing_alerts_pushed_total {d}\n",
        .{metrics.alerts_pushed.get()});

    // ---- Gauges ----

    try wf(buf, &pos,
        "# HELP billing_connections_active Active ingest TCP connections\n" ++
        "# TYPE billing_connections_active gauge\n", .{});
    try wf(buf, &pos, "billing_connections_active {d}\n",
        .{metrics.connections_active.get()});

    try wf(buf, &pos,
        "# HELP billing_http_connections_active Active HTTP connections\n" ++
        "# TYPE billing_http_connections_active gauge\n", .{});
    try wf(buf, &pos, "billing_http_connections_active {d}\n",
        .{metrics.http_connections_active.get()});

    try wf(buf, &pos,
        "# HELP billing_wal_offset_bytes Current WAL write offset in bytes\n" ++
        "# TYPE billing_wal_offset_bytes gauge\n", .{});
    try wf(buf, &pos, "billing_wal_offset_bytes {d}\n",
        .{metrics.wal_offset_bytes.get()});

    try wf(buf, &pos,
        "# HELP billing_alert_subscribers Connections with live alert push enabled\n" ++
        "# TYPE billing_alert_subscribers gauge\n", .{});
    try wf(buf, &pos, "billing_alert_subscribers {d}\n",
        .{metrics.alert_subscribers.get()});

    try wf(buf, &pos,
        "# HELP billing_agg_keys_total Live aggregate keys in memtable\n" ++
        "# TYPE billing_agg_keys_total gauge\n", .{});
    try wf(buf, &pos, "billing_agg_keys_total {d}\n",
        .{metrics.agg_keys_total.get()});

    try wf(buf, &pos,
        "# HELP billing_memtable_bytes Approximate memtable data footprint in bytes (excludes HashMap metadata)\n" ++
        "# TYPE billing_memtable_bytes gauge\n", .{});
    try wf(buf, &pos, "billing_memtable_bytes {d}\n",
        .{metrics.memtable_bytes.get()});

    try wf(buf, &pos,
        "# HELP billing_checkpoint_bytes Size of the most recent checkpoint file on disk\n" ++
        "# TYPE billing_checkpoint_bytes gauge\n", .{});
    try wf(buf, &pos, "billing_checkpoint_bytes {d}\n",
        .{metrics.checkpoint_bytes.get()});

    // ---- Histograms ----

    try render_histogram(buf, &pos,
        "billing_ingest_async_duration_seconds",
        "Async ingest end-to-end latency",
        &metrics.ingest_async_duration);

    try render_histogram(buf, &pos,
        "billing_ingest_sync_duration_seconds",
        "Sync ingest end-to-end latency",
        &metrics.ingest_sync_duration);

    try render_histogram(buf, &pos,
        "billing_wal_sync_duration_seconds",
        "WAL and segment fsync latency",
        &metrics.wal_sync_duration);

    return buf[0..pos];
}

// ---- Private helpers ----

fn render_histogram(
    buf:  []u8,
    pos:  *usize,
    name: []const u8,
    help: []const u8,
    h:    *const reg.Histogram,
) !void {
    try wf(buf, pos, "# HELP {s} {s}\n# TYPE {s} histogram\n",
        .{ name, help, name });

    var cumulative: u64 = 0;
    for (0..reg.N_BUCKETS) |i| {
        cumulative += h.get_bucket(i);
        try wf(buf, pos, "{s}_bucket{{le=\"{s}\"}} {d}\n",
            .{ name, BUCKET_LE[i], cumulative });
    }

    const sum_s: f64 = @as(f64, @floatFromInt(h.get_sum_ns())) / 1_000_000_000.0;
    try wf(buf, pos, "{s}_sum {d:.9}\n", .{ name, sum_s });
    try wf(buf, pos, "{s}_count {d}\n",  .{ name, h.get_count() });
}

/// Write a formatted string into buf at *pos. Advances *pos by bytes written.
fn wf(buf: []u8, pos: *usize, comptime fmt: []const u8, args: anytype) !void {
    const written = std.fmt.bufPrint(buf[pos.*..], fmt, args) catch
        return error.NoSpaceLeft;
    pos.* += written.len;
}

// ---- Tests ----

test "render: produces valid Prometheus text" {
    metrics.events_ingested_async.add(100);
    metrics.events_ingested_sync.add(50);
    metrics.connections_active.set(5);

    var buf: [16384]u8 = undefined;
    const out = try render(&buf);

    try std.testing.expect(std.mem.indexOf(u8, out,
        "billing_events_ingested_total{mode=\"async\"} 100") != null);
    try std.testing.expect(std.mem.indexOf(u8, out,
        "billing_connections_active 5") != null);
    try std.testing.expect(std.mem.indexOf(u8, out,
        "billing_ingest_async_duration_seconds_bucket") != null);
}

test "render: buffer too small returns error" {
    var tiny: [10]u8 = undefined;
    try std.testing.expectError(error.NoSpaceLeft, render(&tiny));
}
