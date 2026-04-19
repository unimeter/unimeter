//! Root file for zig build test. Pulls in tests from all modules.

const std = @import("std");

test {
    _ = @import("event.zig");
    _ = @import("tools/fuzzer.zig");
    _ = @import("usagelog/wal.zig");
    _ = @import("usagelog/segment.zig");
    _ = @import("usagelog/dedup.zig");
    _ = @import("usagelog/metric_registry.zig");
    _ = @import("usagelog/usagelog.zig");
    _ = @import("usagelog/props.zig");
    _ = @import("aggstore/memtable.zig");
    _ = @import("aggstore/aggregators.zig");
    _ = @import("aggstore/unique_sets.zig");
    _ = @import("aggstore/watermark.zig");
    _ = @import("aggstore/alert.zig");
    _ = @import("aggstore/worker.zig");
    _ = @import("aggstore/checkpoint.zig");
    _ = @import("aggstore/query.zig");
    _ = @import("cluster/partition_map.zig");
    _ = @import("cluster/vsr.zig");
    _ = @import("cluster/peer_pool.zig");
    _ = @import("cluster/cluster_log.zig");
    _ = @import("io/fake_io.zig");
    _ = @import("simulator/cluster.zig");
    _ = @import("simulator/chaos.zig");
    _ = @import("simulator/invariants.zig");
    // Phase 5: observability
    _ = @import("metrics/registry.zig");
    _ = @import("metrics/exporter.zig");
    _ = @import("http/json.zig");
    _ = @import("http/handler.zig");
}
