//! ClusterLog: replicated configuration log for MetricRegistry and PartitionMap.
//!
//! A single globally-elected leader (elected by the cluster, round-robin initially)
//! serialises all configuration changes. Replicas apply entries in op-number order.
//!
//! This module handles:
//!   - Entry serialisation / deserialisation
//!   - Append-only disk persistence (cluster_log.bin)
//!   - Recovery: replay the file from the beginning on startup
//!   - State machine: apply entries to MetricRegistry + PartitionMap
//!
//! Network replication (VSR PrepareMsg / PREPARE_OK) is wired in the server
//! integration (subtask 6). The atomic hot-swap pointer for the Ingest fast-path
//! is also connected there.
//!
//! Wire format — each entry on disk:
//!   [ClusterEntryHeader: 16B][payload: header.payload_len bytes]

const std = @import("std");

const disk_io       = @import("../io/disk_io.zig");
const MetricRegistry = @import("../usagelog/metric_registry.zig").MetricRegistry;
const MetricSchema   = @import("../usagelog/metric_registry.zig").MetricSchema;

const partition_map_mod = @import("partition_map.zig");
const PartitionMap  = partition_map_mod.PartitionMap;
const NodeId        = partition_map_mod.NodeId;

pub const CLUSTER_LOG_MAGIC: u32 = 0xC1057E01;

pub const ClusterEntryType = enum(u8) {
    put_metric              = 1, // upsert a MetricSchema; payload = MetricSchema (504B)
    delete_metric           = 2, // remove a metric;       payload = u64 (metric_code)
    update_partition_leader = 3, // change partition leader; payload = MapUpdatePayload (4B)
};

/// 16-byte on-disk / on-wire header for a single cluster log entry.
pub const ClusterEntryHeader = extern struct {
    magic:       u32,              // CLUSTER_LOG_MAGIC
    entry_type:  u8,               // ClusterEntryType value
    _pad:        [3]u8,
    op_number:   u32,              // monotonic, assigned by the current leader
    payload_len: u32,              // bytes that follow this header
};

comptime { std.debug.assert(@sizeOf(ClusterEntryHeader) == 16); }

/// Payload for update_partition_leader entries.
pub const MapUpdatePayload = extern struct {
    partition_id: u16,
    new_leader:   u8,
    _pad:         u8,
};

comptime { std.debug.assert(@sizeOf(MapUpdatePayload) == 4); }

/// Maximum payload size. Largest payload is MetricSchema (504B).
pub const MAX_PAYLOAD_LEN: u32 = @sizeOf(MetricSchema);

/// ClusterLog manages the in-memory state (MetricRegistry + PartitionMap) and
/// the append-only persistence file.
pub const ClusterLog = struct {
    alloc:         std.mem.Allocator,
    registry:      MetricRegistry,
    partition_map: PartitionMap,
    op_number:     u32,   // highest op_number applied so far
    log_file:      ?disk_io.RealFile,

    pub fn init(alloc: std.mem.Allocator) ClusterLog {
        return .{
            .alloc         = alloc,
            .registry      = MetricRegistry.init(alloc),
            .partition_map = PartitionMap.init_uniform(1),
            .op_number     = 0,
            .log_file      = null,
        };
    }

    pub fn deinit(self: *ClusterLog) void {
        self.registry.deinit();
        if (self.log_file) |f| f.close();
    }

    /// Open (or create) the persistence file at `path`. Must be called before
    /// append_to_disk(). Existing entries are NOT replayed — call recover() first.
    pub fn open(self: *ClusterLog, path: []const u8) !void {
        const f = try disk_io.open_rw(path);
        // Seek to end so subsequent writes append.
        try f.seek_end();
        self.log_file = f;
    }

    /// Recover state by replaying all entries in `path`.
    /// Creates an empty registry/partition_map if the file does not exist.
    pub fn recover(self: *ClusterLog, path: []const u8) !void {
        const f = disk_io.open_ro(path) catch |err| switch (err) {
            error.FileNotFound => return,
            else               => return err,
        };
        defer f.close();

        var hdr: ClusterEntryHeader = undefined;
        var payload_buf: [MAX_PAYLOAD_LEN]u8 = undefined;

        while (true) {
            const n = try f.read(std.mem.asBytes(&hdr));
            if (n == 0) break; // EOF
            if (n != @sizeOf(ClusterEntryHeader)) return error.CorruptClusterLog;
            if (hdr.magic != CLUSTER_LOG_MAGIC)   return error.CorruptClusterLog;

            if (hdr.payload_len > MAX_PAYLOAD_LEN) return error.OversizedPayload;
            const payload = payload_buf[0..hdr.payload_len];
            var total: usize = 0;
            while (total < hdr.payload_len) {
                const np = try f.read(payload[total..]);
                if (np == 0) break;
                total += np;
            }
            if (total != hdr.payload_len) return error.CorruptClusterLog;

            try self.apply_entry(hdr, payload);
        }
    }

    /// Append a committed entry to the persistence file.
    /// Must be called after apply_entry() succeeds.
    pub fn append_to_disk(self: *ClusterLog, hdr: ClusterEntryHeader, payload: []const u8) !void {
        const f = self.log_file orelse return error.LogFileNotOpen;
        try f.write_all(std.mem.asBytes(&hdr));
        try f.write_all(payload);
    }

    /// Apply a committed entry to the in-memory state.
    /// Used by both the leader (after quorum) and replicas (on receive).
    pub fn apply_entry(self: *ClusterLog, hdr: ClusterEntryHeader, payload: []const u8) !void {
        if (hdr.magic != CLUSTER_LOG_MAGIC) return error.InvalidMagic;
        if (hdr.payload_len != payload.len) return error.PayloadLenMismatch;

        const entry_type: ClusterEntryType = @enumFromInt(hdr.entry_type);

        switch (entry_type) {
            .put_metric => {
                if (payload.len != @sizeOf(MetricSchema)) return error.BadPayloadLen;
                const schema = std.mem.bytesAsValue(MetricSchema, payload[0..@sizeOf(MetricSchema)]).*;
                try self.registry.put(schema);
            },
            .delete_metric => {
                if (payload.len != @sizeOf(u64)) return error.BadPayloadLen;
                const code = std.mem.readInt(u64, payload[0..8], .little);
                self.registry.remove(code);
            },
            .update_partition_leader => {
                if (payload.len != @sizeOf(MapUpdatePayload)) return error.BadPayloadLen;
                const upd = std.mem.bytesAsValue(MapUpdatePayload, payload[0..@sizeOf(MapUpdatePayload)]).*;
                self.partition_map.set_leader(upd.partition_id, upd.new_leader);
            },
        }

        // op_number must be monotonically increasing.
        if (hdr.op_number > self.op_number) self.op_number = hdr.op_number;
    }

    /// Build a ClusterEntryHeader for the given type and payload.
    /// The caller is responsible for assigning a correct op_number.
    pub fn make_header(
        op:          u32,
        entry_type:  ClusterEntryType,
        payload_len: u32,
    ) ClusterEntryHeader {
        return .{
            .magic       = CLUSTER_LOG_MAGIC,
            .entry_type  = @intFromEnum(entry_type),
            ._pad        = .{0, 0, 0},
            .op_number   = op,
            .payload_len = payload_len,
        };
    }

    // ---- Convenience helpers for the leader to propose entries ----

    /// Serialise a put_metric operation. Returns the header + payload bytes
    /// that should be replicated and then passed to apply_entry().
    pub fn encode_put_metric(
        self:    *ClusterLog,
        schema:  MetricSchema,
        out_hdr: *ClusterEntryHeader,
        out_buf: *[MAX_PAYLOAD_LEN]u8,
    ) []const u8 {
        self.op_number += 1;
        out_hdr.* = make_header(self.op_number, .put_metric, @sizeOf(MetricSchema));
        const payload = out_buf[0..@sizeOf(MetricSchema)];
        @memcpy(payload, std.mem.asBytes(&schema));
        return payload;
    }

    pub fn encode_delete_metric(
        self:    *ClusterLog,
        code:    u64,
        out_hdr: *ClusterEntryHeader,
        out_buf: *[MAX_PAYLOAD_LEN]u8,
    ) []const u8 {
        self.op_number += 1;
        out_hdr.* = make_header(self.op_number, .delete_metric, @sizeOf(u64));
        std.mem.writeInt(u64, out_buf[0..8], code, .little);
        return out_buf[0..8];
    }

    pub fn encode_update_partition_leader(
        self:         *ClusterLog,
        partition_id: u16,
        new_leader:   NodeId,
        out_hdr:      *ClusterEntryHeader,
        out_buf:      *[MAX_PAYLOAD_LEN]u8,
    ) []const u8 {
        self.op_number += 1;
        out_hdr.* = make_header(self.op_number, .update_partition_leader, @sizeOf(MapUpdatePayload));
        const upd = MapUpdatePayload{ .partition_id = partition_id, .new_leader = new_leader, ._pad = 0 };
        @memcpy(out_buf[0..@sizeOf(MapUpdatePayload)], std.mem.asBytes(&upd));
        return out_buf[0..@sizeOf(MapUpdatePayload)];
    }
};

// ----------------------------------------------------------------------------
// Tests
// ----------------------------------------------------------------------------

test "cluster_log: put_metric and delete_metric" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();

    var log = ClusterLog.init(gpa.allocator());
    defer log.deinit();

    const fnv1a = @import("../usagelog/metric_registry.zig").fnv1a;

    // Build a schema.
    var schema = std.mem.zeroes(MetricSchema);
    schema.code = fnv1a("api_calls");
    schema.agg_type = @intFromEnum(@import("../usagelog/metric_registry.zig").AggType.count);

    // put_metric via encode + apply.
    var hdr: ClusterEntryHeader = undefined;
    var buf: [MAX_PAYLOAD_LEN]u8 = undefined;
    const put_payload = log.encode_put_metric(schema, &hdr, &buf);
    try log.apply_entry(hdr, put_payload);

    try std.testing.expect(log.registry.get(schema.code) != null);
    try std.testing.expectEqual(@as(u32, 1), log.op_number);

    // delete_metric.
    const del_payload = log.encode_delete_metric(schema.code, &hdr, &buf);
    try log.apply_entry(hdr, del_payload);

    try std.testing.expect(log.registry.get(schema.code) == null);
    try std.testing.expectEqual(@as(u32, 2), log.op_number);
}

test "cluster_log: update_partition_leader" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();

    var log = ClusterLog.init(gpa.allocator());
    defer log.deinit();

    var hdr: ClusterEntryHeader = undefined;
    var buf: [MAX_PAYLOAD_LEN]u8 = undefined;

    const payload = log.encode_update_partition_leader(5, 2, &hdr, &buf);
    try log.apply_entry(hdr, payload);

    // partition 5 should now have leader = 2.
    try std.testing.expectEqual(@as(NodeId, 2), log.partition_map.leader_for(5));
}

test "cluster_log: recover from disk" {
    const path = "/tmp/billing_cluster_log_test.bin";
    defer disk_io.remove(path) catch {};

    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();

    const fnv1a = @import("../usagelog/metric_registry.zig").fnv1a;

    // Write two entries via a log instance.
    {
        var log = ClusterLog.init(gpa.allocator());
        defer log.deinit();
        try log.open(path);

        var schema = std.mem.zeroes(MetricSchema);
        schema.code = fnv1a("bytes_in");
        schema.agg_type = @intFromEnum(@import("../usagelog/metric_registry.zig").AggType.sum);

        var hdr: ClusterEntryHeader = undefined;
        var buf: [MAX_PAYLOAD_LEN]u8 = undefined;

        const p1 = log.encode_put_metric(schema, &hdr, &buf);
        try log.apply_entry(hdr, p1);
        try log.append_to_disk(hdr, p1);

        const p2 = log.encode_update_partition_leader(10, 1, &hdr, &buf);
        try log.apply_entry(hdr, p2);
        try log.append_to_disk(hdr, p2);
    }

    // Recover state from disk in a fresh log.
    var log2 = ClusterLog.init(gpa.allocator());
    defer log2.deinit();
    try log2.recover(path);

    const fnv_bytes_in = fnv1a("bytes_in");
    try std.testing.expect(log2.registry.get(fnv_bytes_in) != null);
    try std.testing.expectEqual(@as(NodeId, 1), log2.partition_map.leader_for(10));
    try std.testing.expectEqual(@as(u32, 2), log2.op_number);
}

test "cluster_log: recover from non-existent file is a no-op" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();

    var log = ClusterLog.init(gpa.allocator());
    defer log.deinit();

    try log.recover("/tmp/billing_cluster_log_does_not_exist.bin");
    // No error, empty state.
    try std.testing.expectEqual(@as(u32, 0), log.op_number);
}

test "cluster_log: header layout is 16 bytes" {
    try std.testing.expectEqual(@as(usize, 16), @sizeOf(ClusterEntryHeader));
}
