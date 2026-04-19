//! Checkpoint: atomic persistence of the current aggregate state.
//!
//! File format:
//!   CheckpointHeader (48 bytes)
//!   [AggKey + AggValue] × entry_count, sorted by AggKey for binary search
//!
//! Atomicity: write to <path>.tmp → fsync → rename to <path>.
//! On crash before rename: .tmp is left behind and ignored.
//!
//! Recovery: load checkpoint → AggWorker.run_once(last_seg_offset).

const std = @import("std");

const disk_io  = @import("../io/disk_io.zig");
const AggKey   = @import("memtable.zig").AggKey;
const AggValue = @import("memtable.zig").AggValue;
const Memtable = @import("memtable.zig").Memtable;
const UniqueSets = @import("unique_sets.zig").UniqueSets;

pub const MAGIC:   u32 = 0xACC10001; // ACC = aggregate checkpoint
pub const VERSION: u8  = 1;

/// On-disk header. 64 bytes (one cache line).
pub const CheckpointHeader = extern struct {
    magic:           u32,
    version:         u8,
    _pad:            [3]u8,
    entry_count:     u64, // number of (AggKey, AggValue) pairs that follow
    last_seg_offset: u64, // resume AggWorker from this offset after loading
    checksum:        u32, // CRC32 over the body (all entries bytes)
    _pad2:           [36]u8,
};

comptime {
    std.debug.assert(@sizeOf(CheckpointHeader) == 64);
}

/// One row in the checkpoint body.
pub const CheckpointEntry = extern struct {
    key:   AggKey,
    value: AggValue,
};

/// Save the current memtable state atomically.
/// last_seg_offset is the highest event offset applied (used for recovery).
pub fn save(alloc: std.mem.Allocator, memtable: *const Memtable, last_seg_offset: u64, path: []const u8) !void {
    var tmp_buf: [512]u8 = undefined;
    const tmp_path = try std.fmt.bufPrint(&tmp_buf, "{s}.tmp", .{path});

    // Collect and sort entries.
    const n = memtable.count();
    const entries = try alloc.alloc(CheckpointEntry, n);
    defer alloc.free(entries);

    {
        var i: usize = 0;
        var it = memtable.map.iterator();
        while (it.next()) |kv| {
            entries[i] = .{ .key = kv.key_ptr.*, .value = kv.value_ptr.* };
            i += 1;
        }
    }
    std.sort.pdq(CheckpointEntry, entries, {}, struct {
        fn lt(_: void, a: CheckpointEntry, b: CheckpointEntry) bool {
            return agg_key_less(a.key, b.key);
        }
    }.lt);

    // Compute body checksum.
    const body_bytes = std.mem.sliceAsBytes(entries);
    const body_crc = std.hash.Crc32.hash(body_bytes);

    // Write tmp file.
    {
        const f = try disk_io.open_rw(tmp_path);
        defer f.close();

        const hdr = CheckpointHeader{
            .magic           = MAGIC,
            .version         = VERSION,
            ._pad            = .{ 0, 0, 0 },
            .entry_count     = @intCast(n),
            .last_seg_offset = last_seg_offset,
            .checksum        = body_crc,
            ._pad2           = .{ 0 } ** 36,
        };
        try f.write_all(std.mem.asBytes(&hdr));
        try f.write_all(body_bytes);
        try f.sync();
    }

    try disk_io.rename(tmp_path, path);
}

/// Result of loading a checkpoint.
pub const LoadResult = struct {
    memtable:        Memtable,
    last_seg_offset: u64,
};

/// Load a checkpoint and reconstruct the memtable.
/// Returns error.FileNotFound if the checkpoint does not exist yet.
pub fn load(alloc: std.mem.Allocator, path: []const u8) !LoadResult {
    const f = try disk_io.open_ro(path);
    defer f.close();

    var hdr: CheckpointHeader = undefined;
    const n_hdr = try f.read(std.mem.asBytes(&hdr));
    if (n_hdr != @sizeOf(CheckpointHeader)) return error.CorruptCheckpoint;
    if (hdr.magic != MAGIC or hdr.version != VERSION) return error.CorruptCheckpoint;

    const n = hdr.entry_count;
    const entries = try alloc.alloc(CheckpointEntry, n);
    defer alloc.free(entries);

    const body_bytes = std.mem.sliceAsBytes(entries);
    const n_body = try f.read(body_bytes);
    if (n_body != body_bytes.len) return error.CorruptCheckpoint;

    // Verify body checksum.
    if (std.hash.Crc32.hash(body_bytes) != hdr.checksum) return error.CorruptCheckpoint;

    var mt = Memtable.init(alloc);
    errdefer mt.deinit();

    for (entries) |entry| {
        try mt.map.put(entry.key, entry.value);
    }

    return LoadResult{
        .memtable        = mt,
        .last_seg_offset = hdr.last_seg_offset,
    };
}

/// Remove memtable entries for periods strictly older than closed_before_period.
/// Also removes corresponding UniqueSet entries.
/// Call after a successful checkpoint.
pub fn evict_closed_periods(
    memtable:            *Memtable,
    unique_sets:         *UniqueSets,
    closed_before_period: u32,
) void {
    // Collect stale keys first (cannot remove while iterating).
    var stale: [4096]AggKey = undefined;
    var stale_n: usize = 0;

    var it = memtable.map.keyIterator();
    while (it.next()) |key| {
        if (key.period_id < closed_before_period) {
            if (stale_n < stale.len) {
                stale[stale_n] = key.*;
                stale_n += 1;
            }
        }
    }

    for (stale[0..stale_n]) |key| {
        _ = memtable.map.remove(key);
        unique_sets.remove_key(key);
    }
}

fn agg_key_less(a: AggKey, b: AggKey) bool {
    if (a.account_id  != b.account_id)  return a.account_id  < b.account_id;
    if (a.period_id   != b.period_id)   return a.period_id   < b.period_id;
    if (a.metric_code != b.metric_code) return a.metric_code < b.metric_code;
    return a.filter_hash < b.filter_hash;
}

// ---- tests ----

test "checkpoint: save and load round-trip" {
    const path = "/tmp/billing_checkpoint_test.bin";
    defer disk_io.remove(path) catch {};
    defer disk_io.remove(path ++ ".tmp") catch {};

    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    // Build a memtable with known entries.
    var mt = Memtable.init(alloc);
    defer mt.deinit();

    const k1 = AggKey{ .account_id = 1, .period_id = 1, .metric_code = 10, .filter_hash = 0 };
    const k2 = AggKey{ .account_id = 2, .period_id = 1, .metric_code = 20, .filter_hash = 0 };
    (try mt.get_or_put(k1)).sum   = 1_000_000;
    (try mt.get_or_put(k1)).count = 5;
    (try mt.get_or_put(k2)).max   = 9_000_000;

    try save(alloc, &mt, 42, path);

    var result = try load(alloc, path);
    defer result.memtable.deinit();

    try std.testing.expectEqual(@as(u64, 42), result.last_seg_offset);
    try std.testing.expectEqual(@as(usize, 2), result.memtable.count());

    const v1 = result.memtable.get(k1).?;
    try std.testing.expectEqual(@as(u128, 1_000_000), v1.sum);
    try std.testing.expectEqual(@as(u64, 5), v1.count);

    const v2 = result.memtable.get(k2).?;
    try std.testing.expectEqual(@as(u64, 9_000_000), v2.max);
}

test "checkpoint: empty memtable" {
    const path = "/tmp/billing_checkpoint_empty.bin";
    defer disk_io.remove(path) catch {};

    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var mt = Memtable.init(alloc);
    defer mt.deinit();

    try save(alloc, &mt, 0, path);

    var result = try load(alloc, path);
    defer result.memtable.deinit();
    try std.testing.expectEqual(@as(usize, 0), result.memtable.count());
}

test "checkpoint: load returns FileNotFound when absent" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();

    const err = load(gpa.allocator(), "/tmp/billing_checkpoint_absent_xyz.bin");
    try std.testing.expectError(error.FileNotFound, err);
}

test "checkpoint: evict_closed_periods removes old entries" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var mt = Memtable.init(alloc);
    defer mt.deinit();
    var us = UniqueSets.init(alloc);
    defer us.deinit();

    for (0..5) |p| {
        const key = AggKey{ .account_id = 1, .period_id = @intCast(p), .metric_code = 1, .filter_hash = 0 };
        _ = try mt.get_or_put(key);
    }
    try std.testing.expectEqual(@as(usize, 5), mt.count());

    evict_closed_periods(&mt, &us, 3); // remove periods 0, 1, 2

    try std.testing.expectEqual(@as(usize, 2), mt.count()); // only periods 3, 4 remain
    const k3 = AggKey{ .account_id = 1, .period_id = 3, .metric_code = 1, .filter_hash = 0 };
    const k4 = AggKey{ .account_id = 1, .period_id = 4, .metric_code = 1, .filter_hash = 0 };
    try std.testing.expect(mt.get(k3) != null);
    try std.testing.expect(mt.get(k4) != null);
}
