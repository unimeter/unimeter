//! Startup validation. Runs before the server accepts connections to detect
//! data directory issues, WAL corruption, checkpoint corruption, and segment
//! file inconsistencies early.

const std     = @import("std");
const disk_io = @import("../io/disk_io.zig");
const wal_mod = @import("wal.zig");
const checkpoint = @import("../aggstore/checkpoint.zig");
const Event   = @import("../event.zig").Event;

const log = std.log.scoped(.startup_check);

pub const ValidateResult = struct {
    wal_entries: usize,
    wal_truncated: bool,
    checkpoint_offset: u64,
    segment_count: u64,
    segments_ok: u64,
    segments_bad: u64,
};

/// Run all startup checks on the data directory.
/// Logs warnings for recoverable issues, returns error for fatal ones.
pub fn validate(alloc: std.mem.Allocator, data_dir: []const u8) !ValidateResult {
    var result = ValidateResult{
        .wal_entries = 0,
        .wal_truncated = false,
        .checkpoint_offset = 0,
        .segment_count = 0,
        .segments_ok = 0,
        .segments_bad = 0,
    };

    // 1. Check data directory is writable.
    try check_writable(data_dir);
    log.info("data directory OK: {s}", .{data_dir});

    // 2. WAL CRC chain validation (segmented WAL directory).
    var wal_dir_buf: [512]u8 = undefined;
    const wal_dir = try std.fmt.bufPrint(&wal_dir_buf, "{s}/wal", .{data_dir});

    const entries = wal_mod.recover_segmented(wal_dir, alloc) catch |err| {
        log.warn("WAL recovery error: {}", .{err});
        return result;
    };
    defer {
        for (entries) |e| alloc.free(e.payload);
        alloc.free(entries);
    }
    result.wal_entries = entries.len;

    if (entries.len > 0) {
        log.info("WAL OK: {d} entries", .{result.wal_entries});
    } else {
        log.info("WAL empty", .{});
    }

    // 3. Checkpoint validation.
    var ckpt_path_buf: [512]u8 = undefined;
    const ckpt_path = try std.fmt.bufPrint(&ckpt_path_buf, "{s}/checkpoint.bin", .{data_dir});

    if (checkpoint.load(alloc, ckpt_path)) |ckpt_result| {
        var mt = ckpt_result.memtable;
        const count = mt.count();
        result.checkpoint_offset = ckpt_result.last_seg_offset;
        mt.deinit();
        log.info("checkpoint OK: {d} entries, last_seg_offset={d}", .{
            count, ckpt_result.last_seg_offset,
        });
    } else |err| switch (err) {
        error.FileNotFound => log.info("no checkpoint found", .{}),
        error.CorruptCheckpoint => {
            log.err("CORRUPT CHECKPOINT: magic/version/checksum mismatch", .{});
            return error.CorruptCheckpoint;
        },
        else => return err,
    }

    // 4. Segment file validation.
    const dir_fd = disk_io.open_dir(data_dir) catch |err| {
        log.warn("cannot open data directory for segment scan: {}", .{err});
        return result;
    };
    defer disk_io.close_dir(dir_fd);

    var it = disk_io.DirIter.init(dir_fd);
    while (try it.next()) |name| {
        if (!ends_with(name, ".seg")) continue;

        result.segment_count += 1;

        // Check file size is a multiple of Event size (64B).
        var seg_path_buf: [512]u8 = undefined;
        const seg_path = std.fmt.bufPrint(&seg_path_buf, "{s}/{s}", .{ data_dir, name }) catch continue;

        const f = disk_io.open_ro(seg_path) catch {
            result.segments_bad += 1;
            log.warn("cannot open segment: {s}", .{name});
            continue;
        };
        const sz = f.size() catch {
            f.close();
            result.segments_bad += 1;
            continue;
        };
        f.close();

        if (sz % @sizeOf(Event) != 0) {
            result.segments_bad += 1;
            log.warn("segment {s}: size {d} not aligned to {d}B events", .{
                name, sz, @sizeOf(Event),
            });
        } else {
            result.segments_ok += 1;
        }
    }

    if (result.segment_count > 0) {
        log.info("segments: {d} total, {d} OK, {d} bad", .{
            result.segment_count, result.segments_ok, result.segments_bad,
        });
    }

    return result;
}

fn check_writable(data_dir: []const u8) !void {
    var probe_buf: [512]u8 = undefined;
    const probe_path = try std.fmt.bufPrint(&probe_buf, "{s}/.startup_probe", .{data_dir});
    const f = disk_io.open_rw(probe_path) catch |err| {
        log.err("data directory not writable: {s}: {}", .{ data_dir, err });
        return error.DataDirNotWritable;
    };
    f.close();
    disk_io.remove(probe_path) catch {};
}

fn ends_with(haystack: []const u8, suffix: []const u8) bool {
    if (haystack.len < suffix.len) return false;
    return std.mem.eql(u8, haystack[haystack.len - suffix.len ..], suffix);
}

// ---- Tests ----

test "startup_check: validate empty data dir" {
    const dir = "/tmp/billing_startup_check_test";
    disk_io.make_path(dir) catch {};
    defer disk_io.remove_tree(dir);

    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();

    const result = try validate(gpa.allocator(), dir);
    try std.testing.expectEqual(@as(usize, 0), result.wal_entries);
    try std.testing.expect(!result.wal_truncated);
    try std.testing.expectEqual(@as(u64, 0), result.segment_count);
}

test "startup_check: validate with segmented WAL" {
    const dir = "/tmp/billing_startup_wal_test";
    disk_io.make_path(dir) catch {};
    defer disk_io.remove_tree(dir);

    // Write a small WAL using segmented format.
    {
        var wal = try wal_mod.wal_open_segmented(dir ++ "/wal", wal_mod.DEFAULT_SEGMENT_SIZE);
        defer wal.deinit();
        try wal.append(.commit, "test_payload");
        try wal.sync();
    }

    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();

    const result = try validate(gpa.allocator(), dir);
    try std.testing.expectEqual(@as(usize, 1), result.wal_entries);
}
