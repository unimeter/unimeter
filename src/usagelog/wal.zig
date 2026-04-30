//! Write-Ahead Log. Append-only, CRC-chained.
//! Every entry carries the CRC of the previous entry, so any truncation or
//! corruption breaks the chain and stops recovery at that point.

const std     = @import("std");
const disk_io = @import("../io/disk_io.zig");

pub const EntryType = enum(u8) {
    commit     = 1, // payload = raw bytes of one or more Events
    checkpoint = 2, // payload = last committed segment base offset (u64 le)
};

/// Result of build_entry: byte length and CRC of the built entry.
pub const BuildResult = struct { len: usize, crc: u32 };

/// On-disk layout: EntryHeader immediately followed by payload bytes.
pub const EntryHeader = extern struct {
    prev_crc:    u32, // CRC32 of the previous entry (header+payload)
    payload_crc: u32, // CRC32 of this entry's payload
    payload_len: u32, // number of payload bytes following this header
    entry_type:  u8,
    _pad:        [3]u8 = .{ 0, 0, 0 },
};

comptime {
    std.debug.assert(@sizeOf(EntryHeader) == 16);
}

/// Generic WAL parameterised on a file type.
/// Production code uses Wal = WalGeneric(RealFile).
/// VOPR uses WalGeneric(FakeFile) from fake_io.zig.
pub fn WalGeneric(comptime FileT: type) type {
    return struct {
        file:         FileT,
        last_crc:     u32,
        write_offset: u64, // next byte position for the next write (async path)

        const Self = @This();

        pub fn init_with_file(file: FileT, write_offset: u64) Self {
            return .{ .file = file, .last_crc = 0, .write_offset = write_offset };
        }

        pub fn deinit(self: *Self) void {
            self.file.close();
        }

        /// Append one entry. Does not fsync — call sync() for durability.
        pub fn append(self: *Self, entry_type: EntryType, payload: []const u8) !void {
            const payload_crc = std.hash.Crc32.hash(payload);
            const header = EntryHeader{
                .prev_crc    = self.last_crc,
                .payload_crc = payload_crc,
                .payload_len = @intCast(payload.len),
                .entry_type  = @intFromEnum(entry_type),
            };
            const hdr_bytes = std.mem.asBytes(&header);

            var h = std.hash.Crc32.init();
            h.update(hdr_bytes);
            h.update(payload);
            const entry_crc = h.final();

            const entry_len = @sizeOf(EntryHeader) + payload.len;
            try self.file.write_all(hdr_bytes);
            try self.file.write_all(payload);
            self.last_crc      = entry_crc;
            self.write_offset += entry_len;
        }

        /// Flush OS buffers to storage.
        pub fn sync(self: *Self) !void {
            try self.file.sync();
        }

        // ---- Async-friendly interface for io_uring path ----

        /// Fill out_buf with a complete WAL entry (header + payload) ready for a
        /// single io_uring write. Returns (entry_len, new_crc) without touching
        /// self — call advance() after the write CQE arrives.
        ///
        /// out_buf must be at least 16 + payload.len bytes.
        pub fn build_entry(
            self: *const Self,
            entry_type: EntryType,
            payload:    []const u8,
            out_buf:    []u8,
        ) BuildResult {
            std.debug.assert(out_buf.len >= @sizeOf(EntryHeader) + payload.len);

            const payload_crc = std.hash.Crc32.hash(payload);
            const header = EntryHeader{
                .prev_crc    = self.last_crc,
                .payload_crc = payload_crc,
                .payload_len = @intCast(payload.len),
                .entry_type  = @intFromEnum(entry_type),
            };
            const hdr_bytes = std.mem.asBytes(&header);
            @memcpy(out_buf[0..@sizeOf(EntryHeader)], hdr_bytes);
            @memcpy(out_buf[@sizeOf(EntryHeader)..@sizeOf(EntryHeader) + payload.len], payload);

            var h = std.hash.Crc32.init();
            h.update(hdr_bytes);
            h.update(payload);

            const entry_len = @sizeOf(EntryHeader) + payload.len;
            return .{ .len = entry_len, .crc = h.final() };
        }

        /// Call after the io_uring write CQE completes successfully.
        pub fn advance(self: *Self, entry_len: usize, new_crc: u32) void {
            self.write_offset += entry_len;
            self.last_crc      = new_crc;
        }
    };
}

/// Production WAL backed by a real file.
pub const Wal = WalGeneric(disk_io.RealFile);

/// Open or create a WAL file at path for appending.
pub fn wal_open(path: []const u8) !Wal {
    const rf = try disk_io.open_rw(path);
    errdefer rf.close();
    const sz = try rf.size();
    try rf.seek_end();
    return Wal.init_with_file(rf, sz);
}

// ---- Segmented WAL ----

pub const DEFAULT_SEGMENT_SIZE: u64 = 256 * 1024 * 1024; // 256 MB

/// Production SegmentedWal backed by real files.
pub const SegmentedWal = SegmentedWalGeneric(RealStorage);

/// RealStorage: wraps disk_io for production use.
pub const RealStorage = struct {
    pub const FileT = disk_io.RealFile;

    _dummy: u8 = 0,

    pub fn make_path(_: *RealStorage, path: []const u8) !void {
        try disk_io.make_path(path);
    }

    pub fn open_rw(_: *RealStorage, path: []const u8) !FileT {
        return disk_io.open_rw(path);
    }

    pub fn open_ro(_: *RealStorage, path: []const u8) !FileT {
        return disk_io.open_ro(path);
    }

    pub fn fallocate(_: *RealStorage, file: FileT, len: u64) !void {
        try disk_io.fallocate(file, len);
    }

    pub fn fsync_dir(_: *RealStorage, path: []const u8) !void {
        try disk_io.fsync_dir(path);
    }

    pub fn list_segment_indices(_: *RealStorage, dir: []const u8, out: *std.ArrayList(u32), alloc: std.mem.Allocator) !void {
        const dfd = try disk_io.open_dir(dir);
        defer disk_io.close_dir(dfd);
        var it = disk_io.DirIter.init(dfd);
        while (try it.next()) |name| {
            if (name.len == 14 and std.mem.startsWith(u8, name, "seg_") and
                std.mem.endsWith(u8, name, ".log"))
            {
                const idx = std.fmt.parseInt(u32, name[4..10], 10) catch continue;
                try out.append(alloc, idx);
            }
        }
    }

    pub fn deinit(_: *RealStorage) void {}
};

fn segment_filename(dir: []const u8, index: u32, buf: []u8) ![]const u8 {
    return std.fmt.bufPrint(buf, "{s}/seg_{d:0>6}.log", .{ dir, index }) catch
        return error.NameTooLong;
}

/// Generic segmented WAL parameterised on storage backend.
/// Production uses RealStorage (disk_io). VOPR uses FakeStorage (in-memory).
pub fn SegmentedWalGeneric(comptime StorageT: type) type {
    const InnerWal = WalGeneric(StorageT.FileT);

    return struct {
        wal:             InnerWal,
        storage:         *StorageT,
        segment_index:   u32,
        segment_size:    u64,
        global_offset:   u64,
        dir_path:        [512]u8,
        dir_path_len:    u16,

        const Self = @This();

        // ---- Public interface (same for production and VOPR) ----

        /// File descriptor of the current segment (for io_uring). Only available on RealFile.
        pub fn fd(self: *const Self) i32 {
            return self.wal.file.fd();
        }

        pub fn write_offset(self: *const Self) u64 {
            return self.wal.write_offset;
        }

        pub fn last_crc_val(self: *const Self) u32 {
            return self.wal.last_crc;
        }

        pub fn build_entry(
            self: *const Self,
            entry_type: EntryType,
            payload:    []const u8,
            out_buf:    []u8,
        ) BuildResult {
            return self.wal.build_entry(entry_type, payload, out_buf);
        }

        pub fn advance(self: *Self, entry_len: usize, new_crc: u32) !void {
            self.wal.advance(entry_len, new_crc);
            self.global_offset += entry_len;
            if (self.wal.write_offset >= self.segment_size) {
                try self.rotate();
            }
        }

        pub fn append(self: *Self, entry_type: EntryType, payload: []const u8) !void {
            try self.wal.append(entry_type, payload);
            const entry_len = @sizeOf(EntryHeader) + payload.len;
            self.global_offset += entry_len;
            if (self.wal.write_offset >= self.segment_size) {
                try self.rotate();
            }
        }

        pub fn sync(self: *Self) !void {
            try self.wal.sync();
        }

        pub fn deinit(self: *Self) void {
            self.wal.deinit();
        }

        /// Raw bytes of the current segment file. For VOPR invariant checks.
        pub fn current_file(self: *const Self) StorageT.FileT {
            return self.wal.file;
        }

        // ---- Internal ----

        fn dir(self: *const Self) []const u8 {
            return self.dir_path[0..self.dir_path_len];
        }

        fn rotate(self: *Self) !void {
            self.wal.sync() catch {};
            self.wal.deinit();
            self.segment_index += 1;
            self.wal = try open_segment(
                self.storage,
                self.dir(),
                self.segment_index,
                self.segment_size,
                self.wal.last_crc,
            );
        }

        fn open_segment(storage: *StorageT, d: []const u8, index: u32, seg_size: u64, prev_crc: u32) !InnerWal {
            var path_buf: [512]u8 = undefined;
            const path = try segment_filename(d, index, &path_buf);
            const f = try storage.open_rw(path);
            errdefer f.close();
            storage.fallocate(f, seg_size) catch {};
            try f.seek_to(0);
            var wal = InnerWal.init_with_file(f, 0);
            wal.last_crc = prev_crc;
            return wal;
        }

        // ---- Open and Recover ----

        pub fn open(storage: *StorageT, d: []const u8, seg_size: u64, alloc: std.mem.Allocator) !Self {
            try storage.make_path(d);

            // Find existing segment indices.
            var indices: std.ArrayList(u32) = .empty;
            defer indices.deinit(alloc);
            storage.list_segment_indices(d, &indices, alloc) catch {};

            if (indices.items.len == 0) {
                var result = Self{
                    .wal           = try open_segment(storage, d, 0, seg_size, 0),
                    .storage       = storage,
                    .segment_index = 0,
                    .segment_size  = seg_size,
                    .global_offset = 0,
                    .dir_path      = undefined,
                    .dir_path_len  = @intCast(d.len),
                };
                @memcpy(result.dir_path[0..d.len], d);
                return result;
            }

            std.mem.sort(u32, indices.items, {}, std.sort.asc(u32));
            const max_index = indices.items[indices.items.len - 1];

            // Walk CRC chain across all segments to find prev_crc and data sizes.
            var prev_crc: u32 = 0;
            var total_bytes: u64 = 0;
            var last_seg_data_size: u64 = 0;

            for (indices.items) |idx| {
                var path_buf: [512]u8 = undefined;
                const path = segment_filename(d, idx, &path_buf) catch continue;
                const file = storage.open_ro(path) catch continue;
                defer file.close();

                const file_size = file.size() catch continue;
                if (file_size == 0) continue;

                const data = alloc.alloc(u8, @intCast(file_size)) catch continue;
                defer alloc.free(data);
                var read_total: usize = 0;
                while (read_total < data.len) {
                    const n = file.read(data[read_total..]) catch break;
                    if (n == 0) break;
                    read_total += n;
                }

                var pos: usize = 0;
                while (pos + @sizeOf(EntryHeader) <= read_total) {
                    var header: EntryHeader = undefined;
                    @memcpy(std.mem.asBytes(&header), data[pos .. pos + @sizeOf(EntryHeader)]);
                    if (header.prev_crc != prev_crc) break;
                    _ = std.enums.fromInt(EntryType, header.entry_type) orelse break;
                    const payload_end = pos + @sizeOf(EntryHeader) + header.payload_len;
                    if (payload_end > read_total) break;
                    const payload = data[pos + @sizeOf(EntryHeader) .. payload_end];
                    if (std.hash.Crc32.hash(payload) != header.payload_crc) break;

                    var h = std.hash.Crc32.init();
                    h.update(std.mem.asBytes(&header));
                    h.update(payload);
                    prev_crc = h.final();
                    pos = payload_end;
                }

                last_seg_data_size = pos;
                total_bytes += pos;
            }

            // Open last segment for appending.
            var path_buf: [512]u8 = undefined;
            const last_path = try segment_filename(d, max_index, &path_buf);
            const rf = try storage.open_rw(last_path);
            errdefer rf.close();
            try rf.seek_to(last_seg_data_size);

            var result = Self{
                .wal           = InnerWal.init_with_file(rf, last_seg_data_size),
                .storage       = storage,
                .segment_index = max_index,
                .segment_size  = seg_size,
                .global_offset = total_bytes,
                .dir_path      = undefined,
                .dir_path_len  = @intCast(d.len),
            };
            result.wal.last_crc = prev_crc;
            @memcpy(result.dir_path[0..d.len], d);
            return result;
        }

        pub fn recover(storage: *StorageT, d: []const u8, alloc: std.mem.Allocator) ![]RecoveryEntry {
            var indices: std.ArrayList(u32) = .empty;
            defer indices.deinit(alloc);
            storage.list_segment_indices(d, &indices, alloc) catch return &[_]RecoveryEntry{};

            if (indices.items.len == 0) return &[_]RecoveryEntry{};
            std.mem.sort(u32, indices.items, {}, std.sort.asc(u32));

            var all_entries: std.ArrayList(RecoveryEntry) = .empty;
            errdefer {
                for (all_entries.items) |e| alloc.free(e.payload);
                all_entries.deinit(alloc);
            }
            var prev_crc: u32 = 0;

            for (indices.items) |idx| {
                var path_buf: [512]u8 = undefined;
                const path = segment_filename(d, idx, &path_buf) catch continue;
                const file = storage.open_ro(path) catch continue;
                defer file.close();

                const sz = file.size() catch continue;
                if (sz == 0) continue;

                const data = alloc.alloc(u8, @intCast(sz)) catch continue;
                defer alloc.free(data);
                var total: usize = 0;
                while (total < data.len) {
                    const n = file.read(data[total..]) catch break;
                    if (n == 0) break;
                    total += n;
                }

                var pos: usize = 0;
                while (pos + @sizeOf(EntryHeader) <= total) {
                    var header: EntryHeader = undefined;
                    @memcpy(std.mem.asBytes(&header), data[pos .. pos + @sizeOf(EntryHeader)]);
                    if (header.prev_crc != prev_crc) break;
                    const entry_type = std.enums.fromInt(EntryType, header.entry_type) orelse break;
                    pos += @sizeOf(EntryHeader);
                    if (pos + header.payload_len > total) break;
                    const payload_slice = data[pos .. pos + header.payload_len];
                    if (std.hash.Crc32.hash(payload_slice) != header.payload_crc) break;

                    const payload = alloc.dupe(u8, payload_slice) catch break;
                    var h = std.hash.Crc32.init();
                    h.update(std.mem.asBytes(&header));
                    h.update(payload_slice);
                    prev_crc = h.final();
                    pos += header.payload_len;

                    all_entries.append(alloc, .{
                        .entry_type = entry_type,
                        .payload    = payload,
                    }) catch break;
                }
            }

            return all_entries.toOwnedSlice(alloc);
        }
    };
}

// Convenience wrappers for production (backward compat with callers).
pub fn wal_open_segmented(dir: []const u8, segment_size: u64) !SegmentedWal {
    var storage = RealStorage{};
    // Use DebugAllocator for CRC scan during open (freed after).
    var tmp_gpa = std.heap.DebugAllocator(.{}){};
    defer _ = tmp_gpa.deinit();
    return SegmentedWal.open(&storage, dir, segment_size, tmp_gpa.allocator());
}

pub fn recover_segmented(dir: []const u8, alloc: std.mem.Allocator) ![]RecoveryEntry {
    var storage = RealStorage{};
    return SegmentedWal.recover(&storage, dir, alloc);
}

// ---- Recovery ----

pub const RecoveryEntry = struct {
    entry_type: EntryType,
    payload:    []u8, // caller owns; free with the same allocator passed to recover*()
};

/// Parse all valid entries from a raw byte buffer, stopping at the first corrupt entry.
pub fn recover_bytes(data: []const u8, alloc: std.mem.Allocator) ![]RecoveryEntry {
    var entries: std.ArrayList(RecoveryEntry) = .empty;
    errdefer {
        for (entries.items) |e| alloc.free(e.payload);
        entries.deinit(alloc);
    }

    var pos: usize = 0;
    var prev_crc: u32 = 0;

    while (pos + @sizeOf(EntryHeader) <= data.len) {
        var header: EntryHeader = undefined;
        @memcpy(std.mem.asBytes(&header), data[pos .. pos + @sizeOf(EntryHeader)]);
        if (header.prev_crc != prev_crc) break;

        const entry_type = std.enums.fromInt(EntryType, header.entry_type) orelse break;
        pos += @sizeOf(EntryHeader);

        if (pos + header.payload_len > data.len) break;
        const payload_slice = data[pos .. pos + header.payload_len];
        if (std.hash.Crc32.hash(payload_slice) != header.payload_crc) break;

        const payload = try alloc.dupe(u8, payload_slice);
        errdefer alloc.free(payload);

        var h = std.hash.Crc32.init();
        h.update(std.mem.asBytes(&header));
        h.update(payload_slice);
        prev_crc = h.final();
        pos += header.payload_len;

        try entries.append(alloc, .{
            .entry_type = entry_type,
            .payload    = payload,
        });
    }

    return entries.toOwnedSlice(alloc);
}

/// Read all valid entries from a single WAL file.
pub fn recover(path: []const u8, alloc: std.mem.Allocator) ![]RecoveryEntry {
    const file = disk_io.open_ro(path) catch |err| switch (err) {
        error.FileNotFound => return &[_]RecoveryEntry{},
        else               => return err,
    };
    defer file.close();
    const sz = try file.size();
    const data = try alloc.alloc(u8, sz);
    defer alloc.free(data);
    var total: usize = 0;
    while (total < sz) {
        const n = try file.read(data[total..]);
        if (n == 0) break;
        total += n;
    }
    return recover_bytes(data[0..total], alloc);
}

// ---- Tests ----

test "wal: append and recover" {
    const path = "/tmp/billing_wal_test.wal";
    defer disk_io.remove(path) catch {};

    {
        var wal = try wal_open(path);
        defer wal.deinit();
        try wal.append(.commit, "first");
        try wal.append(.commit, "second");
        try wal.sync();
    }

    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    const entries = try recover(path, alloc);
    defer {
        for (entries) |e| alloc.free(e.payload);
        alloc.free(entries);
    }

    try std.testing.expectEqual(@as(usize, 2), entries.len);
    try std.testing.expectEqualSlices(u8, "first",  entries[0].payload);
    try std.testing.expectEqualSlices(u8, "second", entries[1].payload);
}

test "wal: missing file returns empty slice" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const entries = try recover("/tmp/billing_wal_no_such_file.wal", gpa.allocator());
    try std.testing.expectEqual(@as(usize, 0), entries.len);
}

test "wal: recover_bytes stops at corrupted entry" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    // Build two valid entries in memory using build_entry.
    var buf: [512]u8 = undefined;
    var fake_file: std.ArrayList(u8) = .empty;
    defer fake_file.deinit(alloc);

    var wal = Wal.init_with_file(disk_io.RealFile{ .inner = undefined }, 0);
    wal.last_crc = 0;

    // Entry 1
    const e1 = wal.build_entry(.commit, "hello", &buf);
    try fake_file.appendSlice(alloc, buf[0..e1.len]);
    wal.last_crc = e1.crc;

    // Entry 2
    const e2 = wal.build_entry(.commit, "world", &buf);
    try fake_file.appendSlice(alloc, buf[0..e2.len]);

    // Corrupt one byte inside entry 2's payload.
    const e2_payload_start = e1.len + @sizeOf(EntryHeader);
    fake_file.items[e2_payload_start] ^= 0xFF;

    const entries = try recover_bytes(fake_file.items, alloc);
    defer {
        for (entries) |e| alloc.free(e.payload);
        alloc.free(entries);
    }

    // Only entry 1 should survive.
    try std.testing.expectEqual(@as(usize, 1), entries.len);
    try std.testing.expectEqualSlices(u8, "hello", entries[0].payload);
}

test "segmented wal: rotation and CRC chain across segments" {
    const dir = "/tmp/billing_segwal_test";
    disk_io.remove_tree(dir);
    defer disk_io.remove_tree(dir);

    // Tiny segment size (512 bytes) to force rotation quickly.
    const seg_size: u64 = 512;

    // Write entries that will span multiple segments.
    // Each entry = 16B header + payload. "payload_XX" = 10B → entry = 26B.
    // 512 / 26 ≈ 19 entries per segment.
    const n_entries: usize = 80; // should produce ~4 segments

    {
        var wal = try wal_open_segmented(dir, seg_size);
        defer wal.deinit();

        for (0..n_entries) |i| {
            var payload: [10]u8 = undefined;
            _ = std.fmt.bufPrint(&payload, "payload_{d:0>2}", .{i}) catch unreachable;
            try wal.append(.commit, &payload);
        }
        try wal.sync();

        // Verify multiple segments were created.
        try std.testing.expect(wal.segment_index >= 3);
    }

    // Recovery: all entries must come back in order with valid CRC chain.
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    const entries = try recover_segmented(dir, alloc);
    defer {
        for (entries) |e| alloc.free(e.payload);
        alloc.free(entries);
    }

    try std.testing.expectEqual(n_entries, entries.len);

    // Verify first and last entries.
    try std.testing.expectEqualSlices(u8, "payload_00", entries[0].payload);

    var last_buf: [10]u8 = undefined;
    _ = std.fmt.bufPrint(&last_buf, "payload_{d:0>2}", .{n_entries - 1}) catch unreachable;
    try std.testing.expectEqualSlices(u8, &last_buf, entries[n_entries - 1].payload);
}

test "segmented wal: reopen preserves CRC chain" {
    const dir = "/tmp/billing_segwal_reopen_test";
    disk_io.remove_tree(dir);
    defer disk_io.remove_tree(dir);

    const seg_size: u64 = 512;

    // Write some entries, close, reopen, write more.
    {
        var wal = try wal_open_segmented(dir, seg_size);
        defer wal.deinit();
        try wal.append(.commit, "before_close");
        try wal.sync();
    }

    {
        var wal = try wal_open_segmented(dir, seg_size);
        defer wal.deinit();
        try wal.append(.commit, "after_reopen");
        try wal.sync();
    }

    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    const entries = try recover_segmented(dir, alloc);
    defer {
        for (entries) |e| alloc.free(e.payload);
        alloc.free(entries);
    }

    try std.testing.expectEqual(@as(usize, 2), entries.len);
    try std.testing.expectEqualSlices(u8, "before_close", entries[0].payload);
    try std.testing.expectEqualSlices(u8, "after_reopen", entries[1].payload);
}

test "segmented wal: empty dir returns empty recovery" {
    const dir = "/tmp/billing_segwal_empty_test";
    disk_io.remove_tree(dir);
    defer disk_io.remove_tree(dir);
    disk_io.make_path(dir) catch {};

    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();

    const entries = try recover_segmented(dir, gpa.allocator());
    try std.testing.expectEqual(@as(usize, 0), entries.len);
}

test "segmented wal: rotation exactly at segment boundary" {
    const dir = "/tmp/billing_segwal_boundary_test";
    disk_io.remove_tree(dir);
    defer disk_io.remove_tree(dir);

    // Entry with payload "x" = 16 + 1 = 17 bytes.
    // Segment size 51 = 3 entries exactly, rotation on 4th.
    const seg_size: u64 = 51;

    {
        var wal = try wal_open_segmented(dir, seg_size);
        defer wal.deinit();

        try wal.append(.commit, "a");
        try wal.append(.commit, "b");
        try wal.append(.commit, "c"); // fills segment 0 exactly (3×17=51)
        try std.testing.expectEqual(@as(u32, 1), wal.segment_index); // rotated
        try wal.append(.commit, "d"); // first entry in segment 1
        try wal.sync();
    }

    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    const entries = try recover_segmented(dir, alloc);
    defer {
        for (entries) |e| alloc.free(e.payload);
        alloc.free(entries);
    }

    try std.testing.expectEqual(@as(usize, 4), entries.len);
    try std.testing.expectEqualSlices(u8, "a", entries[0].payload);
    try std.testing.expectEqualSlices(u8, "d", entries[3].payload);
}

test "segmented wal: multiple reopen cycles across rotations" {
    const dir = "/tmp/billing_segwal_multi_reopen";
    disk_io.remove_tree(dir);
    defer disk_io.remove_tree(dir);

    const seg_size: u64 = 128;

    // 5 cycles: open → write 10 entries → close. Total 50 entries.
    // With small segments, should rotate multiple times across reopens.
    for (0..5) |cycle| {
        var wal = try wal_open_segmented(dir, seg_size);
        defer wal.deinit();
        for (0..10) |j| {
            var buf: [20]u8 = undefined;
            const payload = std.fmt.bufPrint(&buf, "c{d}_e{d}", .{ cycle, j }) catch unreachable;
            try wal.append(.commit, payload);
        }
        try wal.sync();
    }

    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    const entries = try recover_segmented(dir, alloc);
    defer {
        for (entries) |e| alloc.free(e.payload);
        alloc.free(entries);
    }

    try std.testing.expectEqual(@as(usize, 50), entries.len);
    try std.testing.expectEqualSlices(u8, "c0_e0", entries[0].payload);
    try std.testing.expectEqualSlices(u8, "c4_e9", entries[49].payload);
}

test "segmented wal: build_entry + advance triggers rotation" {
    const dir = "/tmp/billing_segwal_advance_test";
    disk_io.remove_tree(dir);
    defer disk_io.remove_tree(dir);

    const seg_size: u64 = 100;

    var wal = try wal_open_segmented(dir, seg_size);
    defer wal.deinit();

    // Simulate the io_uring async path: build_entry → write → advance.
    var out_buf: [256]u8 = undefined;
    const initial_seg = wal.segment_index;

    var i: usize = 0;
    while (i < 20) : (i += 1) {
        var payload_buf: [8]u8 = undefined;
        const payload = std.fmt.bufPrint(&payload_buf, "ev_{d:0>3}", .{i}) catch unreachable;
        const built = wal.build_entry(.commit, payload, &out_buf);

        // Simulate write (in real server, io_uring does this).
        try wal.wal.file.seek_to(wal.wal.write_offset);
        try wal.wal.file.write_all(out_buf[0..built.len]);

        try wal.advance(built.len, built.crc);
    }
    try wal.sync();

    // Verify rotation happened.
    try std.testing.expect(wal.segment_index > initial_seg);

    // Recovery must find all 20 entries.
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    const entries = try recover_segmented(dir, alloc);
    defer {
        for (entries) |e| alloc.free(e.payload);
        alloc.free(entries);
    }

    try std.testing.expectEqual(@as(usize, 20), entries.len);
    try std.testing.expectEqualSlices(u8, "ev_000", entries[0].payload);
    try std.testing.expectEqualSlices(u8, "ev_019", entries[19].payload);
}

test "segmented wal: recovery ignores trailing zeros in pre-allocated files" {
    const dir = "/tmp/billing_segwal_zeros_test";
    disk_io.remove_tree(dir);
    defer disk_io.remove_tree(dir);

    // Create a segment and write one entry. File will be pre-allocated
    // (fallocate) with zeros after the entry.
    const seg_size: u64 = 4096;
    {
        var wal = try wal_open_segmented(dir, seg_size);
        defer wal.deinit();
        try wal.append(.commit, "only_entry");
        try wal.sync();
    }

    // Verify file is pre-allocated (larger than data).
    var path_buf: [512]u8 = undefined;
    const path = try segment_filename(dir, 0, &path_buf);
    const f = try disk_io.open_ro(path);
    defer f.close();
    const file_size = try f.size();
    try std.testing.expect(file_size >= seg_size); // pre-allocated

    // Recovery must return exactly 1 entry despite file being 4KB.
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    const entries = try recover_segmented(dir, alloc);
    defer {
        for (entries) |e| alloc.free(e.payload);
        alloc.free(entries);
    }

    try std.testing.expectEqual(@as(usize, 1), entries.len);
    try std.testing.expectEqualSlices(u8, "only_entry", entries[0].payload);
}

test "segmented wal: global_offset tracks total bytes" {
    const dir = "/tmp/billing_segwal_global_test";
    disk_io.remove_tree(dir);
    defer disk_io.remove_tree(dir);

    const seg_size: u64 = 100;

    var wal = try wal_open_segmented(dir, seg_size);
    defer wal.deinit();

    const before = wal.global_offset;
    try wal.append(.commit, "hello"); // 16 + 5 = 21 bytes
    try std.testing.expectEqual(before + 21, wal.global_offset);

    // Write enough to trigger rotation.
    for (0..10) |_| {
        try wal.append(.commit, "padding_");
    }

    // global_offset must be monotonically increasing across rotations.
    try std.testing.expect(wal.global_offset > before + 21);
    try std.testing.expect(wal.segment_index > 0);
    // write_offset (within current segment) must be less than segment_size.
    try std.testing.expect(wal.write_offset() < seg_size);
}

test "segmented wal: large payload spanning near segment boundary" {
    const dir = "/tmp/billing_segwal_large_payload";
    disk_io.remove_tree(dir);
    defer disk_io.remove_tree(dir);

    // Segment size just above one large entry.
    // Large payload = 200 bytes, entry = 216 bytes. Segment = 250.
    // First entry fills most of segment. Second triggers rotation.
    const seg_size: u64 = 250;
    const large_payload = [_]u8{'X'} ** 200;

    {
        var wal = try wal_open_segmented(dir, seg_size);
        defer wal.deinit();

        try wal.append(.commit, &large_payload); // 216 bytes, fits
        try std.testing.expectEqual(@as(u32, 0), wal.segment_index);

        try wal.append(.commit, "small"); // 21 bytes, 216+21=237 < 250, fits
        try wal.append(.commit, "trigger"); // 23 bytes, 237+23=260 > 250, rotates
        try std.testing.expect(wal.segment_index >= 1);
        try wal.sync();
    }

    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    const entries = try recover_segmented(dir, alloc);
    defer {
        for (entries) |e| alloc.free(e.payload);
        alloc.free(entries);
    }

    try std.testing.expectEqual(@as(usize, 3), entries.len);
    try std.testing.expectEqual(@as(usize, 200), entries[0].payload.len);
    try std.testing.expectEqualSlices(u8, "small", entries[1].payload);
    try std.testing.expectEqualSlices(u8, "trigger", entries[2].payload);
}
