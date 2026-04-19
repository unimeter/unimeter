//! Write-Ahead Log. Append-only, CRC-chained.
//! Every entry carries the CRC of the previous entry, so any truncation or
//! corruption breaks the chain and stops recovery at that point.

const std     = @import("std");
const disk_io = @import("../io/disk_io.zig");

pub const EntryType = enum(u8) {
    commit     = 1, // payload = raw bytes of one or more Events
    checkpoint = 2, // payload = last committed segment base offset (u64 le)
};

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
        ) struct { len: usize, crc: u32 } {
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

// ---- Recovery ----

pub const RecoveryEntry = struct {
    entry_type: EntryType,
    payload:    []u8, // caller owns; free with the same allocator passed to recover*()
};

/// Parse all valid entries from a raw byte buffer, stopping at the first corrupt entry.
/// Allows VOPR to test recovery on arbitrary (possibly corrupted) data without disk I/O.
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
        if (header.prev_crc != prev_crc) break; // chain broken
        pos += @sizeOf(EntryHeader);

        if (pos + header.payload_len > data.len) break; // truncated payload
        const payload_slice = data[pos .. pos + header.payload_len];
        if (std.hash.Crc32.hash(payload_slice) != header.payload_crc) break; // corrupt payload

        const payload = try alloc.dupe(u8, payload_slice);
        errdefer alloc.free(payload);

        var h = std.hash.Crc32.init();
        h.update(std.mem.asBytes(&header));
        h.update(payload_slice);
        prev_crc = h.final();
        pos += header.payload_len;

        try entries.append(alloc, .{
            .entry_type = @enumFromInt(header.entry_type),
            .payload    = payload,
        });
    }

    return entries.toOwnedSlice(alloc);
}

/// Read all valid entries from a WAL file, stopping at the first corrupt entry.
/// Returns an empty slice if the file does not exist.
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
