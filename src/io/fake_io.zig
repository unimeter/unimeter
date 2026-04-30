//! FakeIO: deterministic I/O simulator for VOPR.
//! Provides in-memory substitutes for disk I/O used by Wal and Segment.
//! Virtual network message bus added in Subtask 3.

const std     = @import("std");
const wal_mod = @import("../usagelog/wal.zig");
const seg_mod = @import("../usagelog/segment.zig");

// Re-export helpers from real_io so callers don't need to switch import paths.
pub const OpTag      = @import("real_io.zig").OpTag;
pub const encode     = @import("real_io.zig").encode;
pub const decode_tag = @import("real_io.zig").decode_tag;
pub const decode_fd  = @import("real_io.zig").decode_fd;

// ---- In-memory file ----

const FakeFileData = struct {
    buf:       std.ArrayList(u8),
    cursor:    usize,
    ownership: enum { self_owned, storage_managed, read_only_copy } = .self_owned,
};

/// In-memory substitute for a real file.
/// Value-copyable: multiple copies share the same backing buffer via pointer.
/// Owner calls close() exactly once to release memory.
pub const FakeFile = struct {
    data:  *FakeFileData,
    alloc: std.mem.Allocator,

    pub fn create(alloc: std.mem.Allocator) !FakeFile {
        const d = try alloc.create(FakeFileData);
        d.* = .{ .buf = .empty, .cursor = 0 };
        return .{ .data = d, .alloc = alloc };
    }

    pub fn write_all(self: FakeFile, buf: []const u8) !void {
        const d = self.data;
        // Pad with zeros if cursor is ahead of buffer end (gap write).
        if (d.cursor > d.buf.items.len) {
            const gap = d.cursor - d.buf.items.len;
            for (0..gap) |_| try d.buf.append(self.alloc, 0);
        }
        if (d.cursor == d.buf.items.len) {
            // Common path: sequential append.
            try d.buf.appendSlice(self.alloc, buf);
        } else {
            // Overwrite path: used by seek_to + write (e.g. chaos corruption).
            if (d.cursor + buf.len > d.buf.items.len) {
                const extend = d.cursor + buf.len - d.buf.items.len;
                for (0..extend) |_| try d.buf.append(self.alloc, 0);
            }
            @memcpy(d.buf.items[d.cursor .. d.cursor + buf.len], buf);
        }
        d.cursor += buf.len;
    }

    pub fn read(self: FakeFile, buf: []u8) !usize {
        const d = self.data;
        if (d.cursor >= d.buf.items.len) return 0;
        const available = d.buf.items.len - d.cursor;
        const n = @min(buf.len, available);
        @memcpy(buf[0..n], d.buf.items[d.cursor .. d.cursor + n]);
        d.cursor += n;
        return n;
    }

    pub fn seek_to(self: FakeFile, pos: u64) !void {
        self.data.cursor = @intCast(pos);
    }

    pub fn seek_end(self: FakeFile) !void {
        self.data.cursor = self.data.buf.items.len;
    }

    /// No-op for in-memory storage. Chaos injection added in Subtask 4.
    pub fn sync(self: FakeFile) !void {
        _ = self;
    }

    pub fn size(self: FakeFile) !u64 {
        return @intCast(self.data.buf.items.len);
    }

    pub fn close(self: FakeFile) void {
        switch (self.data.ownership) {
            .self_owned => {
                self.data.buf.deinit(self.alloc);
                self.alloc.destroy(self.data);
            },
            .storage_managed => {}, // FakeStorage owns the data
            .read_only_copy => {
                // Free the FakeFileData struct but not the buf (shared with source).
                self.alloc.destroy(self.data);
            },
        }
    }

    /// Read-only view of the underlying buffer. Used by invariant checker.
    pub fn bytes(self: FakeFile) []const u8 {
        return self.data.buf.items;
    }

    /// Corrupt byte at position pos by XOR-ing with mask. Used by chaos tests.
    pub fn corrupt(self: FakeFile, pos: usize, mask: u8) void {
        if (pos < self.data.buf.items.len) {
            self.data.buf.items[pos] ^= mask;
        }
    }
};

// ---- In-memory storage for VOPR ----

/// FakeStorage: in-memory directory of named files.
/// Drop-in replacement for RealStorage in SegmentedWal.
pub const FakeStorage = struct {
    alloc: std.mem.Allocator,
    files: std.StringHashMap(*FakeFileData),

    pub const FileT = FakeFile;

    pub fn init(alloc: std.mem.Allocator) FakeStorage {
        return .{
            .alloc = alloc,
            .files = std.StringHashMap(*FakeFileData).init(alloc),
        };
    }

    pub fn deinit(self: *FakeStorage) void {
        var it = self.files.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.*.buf.deinit(self.alloc);
            self.alloc.destroy(entry.value_ptr.*);
            self.alloc.free(entry.key_ptr.*);
        }
        self.files.deinit();
    }

    pub fn make_path(self: *FakeStorage, path: []const u8) !void {
        _ = self;
        _ = path;
    }

    pub fn open_rw(self: *FakeStorage, path: []const u8) !FakeFile {
        if (self.files.get(path)) |data| {
            return FakeFile{ .data = data, .alloc = self.alloc };
        }
        const d = try self.alloc.create(FakeFileData);
        d.* = .{ .buf = .empty, .cursor = 0, .ownership = .storage_managed };
        const key = try self.alloc.dupe(u8, path);
        try self.files.put(key, d);
        return FakeFile{ .data = d, .alloc = self.alloc };
    }

    pub fn open_ro(self: *FakeStorage, path: []const u8) !FakeFile {
        const data = self.files.get(path) orelse return error.FileNotFound;
        // Create a separate FakeFileData for reading so cursor doesn't
        // conflict with ongoing writes to the same file.
        const rd = try self.alloc.create(FakeFileData);
        rd.* = .{ .buf = data.buf, .cursor = 0, .ownership = .read_only_copy };
        // Share the same buffer but independent cursor.
        // Note: rd.buf is a copy of the ArrayList struct (pointer + len + capacity),
        // pointing to the same backing memory. Reads see the same data.
        return FakeFile{ .data = rd, .alloc = self.alloc };
    }

    pub fn fallocate(_: *FakeStorage, file: FakeFile, len: u64) !void {
        const d = file.data;
        const target: usize = @intCast(len);
        while (d.buf.items.len < target) {
            try d.buf.append(file.alloc, 0);
        }
    }

    pub fn fsync_dir(_: *FakeStorage, _: []const u8) !void {}

    /// List segment indices (files matching seg_NNNNNN.log pattern) in a directory.
    pub fn list_segment_indices(self: *FakeStorage, dir: []const u8, out: *std.ArrayList(u32), alloc: std.mem.Allocator) !void {
        _ = alloc;
        var it = self.files.iterator();
        while (it.next()) |entry| {
            const path = entry.key_ptr.*;
            // Check path starts with dir + "/"
            if (!std.mem.startsWith(u8, path, dir)) continue;
            if (path.len <= dir.len + 1) continue;
            if (path[dir.len] != '/') continue;
            const name = path[dir.len + 1 ..];
            if (name.len == 14 and std.mem.startsWith(u8, name, "seg_") and
                std.mem.endsWith(u8, name, ".log"))
            {
                const idx = std.fmt.parseInt(u32, name[4..10], 10) catch continue;
                try out.append(self.alloc, idx);
            }
        }
    }

    /// Read-only: get raw bytes of a file. For invariant checks.
    pub fn file_bytes(self: *FakeStorage, path: []const u8) []const u8 {
        const data = self.files.get(path) orelse return &[_]u8{};
        return data.buf.items;
    }
};

// ---- In-memory WAL and Segment ----

/// In-memory WAL (low-level, used for unit tests).
pub const FakeWal = wal_mod.WalGeneric(FakeFile);

/// In-memory SegmentedWal for VOPR (uses same code path as production).
pub const FakeSegmentedWal = wal_mod.SegmentedWalGeneric(FakeStorage);

/// In-memory Segment for VOPR.
pub const FakeSegment = seg_mod.SegmentGeneric(FakeFile);

/// Create an empty FakeWal backed by in-memory storage.
pub fn fake_wal_create(alloc: std.mem.Allocator) !FakeWal {
    const f = try FakeFile.create(alloc);
    return FakeWal.init_with_file(f, 0);
}

/// Create an empty FakeSegment backed by in-memory storage.
pub fn fake_segment_create(alloc: std.mem.Allocator, base_offset: u64, max_events: u32) !FakeSegment {
    const seg_f = try FakeFile.create(alloc);
    const idx_f = try FakeFile.create(alloc);
    return FakeSegment.init_from_files(seg_f, idx_f, base_offset, max_events, 0);
}

// ---- FakeIO stub (network added in Subtask 3) ----

pub const FakeIO = struct {
    pub fn init() FakeIO {
        return .{};
    }
};

// ---- Tests ----

test "fake_file: sequential write and read" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var f = try FakeFile.create(alloc);
    defer f.close();

    try f.write_all("hello");
    try f.write_all(" world");
    try std.testing.expectEqual(@as(u64, 11), try f.size());

    try f.seek_to(0);
    var buf: [11]u8 = undefined;
    const n = try f.read(&buf);
    try std.testing.expectEqual(@as(usize, 11), n);
    try std.testing.expectEqualSlices(u8, "hello world", &buf);
}

test "fake_file: seek_end positions after last byte" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var f = try FakeFile.create(alloc);
    defer f.close();

    try f.write_all("abc");
    try f.seek_to(1);
    try f.seek_end();
    try f.write_all("d");

    try std.testing.expectEqualSlices(u8, "abcd", f.bytes());
}

test "fake_file: read returns 0 at eof" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var f = try FakeFile.create(alloc);
    defer f.close();

    try f.write_all("x");
    try f.seek_end();
    var buf: [4]u8 = undefined;
    const n = try f.read(&buf);
    try std.testing.expectEqual(@as(usize, 0), n);
}

test "fake_file: corrupt flips byte" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var f = try FakeFile.create(alloc);
    defer f.close();

    try f.write_all("ABC");
    f.corrupt(1, 0xFF); // flip 'B' (0x42) → 0xBD
    try std.testing.expectEqual(@as(u8, 0x42 ^ 0xFF), f.bytes()[1]);
}

test "fake_wal: append and recover_bytes" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var wal = try fake_wal_create(alloc);
    defer wal.deinit();

    try wal.append(.commit, "ping");
    try wal.append(.commit, "pong");
    try wal.sync();

    const data = wal.file.bytes();
    const entries = try wal_mod.recover_bytes(data, alloc);
    defer {
        for (entries) |e| alloc.free(e.payload);
        alloc.free(entries);
    }

    try std.testing.expectEqual(@as(usize, 2), entries.len);
    try std.testing.expectEqualSlices(u8, "ping", entries[0].payload);
    try std.testing.expectEqualSlices(u8, "pong", entries[1].payload);
}

test "fake_segment: append and read_at" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    const Event = @import("../event.zig").Event;

    var seg = try fake_segment_create(alloc, 0, 1000);
    defer seg.deinit();

    const ev = Event{
        .offset          = seg.next_offset(),
        .timestamp       = 42,
        .idempotency_key = 1,
        .account_id      = 99,
        .metric_code     = 7,
        .value           = 500,
        .operation_type  = 0,
        ._pad            = .{ 0, 0, 0 },
        .checksum        = 0,
    };
    var ev_mut = ev;
    try seg.append(&ev_mut);

    const got = try seg.read_at(0);
    try std.testing.expectEqual(ev.account_id, got.account_id);
    try std.testing.expectEqual(ev.value,      got.value);
}
