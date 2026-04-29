//! Disk file abstraction used by Wal and Segment.
//! RealFile wraps a raw Linux file descriptor for production.
//! FakeFile (see fake_io.zig) provides an in-memory substitute for VOPR.
//!
//! Uses std.os.linux.* syscalls directly rather than std.Io.File — the async
//! path already goes through io_uring, and blocking paths (open, recovery scan,
//! fsync, size) are simpler as direct syscalls than plumbing an Io handle
//! through every caller. Platform remains Linux-only.

const std   = @import("std");
const posix = std.posix;
const linux = std.os.linux;

/// Map a syscall return value to zig error. Negative values are -errno.
fn checkRc(rc: usize) !void {
    const signed: i64 = @bitCast(@as(u64, rc));
    if (signed < 0) return switch (@as(linux.E, @enumFromInt(@as(u32, @intCast(-signed))))) {
        .INTR      => error.Interrupted,
        .BADF      => error.InvalidHandle,
        .IO        => error.InputOutput,
        .NOSPC     => error.NoSpaceLeft,
        .FBIG      => error.FileTooBig,
        .ACCES     => error.AccessDenied,
        .PERM      => error.AccessDenied,
        .NOENT     => error.FileNotFound,
        .EXIST     => error.PathAlreadyExists,
        .ISDIR     => error.IsDir,
        .NOTDIR    => error.NotDir,
        .MFILE     => error.ProcessFdQuotaExceeded,
        .NFILE     => error.SystemFdQuotaExceeded,
        .NOMEM     => error.SystemResources,
        .SPIPE     => error.Unseekable,
        .OVERFLOW  => error.Unseekable,
        else       => error.Unexpected,
    };
}

/// Wraps a Linux file descriptor exposing the minimal interface required by
/// Wal and Segment. Callers that need the raw fd for io_uring use fd().
pub const RealFile = struct {
    inner: i32,

    pub fn write_all(self: RealFile, buf: []const u8) !void {
        var pos: usize = 0;
        while (pos < buf.len) {
            const rc = linux.write(self.inner, buf.ptr + pos, buf.len - pos);
            try checkRc(rc);
            pos += rc;
        }
    }

    pub fn read(self: RealFile, buf: []u8) !usize {
        const rc = linux.read(self.inner, buf.ptr, buf.len);
        try checkRc(rc);
        return rc;
    }

    pub fn seek_to(self: RealFile, pos: u64) !void {
        const rc = linux.lseek(self.inner, @intCast(pos), linux.SEEK.SET);
        try checkRc(rc);
    }

    pub fn seek_end(self: RealFile) !void {
        const rc = linux.lseek(self.inner, 0, linux.SEEK.END);
        try checkRc(rc);
    }

    pub fn sync(self: RealFile) !void {
        const rc = linux.fsync(self.inner);
        try checkRc(rc);
    }

    pub fn size(self: RealFile) !u64 {
        // Use lseek(SEEK_END) then back to current position to get size without fstat.
        const cur = linux.lseek(self.inner, 0, linux.SEEK.CUR);
        try checkRc(cur);
        const end = linux.lseek(self.inner, 0, linux.SEEK.END);
        try checkRc(end);
        const back = linux.lseek(self.inner, @intCast(cur), linux.SEEK.SET);
        try checkRc(back);
        return end;
    }

    /// Raw file descriptor for io_uring operations.
    pub fn fd(self: RealFile) i32 {
        return self.inner;
    }

    pub fn close(self: RealFile) void {
        _ = linux.close(self.inner);
    }
};

/// Pre-allocate file space without writing. Prevents metadata updates on
/// subsequent appends, keeping fdatasync fast. No-op if len is 0.
pub fn fallocate(file: RealFile, len: u64) !void {
    if (len == 0) return;
    const rc = linux.fallocate(file.inner, 0, 0, @intCast(len));
    try checkRc(rc);
}

/// Sync the directory entry (ensures newly created files survive crash).
pub fn fsync_dir(path: []const u8) !void {
    const fd = try open_dir(path);
    defer close_dir(fd);
    const rc = linux.fsync(fd);
    try checkRc(rc);
}

/// Open or create a file at path with read+write permissions, preserving contents.
/// Equivalent to `fs.cwd().createFile(path, .{ .truncate = false, .read = true })`.
pub fn open_rw(path: []const u8) !RealFile {
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    if (path.len >= path_buf.len) return error.NameTooLong;
    @memcpy(path_buf[0..path.len], path);
    path_buf[path.len] = 0;
    const path_z: [*:0]const u8 = @ptrCast(&path_buf);

    const flags: linux.O = .{
        .ACCMODE = .RDWR,
        .CREAT   = true,
    };
    const rc = linux.open(path_z, flags, 0o644);
    const signed: i64 = @bitCast(@as(u64, rc));
    if (signed < 0) try checkRc(rc);
    return .{ .inner = @intCast(rc) };
}

/// Open an existing file read-only. Returns error.FileNotFound if missing.
pub fn open_ro(path: []const u8) !RealFile {
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    if (path.len >= path_buf.len) return error.NameTooLong;
    @memcpy(path_buf[0..path.len], path);
    path_buf[path.len] = 0;
    const path_z: [*:0]const u8 = @ptrCast(&path_buf);

    const flags: linux.O = .{ .ACCMODE = .RDONLY };
    const rc = linux.open(path_z, flags, 0);
    const signed: i64 = @bitCast(@as(u64, rc));
    if (signed < 0) try checkRc(rc);
    return .{ .inner = @intCast(rc) };
}

fn nul_terminate(path: []const u8, buf: []u8) ![*:0]const u8 {
    if (path.len >= buf.len) return error.NameTooLong;
    @memcpy(buf[0..path.len], path);
    buf[path.len] = 0;
    return @ptrCast(buf.ptr);
}

/// Remove a file. Ignores error.FileNotFound.
pub fn remove(path: []const u8) !void {
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const z = try nul_terminate(path, &buf);
    const rc = linux.unlink(z);
    const signed: i64 = @bitCast(@as(u64, rc));
    if (signed < 0) {
        const e: linux.E = @enumFromInt(@as(u32, @intCast(-signed)));
        if (e == .NOENT) return;
        return checkRc(rc);
    }
}

/// Atomically rename a file.
pub fn rename(old_path: []const u8, new_path: []const u8) !void {
    var old_buf: [std.fs.max_path_bytes]u8 = undefined;
    var new_buf: [std.fs.max_path_bytes]u8 = undefined;
    const old_z = try nul_terminate(old_path, &old_buf);
    const new_z = try nul_terminate(new_path, &new_buf);
    const rc = linux.rename(old_z, new_z);
    try checkRc(rc);
}

/// Open a directory for iteration. Returns an fd to be passed to `DirIter.init`.
pub fn open_dir(path: []const u8) !i32 {
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const z = try nul_terminate(path, &buf);
    const flags: linux.O = .{
        .ACCMODE   = .RDONLY,
        .DIRECTORY = true,
    };
    const rc = linux.open(z, flags, 0);
    const signed: i64 = @bitCast(@as(u64, rc));
    if (signed < 0) try checkRc(rc);
    return @intCast(rc);
}

/// Close a dir fd opened with `open_dir`.
pub fn close_dir(fd: i32) void {
    _ = linux.close(fd);
}

/// Iterator over directory entries. Holds a read buffer for getdents64 syscall.
/// Skips "." and "..". Entry names point into the iterator's internal buffer
/// and are invalidated on the next call to next().
pub const DirIter = struct {
    fd:      i32,
    buf:     [4096]u8 align(8) = undefined,
    valid:   usize = 0,
    pos:     usize = 0,

    pub fn init(fd: i32) DirIter {
        return .{ .fd = fd };
    }

    pub fn next(self: *DirIter) !?[]const u8 {
        while (true) {
            if (self.pos >= self.valid) {
                const rc = linux.getdents64(self.fd, @ptrCast(&self.buf), self.buf.len);
                const signed: i64 = @bitCast(@as(u64, rc));
                if (signed < 0) try checkRc(rc);
                if (rc == 0) return null;
                self.valid = rc;
                self.pos   = 0;
            }

            const entry: *const linux.dirent64 = @ptrCast(@alignCast(&self.buf[self.pos]));
            const reclen: usize = entry.reclen;
            const name_offset = @offsetOf(linux.dirent64, "name");
            const name_start = self.pos + name_offset;
            self.pos += reclen;

            // Name is null-terminated within the record.
            const name_max_end = @min(self.valid, self.pos);
            var name_len: usize = 0;
            while (name_start + name_len < name_max_end and self.buf[name_start + name_len] != 0) {
                name_len += 1;
            }
            const name = self.buf[name_start .. name_start + name_len];
            if (std.mem.eql(u8, name, ".") or std.mem.eql(u8, name, "..")) continue;
            return name;
        }
    }
};

/// statfs result struct matching the Linux kernel layout for 64-bit architectures.
const StatfsResult = extern struct {
    f_type:    i64,
    f_bsize:   i64,
    f_blocks:  u64,
    f_bfree:   u64,
    f_bavail:  u64,
    f_files:   u64,
    f_ffree:   u64,
    f_fsid:    [2]i32,
    f_namelen: i64,
    f_frsize:  i64,
    f_flags:   i64,
    f_spare:   [4]i64,
};

/// Return the number of bytes available to unprivileged users on the filesystem
/// containing the given path. Opens the path as a directory, calls fstatfs(2),
/// then closes.
pub fn free_space(path: []const u8) !u64 {
    const fd = try open_dir(path);
    defer close_dir(fd);
    var stat: StatfsResult = undefined;
    const rc = linux.syscall2(
        .fstatfs,
        @as(u64, @bitCast(@as(i64, fd))),
        @intFromPtr(&stat),
    );
    try checkRc(rc);
    return @as(u64, @intCast(stat.f_bavail)) * @as(u64, @intCast(stat.f_bsize));
}

/// Recursively delete a directory tree. Best-effort for tests; ignores errors.
pub fn remove_tree(path: []const u8) void {
    const fd = open_dir(path) catch return;
    var it = DirIter.init(fd);
    while (it.next() catch null) |name| {
        var child_buf: [std.fs.max_path_bytes]u8 = undefined;
        const child = std.fmt.bufPrint(&child_buf, "{s}/{s}", .{ path, name }) catch continue;
        // Try file delete first; if it's a dir, recurse.
        remove(child) catch {
            remove_tree(child);
        };
    }
    close_dir(fd);
    // Remove the now-empty directory.
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    const z = nul_terminate(path, &buf) catch return;
    _ = linux.rmdir(z);
}

/// Recursively create directories (mkdir -p). No-op if already exists.
pub fn make_path(path: []const u8) !void {
    if (path.len == 0) return;
    var buf: [std.fs.max_path_bytes]u8 = undefined;
    if (path.len >= buf.len) return error.NameTooLong;
    @memcpy(buf[0..path.len], path);
    buf[path.len] = 0;

    // Walk components left-to-right, mkdir each partial.
    var i: usize = 0;
    while (i <= path.len) : (i += 1) {
        const is_sep = i < path.len and path[i] == '/';
        const is_end = i == path.len;
        if ((is_sep and i > 0) or is_end) {
            const save = buf[i];
            buf[i] = 0;
            const z: [*:0]const u8 = @ptrCast(&buf);
            const rc = linux.mkdir(z, 0o755);
            const signed: i64 = @bitCast(@as(u64, rc));
            if (signed < 0) {
                const e: linux.E = @enumFromInt(@as(u32, @intCast(-signed)));
                if (e != .EXIST) return checkRc(rc);
            }
            buf[i] = save;
        }
    }
}
