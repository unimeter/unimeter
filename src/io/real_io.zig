//! RealIO: io_uring-based async I/O for production use.
//! All operations are submitted to the SQ ring and completed via CQ ring —
//! single syscall per batch, no per-operation context switches.

const std = @import("std");
const time_util = @import("../util/time.zig");
const linux = std.os.linux;
const posix = std.posix;

/// Tags packed into the upper 32 bits of io_uring user_data.
/// Lower 32 bits hold the file descriptor (0 for timer ops).
pub const OpTag = enum(u32) {
    accept     = 0,
    recv       = 1,
    send       = 2,
    fsync      = 3,
    write      = 4, // disk write; conn fd encoded in lower 32 bits of user_data
    connect    = 5, // async TCP connect to a peer node
    timeout    = 6, // periodic timer for view-change tick (fd=0 in user_data)
    alert_push = 7, // server-initiated broadcast; completion should not touch conn state machine
    signal     = 8,  // signalfd read; used for graceful shutdown
    sync_group_timeout = 9,  // group commit delay expired
    sync_group_fsync   = 10, // group fsync completion (WAL or segment)
};

pub fn encode(tag: OpTag, fd: i32) u64 {
    const tag_bits: u64 = @as(u64, @intFromEnum(tag)) << 32;
    const fd_bits:  u64 = @as(u64, @as(u32, @bitCast(fd))); // i32→u32 (bits), u32→u64 (widen)
    return tag_bits | fd_bits;
}

pub fn decode_tag(user_data: u64) OpTag {
    return @enumFromInt(@as(u32, @truncate(user_data >> 32)));
}

pub fn decode_fd(user_data: u64) i32 {
    return @bitCast(@as(u32, @truncate(user_data)));
}

pub const RealIO = struct {
    // Zig 0.15: IO_Uring → IoUring
    ring: linux.IoUring,

    // Zig 0.15: init takes u16, not u13
    pub fn init(queue_depth: u16) !RealIO {
        return .{ .ring = try linux.IoUring.init(queue_depth, 0) };
    }

    pub fn deinit(self: *RealIO) void {
        self.ring.deinit();
    }

    // Zig 0.15: all socket operations use posix.fd_t (not socket_t)
    pub fn queue_accept(self: *RealIO, user_data: u64, fd: posix.fd_t) !void {
        _ = try self.ring.accept(user_data, fd, null, null, 0);
    }

    pub fn queue_recv(self: *RealIO, user_data: u64, fd: posix.fd_t, buf: []u8) !void {
        _ = try self.ring.recv(user_data, fd, .{ .buffer = buf }, 0);
    }

    pub fn queue_send(self: *RealIO, user_data: u64, fd: posix.fd_t, buf: []const u8) !void {
        _ = try self.ring.send(user_data, fd, buf, 0);
    }

    /// Queue an async disk write at the given byte offset.
    pub fn queue_write(self: *RealIO, user_data: u64, fd: posix.fd_t, buf: []const u8, offset: u64) !void {
        _ = try self.ring.write(user_data, fd, buf, offset);
    }

    pub fn queue_fsync(self: *RealIO, user_data: u64, fd: posix.fd_t) !void {
        _ = try self.ring.fsync(user_data, fd, 0);
    }

    /// Queue a one-shot timeout. CQE fires after `ts` elapses with res == -ETIME.
    /// `ts` must remain valid until io.submit() is called.
    pub fn queue_timeout(self: *RealIO, user_data: u64, ts: *const linux.kernel_timespec) !void {
        _ = try self.ring.timeout(user_data, ts, 0, 0);
    }

    /// Queue an async TCP connect. CQE.res == 0 on success, < 0 on error.
    pub fn queue_connect(
        self:    *RealIO,
        user_data: u64,
        fd:      posix.fd_t,
        addr:    *const posix.sockaddr,
        addrlen: posix.socklen_t,
    ) !void {
        _ = try self.ring.connect(user_data, fd, addr, addrlen);
    }

    /// Flush SQ ring to kernel. Returns number of submitted operations.
    pub fn submit(self: *RealIO) !u32 {
        return self.ring.submit();
    }

    /// Block until one CQE is available, then return it.
    /// Zig 0.15: copy_cqe takes no arguments.
    pub fn wait_cqe(self: *RealIO) !linux.io_uring_cqe {
        return self.ring.copy_cqe();
    }

    /// Monotonic time in nanoseconds.
    pub fn now_ns(_: *RealIO) i64 {
        return time_util.wallNanos();
    }
};
