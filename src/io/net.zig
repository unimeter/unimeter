//! Socket helpers using raw Linux syscalls.
//!
//! Zig 0.16 removed std.posix.socket/bind/listen/accept/connect/close/send/recv.
//! The canonical replacement is std.Io.net which routes through an Io handle,
//! but our server already uses io_uring for all runtime I/O — the socket()/bind()
//! pair only runs at startup, and even accept/close are proxied via io_uring.
//!
//! Using raw linux syscalls keeps the networking layer free of Io plumbing.

const std   = @import("std");
const posix = std.posix;
const linux = std.os.linux;

fn checkRc(rc: usize) !void {
    const signed: i64 = @bitCast(@as(u64, rc));
    if (signed < 0) return switch (@as(linux.E, @enumFromInt(@as(u32, @intCast(-signed))))) {
        .ADDRINUSE      => error.AddressInUse,
        .ADDRNOTAVAIL   => error.AddressNotAvailable,
        .ACCES          => error.AccessDenied,
        .PERM           => error.AccessDenied,
        .AFNOSUPPORT    => error.AddressFamilyNotSupported,
        .BADF           => error.InvalidHandle,
        .CONNREFUSED    => error.ConnectionRefused,
        .CONNRESET      => error.ConnectionResetByPeer,
        .HOSTUNREACH    => error.HostUnreachable,
        .INPROGRESS     => error.WouldBlock,
        .AGAIN          => error.WouldBlock,
        .INTR           => error.Interrupted,
        .INVAL          => error.InvalidArgument,
        .MFILE          => error.ProcessFdQuotaExceeded,
        .NFILE          => error.SystemFdQuotaExceeded,
        .NETUNREACH     => error.NetworkUnreachable,
        .NOBUFS         => error.SystemResources,
        .NOMEM          => error.SystemResources,
        .NOTCONN        => error.SocketNotConnected,
        .PIPE           => error.BrokenPipe,
        .PROTONOSUPPORT => error.ProtocolNotSupported,
        .TIMEDOUT       => error.ConnectionTimedOut,
        else            => error.Unexpected,
    };
}

/// Create a socket. `domain`/`type`/`protocol` match posix.AF.*/SOCK.*.
pub fn socket(domain: u32, sock_type: u32, protocol: u32) !i32 {
    const rc = linux.socket(domain, sock_type, protocol);
    const signed: i64 = @bitCast(@as(u64, rc));
    if (signed < 0) try checkRc(rc);
    return @intCast(rc);
}

pub fn close(fd: i32) void {
    _ = linux.close(fd);
}

pub fn bind(fd: i32, addr: *const posix.sockaddr, addrlen: posix.socklen_t) !void {
    const rc = linux.bind(fd, addr, addrlen);
    try checkRc(rc);
}

pub fn listen(fd: i32, backlog: u32) !void {
    const rc = linux.listen(fd, backlog);
    try checkRc(rc);
}

pub fn connect(fd: i32, addr: *const posix.sockaddr, addrlen: posix.socklen_t) !void {
    const rc = linux.connect(fd, addr, addrlen);
    try checkRc(rc);
}

/// Accept a connection. Returns the new socket fd.
pub fn accept(fd: i32, addr: ?*posix.sockaddr, addrlen: ?*posix.socklen_t) !i32 {
    const rc = linux.accept(fd, addr, addrlen);
    const signed: i64 = @bitCast(@as(u64, rc));
    if (signed < 0) try checkRc(rc);
    return @intCast(rc);
}

/// Send bytes over a connected socket. Partial writes handled by caller.
/// For TCP sockets, `write()` is slightly faster than `sendto(..., null)` since
/// it avoids the destination-address check in the kernel.
pub fn send(fd: i32, buf: []const u8, flags: u32) !usize {
    _ = flags;
    const rc = linux.write(fd, buf.ptr, buf.len);
    const signed: i64 = @bitCast(@as(u64, rc));
    if (signed < 0) try checkRc(rc);
    return rc;
}

pub fn recv(fd: i32, buf: []u8, flags: u32) !usize {
    _ = flags;
    const rc = linux.read(fd, buf.ptr, buf.len);
    const signed: i64 = @bitCast(@as(u64, rc));
    if (signed < 0) try checkRc(rc);
    return rc;
}
