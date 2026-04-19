//! Time helpers that bypass std.Io for hot paths.
//!
//! In Zig 0.16 `std.time.nanoTimestamp` was removed. The canonical replacement
//! is `std.Io.Timestamp.now(io)`, which routes through an Io interface.
//! For measurement-only uses in performance-critical paths (hot ingest loop,
//! latency histograms, timestamps on committed events) this detour costs
//! nothing functionally but requires plumbing `io` everywhere.
//!
//! This module provides direct syscall helpers instead. They work on Linux
//! only (matching the project's platform constraint) and avoid the Io dance.

const std   = @import("std");
const linux = std.os.linux;

/// Return the current monotonic time in nanoseconds. Suitable for latency
/// measurement (not wall-clock) — never jumps backwards, not affected by NTP.
pub inline fn monoNanos() i64 {
    var ts: linux.timespec = undefined;
    _ = linux.clock_gettime(linux.CLOCK.MONOTONIC, &ts);
    return @as(i64, ts.sec) * std.time.ns_per_s + @as(i64, ts.nsec);
}

/// Return the current wall-clock time in nanoseconds (UNIX epoch).
/// Use for event timestamps that will be persisted and exchanged with clients.
pub inline fn wallNanos() i64 {
    var ts: linux.timespec = undefined;
    _ = linux.clock_gettime(linux.CLOCK.REALTIME, &ts);
    return @as(i64, ts.sec) * std.time.ns_per_s + @as(i64, ts.nsec);
}
