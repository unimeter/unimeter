//! Watermark: tracks the maximum event timestamp seen so far.
//! Events with timestamp below (watermark - grace_period_ns) are classified as late.

const std = @import("std");

pub const grace_period_ns: i64 = 5 * 60 * std.time.ns_per_s; // 5-minute grace window

/// Advance the watermark to max(watermark, event_ts).
pub fn advance(watermark: *i64, event_ts: i64) void {
    if (event_ts > watermark.*) watermark.* = event_ts;
}

/// Return true when event_ts is older than (watermark - grace_period_ns).
pub fn is_late(event_ts: i64, watermark: i64) bool {
    return event_ts < watermark - grace_period_ns;
}

// ---- tests ----

test "watermark: advance moves forward only" {
    var wm: i64 = 1000;
    advance(&wm, 2000);
    try std.testing.expectEqual(@as(i64, 2000), wm);
    advance(&wm, 500); // older — must not move back
    try std.testing.expectEqual(@as(i64, 2000), wm);
}

test "watermark: is_late" {
    const wm: i64 = 1_000_000_000_000; // 1000s in ns
    // Within grace window — not late.
    try std.testing.expect(!is_late(wm - grace_period_ns + 1, wm));
    // Exactly at boundary — not late.
    try std.testing.expect(!is_late(wm - grace_period_ns, wm));
    // One nanosecond past boundary — late.
    try std.testing.expect(is_late(wm - grace_period_ns - 1, wm));
}
