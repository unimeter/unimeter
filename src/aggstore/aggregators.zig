//! Per-AggType update logic and period_id computation. Pure functions — no allocations, no I/O.
//!
//! Period modes:
//!   fixed    — floor(timestamp_ns / period_ns), default 30-day windows
//!   calendar — (year-2000)*12 + (month-1), adjusted for billing_cycle_day (1-28)
//!
//! resolve_period_id() dispatches based on MetricSchema.period_type.
//! nanos_to_ymd() uses Howard Hinnant's civil_from_days algorithm.
//!
//! COUNT UNIQUE: update() here only tracks last_seg_offset.
//! The caller must also call UniqueSets.update() and then set
//! agg.count = unique_sets.count(key).

const std    = @import("std");
const Event  = @import("../event.zig").Event;

pub const AggType  = @import("../usagelog/metric_registry.zig").AggType;
pub const AggValue = @import("memtable.zig").AggValue;

/// Update agg in-place for one event.
pub fn update(agg: *AggValue, event: *const Event, agg_type: AggType) void {
    switch (agg_type) {
        .count => {
            agg.count += 1;
        },
        .sum => {
            agg.sum += @as(u128, event.value);
        },
        .max => {
            agg.max = @max(agg.max, event.value);
        },
        .latest => {
            // Late events (older timestamp) must not overwrite a newer reading.
            if (event.timestamp > agg.last_timestamp) {
                agg.last_value     = event.value;
                agg.last_timestamp = event.timestamp;
            }
        },
        .count_unique => {
            // Deferred to caller: UniqueSets.update() + agg.count = set.count().
        },
    }
    // Advance the exactly-once barrier so checkpoint recovery knows where to resume.
    if (event.offset > agg.last_seg_offset) agg.last_seg_offset = event.offset;
}

/// floor(timestamp_ns / period_ns) cast to u32.
/// Returns 0 for non-positive timestamps (pre-epoch events).
/// period_ns must be > 0.
pub fn period_id_of(timestamp_ns: i64, period_ns: i64) u32 {
    if (timestamp_ns <= 0) return 0;
    return @intCast(@divFloor(timestamp_ns, period_ns));
}

/// Convert unix nanoseconds to (year, month, day) using the Howard Hinnant civil_from_days algorithm.
/// Only valid for timestamps >= 0 (post-epoch).
pub fn nanos_to_ymd(timestamp_ns: i64) struct { year: i32, month: u8, day: u8 } {
    const ns_per_day: i64 = 86_400_000_000_000;
    const z: i64 = @divFloor(timestamp_ns, ns_per_day) + 719468; // days since 0000-03-01
    const era: i64 = @divFloor(if (z >= 0) z else z - 146096, 146097);
    const doe: u32 = @intCast(z - era * 146097); // day of era [0, 146096]
    const yoe: u32 = @intCast(@divFloor(doe -| @as(u32, @intCast(@divFloor(doe, 1460))) +| @as(u32, @intCast(@divFloor(doe, 36524))) -| @as(u32, @intCast(@divFloor(doe, 146096))), 365));
    const y: i32 = @intCast(@as(i64, yoe) + era * 400);
    const doy: u32 = doe - (365 * yoe + yoe / 4 - yoe / 100); // day of year [0, 365]
    const mp: u32 = (5 * doy + 2) / 153; // month index [0, 11]
    const d: u8 = @intCast(doy - (153 * mp + 2) / 5 + 1);
    const m: u8 = if (mp < 10) @intCast(mp + 3) else @intCast(mp - 9);
    const adj_y: i32 = if (m <= 2) y + 1 else y;
    return .{ .year = adj_y, .month = m, .day = d };
}

/// Calendar-month period_id: (year-2000)*12 + (month-1), adjusted for billing_cycle_day.
/// billing_cycle_day: 1..28; day of month when the billing period starts.
/// If day < billing_cycle_day, the event belongs to the previous month's period.
/// Returns u32 compatible with AggKey.period_id.
pub fn calendar_period_id_of(timestamp_ns: i64, billing_cycle_day: u8) u32 {
    if (timestamp_ns <= 0) return 0;
    const ymd = nanos_to_ymd(timestamp_ns);
    const cycle_day: u8 = if (billing_cycle_day == 0) 1 else billing_cycle_day;
    var year = ymd.year;
    var month = @as(i32, ymd.month);
    if (ymd.day < cycle_day) {
        month -= 1;
        if (month < 1) {
            month = 12;
            year -= 1;
        }
    }
    const base: i32 = (year - 2000) * 12 + (month - 1);
    if (base < 0) return 0;
    return @intCast(base);
}

/// One UTC day in nanoseconds.
pub const DAY_NS: i64 = 24 * 3600 * std.time.ns_per_s;

/// Unified period_id dispatch: fixed, calendar, or day based on period_type.
pub fn resolve_period_id(timestamp_ns: i64, period_type: u8, period_ns: u64, billing_cycle_day: u8) u32 {
    const MetricRegistry = @import("../usagelog/metric_registry.zig");
    const pt = std.enums.fromInt(MetricRegistry.PeriodType, period_type);
    if (pt) |p| {
        return switch (p) {
            .fixed => period_id_of(timestamp_ns, if (period_ns > 0) @intCast(period_ns) else @as(i64, 30 * 24 * 3600 * std.time.ns_per_s)),
            .calendar => calendar_period_id_of(timestamp_ns, billing_cycle_day),
            .day => period_id_of(timestamp_ns, DAY_NS),
        };
    }
    // Unknown period_type — fall back to fixed.
    return period_id_of(timestamp_ns, if (period_ns > 0) @intCast(period_ns) else @as(i64, 30 * 24 * 3600 * std.time.ns_per_s));
}

/// Horizontal sum over a u64 slice using SIMD vectors.
/// Compiles to vpaddq / equivalent on the target platform.
/// Returns u128 to prevent overflow when accumulating large billing values.
pub fn simd_sum_u64(values: []const u64) u128 {
    var acc: @Vector(8, u64) = @splat(0);
    var i: usize = 0;
    while (i + 8 <= values.len) : (i += 8) {
        const v: @Vector(8, u64) = values[i..][0..8].*;
        acc += v;
    }
    var result: u128 = @as(u128, @reduce(.Add, acc));
    while (i < values.len) : (i += 1) result += values[i];
    return result;
}

// ---- tests ----

fn make_event(value: u64, timestamp: i64, offset: u64) Event {
    return .{
        .offset          = offset,
        .timestamp       = timestamp,
        .idempotency_key = 0,
        .account_id      = 1,
        .metric_code     = 1,
        .value           = value,
        .operation_type  = 0,
        ._pad            = .{ 0, 0, 0 },
        .checksum        = 0,
    };
}

test "aggregators: count" {
    var agg = AggValue{};
    const e1 = make_event(100, 1000, 1);
    const e2 = make_event(200, 2000, 2);
    update(&agg, &e1, .count);
    update(&agg, &e2, .count);
    try std.testing.expectEqual(@as(u64, 2), agg.count);
    try std.testing.expectEqual(@as(u64, 2), agg.last_seg_offset);
}

test "aggregators: sum" {
    var agg = AggValue{};
    const e1 = make_event(1_000_000, 1000, 1);
    const e2 = make_event(2_500_000, 2000, 2);
    update(&agg, &e1, .sum);
    update(&agg, &e2, .sum);
    try std.testing.expectEqual(@as(u128, 3_500_000), agg.sum);
}

test "aggregators: max" {
    var agg = AggValue{};
    const e1 = make_event(5_000_000, 1000, 1);
    const e2 = make_event(3_000_000, 2000, 2);
    const e3 = make_event(9_000_000, 3000, 3);
    update(&agg, &e1, .max);
    update(&agg, &e2, .max);
    update(&agg, &e3, .max);
    try std.testing.expectEqual(@as(u64, 9_000_000), agg.max);
}

test "aggregators: latest ignores older events" {
    var agg = AggValue{};
    const newer = make_event(999, 3000, 2);
    const older = make_event(111, 1000, 1);
    update(&agg, &newer, .latest);
    update(&agg, &older, .latest); // must not overwrite
    try std.testing.expectEqual(@as(u64, 999), agg.last_value);
    try std.testing.expectEqual(@as(i64, 3000), agg.last_timestamp);
}

test "aggregators: last_seg_offset tracks maximum" {
    var agg = AggValue{};
    const e1 = make_event(1, 1, 10);
    const e2 = make_event(1, 2, 5);  // lower offset
    const e3 = make_event(1, 3, 20);
    update(&agg, &e1, .count);
    update(&agg, &e2, .count);
    update(&agg, &e3, .count);
    try std.testing.expectEqual(@as(u64, 20), agg.last_seg_offset);
}

test "aggregators: period_id_of" {
    const day_ns: i64 = 24 * 3600 * std.time.ns_per_s;
    try std.testing.expectEqual(@as(u32, 0), period_id_of(0, day_ns));
    try std.testing.expectEqual(@as(u32, 0), period_id_of(-1, day_ns));
    try std.testing.expectEqual(@as(u32, 1), period_id_of(day_ns, day_ns));
    try std.testing.expectEqual(@as(u32, 1), period_id_of(day_ns + 1, day_ns));
    try std.testing.expectEqual(@as(u32, 2), period_id_of(2 * day_ns, day_ns));
}

test "resolve_period_id: day buckets ignore period_ns" {
    const day = @as(u8, 2); // PeriodType.day
    const just_before_midnight: i64 = DAY_NS - 1;
    const just_after_midnight: i64 = DAY_NS;
    // period_ns and billing_cycle_day are ignored for .day
    try std.testing.expectEqual(@as(u32, 0), resolve_period_id(0,                    day, 999, 99));
    try std.testing.expectEqual(@as(u32, 0), resolve_period_id(just_before_midnight, day, 999, 99));
    try std.testing.expectEqual(@as(u32, 1), resolve_period_id(just_after_midnight,  day, 999, 99));
    try std.testing.expectEqual(@as(u32, 7), resolve_period_id(7 * DAY_NS,           day, 0,   0));
}

test "aggregators: simd_sum_u64" {
    // Exactly 8 elements (one SIMD pass).
    const v8 = [_]u64{ 1, 2, 3, 4, 5, 6, 7, 8 };
    try std.testing.expectEqual(@as(u128, 36), simd_sum_u64(&v8));

    // More than 8 elements — tests scalar tail.
    const v10 = [_]u64{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    try std.testing.expectEqual(@as(u128, 55), simd_sum_u64(&v10));

    // Empty slice.
    try std.testing.expectEqual(@as(u128, 0), simd_sum_u64(&[_]u64{}));

    // Single element.
    try std.testing.expectEqual(@as(u128, 42), simd_sum_u64(&[_]u64{42}));
}

test "nanos_to_ymd: epoch is 1970-01-01" {
    const ymd = nanos_to_ymd(0);
    try std.testing.expectEqual(@as(i32, 1970), ymd.year);
    try std.testing.expectEqual(@as(u8, 1), ymd.month);
    try std.testing.expectEqual(@as(u8, 1), ymd.day);
}

test "nanos_to_ymd: 2026-04-17" {
    // 2026-04-17 00:00:00 UTC = 1776384000 seconds since epoch
    const ts: i64 = 1_776_384_000 * std.time.ns_per_s;
    const ymd = nanos_to_ymd(ts);
    try std.testing.expectEqual(@as(i32, 2026), ymd.year);
    try std.testing.expectEqual(@as(u8, 4), ymd.month);
    try std.testing.expectEqual(@as(u8, 17), ymd.day);
}

test "nanos_to_ymd: leap year 2024-02-29" {
    // 2024-02-29 00:00:00 UTC = 1709164800 seconds
    const ts: i64 = 1_709_164_800 * std.time.ns_per_s;
    const ymd = nanos_to_ymd(ts);
    try std.testing.expectEqual(@as(i32, 2024), ymd.year);
    try std.testing.expectEqual(@as(u8, 2), ymd.month);
    try std.testing.expectEqual(@as(u8, 29), ymd.day);
}

test "calendar_period_id_of: standard month (cycle_day=1)" {
    // 2026-04-17 → period starting April 1 → (2026-2000)*12 + 3 = 315
    const ts: i64 = 1_776_384_000 * std.time.ns_per_s;
    try std.testing.expectEqual(@as(u32, 315), calendar_period_id_of(ts, 1));
}

test "calendar_period_id_of: cycle_day=15, after cycle day" {
    // 2026-04-17, cycle_day=15 → day 17 >= 15 → April period → 315
    const ts: i64 = 1_776_384_000 * std.time.ns_per_s;
    try std.testing.expectEqual(@as(u32, 315), calendar_period_id_of(ts, 15));
}

test "calendar_period_id_of: cycle_day=15, before cycle day" {
    // 2026-04-10, cycle_day=15 → day 10 < 15 → March period → 314
    const ts: i64 = 1_775_779_200 * std.time.ns_per_s;
    try std.testing.expectEqual(@as(u32, 314), calendar_period_id_of(ts, 15));
}

test "calendar_period_id_of: cycle_day=15, January 5 wraps to December" {
    // 2026-01-05 00:00:00 UTC = 1767225600 seconds
    // day 5 < 15 → December 2025 period → (2025-2000)*12 + 11 = 311
    const ts: i64 = 1_767_225_600 * std.time.ns_per_s;
    try std.testing.expectEqual(@as(u32, 311), calendar_period_id_of(ts, 15));
}

test "calendar_period_id_of: leap year Feb 29, cycle_day=1" {
    // 2024-02-29 → February period → (2024-2000)*12 + 1 = 289
    const ts: i64 = 1_709_164_800 * std.time.ns_per_s;
    try std.testing.expectEqual(@as(u32, 289), calendar_period_id_of(ts, 1));
}

test "resolve_period_id: fixed mode matches period_id_of" {
    const day_ns: u64 = 24 * 3600 * std.time.ns_per_s;
    const ts: i64 = @intCast(day_ns * 5 + 1000);
    try std.testing.expectEqual(period_id_of(ts, @intCast(day_ns)), resolve_period_id(ts, 0, day_ns, 0));
}

test "resolve_period_id: calendar mode matches calendar_period_id_of" {
    const ts: i64 = 1_776_384_000 * std.time.ns_per_s;
    try std.testing.expectEqual(calendar_period_id_of(ts, 15), resolve_period_id(ts, 1, 0, 15));
}
