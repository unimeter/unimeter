//! Atomic metrics primitives: Counter, Gauge, Histogram.
//! All operations are lock-free and safe for concurrent access from multiple
//! threads. The server is single-threaded, but atomics allow safe reads from
//! a Prometheus scrape thread in future without restructuring.

const std = @import("std");

pub const Counter = struct {
    value: std.atomic.Value(u64) = .{ .raw = 0 },

    pub fn inc(self: *Counter) void {
        _ = self.value.fetchAdd(1, .monotonic);
    }

    pub fn add(self: *Counter, n: u64) void {
        _ = self.value.fetchAdd(n, .monotonic);
    }

    pub fn get(self: *const Counter) u64 {
        return self.value.load(.monotonic);
    }
};

pub const Gauge = struct {
    value: std.atomic.Value(i64) = .{ .raw = 0 },

    pub fn set(self: *Gauge, v: i64) void {
        self.value.store(v, .monotonic);
    }

    pub fn inc(self: *Gauge) void {
        _ = self.value.fetchAdd(1, .monotonic);
    }

    pub fn dec(self: *Gauge) void {
        _ = self.value.fetchSub(1, .monotonic);
    }

    pub fn get(self: *const Gauge) i64 {
        return self.value.load(.monotonic);
    }
};

/// Latency bucket upper bounds in nanoseconds.
/// Last entry is maxInt(u64) — the +Inf catch-all bucket.
pub const BUCKET_NS = [_]u64{
    500_000,           //  0.5 ms
    1_000_000,         //  1   ms
    5_000_000,         //  5   ms
    10_000_000,        //  10  ms
    25_000_000,        //  25  ms
    50_000_000,        //  50  ms
    100_000_000,       //  100 ms
    250_000_000,       //  250 ms
    500_000_000,       //  500 ms
    1_000_000_000,     //  1   s
    2_500_000_000,     //  2.5 s
    std.math.maxInt(u64), // +Inf
};

pub const N_BUCKETS: usize = BUCKET_NS.len;

/// Prometheus-style latency histogram. Stores per-bucket counts (non-cumulative).
/// The exporter converts to cumulative counts when rendering.
pub const Histogram = struct {
    buckets: [N_BUCKETS]std.atomic.Value(u64) =
        [_]std.atomic.Value(u64){.{ .raw = 0 }} ** N_BUCKETS,
    sum_ns: std.atomic.Value(u64) = .{ .raw = 0 },
    count:  std.atomic.Value(u64) = .{ .raw = 0 },

    pub fn observe(self: *Histogram, duration_ns: u64) void {
        _ = self.count.fetchAdd(1, .monotonic);
        _ = self.sum_ns.fetchAdd(duration_ns, .monotonic);
        for (BUCKET_NS, 0..) |threshold, i| {
            if (duration_ns <= threshold) {
                _ = self.buckets[i].fetchAdd(1, .monotonic);
                return;
            }
        }
    }

    pub fn get_count(self: *const Histogram) u64 {
        return self.count.load(.monotonic);
    }

    pub fn get_sum_ns(self: *const Histogram) u64 {
        return self.sum_ns.load(.monotonic);
    }

    pub fn get_bucket(self: *const Histogram, i: usize) u64 {
        return self.buckets[i].load(.monotonic);
    }
};

// ---- Tests ----

test "counter: zero, inc, add" {
    var c = Counter{};
    try std.testing.expectEqual(@as(u64, 0), c.get());
    c.inc();
    try std.testing.expectEqual(@as(u64, 1), c.get());
    c.add(9);
    try std.testing.expectEqual(@as(u64, 10), c.get());
}

test "gauge: set, inc, dec" {
    var g = Gauge{};
    try std.testing.expectEqual(@as(i64, 0), g.get());
    g.set(42);
    try std.testing.expectEqual(@as(i64, 42), g.get());
    g.inc();
    try std.testing.expectEqual(@as(i64, 43), g.get());
    g.dec();
    try std.testing.expectEqual(@as(i64, 42), g.get());
}

test "histogram: bucket assignment and cumulative semantics" {
    var h = Histogram{};
    h.observe(400_000);              // 0.4ms → bucket 0 (≤0.5ms)
    h.observe(600_000);              // 0.6ms → bucket 1 (≤1ms)
    h.observe(std.math.maxInt(u64)); // +Inf  → last bucket

    try std.testing.expectEqual(@as(u64, 1), h.get_bucket(0));
    try std.testing.expectEqual(@as(u64, 1), h.get_bucket(1));
    try std.testing.expectEqual(@as(u64, 1), h.get_bucket(N_BUCKETS - 1));
    try std.testing.expectEqual(@as(u64, 3), h.get_count());
    try std.testing.expect(h.get_sum_ns() > 0);
}

test "histogram: all in first bucket" {
    var h = Histogram{};
    h.observe(100_000); // 0.1ms → bucket 0
    h.observe(200_000); // 0.2ms → bucket 0

    try std.testing.expectEqual(@as(u64, 2), h.get_bucket(0));
    for (1..N_BUCKETS) |i| {
        try std.testing.expectEqual(@as(u64, 0), h.get_bucket(i));
    }
}
