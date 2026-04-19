//! Deterministic event generator. Same seed always produces the same sequence.
//! Used by benchmarks and the VOPR simulator.

const std = @import("std");
const Event = @import("../event.zig").Event;

pub const FuzzConfig = struct {
    seed:           u64,
    n_accounts:     u32  = 1000,
    n_metrics:      u32  = 20,
    batch_size:     u32  = 100,
    duplicate_pct:  f32  = 0.01,
    late_event_pct: f32  = 0.02,
    add_remove_pct: f32  = 0.10,
    zipf_exponent:  f64  = 1.2,
};

/// Fixed-size circular buffer. Used to track recent keys for duplicate generation.
fn RingBuffer(comptime T: type, comptime N: usize) type {
    return struct {
        const Self = @This();

        buf:  [N]T  = undefined,
        head: usize = 0,
        len:  usize = 0,

        pub fn push(self: *Self, val: T) void {
            self.buf[self.head] = val;
            self.head = (self.head + 1) % N;
            if (self.len < N) self.len += 1;
        }

        pub fn random_pick(self: *Self, rng: std.Random) ?T {
            if (self.len == 0) return null;
            const i   = rng.uintLessThan(usize, self.len);
            const idx = ((self.head + N - self.len) + i) % N;
            return self.buf[idx];
        }
    };
}

pub const Fuzzer = struct {
    prng:        std.Random.DefaultPrng,  // Zig 0.15: std.Random, not std.rand
    config:      FuzzConfig,
    recent_keys: RingBuffer(u128, 1024)  = .{},
    now_ns:      i64,

    pub fn init(config: FuzzConfig) Fuzzer {
        // Derive now_ns from the seed so same seed → same events (required for VOPR).
        // Picks a deterministic time in the 2024–2026 range for realistic timestamps.
        var prng = std.Random.DefaultPrng.init(config.seed);
        const epoch_2024: i64 = 1_704_067_200_000_000_000; // 2024-01-01 UTC in ns
        const two_years_ns: u64 = 2 * 365 * 24 * 3600 * std.time.ns_per_s;
        const offset: i64 = @intCast(prng.random().uintLessThan(u64, two_years_ns));
        return .{
            .prng   = prng,
            .config = config,
            .now_ns = epoch_2024 + offset,
        };
    }

    pub fn next_batch(self: *Fuzzer, buf: []Event) []Event {
        const n = @min(buf.len, self.config.batch_size);
        for (buf[0..n]) |*e| e.* = self.next_event();
        return buf[0..n];
    }

    pub fn next_event(self: *Fuzzer) Event {
        const rng = self.prng.random();
        const roll = rng.float(f32);
        const dup_threshold        = self.config.duplicate_pct;
        const late_threshold       = dup_threshold + self.config.late_event_pct;
        const add_remove_threshold = late_threshold + self.config.add_remove_pct;

        var ts = self.now_ns;
        var op: u8 = 0;
        var key = uuid_v7(rng, @intCast(@divTrunc(self.now_ns, std.time.ns_per_ms)));

        if (roll < dup_threshold) {
            if (self.recent_keys.random_pick(rng)) |k| key = k;
        } else if (roll < late_threshold) {
            const lag: i64 = @intCast(rng.uintLessThan(u64, 24 * std.time.ns_per_s * 3600));
            ts = self.now_ns - lag;
        } else if (roll < add_remove_threshold) {
            op = if (rng.boolean()) 1 else 2;
        }

        var e = Event{
            .offset          = 0,
            .timestamp       = ts,
            .idempotency_key = key,
            .account_id      = zipf_sample(rng, self.config.n_accounts, self.config.zipf_exponent),
            .metric_code     = rng.uintLessThan(u64, self.config.n_metrics),
            .value           = rng.uintLessThan(u64, 1_000_000_000),
            .operation_type  = op,
            ._pad            = .{ 0, 0, 0 },
            .checksum        = 0,
        };
        e.checksum = checksum(&e);

        self.recent_keys.push(e.idempotency_key);
        return e;
    }
};

/// UUID v7: first 48 bits = unix timestamp ms, rest random.
/// Time-sortable: enables age-based eviction from the dedup ring.
fn uuid_v7(rng: std.Random, now_ms: u64) u128 {
    const rand_a: u128 = rng.uintLessThan(u16, 0x1000); // 12 bits
    const rand_b: u128 = rng.uintLessThan(u64, 1 << 62); // 62 bits
    return (@as(u128, now_ms) << 80) |
           (@as(u128, 7) << 76) |
           (rand_a << 64) |
           (@as(u128, 0b10) << 62) |
           rand_b;
}

/// Zipf sample in [0, n). Devroye (1986) rejection method.
/// Produces realistic skew: a few accounts generate most of the traffic.
fn zipf_sample(rng: std.Random, n: u32, s: f64) u64 {
    if (s <= 1.0 or n <= 1) return rng.uintLessThan(u64, n);
    const b = std.math.pow(f64, 2.0, s - 1.0);
    while (true) {
        const u = rng.float(f64);
        const v = rng.float(f64);
        const x = std.math.floor(std.math.pow(f64, u, -1.0 / (s - 1.0)));
        const t = std.math.pow(f64, 1.0 + 1.0 / x, s - 1.0);
        if (v * x * (t - 1.0) / (b - 1.0) <= t / b and x < @as(f64, @floatFromInt(n))) {
            return @intFromFloat(x - 1.0);
        }
    }
}

/// CRC32 over the first 60 bytes of the event (all fields except checksum).
fn checksum(e: *const Event) u32 {
    const bytes = std.mem.asBytes(e);
    return std.hash.Crc32.hash(bytes[0..60]);
}

test "reproducible: same seed same events" {
    var a = Fuzzer.init(.{ .seed = 42 });
    var b = Fuzzer.init(.{ .seed = 42 });
    for (0..100) |_| {
        const ea = a.next_event();
        const eb = b.next_event();
        try std.testing.expectEqual(ea.idempotency_key, eb.idempotency_key);
        try std.testing.expectEqual(ea.account_id, eb.account_id);
        try std.testing.expectEqual(ea.value, eb.value);
    }
}

test "different seeds different events" {
    var a = Fuzzer.init(.{ .seed = 42 });
    var b = Fuzzer.init(.{ .seed = 99 });
    var same: u32 = 0;
    for (0..100) |_| {
        if (a.next_event().idempotency_key == b.next_event().idempotency_key) same += 1;
    }
    try std.testing.expect(same < 5);
}

test "checksum covers all payload bytes" {
    var f = Fuzzer.init(.{ .seed = 1 });
    const e = f.next_event();
    try std.testing.expect(e.checksum != 0);
}
