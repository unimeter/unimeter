//! Idempotency dedup ring keyed on UUID v7 idempotency_key.
//! UUID v7 encodes the unix-ms timestamp in bits [127:80], enabling
//! age-based eviction without storing the insertion time separately.

const std = @import("std");

pub const DEFAULT_WINDOW_MS: i64 = 5 * 60 * 1000; // 5 minutes

pub const DedupRing = struct {
    map:       std.AutoHashMap(u128, void),
    window_ms: i64,

    pub fn init(alloc: std.mem.Allocator, window_ms: i64) DedupRing {
        return .{
            .map       = std.AutoHashMap(u128, void).init(alloc),
            .window_ms = window_ms,
        };
    }

    pub fn deinit(self: *DedupRing) void {
        self.map.deinit();
    }

    /// Returns true if key has already been seen.
    pub fn seen(self: *const DedupRing, key: u128) bool {
        return self.map.contains(key);
    }

    /// Record key as seen. Safe to call even if already present.
    pub fn insert(self: *DedupRing, key: u128) !void {
        try self.map.put(key, {});
    }

    /// Remove entries whose embedded UUID v7 timestamp is older than the window.
    /// Runs in O(n) over current map size; call periodically, not per-event.
    pub fn evict_stale(self: *DedupRing, now_ms: i64) void {
        if (now_ms <= self.window_ms) return;
        const cutoff: u64 = @intCast(now_ms - self.window_ms);

        // Collect keys to remove (avoids mutating map during iteration).
        var stale: [4096]u128 = undefined;
        var stale_n: usize = 0;
        var it = self.map.keyIterator();
        while (it.next()) |k| {
            // UUID v7: upper 48 bits = unix timestamp in ms.
            const ts_ms: u64 = @truncate(k.* >> 80);
            if (ts_ms < cutoff) {
                if (stale_n < stale.len) {
                    stale[stale_n] = k.*;
                    stale_n += 1;
                }
            }
        }
        for (stale[0..stale_n]) |k| _ = self.map.remove(k);
    }
};

test "dedup: seen and insert" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();

    var ring = DedupRing.init(gpa.allocator(), DEFAULT_WINDOW_MS);
    defer ring.deinit();

    try std.testing.expect(!ring.seen(0xABCD));
    try ring.insert(0xABCD);
    try std.testing.expect(ring.seen(0xABCD));
}

test "dedup: evict_stale removes old uuid v7 keys" {
    var gpa = std.heap.DebugAllocator(.{}){};
    defer _ = gpa.deinit();

    var ring = DedupRing.init(gpa.allocator(), 1000); // 1-second window
    defer ring.deinit();

    // Craft a UUID v7 with timestamp 0 ms (ancient).
    const old_key: u128 = @as(u128, 0) << 80; // ts=0ms
    try ring.insert(old_key);
    try std.testing.expect(ring.seen(old_key));

    // Evict with now = 5000 ms; cutoff = 4000 ms → old_key (ts=0) is stale.
    ring.evict_stale(5000);
    try std.testing.expect(!ring.seen(old_key));
}
