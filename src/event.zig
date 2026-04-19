//! Core event type. Fixed-width extern struct: one cache line, SIMD-friendly.

const std = @import("std");

pub const OperationType = enum(u8) {
    none   = 0,
    add    = 1, // add a unique value (COUNT UNIQUE)
    remove = 2, // remove a unique value (COUNT UNIQUE)
};

/// A single usage event. 64 bytes = exactly one CPU cache line.
/// Fixed layout allows direct mmap reads and SIMD batch processing.
pub const Event = extern struct {
    offset:          u64,           //  8B [ 0.. 7] monotonic, assigned by server on write
    timestamp:       i64,           //  8B [ 8..15] unix nanoseconds (signed for arithmetic)
    idempotency_key: u128,          // 16B [16..31] UUID v7: time-sortable, dedup window
    account_id:      u64,           //  8B [32..39] billing entity
    metric_code:     u64,           //  8B [40..47] FNV-1a hash of the metric string
    value:           u64,           //  8B [48..55] scaled integer, SCALE = 1_000_000
    operation_type:  u8,            //  1B [56]     see OperationType
    _pad:            [3]u8,         //  3B [57..59] alignment pad
    checksum:        u32,           //  4B [60..63] CRC32 over bytes [0..59]
};

// Compile-time guarantee: changing the struct must update this assertion.
comptime {
    std.debug.assert(@sizeOf(Event) == 64);
    std.debug.assert(@alignOf(Event) == 16); // u128 field drives alignment
}
