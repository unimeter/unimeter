//! Side-car property storage for event properties.
//!
//! Properties are variable-length key-value pairs attached to events at ingest time.
//! They are stored in a companion .props file alongside each WAL segment.
//! This file is indexed by event.offset for scan queries.
//!
//! MVP: write + linear scan only.

const std     = @import("std");
const disk_io = @import("../io/disk_io.zig");

/// One persisted property entry. 104 bytes, fixed-width for sequential scan.
pub const PropEntry = extern struct {
    event_offset: u64,   //  8B: which event this property belongs to
    key:          [32]u8, // 32B: property key, null-padded
    value:        [64]u8, // 64B: property value, null-padded
};

comptime {
    std.debug.assert(@sizeOf(PropEntry) == 104);
}

/// Append property entries for a batch to the .props file.
pub fn write(fd: i32, entries: []const PropEntry) !void {
    const file = disk_io.RealFile{ .inner = fd };
    for (entries) |*e| {
        try file.write_all(std.mem.asBytes(e));
    }
}

/// Scan the .props file for properties belonging to event_offset.
/// Returns the number of entries written into out_buf.
pub fn scan(fd: i32, event_offset: u64, out_buf: []PropEntry) !usize {
    const file = disk_io.RealFile{ .inner = fd };
    try file.seek_to(0);

    var n: usize = 0;
    var entry: PropEntry = undefined;
    while (n < out_buf.len) {
        const bytes = try file.read(std.mem.asBytes(&entry));
        if (bytes == 0) break;
        if (bytes != @sizeOf(PropEntry)) return error.CorruptPropsFile;
        if (entry.event_offset == event_offset) {
            out_buf[n] = entry;
            n += 1;
        }
    }
    return n;
}

test "props: write and scan round-trip" {
    const path = "/tmp/billing_props_test.bin";
    defer disk_io.remove(path) catch {};

    const file = try disk_io.open_rw(path);
    defer file.close();

    var entries = [_]PropEntry{
        .{
            .event_offset = 7,
            .key   = [_]u8{0} ** 32,
            .value = [_]u8{0} ** 64,
        },
        .{
            .event_offset = 8,
            .key   = [_]u8{0} ** 32,
            .value = [_]u8{0} ** 64,
        },
    };
    @memcpy(entries[0].key[0..4], "tier");
    @memcpy(entries[0].value[0..3], "pro");
    @memcpy(entries[1].key[0..6], "region");
    @memcpy(entries[1].value[0..2], "eu");

    try write(file.inner, &entries);

    var out: [4]PropEntry = undefined;
    const n = try scan(file.inner, 7, &out);
    try std.testing.expectEqual(@as(usize, 1), n);
    try std.testing.expectEqualSlices(u8, "tier", out[0].key[0..4]);
    try std.testing.expectEqualSlices(u8, "pro",  out[0].value[0..3]);
}
