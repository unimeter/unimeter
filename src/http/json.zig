//! Minimal JSON parser for the event batch endpoint.
//! Parses: [{"account_id":N,"metric_code":"str","value":N,"delivery_mode":"str"},...]
//! No general-purpose parser — fixed schema only.

const std       = @import("std");
const time_util = @import("../util/time.zig");
const WireEvent = @import("../usagelog/protocol.zig").WireEvent;

pub const ParsedEvent = struct {
    wire:      WireEvent,
    sync_mode: bool,
};

/// Parse a JSON array of event objects into `out`.
/// Returns the number of events parsed (≤ out.len).
/// Returns error.InvalidJson on malformed input.
pub fn parse_events(json: []const u8, out: []ParsedEvent) !usize {
    var pos: usize = 0;
    var n:   usize = 0;

    skip_ws(json, &pos);
    if (pos >= json.len or json[pos] != '[') return error.InvalidJson;
    pos += 1;

    while (n < out.len) {
        skip_ws(json, &pos);
        if (pos >= json.len) break;
        if (json[pos] == ']') break;
        if (json[pos] == ',') { pos += 1; continue; }
        if (json[pos] != '{') return error.InvalidJson;
        pos += 1;

        var ev  = std.mem.zeroes(WireEvent);
        var sync_mode = false;
        ev.timestamp = @truncate(time_util.wallNanos());

        while (true) {
            skip_ws(json, &pos);
            if (pos >= json.len) return error.InvalidJson;
            if (json[pos] == '}') { pos += 1; break; }
            if (json[pos] == ',') { pos += 1; continue; }

            // Parse key string.
            if (json[pos] != '"') return error.InvalidJson;
            const key = try parse_string_raw(json, &pos);

            skip_ws(json, &pos);
            if (pos >= json.len or json[pos] != ':') return error.InvalidJson;
            pos += 1;
            skip_ws(json, &pos);

            if (std.mem.eql(u8, key, "account_id")) {
                ev.account_id = try parse_u64(json, &pos);
            } else if (std.mem.eql(u8, key, "value")) {
                ev.value = try parse_u64(json, &pos);
            } else if (std.mem.eql(u8, key, "metric_code")) {
                const s = try parse_string_raw(json, &pos);
                const n_copy = @min(s.len, ev.metric_code_str.len);
                @memcpy(ev.metric_code_str[0..n_copy], s[0..n_copy]);
            } else if (std.mem.eql(u8, key, "delivery_mode")) {
                const mode = try parse_string_raw(json, &pos);
                sync_mode = std.mem.eql(u8, mode, "sync");
            } else {
                try skip_value(json, &pos);
            }
        }

        out[n] = .{ .wire = ev, .sync_mode = sync_mode };
        n += 1;
    }

    return n;
}

// ---- Private helpers ----

fn skip_ws(json: []const u8, pos: *usize) void {
    while (pos.* < json.len) {
        switch (json[pos.*]) {
            ' ', '\t', '\n', '\r' => pos.* += 1,
            else => return,
        }
    }
}

/// Returns a slice into `json` covering the string content (between quotes).
fn parse_string_raw(json: []const u8, pos: *usize) ![]const u8 {
    if (pos.* >= json.len or json[pos.*] != '"') return error.InvalidJson;
    pos.* += 1;
    const start = pos.*;
    while (pos.* < json.len) {
        if (json[pos.*] == '"') {
            const s = json[start..pos.*];
            pos.* += 1;
            return s;
        }
        if (json[pos.*] == '\\') pos.* += 1; // skip escaped char
        pos.* += 1;
    }
    return error.InvalidJson;
}

fn parse_u64(json: []const u8, pos: *usize) !u64 {
    const start = pos.*;
    while (pos.* < json.len and json[pos.*] >= '0' and json[pos.*] <= '9') {
        pos.* += 1;
    }
    if (pos.* == start) return error.InvalidJson;
    return std.fmt.parseInt(u64, json[start..pos.*], 10) catch error.InvalidJson;
}

/// Skip any JSON value: string, number, bool, null, object, array.
fn skip_value(json: []const u8, pos: *usize) !void {
    if (pos.* >= json.len) return error.InvalidJson;
    switch (json[pos.*]) {
        '"' => _ = try parse_string_raw(json, pos),
        '{' => {
            pos.* += 1;
            var depth: usize = 1;
            while (pos.* < json.len and depth > 0) : (pos.* += 1) {
                switch (json[pos.*]) {
                    '{' => depth += 1,
                    '}' => depth -= 1,
                    '"' => {
                        pos.* += 1;
                        while (pos.* < json.len and json[pos.*] != '"') {
                            if (json[pos.*] == '\\') pos.* += 1;
                            pos.* += 1;
                        }
                    },
                    else => {},
                }
            }
        },
        '[' => {
            pos.* += 1;
            var depth: usize = 1;
            while (pos.* < json.len and depth > 0) : (pos.* += 1) {
                switch (json[pos.*]) {
                    '[' => depth += 1,
                    ']' => depth -= 1,
                    '"' => {
                        pos.* += 1;
                        while (pos.* < json.len and json[pos.*] != '"') {
                            if (json[pos.*] == '\\') pos.* += 1;
                            pos.* += 1;
                        }
                    },
                    else => {},
                }
            }
        },
        else => {
            // number, bool, null — skip until delimiter
            while (pos.* < json.len) {
                switch (json[pos.*]) {
                    ',', '}', ']', ' ', '\t', '\n', '\r' => return,
                    else => pos.* += 1,
                }
            }
        },
    }
}

// ---- Tests ----

test "json: parse single event" {
    var out: [8]ParsedEvent = undefined;
    const json = "[{\"account_id\":42,\"metric_code\":\"api_calls\",\"value\":1000}]";
    const n = try parse_events(json, &out);
    try std.testing.expectEqual(@as(usize, 1), n);
    try std.testing.expectEqual(@as(u64, 42),   out[0].wire.account_id);
    try std.testing.expectEqual(@as(u64, 1000),  out[0].wire.value);
    try std.testing.expectEqualSlices(u8, "api_calls",
        std.mem.sliceTo(&out[0].wire.metric_code_str, 0));
    try std.testing.expectEqual(false, out[0].sync_mode);
}

test "json: parse multiple events with delivery_mode" {
    var out: [8]ParsedEvent = undefined;
    const json =
        "[{\"account_id\":1,\"metric_code\":\"m\",\"value\":10,\"delivery_mode\":\"sync\"}," ++
        "{\"account_id\":2,\"metric_code\":\"n\",\"value\":20}]";
    const n = try parse_events(json, &out);
    try std.testing.expectEqual(@as(usize, 2), n);
    try std.testing.expectEqual(true,  out[0].sync_mode);
    try std.testing.expectEqual(false, out[1].sync_mode);
}

test "json: empty array" {
    var out: [8]ParsedEvent = undefined;
    const n = try parse_events("[]", &out);
    try std.testing.expectEqual(@as(usize, 0), n);
}

test "json: invalid json returns error" {
    var out: [8]ParsedEvent = undefined;
    try std.testing.expectError(error.InvalidJson, parse_events("not json", &out));
}
