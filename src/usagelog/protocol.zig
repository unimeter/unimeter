//! Binary protocol over TCP.
//! Client sends: RequestHeader (16B) + payload.
//! Server replies: ResponseHeader (12B) + payload.
//!
//! Ingest payload layout:
//!   IngestPayloadHeader (8B)
//!   WireEvent × event_count (96B each)
//!   PropPair  × props_count (96B each)
//!
//! All multi-byte integers are little-endian.

const std = @import("std");

pub const MAGIC:   u32 = 0xC0FFEE01;
pub const VERSION: u8  = 1;

/// Byte width for on-wire "host:port" address strings.
pub const ADDR_LEN: usize = 24;
/// Size of one PropPair on the wire: key[32] + value[64].
pub const PROP_PAIR_SIZE: usize = 96;
/// Size of one AggValueWire response payload.
pub const AGG_VALUE_WIRE_SIZE: usize = 56;
/// Full partition map payload: 256 entries × 96B each.
pub const PARTITION_MAP_PAYLOAD_SIZE: usize = 24576;
/// Size of an ingest response payload.
pub const INGEST_RESPONSE_SIZE: usize = 16; // n_stored:u32 + n_dups:u32 + last_offset:u64

/// Operation type carried by a request packet.
pub const PacketType = enum(u8) {
    ingest_async      = 0x00, // batch of events; no fsync wait
    ingest_sync       = 0x01, // batch of events; fsync + ack
    get_partition_map = 0x10, // → PartitionMapPayload (24576B)
    partition_map_upd = 0x11, // server→client broadcast; request_id=0
    metric_put        = 0x20, // create or update metric schema
    metric_delete     = 0x21, // delete metric by code
    metric_list       = 0x22, // → []MetricSchemaWire
    usage_query       = 0x30, // → AggValueWire (56B)
    usage_realtime    = 0x31, // → AggValueWire (56B), current period only
    events_list       = 0x32, // → []EventRecordWire (104B each)
    alerts_list       = 0x33, // → []AlertRecordWire (136B each)
    alert_push_enable = 0x34, // client→server, empty payload; flips wants_alerts on conn
    alert_push        = 0x35, // server→client, request_id=0; payload = AlertPushPayload (145B)
    usage_query_breakdown = 0x36, // → []BreakdownEntryWire; one per non-empty (dims) combination
    cluster_status    = 0x40, // → ClusterStatusPayload (40B header + 256×8B partitions)
    cluster_rebalance = 0x41, // trigger rebalance; payload: new_node_count:u8
    transfer_start    = 0x42, // begin partition state transfer
    transfer_chunk    = 0x43, // data chunk for state transfer
    transfer_done     = 0x44, // state transfer complete
    _,
};

/// Response status code (1 byte at wire offset 0).
pub const StatusCode = enum(u8) {
    ok             = 0,
    duplicate      = 1, // all events were duplicates; nothing stored
    err            = 2, // protocol error or server fault
    unknown_metric = 3, // stored with warning; metric not in registry
    redirect       = 4, // not the leader; payload contains new leader addr
    not_leader     = 5, // not the leader; no redirect addr available
    backpressure   = 6, // server overloaded or disk full; retry later
};

/// 16-byte request header, little-endian on wire.
pub const RequestHeader = extern struct {
    magic:       u32,  // 0xC0FFEE01
    version:     u8,
    packet_type: u8,   // PacketType value
    partition:   u16,  // 0xFFFF = auto-select by account_id
    payload_len: u32,  // bytes of payload that follow this header
    request_id:  u32,  // echoed in response for client-side multiplexing
};

comptime { std.debug.assert(@sizeOf(RequestHeader) == 16); }

/// 12-byte response header, little-endian on wire.
/// Wire layout: status:u8 + _pad:[3]u8 + request_id:u32 + payload_len:u32
pub const ResponseHeader = extern struct {
    status:      u8,
    _pad:        [3]u8,
    request_id:  u32,
    payload_len: u32,
};

comptime { std.debug.assert(@sizeOf(ResponseHeader) == 12); }

/// 8-byte ingest payload prefix.
pub const IngestPayloadHeader = extern struct {
    event_count: u32, // number of WireEvent entries that follow
    props_count: u32, // total number of PropPair entries at the end of the payload
};

comptime { std.debug.assert(@sizeOf(IngestPayloadHeader) == 8); }

/// Wire-format event sent by the client. 96 bytes.
pub const WireEvent = extern struct {
    metric_code_str: [64]u8, // human-readable metric code, null-padded
    account_id:      u64,
    timestamp:       i64,    // unix nanoseconds; 0 = server assigns current time
    value:           u64,    // scaled integer (SCALE = 1_000_000)
    operation_type:  u8,     // 0=none, 1=add, 2=remove (COUNT_UNIQUE)
    props_count:     u8,     // props for this specific event (informational; server ignores)
    _pad:            [6]u8,
};

comptime { std.debug.assert(@sizeOf(WireEvent) == 96); }

/// One property key-value pair on the wire. 96 bytes.
pub const PropPair = extern struct {
    key:   [32]u8,
    value: [64]u8,
};

comptime { std.debug.assert(@sizeOf(PropPair) == 96); }

/// One alert record on the wire. 136 bytes.
/// Same layout as one entry inside an ALERTS_LIST response body.
/// In ALERT_PUSH the `offset` field carries the entry index in the sending
/// node's alert_log.bin so clients can track last-seen offsets per node.
pub const AlertRecordWire = extern struct {
    offset:          u64,
    account_id:      u64,
    metric_code_str: [64]u8,
    threshold_code:  [32]u8,
    value_at_cross:  u64,
    event_timestamp: i64,
    _pad:            [8]u8 = [_]u8{0} ** 8,
};

comptime { std.debug.assert(@sizeOf(AlertRecordWire) == 136); }

/// Payload of an ALERT_PUSH packet. 144 bytes (136B AlertRecord + 8B node tag).
/// Sent by the leader of the partition that ingested the triggering event.
pub const AlertPushPayload = extern struct {
    record:   AlertRecordWire,
    node_id:  u8,
    _pad:     [7]u8 = [_]u8{0} ** 7,
};

comptime { std.debug.assert(@sizeOf(AlertPushPayload) == 144); }

pub const ALERT_PUSH_PAYLOAD_SIZE: usize = 144;

/// One row in a USAGE_QUERY_BREAKDOWN response. 320 bytes.
/// dims_count tells the client how many of the (key, value) slots in
/// dim_keys/dim_values carry real data; the rest are zero-padded.
/// Schema's MAX_FILTERS = 4, so 4 slots is always enough.
pub const BreakdownEntryWire = extern struct {
    dims_count:  u8,
    _pad:        [7]u8 = [_]u8{0} ** 7,
    dim_keys:    [4][32]u8,
    dim_values:  [4][32]u8,
    agg_sum_lo:  u64,
    agg_sum_hi:  u64,
    agg_count:   u64,
    agg_max:     u64,
    last_value:  u64,
    last_ts:     i64,
    alert_flags: u64,
};

comptime { std.debug.assert(@sizeOf(BreakdownEntryWire) == 320); }

pub const BREAKDOWN_ENTRY_WIRE_SIZE: usize = 320;

pub fn validate_header(hdr: *const RequestHeader) bool {
    return hdr.magic == MAGIC and hdr.version == VERSION;
}

// ---- CLUSTER_STATUS payload ----

/// Per-partition status entry in CLUSTER_STATUS response. 8 bytes.
pub const PartitionStatusEntry = extern struct {
    view_number:    u32,
    commit_number:  u16,
    role:           u8,  // 0=leader, 1=replica
    leader:         u8,  // NodeId of current leader
};

comptime { std.debug.assert(@sizeOf(PartitionStatusEntry) == 8); }

/// Fixed header of CLUSTER_STATUS response. 40 bytes.
pub const ClusterStatusHeader = extern struct {
    node_id:          u8,
    n_nodes:          u8,
    _pad:             [6]u8,
    wal_offset:       u64,
    segment_count:    u64,
    memtable_entries: u64,
    uptime_ns:        i64,
};

comptime { std.debug.assert(@sizeOf(ClusterStatusHeader) == 40); }

/// Total CLUSTER_STATUS payload: 40B header + 256 × 8B per-partition.
pub const CLUSTER_STATUS_PAYLOAD_SIZE: usize =
    @sizeOf(ClusterStatusHeader) + 256 * @sizeOf(PartitionStatusEntry);
