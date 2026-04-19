//! Global singleton metrics instances.
//! Import this module anywhere in the server to read or update metrics.
//! All operations are atomic — no locks required.

const registry = @import("registry.zig");

pub const Counter   = registry.Counter;
pub const Gauge     = registry.Gauge;
pub const Histogram = registry.Histogram;

// ---- Counters ----

pub var events_ingested_async: Counter = .{}; // events ingested via async delivery
pub var events_ingested_sync:  Counter = .{}; // events ingested via sync delivery
pub var events_duplicate:      Counter = .{}; // events rejected by dedup ring
pub var wal_writes:            Counter = .{}; // WAL write completions
pub var wal_syncs:             Counter = .{}; // WAL fsync completions
pub var view_changes:          Counter = .{}; // VSR leadership transitions (aggregate)
pub var alerts_pushed:         Counter = .{}; // alert frames queued for live subscribers
pub var alerts_recorded:       Counter = .{}; // threshold crossings durably appended to alert_log

// ---- Gauges ----

pub var connections_active:      Gauge = .{}; // active ingest TCP connections
pub var http_connections_active: Gauge = .{}; // active HTTP connections
pub var wal_offset_bytes:        Gauge = .{}; // current WAL write offset in bytes
pub var alert_subscribers:       Gauge = .{}; // connections with wants_alerts=true

// ---- Histograms ----

pub var ingest_async_duration: Histogram = .{}; // async ingest latency (ns)
pub var ingest_sync_duration:  Histogram = .{}; // sync ingest latency (ns)
pub var wal_sync_duration:     Histogram = .{}; // WAL+segment fsync latency (ns)
