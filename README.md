# Unimeter

The open-source usage metering engine for teams that bill by what customers actually use.

Unimeter counts, aggregates, and queries usage events in real time. One binary, no external infrastructure, deploys anywhere from a $5 VPS to a multi-node cluster.

## Why Unimeter

Most usage metering setups require a message broker, a database, and a batch pipeline glued together with custom code. Unimeter replaces all of that with a single self-contained server:

- **Real-time queries** — ask for a customer's current usage and get an answer in under a millisecond.
- **Exactly-once** — duplicate events are detected and discarded automatically. Your totals stay correct even when clients retry.
- **Replication** — 3-node clusters with automatic leader election. Kill a node and the cluster keeps going.
- **Zero dependencies** — no external databases, no message queues, no third-party libraries. One binary, nothing to install alongside it.

## Quickstart

```bash
docker run -d --name unimeter \
  --security-opt seccomp:unconfined \
  -p 7001:7001 -p 9090:9090 \
  ghcr.io/unimeter/unimeter:latest
```

Install the Go or Python SDK, then:

```go
client, _ := billing.New([]string{"localhost:7001"})

// Define a metric
client.Metrics.Create(ctx, billing.MetricSchema{
    Code:            "api_calls",
    AggType:         billing.AggCount,
    PeriodType:      billing.PeriodCalendar,
    BillingCycleDay: 1,
})

// Record usage
client.Ingest(ctx, []billing.Event{
    {AccountID: 42, MetricCode: "api_calls", Value: 1},
})

// Query
usage, _ := client.Query(ctx, billing.QueryRequest{
    AccountID: 42, MetricCode: "api_calls",
    Period: billing.CurrentMonth(),
})
fmt.Println(usage.Value.Count) // 1
```

See the [full quickstart](https://unimeter.io/quickstart/) for a step-by-step walkthrough, or browse the [examples](https://github.com/unimeter/examples) for complete runnable scenarios in Go and Python.

## Features

### Aggregation types

| Type | Description |
|------|-------------|
| COUNT | Number of events in a period |
| SUM | Sum of event values |
| MAX | Largest value observed |
| LATEST | Most recent value by timestamp |
| COUNT UNIQUE | Distinct values with add/remove operations |

### Billing periods

- **Fixed windows** — configurable duration (default 30 days).
- **Calendar months** — variable-length months with optional billing cycle start day (1–28).

### Dimension filters

Attach properties to events and query usage sliced by any dimension. Supports single-dimension queries (`provider=aws`) and multi-dimension AND queries (`provider=aws AND region=us-east`).

### Alerts

Up to 8 thresholds per metric, checked on every event. Alert history accessible via query API and push subscriptions in the SDK.

### Replication

3-node clusters with automatic leader election. If a node goes down, the remaining two continue serving reads and writes. When it comes back, it catches up automatically.

### Observability

Built-in Prometheus metrics endpoint with a Grafana dashboard and k6 load test scenarios.

## SDKs

| Language | Repository | Install |
|----------|-----------|---------|
| Go | [unimeter/go-unimeter](https://github.com/unimeter/go-unimeter) | `go get github.com/unimeter/go-unimeter` |
| Python | [unimeter/python-unimeter](https://github.com/unimeter/python-unimeter) | `pip install unimeter-python` |

## Building from source

Requires [Zig 0.16.0](https://ziglang.org/download/).

```bash
zig build              # build
zig build test         # run tests
zig build run          # start a single node
```

Or use Docker:

```bash
docker build -t unimeter .
docker run --rm unimeter
```

## Documentation

Full documentation at [unimeter.io](https://unimeter.io):

- [What is Unimeter](https://unimeter.io/what-is-unimeter/)
- [Quickstart](https://unimeter.io/quickstart/)
- [Concepts](https://unimeter.io/concepts/events-and-metrics/)
- [Go SDK](https://unimeter.io/sdk/go/)
- [Python SDK](https://unimeter.io/sdk/python/)
- [Cluster operations](https://unimeter.io/operations/cluster/)
- [Stripe integration guide](https://unimeter.io/guides/stripe/)
- [Benchmarks](https://unimeter.io/operations/benchmarks/)

## Examples

Complete, runnable examples in Go and Python: [unimeter/examples](https://github.com/unimeter/examples)

| Example | Description |
|---------|------------|
| saas-api | API call counting and monthly usage query |
| seat-based | COUNT_UNIQUE seats with add/remove |
| infra-metering | Dimension filters, per-provider breakdown |
| high-throughput | Buffered async ingest at high event rates |
| free-tier-alerts | Alert thresholds and enforcement |
| stripe-integration | Stripe webhook → usage → invoice |

## License

[O'SaaSy](LICENSE.md) — MIT with SaaS restriction. Free to use, modify, and self-host. Commercial SaaS offering of the software itself requires a separate agreement.
