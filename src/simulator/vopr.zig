//! VOPR: Viewstamped Operations + Property-checking Runtime.
//!
//! Deterministic cluster simulator. Each run is fully reproducible from its seed.
//!
//! Usage:
//!   vopr [--seed N] [--iterations N]
//!
//! Defaults: --seed 0  --iterations 10000
//!
//! The simulator drives a 3-node virtual cluster through random chaos actions
//! and checks VSR-level and AggStore-level invariants after every step:
//!
//!   I_A  Leader uniqueness   — at most one leader per partition at any time
//!   I_B  Commit bound        — commit_number ≤ op_number for all alive nodes
//!   I_C  Monotonicity        — view/commit only increase for nodes alive in both snapshots
//!   I_D  Commit durability   — max(commit_number) per partition never decreases
//!   I_2  Sum consistency     — agg.sum == Σ value of all committed SUM events
//!   I_6  Unique consistency  — agg.count == |unique_set| for COUNT_UNIQUE keys
//!
//! On invariant violation the simulator panics with the seed so the run is
//! reproducible: re-run with the same --seed to reproduce the failure.

const std        = @import("std");
const cluster    = @import("cluster.zig");
const chaos_mod  = @import("chaos.zig");
const inv_mod    = @import("invariants.zig");

const VirtualCluster  = cluster.VirtualCluster;
const ClusterSnapshot = inv_mod.ClusterSnapshot;
const Checker         = inv_mod.Checker;
const AggRef          = inv_mod.AggRef;

// ---- CLI ----

const DEFAULT_SEED:       u64   = 0;
const DEFAULT_ITERATIONS: usize = 10_000;

/// Ticks to run between chaos action batches. Gives leaders time to send
/// heartbeats and replicas time to process messages.
const TICKS_PER_STEP: usize = 3;

/// How often to print a progress line.
const PROGRESS_EVERY: usize = 1_000;

const Config = struct {
    seed:       u64   = DEFAULT_SEED,
    iterations: usize = DEFAULT_ITERATIONS,
};

fn parse_args(args_iter: *std.process.Args.Iterator) !Config {
    var cfg = Config{};
    _ = args_iter.next(); // skip argv[0]

    while (args_iter.next()) |arg| {
        if (std.mem.eql(u8, arg, "--seed")) {
            const v = args_iter.next() orelse return cfg;
            cfg.seed = try std.fmt.parseInt(u64, v, 10);
        } else if (std.mem.eql(u8, arg, "--iterations")) {
            const v = args_iter.next() orelse return cfg;
            cfg.iterations = try std.fmt.parseInt(usize, v, 10);
        }
    }
    return cfg;
}

// ---- Regression seeds ----
//
// Each seed is chosen to exercise a specific chaos scenario.
// They are run first in the regression suite before the random sweep.

const REGRESSION_SEEDS = [_]u64{
    // Seed 0: baseline — all chaos actions, no specific scenario.
    0,
    // Seed 1: heavy crash/restart cycles — exercises view changes.
    1,
    // Seed 7: network partitions — exercises split-brain avoidance.
    7,
    // Seed 42: mixed crash + partitions — exercises I_D (commit durability).
    42,
    // Seed 137: high drop rate — exercises I_C (monotonicity under packet loss).
    137,
    // Seed 999: disk corruption — exercises that corrupt WAL bytes don't break VSR.
    999,
};

// ---- Main ----

pub fn main(init: std.process.Init) !void {
    const alloc = init.gpa;

    var args_iter = std.process.Args.Iterator.init(init.minimal.args);
    const cfg = try parse_args(&args_iter);

    var out_buf: [4096]u8 = undefined;
    var fw = std.Io.File.stdout().writer(init.io, &out_buf);
    defer fw.interface.flush() catch {};

    try fw.interface.print("VOPR  seed={d}  iterations={d}\n", .{ cfg.seed, cfg.iterations });

    // Run regression seeds first.
    for (REGRESSION_SEEDS) |reg_seed| {
        try run_simulation(alloc, reg_seed, cfg.iterations, &fw.interface);
    }

    // Run the user-specified seed.
    const user_is_regression = for (REGRESSION_SEEDS) |s| {
        if (s == cfg.seed) break true;
    } else false;

    if (!user_is_regression) {
        try run_simulation(alloc, cfg.seed, cfg.iterations, &fw.interface);
    }

    try fw.interface.print("VOPR  all simulations passed.\n", .{});
    try fw.interface.flush();
}

// ---- Simulation loop ----

fn run_simulation(
    alloc:      std.mem.Allocator,
    seed:       u64,
    iterations: usize,
    out:        *std.Io.Writer,
) !void {
    try out.print("  [seed={d}] starting {d} iterations\n", .{ seed, iterations });

    var c = try VirtualCluster.init(alloc, seed);
    defer c.deinit();

    var ref = try AggRef.init(alloc);
    defer ref.deinit();

    var prng      = std.Random.DefaultPrng.init(seed);
    const rng     = prng.random();
    const checker = Checker.init(seed);

    var snap = ClusterSnapshot.capture(&c);

    var i: usize = 0;
    while (i < iterations) : (i += 1) {
        // Execute one random chaos action.
        chaos_mod.step(&c, rng) catch |err| switch (err) {
            error.NoLeader => {}, // expected when all leaders are crashed
            else           => return err,
        };

        // Advance virtual time so VSR state machines can react.
        for (0..TICKS_PER_STEP) |_| {
            c.tick();
            c.drain();
        }

        // VSR safety checks (point-in-time + temporal against previous snapshot).
        checker.check_with_prev(&c, &snap);
        snap = ClusterSnapshot.capture(&c);

        // AggStore consistency checks.
        checker.check_agg(&ref, &c);

        // WAL durability: every committed event must be recoverable from WAL bytes.
        checker.check_wal(&c, alloc);

        if ((i + 1) % PROGRESS_EVERY == 0) {
            try out.print("  [seed={d}] {d}/{d} ok\n", .{ seed, i + 1, iterations });
            try out.flush();
        }
    }

    try out.print("  [seed={d}] done — {d} iterations passed\n", .{ seed, iterations });
}
