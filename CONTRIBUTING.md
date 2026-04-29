# Contributing to Unimeter

Thanks for your interest in contributing. This guide covers the basics.

## Getting started

1. Fork the repository and clone it locally.
2. Install [Zig 0.16.0](https://ziglang.org/download/) or use Docker (see below).
3. Build and run tests to make sure everything works before making changes.

### With Docker (recommended)

```bash
docker build -t unimeter .
docker run --rm --security-opt seccomp=unconfined unimeter zig build test
```

`seccomp=unconfined` is required because Unimeter uses io_uring.

### With Zig directly (Linux only)

```bash
zig build test
zig build run    # start a single node on port 7001
```

Unimeter targets Linux only. If you're on macOS or Windows, use Docker.

## Making changes

1. Create a branch from `main`.
2. Make your changes. Run `zig build test` to verify.
3. Run the VOPR simulator if your change touches the server core:
   ```bash
   zig build -Doptimize=ReleaseFast && ./zig-out/bin/vopr --iterations=1000
   ```
4. Commit with a short message using [conventional commits](https://www.conventionalcommits.org/):
   `feat:`, `fix:`, `perf:`, `refactor:`, `test:`, `docs:`, `chore:`.
5. Open a pull request against `main`.

## Code style

- All I/O goes through the IO interface (`src/io/`). Direct syscalls in business logic break the simulator.
- No dynamic allocations in the hot path. Pre-allocate at startup.
- Events are 64-byte fixed-width structs. Don't add variable-length fields to `Event`.
- Comments explain *why*, not *what*. English only.
- No external dependencies. If you need something, write it.

## Tests

Every module should have unit tests. Run the full suite with `zig build test`.

The VOPR simulator (`src/simulator/`) is the primary correctness tool. It runs the full cluster in a single process with deterministic randomness and checks invariants after every step. If VOPR fails, the PR won't be merged.

## Reporting bugs

Open an issue with:
- What you expected to happen.
- What actually happened.
- Steps to reproduce, or a VOPR seed if applicable.

## Feature requests

Open an issue describing the use case. Explain what billing scenario you're trying to solve — that context helps us evaluate whether it fits the project.

## License

By contributing, you agree that your contributions will be licensed under the same [license](LICENSE.md) as the project.
