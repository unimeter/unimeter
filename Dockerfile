FROM debian:bookworm-slim AS base

ARG ZIG_VERSION=0.16.0
ARG TARGETARCH

RUN apt-get update && apt-get install -y \
    curl \
    xz-utils \
    && rm -rf /var/lib/apt/lists/*

# Map Docker TARGETARCH to Zig target triple
RUN case "$TARGETARCH" in \
      amd64) ZIG_ARCH=x86_64 ;; \
      arm64) ZIG_ARCH=aarch64 ;; \
      *) echo "Unsupported arch: $TARGETARCH" && exit 1 ;; \
    esac && \
    curl -fsSL "https://ziglang.org/download/${ZIG_VERSION}/zig-${ZIG_ARCH}-linux-${ZIG_VERSION}.tar.xz" \
    | tar -xJ -C /usr/local && \
    ln -s "/usr/local/zig-${ZIG_ARCH}-linux-${ZIG_VERSION}/zig" /usr/local/bin/zig

WORKDIR /app

# dev: source mounted via volume, zig-cache persisted via named volume
FROM base AS dev
CMD ["zig", "build", "run"]

# builder: copies source and compiles release binary
FROM base AS builder
COPY . .
RUN zig build -Doptimize=ReleaseFast

# prod: minimal image with compiled binary only
FROM debian:bookworm-slim AS prod
COPY --from=builder /app/zig-out/bin/billing /usr/local/bin/billing
EXPOSE 7001 9090
CMD ["billing"]
