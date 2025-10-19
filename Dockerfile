# ===== Build Stage =====
FROM rust:1.90.0-alpine AS builder

RUN apk add --no-cache musl-dev make protobuf-dev

RUN wget -O - https://github.com/jemalloc/jemalloc/releases/download/5.2.1/jemalloc-5.2.1.tar.bz2 | tar -xj && \
    cd jemalloc-5.2.1 && \
    ./configure && \
    make && \
    make install

WORKDIR /app
COPY . .
RUN cargo build --release

# ===== Runtime Stage =====
FROM alpine:3.22.1 AS runtime

WORKDIR /app

COPY --from=builder /app/target/release/barus /app/barus

ENTRYPOINT ["/app/barus"]