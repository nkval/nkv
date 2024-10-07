ARG RUST_VERSION=1.80.1

FROM rust:${RUST_VERSION}-alpine3.20 AS toolchain
ENV CARGO_BUILD_TARGET="x86_64-unknown-linux-musl"
RUN rustup target add ${CARGO_BUILD_TARGET}
RUN apk add --no-cache musl-dev linux-headers make clang mold
RUN cargo install cargo-chef@0.1.67

FROM toolchain AS cacher
WORKDIR /app
COPY ./Cargo.toml ./Cargo.lock ./
COPY ./benches ./benches
RUN cargo chef prepare --recipe-path recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

FROM toolchain AS builder
WORKDIR /app
COPY --from=cacher /app /app
COPY . /app
RUN echo "Cargo target: $CARGO_BUILD_TARGET"
RUN cargo build --release --target $CARGO_BUILD_TARGET

FROM scratch
WORKDIR /app
COPY --from=builder /tmp /tmp
COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/nkv-server .
COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/nkv-client .
ENTRYPOINT []
