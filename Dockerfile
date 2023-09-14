# syntax=docker/dockerfile:1

## Builder image uses heavyweight rust on debian, for convenience.
FROM rust:1.72 AS builder

RUN mkdir /workdir
WORKDIR /workdir
COPY . /workdir

# Setup for static-build, from https://github.com/briansmith/ring/issues/1414#issuecomment-1055177218
RUN apt update && apt install musl-tools clang llvm -y
ENV CC_aarch64_unknown_linux_musl=clang
ENV AR_aarch64_unknown_linux_musl=llvm-ar
ENV CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_RUSTFLAGS="-Clink-self-contained=yes -Clinker=rust-lld"

# execute the build
RUN rustup target add aarch64-unknown-linux-musl
RUN cargo build --release --target aarch64-unknown-linux-musl

####
# For the deployment image, we can use "scratch" (i.e., zero byte image) because we're using a statically-linked binary
# that uses MUSL instead of glibc.
####
FROM scratch
COPY --from=builder /workdir/target/aarch64-unknown-linux-musl/release/querylog /

CMD ["/querylog"]