FROM rust:1.80.0 AS chef
RUN cargo install cargo-chef --version 0.1.67
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare  --recipe-path recipe.json

FROM chef AS builder
RUN apt-get update
RUN apt-get install -y protobuf-compiler
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
# RUN cargo chef cook --recipe-path recipe.json
COPY . .
RUN cargo build --release -p raphtory-graphql
# RUN cargo build -p raphtory-graphql

# FROM alpine:3.20.3
FROM debian:bookworm-slim
ENV PORT=1736
COPY --from=builder /app/target/release/raphtory-graphql /raphtory-graphql
# COPY --from=builder /app/target/debug/raphtory-graphql /usr/local/bin/raphtory-graphql
WORKDIR /graphs

ENTRYPOINT ["/raphtory-graphql"]
