FROM rust:1.80.0 AS chef
RUN cargo install cargo-chef --version 0.1.67
WORKDIR /app

FROM chef AS planner
COPY . .
RUN sed -i '/default-members/d' Cargo.toml
RUN sed -i '/members = \[/,/\]/c\members = ["raphtory", "raphtory-graphql"]' Cargo.toml
RUN cargo chef prepare  --recipe-path recipe.json

FROM chef AS builder
RUN apt-get update
RUN apt-get install -y protobuf-compiler
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build --release -p raphtory-graphql

FROM debian:bookworm-slim
ENV PORT=1736
COPY --from=builder /app/target/release/raphtory-graphql /raphtory-graphql
WORKDIR /graphs

ENTRYPOINT ["/raphtory-graphql"]
