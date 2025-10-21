ARG RUST_VERSION=1.86.0
ARG RAPHTORY_PROFILE="release"

FROM rust:${RUST_VERSION} AS chef
RUN cargo install cargo-chef --version 0.1.67
WORKDIR /app

FROM chef AS planner
COPY . .
RUN sed -i '/default-members/d' Cargo.toml
RUN sed -i '/members = \[/,/\]/c\members = ["raphtory", "raphtory-graphql"]' Cargo.toml
RUN cargo chef prepare  --recipe-path recipe.json

FROM chef AS builder
ARG RAPHTORY_PROFILE
RUN apt-get update && apt-get install -y protobuf-compiler
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --profile=${RAPHTORY_PROFILE} --recipe-path recipe.json
COPY . .
RUN cargo build --profile=${RAPHTORY_PROFILE} -p raphtory-graphql

FROM debian:bookworm-slim
ARG RAPHTORY_PROFILE
COPY --from=builder /app/target/${RAPHTORY_PROFILE}/raphtory-graphql /raphtory-graphql
WORKDIR /var/lib/raphtory

ENTRYPOINT ["/raphtory-graphql"]
