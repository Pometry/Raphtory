FROM rust:1.77 AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
RUN sed -i '/default-members/d' Cargo.toml
RUN sed -i '/members = \[/,/\]/c\members = ["raphtory", "raphtory-graphql"]' Cargo.toml

WORKDIR /app/raphtory
COPY raphtory/Cargo.toml ./
RUN mkdir src && echo "fn main() {}" > src/main.rs

WORKDIR /app/raphtory-graphql
COPY raphtory-graphql/Cargo.toml ./
RUN mkdir src && echo "fn main() {}" > src/main.rs

WORKDIR /app
RUN cargo build --release --workspace

COPY raphtory ./raphtory
COPY raphtory-graphql ./raphtory-graphql

WORKDIR /app/raphtory-graphql
RUN cargo build --release

FROM rust:1.77-slim AS runner

WORKDIR /app

RUN groupadd -g 999 appuser && \
    useradd -r -u 999 -g appuser appuser
USER appuser

COPY --from=builder /app/target/release/raphtory-graphql /app

EXPOSE 1736

ENV GRAPH_DIRECTORY=graphs

CMD ["./raphtory-graphql"]
