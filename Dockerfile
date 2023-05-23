FROM --platform=linux/amd64 rust:1.67.1 AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
RUN sed -i '/raphtory-benchmark/d' Cargo.toml
RUN sed -i '/examples\/rust/d' Cargo.toml
RUN sed -i '/python/d' Cargo.toml
RUN sed -i '/py-raphtory/d' Cargo.toml

WORKDIR /app/raphtory
COPY raphtory/Cargo.toml ./
RUN mkdir src && echo "fn main() {}" > src/main.rs

WORKDIR /app/raphtory-io
COPY raphtory-io/Cargo.toml ./
RUN mkdir src && echo "fn main() {}" > src/main.rs

WORKDIR /app/raphtory-graphql
COPY raphtory-graphql/Cargo.toml ./
RUN mkdir src && echo "fn main() {}" > src/main.rs

WORKDIR /app
RUN cargo build --release --workspace

COPY raphtory ./raphtory
COPY raphtory-io ./raphtory-io
COPY raphtory-graphql ./raphtory-graphql

WORKDIR /app/raphtory-graphql
RUN cargo build --release

FROM --platform=linux/amd64 rust:1.67.1 AS runner

WORKDIR /app

RUN groupadd -g 999 appuser && \
    useradd -r -u 999 -g appuser appuser
USER appuser

COPY --from=builder /app/target/release/raphtory-graphql /app

EXPOSE 1736

CMD ["./raphtory-graphql"]
