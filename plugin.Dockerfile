FROM rust:1.67.1

WORKDIR /tmp/raphtory

#WORKDIR /sources/raphtory
#COPY raphtory/Cargo.toml ./
#RUN mkdir src && echo "fn main() {}" > src/main.rs
#
#WORKDIR /sources/raphtory-io
#COPY raphtory-io/Cargo.toml ./
#RUN mkdir src && echo "fn main() {}" > src/main.rs
#
#WORKDIR /sources/raphtory-graphql
#COPY raphtory-graphql/Cargo.toml ./
#RUN mkdir src && echo "fn main() {}" > src/main.rs
#
#WORKDIR /app
#RUN cargo build --release --workspace

COPY Cargo.toml Cargo.lock ./
RUN sed -i '/raphtory-benchmark/d' Cargo.toml
#RUN sed -i '/raphtory-io/d' Cargo.toml TODO: bring this back
RUN sed -i '/examples\/rust/d' Cargo.toml
RUN sed -i '/examples\/docker\/lotr\/plugin/d' Cargo.toml
RUN sed -i '/python/d' Cargo.toml
RUN sed -i '/py-raphtory/d' Cargo.toml

COPY raphtory ./raphtory
COPY raphtory-graphql ./raphtory-graphql

# TODO: delete this
COPY raphtory-io ./raphtory-io


WORKDIR /plugin

CMD ["cargo", "build", "--release"]




#RUN cargo install cargo-local-registry # TODO: move this a little below
#
#RUN cd raphtory && cargo package
#RUN cd raphtory-graphql && cargo package
#
#RUN mkdir /packages
#RUN cp target/package/raphtory-*.crate /packages/
#
#WORKDIR packages









#RUN
#
#RUN cargo package -p raphtory -p raphtory-graphql
##RUN cargo local-registry --sync Cargo.lock /registry
#
#WORKDIR registry
#RUN cp target/package/raphtory*.crate ./
#
#WORKDIR /.cargo
## Create and write text to the file using a Here Document
#RUN echo              '\n\
#[source]               \n\
#                       \n\
#[source.local]         \n\
#directory = "/registry" \n\
#                       \n\
#[source.crates-io]     \n\
#replace-with = "local" \n\
#' > config.toml
#
#WORKDIR /app
#
#COPY examples/docker/lotr/plugin/. .
