RUST_READTHEDOCS_DOCS_TARGET=docs/source/_rustdoc

rust-fmt:
	cargo +nightly fmt

rust-build:
	cargo build -q

rust-build-docs: 
	cargo doc --no-deps -p raphtory -q

rust-build-readthedocs:
	cargo doc --no-deps -p raphtory -q --target-dir $(RUST_READTHEDOCS_DOCS_TARGET)
	rm -rf $(RUST_READTHEDOCS_DOCS_TARGET)/debug
	mv $(RUST_READTHEDOCS_DOCS_TARGET)/doc/* $(RUST_READTHEDOCS_DOCS_TARGET)
	rm -rf $(RUST_READTHEDOCS_DOCS_TARGET)/doc

build-all: rust-build
	cd python && maturin develop

rust-test:
	cargo test -q

test-all: rust-test
	cd python && pytest

install-python:
	cd python && maturin build && pip install ../target/wheels/*.whl

run-graphql:
	cargo run --release -p raphtory-graphql

rust-test-all:
	cargo test --all --no-default-features
	cargo check -p raphtory --no-default-features --features "io"
	cargo check -p raphtory --no-default-features --features "python"
	cargo check -p raphtory --no-default-features --features "search"
	cargo check -p raphtory --no-default-features --features "vectors"

activate-storage:
	./scripts/activate_private_storage.py

deactivate-storage:
	./scripts/deactivate_private_storage.py

pull-storage: activate-storage
	git submodule update --init --recursive

stubs:
	cd python && ./scripts/gen-stubs.py && mypy python/raphtory/**/*.pyi

python-fmt:
	cd python && black .

tidy: rust-fmt build-python stubs python-fmt

debug-stubs: debug-python stubs

build-python: activate-storage
	cd python && maturin develop -r --features=storage --extras=dev

debug-python: activate-storage
	cd python && maturin develop --features=storage --extras=dev

python-docs:
	cd docs && make html

docker-base-build-amd64:
	cd docker/base && docker build --platform linux/amd64 -t pometry/raphtory_base .

docker-base-build-arm64:
	cd docker/base && docker build --platform linux/arm64 -t pometry/raphtory_base .

docker-build-amd64:
	./scripts/deactivate_private_storage.py
	docker build --platform linux/amd64 -t pometry/raphtory -f docker/dockerfile .

docker-build-arm64:
	./scripts/deactivate_private_storage.py
	docker build --platform linux/arm64 -t pometry/raphtory -f docker/dockerfile .

docker-run:
	docker run -p 1736:1736 pometry/raphtory 

IMAGE_NAME := raphtory-graphql-app
WORKING_DIR ?= /tmp/graphs
PORT ?= 1736

docker-build-rust-graphql:
	docker build -t $(IMAGE_NAME) .

docker-run-rust-graphql:
	docker run --rm -p $(PORT):$(PORT) \
		-v $(WORKING_DIR):/tmp/graphs \
		$(IMAGE_NAME) \
		$(if $(WORKING_DIR),--working-dir=$(WORKING_DIR)) \
		$(if $(PORT),--port=$(PORT)) \
		$(if $(CACHE_CAPACITY),--cache-capacity=$(CACHE_CAPACITY)) \
		$(if $(CACHE_TTI_SECONDS),--cache-tti-seconds=$(CACHE_TTI_SECONDS)) \
		$(if $(LOG_LEVEL),--log-level=$(LOG_LEVEL)) \
		$(if $(TRACING),--tracing) \
		$(if $(OTLP_AGENT_HOST),--otlp-agent-host=$(OTLP_AGENT_HOST)) \
		$(if $(OTLP_AGENT_PORT),--otlp-agent-port=$(OTLP_AGENT_PORT)) \
		$(if $(OTLP_TRACING_SERVICE_NAME),--otlp-tracing-service-name=$(OTLP_TRACING_SERVICE_NAME))
