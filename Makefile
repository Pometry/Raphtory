RUST_READTHEDOCS_DOCS_TARGET=docs/source/_rustdoc

rust-fmt:
	cargo +nightly fmt

rust-build:
	cargo build -q

rust-build-docs: 
	cargo doc --no-deps -p raphtory -q

build-all: rust-build
	cd python && maturin develop

rust-test:
	cargo test -q

install-python:
	cd python && maturin build && pip install ../target/wheels/*.whl

run-graphql:
	cargo run --release -p raphtory-graphql

rust-test-all: activate-storage
	cargo nextest run --all --features=storage
	cargo hack check --workspace --all-targets --each-feature  --skip extension-module,default

rust-test-all-public:
	cargo nextest run --all
	cargo hack check --workspace --all-targets --each-feature  --skip extension-module,default,storage


python-test: activate-storage
	cd python && tox run && tox run -e storage

python-test-public:
	cd python && tox run

test-all: rust-test-all python-test

test-all-public: rust-test-all-public python-test-public

activate-storage:
	./scripts/activate_private_storage.py

deactivate-storage:
	./scripts/deactivate_private_storage.py

pull-storage: activate-storage
	git submodule update --init --recursive

install-stub-gen:
	python -mpip install -e stub_gen

stubs: install-stub-gen
	cd python && ./scripts/gen-stubs.py && mypy -m raphtory

python-fmt:
	cd python && black .

tidy: rust-fmt build-python stubs python-fmt

tidy-public: rust-fmt build-python-public stubs python-fmt

check-pr: tidy-public test-all

build-python-public: deactivate-storage
	cd python && maturin develop -r --extras=dev

debug-python-public: deactivate-storage
	cd python && maturin develop --profile=debug

build-python-rtd:
	cd python && maturin build --profile=build-fast && pip install ../target/wheels/*.whl

debug-stubs: debug-python stubs

build-python: activate-storage
	cd python && maturin develop -r --features=storage --extras=dev

debug-python: activate-storage
	cd python && maturin develop --features=storage --extras=dev

install-mkdocs:
	pip install mkdocs

install-doc-deps:
	pip install -r docs/requirements.txt 

gen-doc-pages: install-doc-deps
	python docs/scripts/gen_docs_pages.py

clean-doc-pages:
	rm -r docs/reference && rm -r docs/tmp/saved_graph

python-docs-serve: install-doc-deps
	mkdocs serve

python-docs-build: install-doc-deps
	mkdocs build

run-docs-tests: install-doc-deps clean-doc-pages
	cd docs/user-guide && \
	pytest --markdown-docs -m markdown-docs --markdown-docs-syntax=superfences

WORKING_DIR ?= /tmp/graphs
PORT ?= 1736

PACKAGE_VERSION := $(shell grep -m 1 '^version' Cargo.toml | sed 's/version = "\(.*\)"/\1/')
RUST_VERSION := $(shell grep -m 1 '^rust-version' Cargo.toml | sed 's/rust-version = "\(.*\)"/\1/')

print-versions:
	@echo "Package Version: $(PACKAGE_VERSION)"
	@echo "Rust Version: $(RUST_VERSION)"

BASE_IMAGE_NAME_AMD64 := pometry/raphtory_base:$(RUST_VERSION)-amd64
BASE_IMAGE_NAME_ARM64 := pometry/raphtory_base:$(RUST_VERSION)-arm64
IMAGE_NAME_AMD64 := pometry/raphtory:$(PACKAGE_VERSION)-rust-amd64
IMAGE_NAME_ARM64 := pometry/raphtory:$(PACKAGE_VERSION)-rust-arm64
PY_IMAGE_NAME_AMD64 := pometry/raphtory:$(PACKAGE_VERSION)-python-amd64
PY_IMAGE_NAME_ARM64 := pometry/raphtory:$(PACKAGE_VERSION)-python-arm64

docker-build-pyraphtory-base-amd64:
	cd docker/base && docker build --platform linux/amd64 -t $(BASE_IMAGE_NAME_AMD64) .

docker-build-pyraphtory-base-arm64:
	cd docker/base && docker build --platform linux/arm64 -t $(BASE_IMAGE_NAME_ARM64) .

docker-build-pyraphtory-amd64:
	./scripts/deactivate_private_storage.py
	docker build -f docker/dockerfile --build-arg BASE_IMAGE=$(BASE_IMAGE_NAME_AMD64) --platform linux/amd64 -t $(PY_IMAGE_NAME_AMD64) .

docker-build-pyraphtory-arm64:
	./scripts/deactivate_private_storage.py
	docker build -f docker/dockerfile --build-arg BASE_IMAGE=$(BASE_IMAGE_NAME_ARM64) --platform linux/arm64 -t $(PY_IMAGE_NAME_ARM64) .

docker-build-raphtory-amd64:
	./scripts/deactivate_private_storage.py
	docker build --platform linux/amd64 -t $(IMAGE_NAME_AMD64) .

docker-build-raphtory-arm64:
	./scripts/deactivate_private_storage.py
	docker build --platform linux/arm64 -t $(IMAGE_NAME_ARM64) .

# Docker run targets for pyraphtory
docker-run-pyraphtory-amd64:
	docker run --rm -p $(PORT):$(PORT) \
		-v $(WORKING_DIR):/tmp/graphs \
		$(PY_IMAGE_NAME_AMD64) \
		$(if $(WORKING_DIR),--working-dir=$(WORKING_DIR)) \
		$(if $(PORT),--port=$(PORT)) \
		$(if $(CACHE_CAPACITY),--cache-capacity=$(CACHE_CAPACITY)) \
		$(if $(CACHE_TTI_SECONDS),--cache-tti-seconds=$(CACHE_TTI_SECONDS)) \
		$(if $(LOG_LEVEL),--log-level=$(LOG_LEVEL)) \
		$(if $(TRACING),--tracing) \
		$(if $(OTLP_AGENT_HOST),--otlp-agent-host=$(OTLP_AGENT_HOST)) \
		$(if $(OTLP_AGENT_PORT),--otlp-agent-port=$(OTLP_AGENT_PORT)) \
		$(if $(OTLP_TRACING_SERVICE_NAME),--otlp-tracing-service-name=$(OTLP_TRACING_SERVICE_NAME))

docker-run-pyraphtory-arm64:
	docker run --rm -p $(PORT):$(PORT) \
		-v $(WORKING_DIR):/tmp/graphs \
		$(PY_IMAGE_NAME_ARM64) \
		$(if $(WORKING_DIR),--working-dir=$(WORKING_DIR)) \
		$(if $(PORT),--port=$(PORT)) \
		$(if $(CACHE_CAPACITY),--cache-capacity=$(CACHE_CAPACITY)) \
		$(if $(CACHE_TTI_SECONDS),--cache-tti-seconds=$(CACHE_TTI_SECONDS)) \
		$(if $(LOG_LEVEL),--log-level=$(LOG_LEVEL)) \
		$(if $(TRACING),--tracing) \
		$(if $(OTLP_AGENT_HOST),--otlp-agent-host=$(OTLP_AGENT_HOST)) \
		$(if $(OTLP_AGENT_PORT),--otlp-agent-port=$(OTLP_AGENT_PORT)) \
		$(if $(OTLP_TRACING_SERVICE_NAME),--otlp-tracing-service-name=$(OTLP_TRACING_SERVICE_NAME))

# Docker run targets for raphtory
docker-run-raphtory-amd64:
	docker run --rm -p $(PORT):$(PORT) \
		-v $(WORKING_DIR):/tmp/graphs \
		$(IMAGE_NAME_AMD64) \
		$(if $(WORKING_DIR),--working-dir=$(WORKING_DIR)) \
		$(if $(PORT),--port=$(PORT)) \
		$(if $(CACHE_CAPACITY),--cache-capacity=$(CACHE_CAPACITY)) \
		$(if $(CACHE_TTI_SECONDS),--cache-tti-seconds=$(CACHE_TTI_SECONDS)) \
		$(if $(LOG_LEVEL),--log-level=$(LOG_LEVEL)) \
		$(if $(TRACING),--tracing) \
		$(if $(OTLP_AGENT_HOST),--otlp-agent-host=$(OTLP_AGENT_HOST)) \
		$(if $(OTLP_AGENT_PORT),--otlp-agent-port=$(OTLP_AGENT_PORT)) \
		$(if $(OTLP_TRACING_SERVICE_NAME),--otlp-tracing-service-name=$(OTLP_TRACING_SERVICE_NAME))

docker-run-raphtory-arm64:
	docker run --rm -p $(PORT):$(PORT) \
		-v $(WORKING_DIR):/tmp/graphs \
		$(IMAGE_NAME_ARM64) \
		$(if $(WORKING_DIR),--working-dir=$(WORKING_DIR)) \
		$(if $(PORT),--port=$(PORT)) \
		$(if $(CACHE_CAPACITY),--cache-capacity=$(CACHE_CAPACITY)) \
		$(if $(CACHE_TTI_SECONDS),--cache-tti-seconds=$(CACHE_TTI_SECONDS)) \
		$(if $(LOG_LEVEL),--log-level=$(LOG_LEVEL)) \
		$(if $(TRACING),--tracing) \
		$(if $(OTLP_AGENT_HOST),--otlp-agent-host=$(OTLP_AGENT_HOST)) \
		$(if $(OTLP_AGENT_PORT),--otlp-agent-port=$(OTLP_AGENT_PORT)) \
		$(if $(OTLP_TRACING_SERVICE_NAME),--otlp-tracing-service-name=$(OTLP_TRACING_SERVICE_NAME))
