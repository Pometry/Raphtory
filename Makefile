RUST_READTHEDOCS_DOCS_TARGET=docs/source/_rustdoc

###########
# General #
###########

build-all: rust-build
	cd python && maturin develop

test-all: rust-test-all python-test

test-all-public: rust-test-all-public python-test-public

# Tidying

tidy: rust-fmt build-python stubs python-fmt

tidy-public: rust-fmt build-python-public stubs python-fmt

python-tidy: stubs python-fmt test-graphql-schema

check-pr: tidy-public test-all

gen-graphql-schema:
	raphtory schema > raphtory-graphql/schema.graphql

test-graphql-schema: install-node-tools
	npx graphql-schema-linter --rules fields-have-descriptions,types-have-descriptions raphtory-graphql/schema.graphql

# Utilities

activate-storage:
	./scripts/activate_private_storage.py

deactivate-storage:
	./scripts/deactivate_private_storage.py

pull-storage: activate-storage
	git submodule update --init --recursive

install-node-tools:
	@if command -v npx >/dev/null 2>&1; then \
		echo "npx is already installed."; \
	else \
		curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.3/install.sh | bash && nvm install node; \
	fi

########
# Rust #
########
rust-fmt:
	cargo +nightly fmt

rust-build:
	cargo build -q

rust-build-docs: 
	cargo doc --no-deps -p raphtory -q

run-graphql:
	cargo run --release -p raphtory-graphql

# Testing

rust-test:
	cargo test -q

rust-test-all: activate-storage
	cargo nextest run --all --features=storage
	cargo hack check --workspace --all-targets --each-feature  --skip extension-module,default

rust-test-all-public:
	cargo nextest run --all
	cargo hack check --workspace --all-targets --each-feature  --skip extension-module,default,storage

##########
# Python #
##########

install-python:
	cd python && maturin build && pip install ../target/wheels/*.whl

build-python-public: deactivate-storage
	cd python && maturin develop -r --extras=dev

build-python: activate-storage
	cd python && maturin develop -r --features=storage,extension-module --extras=dev

# Testing

python-test: activate-storage
	cd python && tox run && tox run -e storage

python-test-public:
	cd python && tox run

python-fmt:
	cd python && black .

debug-python-public: deactivate-storage
	cd python && maturin develop --profile=debug

build-python-rtd:
	cd python && maturin build --profile=build-fast && pip install ../target/wheels/*.whl

debug-python: activate-storage
	cd python && maturin develop --features=storage,extension-module --extras=dev

########
# Docs #
########

install-mkdocs:
	pip install mkdocs

install-doc-deps:
	pip install -r docs/requirements.txt

install-stub-gen:
	python -mpip install -e stub_gen

stubs: install-stub-gen
	cd python && ./scripts/gen-stubs.py && mypy -m raphtory

debug-stubs: debug-python stubs

gen-py-doc-pages: install-doc-deps
	python docs/scripts/gen_docs_pages.py

gen-graphql-doc-pages: install-node-tools gen-graphql-schema
	python docs/scripts/gen_docs_graphql_pages.py

clean-doc-pages:
	rm -rf docs/reference && rm -rf docs/tmp/saved_graph

python-docs-serve: install-doc-deps
	mkdocs serve

python-docs-build: install-doc-deps
	mkdocs build

# Testing

run-docs-tests: install-doc-deps clean-doc-pages
	cd docs/user-guide && \
	pytest --markdown-docs -m markdown-docs --markdown-docs-syntax=superfences

##########
# Docker #
##########

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
