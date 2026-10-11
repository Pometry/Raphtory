RUST_READTHEDOCS_DOCS_TARGET=docs/source/_rustdoc

###########
# General #
###########

print-version: # this is used by the CI, don't change
	@cargo metadata --no-deps --format-version 1 | jq -r '.packages[0].version'

build-all: rust-build
	cd python && maturin develop

test-all: rust-test-all python-test

# Tidying

tidy: rust-fmt build-python stubs python-fmt

python-tidy: stubs python-fmt test-graphql-schema

check-pr: tidy test-all

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
	git submodule update --init --recursive pometry-storage-private

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

rust-check:
	cargo hack check --workspace --all-targets --each-feature  --skip extension-module,default,vectors,datafusion

rust-test-all: rust-check
	cargo nextest run --all

# RDF tests and benchmarks (raphtory-rdf-tests). The W3C suites are git submodules of raphtory-rdf-tests.
W3C_SUITES := raphtory-rdf-tests/test-suites/rdf-tests raphtory-rdf-tests/test-suites/data-shapes

# --checkout works whatever `update` strategy .gitmodules sets.
w3c-tests-init:
	git submodule update --init --checkout $(W3C_SUITES)

rust-test-rdf-w3c: w3c-tests-init
	cargo test --profile build-fast -p raphtory-rdf-tests --features rdf --test sparql_w3c -- --test-threads=1

rust-test-shacl-w3c: w3c-tests-init
	cargo test --profile build-fast -p raphtory-rdf-tests --features shacl --test shacl_w3c

# The BEAR-B, BSBM and GO data is downloaded to RDF_DATA_DIR, outside the repository, and checked against the
# checksums in raphtory-rdf-tests/tests/common (BEAR-B) and raphtory-rdf-tests/benches/common (BSBM, GO).
# Existing files are not downloaded again.
RDF_DATA_DIR ?= $(HOME)/.cache/raphtory-rdf
# cargo runs tests and benchmarks in the package directory
override RDF_DATA_DIR := $(abspath $(RDF_DATA_DIR))

# $(call fetch,<name>,<directory in RDF_DATA_DIR>,<checksums>,<shell code setting url for file $$f>)
# downloads every file of the checksums that is missing, then checks them all.
define fetch
	@set -e; dir="$(RDF_DATA_DIR)/$(2)"; mkdir -p "$$dir"; \
	for f in $$(awk '{print $$2}' "$(3)"); do \
		f="$${f#./}"; \
		if [ ! -f "$$dir/$$f" ]; then \
			$(4); \
			echo "downloading $$url"; \
			curl -fsSL --create-dirs -o "$$dir/$$f.part" "$$url"; \
			mv "$$dir/$$f.part" "$$dir/$$f"; \
		fi; \
	done; \
	cp "$(3)" "$$dir/SHA256SUMS"; \
	if command -v sha256sum >/dev/null; then sum="sha256sum"; else sum="shasum -a 256"; fi; \
	cd "$$dir"; \
	$$sum --quiet -c SHA256SUMS || { echo "delete the files that FAILED from $$dir and run this again"; exit 1; }; \
	echo "$(1) is in $$dir"
endef

# BEAR-B, the DBpedia Live part of the BEnchmark of RDF ARchives (https://aic.ai.wu.ac.at/qadlod/bear.html).
BEAR_B_URL ?= https://aic.ai.wu.ac.at/qadlod/bear/BEAR_B

bear-b-fetch:
	$(call fetch,BEAR-B,bear-b,$(CURDIR)/raphtory-rdf-tests/tests/common/bear-b.sha256,url="$(BEAR_B_URL)/$$f")

# BSBM (5,000 products, Zenodo record 12663333) and oxigraph's sparqloscope-bsbm-5000.csv.
BSBM_URL ?= https://zenodo.org/records/12663333/files
SPARQLOSCOPE_URL ?= https://raw.githubusercontent.com/oxigraph/oxigraph/de0e18f5e7c24d54bb794328746bb9a114f41281/lib/oxigraph/benches

bsbm-fetch:
	$(call fetch,BSBM,bsbm,$(CURDIR)/raphtory-rdf-tests/benches/common/bsbm.sha256,case "$$f" in (sparqloscope-*) url="$(SPARQLOSCOPE_URL)/$$f";; (*) url="$(BSBM_URL)/$$f";; esac)

# The Gene Ontology (https://geneontology.org/docs/download-ontology/): the releases of go.owl in go.sha256.
GO_URL ?= https://release.geneontology.org

go-fetch:
	$(call fetch,GO,go,$(CURDIR)/raphtory-rdf-tests/benches/common/go.sha256,url="$(GO_URL)/$${f%/go.owl}/ontology/go.owl")

rust-test-rdf-bear: bear-b-fetch
	RAPHTORY_RDF_DATA=$(RDF_DATA_DIR) cargo test --profile build-fast -p raphtory-rdf-tests --features rdf --test bear -- --ignored

# load_rdf of generated data as N-Triples and Turtle.
bench-rdf-load:
	cargo bench -p raphtory-rdf-tests --features bench --bench load

# BEAR-B instant: ingestion, Mat and Ver through SPARQL, and to_rdf.
bench-rdf-temporal: bear-b-fetch
	RAPHTORY_RDF_DATA=$(RDF_DATA_DIR) cargo bench -p raphtory-rdf-tests --features bench --bench temporal

# BSBM (5,000 products) explore and Sparqloscope on Raphtory and oxigraph's in-memory Store.
bench-rdf-sparql: bsbm-fetch
	RAPHTORY_RDF_DATA=$(RDF_DATA_DIR) cargo bench -p raphtory-rdf-tests --features bench --bench sparql

# The Gene Ontology: the current release on Raphtory and oxigraph's in-memory Store, and every
# release as versions of one graph against a Store with a named graph per release.
bench-rdf-go: go-fetch
	RAPHTORY_RDF_DATA=$(RDF_DATA_DIR) cargo bench -p raphtory-rdf-tests --features bench --bench go


##########
# Python #
##########

build-wheel:
	cd python && maturin build -r

build-minimal-wheel:
	cd python && maturin build -r --no-default-features

clean:
	cargo clean

install-python: build-wheel
	pip install target/wheels/*.whl

install-minimal-python: build-minimal-wheel
	pip install target/wheels/*.whl

build-python:
	cd python && maturin develop -r --extras=dev

debug-python:
	cd python && maturin develop --profile=dev --extras=dev

# Testing
python-test:
	cd python && tox run

python-fmt:
	cd python && black .


build-python-rtd:
	cd python && maturin build --profile=build-fast && pip install ../target/wheels/*.whl

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
