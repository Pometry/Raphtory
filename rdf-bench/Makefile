# The benchmark data is downloaded to RDF_DATA_DIR, outside the repository, and checked against the
# checksums in raphtory-rdf-tests/tests/common. Existing files are not downloaded again.
RDF_DATA_DIR ?= $(HOME)/.cache/raphtory-rdf
# cargo runs benchmarks in the package directory
override RDF_DATA_DIR := $(abspath $(RDF_DATA_DIR))
SUMS_DIR := $(abspath $(CURDIR)/../raphtory-rdf-tests/tests/common)

.PHONY: bench bench-load bench-temporal bench-sparql bench-go bear-b-fetch bsbm-fetch go-fetch

bench: bench-load bench-temporal bench-sparql bench-go

# load_rdf and retract_rdf on generated data.
bench-load:
	cargo bench -p raphtory-rdf-bench --features rdf --bench load

# BEAR-B: ingestion, and Mat, Diff and Ver through SPARQL and natively.
bench-temporal: bear-b-fetch
	RAPHTORY_RDF_DATA=$(RDF_DATA_DIR) cargo bench -p raphtory-rdf-bench --features rdf --bench temporal

# BSBM and Sparqloscope on Raphtory, oxigraph's in-memory Store and spareval over an in-memory dataset.
bench-sparql: bsbm-fetch
	RAPHTORY_RDF_DATA=$(RDF_DATA_DIR) cargo bench -p raphtory-rdf-bench --features rdf --bench sparql

# The Gene Ontology: the current release on Raphtory, oxigraph's in-memory Store and spareval over an in-memory
# dataset, and every release as versions of one graph against a Store with a named graph per release.
bench-go: go-fetch
	RAPHTORY_RDF_DATA=$(RDF_DATA_DIR) cargo bench -p raphtory-rdf-bench --features rdf --bench go

# BEAR-B, the DBpedia Live part of the BEnchmark of RDF ARchives (https://aic.ai.wu.ac.at/qadlod/bear.html).
BEAR_B_URL ?= https://aic.ai.wu.ac.at/qadlod/bear/BEAR_B
BEAR_B_SUMS := $(SUMS_DIR)/bear-b.sha256

bear-b-fetch:
	@set -e; dir="$(RDF_DATA_DIR)/bear-b"; mkdir -p "$$dir"; \
	for f in $$(awk '{print $$2}' "$(BEAR_B_SUMS)"); do \
		f="$${f#./}"; \
		if [ ! -f "$$dir/$$f" ]; then \
			echo "downloading $(BEAR_B_URL)/$$f"; \
			curl -fsSL --create-dirs -o "$$dir/$$f.part" "$(BEAR_B_URL)/$$f"; \
			mv "$$dir/$$f.part" "$$dir/$$f"; \
		fi; \
	done; \
	cp "$(BEAR_B_SUMS)" "$$dir/SHA256SUMS"; \
	if command -v sha256sum >/dev/null; then sum="sha256sum"; else sum="shasum -a 256"; fi; \
	cd "$$dir" && $$sum --quiet -c SHA256SUMS && echo "BEAR-B is in $$dir"

# BSBM (1,000 and 5,000 products, Zenodo record 12663333) and oxigraph's sparqloscope-bsbm-5000.csv.
BSBM_URL ?= https://zenodo.org/records/12663333/files
SPARQLOSCOPE_URL ?= https://raw.githubusercontent.com/oxigraph/oxigraph/de0e18f5e7c24d54bb794328746bb9a114f41281/lib/oxigraph/benches
BSBM_SUMS := $(SUMS_DIR)/bsbm.sha256

bsbm-fetch:
	@set -e; dir="$(RDF_DATA_DIR)/bsbm"; mkdir -p "$$dir"; \
	for f in $$(awk '{print $$2}' "$(BSBM_SUMS)"); do \
		f="$${f#./}"; \
		case "$$f" in sparqloscope-*) url="$(SPARQLOSCOPE_URL)/$$f";; *) url="$(BSBM_URL)/$$f";; esac; \
		if [ ! -f "$$dir/$$f" ]; then \
			echo "downloading $$url"; \
			curl -fsSL -o "$$dir/$$f.part" "$$url"; \
			mv "$$dir/$$f.part" "$$dir/$$f"; \
		fi; \
	done; \
	if command -v sha256sum >/dev/null; then sum="sha256sum"; else sum="shasum -a 256"; fi; \
	cd "$$dir" && $$sum --quiet -c "$(BSBM_SUMS)" && echo "BSBM is in $$dir"

# The Gene Ontology (https://geneontology.org/docs/download-ontology/): the releases of go.owl in go.sha256.
GO_URL ?= https://release.geneontology.org
GO_SUMS := $(SUMS_DIR)/go.sha256

go-fetch:
	@set -e; dir="$(RDF_DATA_DIR)/go"; mkdir -p "$$dir"; \
	for f in $$(awk '{print $$2}' "$(GO_SUMS)"); do \
		if [ ! -f "$$dir/$$f" ]; then \
			url="$(GO_URL)/$${f%/go.owl}/ontology/go.owl"; \
			echo "downloading $$url"; \
			curl -fsSL --create-dirs -o "$$dir/$$f.part" "$$url"; \
			mv "$$dir/$$f.part" "$$dir/$$f"; \
		fi; \
	done; \
	cp "$(GO_SUMS)" "$$dir/SHA256SUMS"; \
	if command -v sha256sum >/dev/null; then sum="sha256sum"; else sum="shasum -a 256"; fi; \
	cd "$$dir" && $$sum --quiet -c SHA256SUMS && echo "GO is in $$dir"
