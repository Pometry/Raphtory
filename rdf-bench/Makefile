# The benchmark data is downloaded to RDF_DATA_DIR, outside the repository, and checked against the
# checksums in benches/common (BEAR-B's in raphtory-rdf-tests/tests/common, which its test shares).
# Existing files are not downloaded again.
RDF_DATA_DIR ?= $(HOME)/.cache/raphtory-rdf
# cargo runs benchmarks in the package directory
override RDF_DATA_DIR := $(abspath $(RDF_DATA_DIR))
SUMS_DIR := $(abspath $(CURDIR)/benches/common)
BEAR_SUMS_DIR := $(abspath $(CURDIR)/../raphtory-rdf-tests/tests/common)

.PHONY: bench bench-load bench-temporal bench-sparql bench-go bear-b-fetch bsbm-fetch go-fetch

bench: bench-load bench-temporal bench-sparql bench-go

# load_rdf of generated data as N-Triples and Turtle.
bench-load:
	cargo bench -p raphtory-rdf-bench --features rdf --bench load

# BEAR-B instant: ingestion, Mat and Ver through SPARQL, and to_rdf.
bench-temporal: bear-b-fetch
	RAPHTORY_RDF_DATA=$(RDF_DATA_DIR) cargo bench -p raphtory-rdf-bench --features rdf --bench temporal

# BSBM (5,000 products) explore and Sparqloscope on Raphtory and oxigraph's in-memory Store.
bench-sparql: bsbm-fetch
	RAPHTORY_RDF_DATA=$(RDF_DATA_DIR) cargo bench -p raphtory-rdf-bench --features rdf --bench sparql

# The Gene Ontology: the current release on Raphtory and oxigraph's in-memory Store, and every
# release as versions of one graph against a Store with a named graph per release.
bench-go: go-fetch
	RAPHTORY_RDF_DATA=$(RDF_DATA_DIR) cargo bench -p raphtory-rdf-bench --features rdf --bench go

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
	cd "$$dir" && $$sum --quiet -c SHA256SUMS && echo "$(1) is in $$dir"
endef

# BEAR-B, the DBpedia Live part of the BEnchmark of RDF ARchives (https://aic.ai.wu.ac.at/qadlod/bear.html).
BEAR_B_URL ?= https://aic.ai.wu.ac.at/qadlod/bear/BEAR_B

bear-b-fetch:
	$(call fetch,BEAR-B,bear-b,$(BEAR_SUMS_DIR)/bear-b.sha256,url="$(BEAR_B_URL)/$$f")

# BSBM (1,000 and 5,000 products, Zenodo record 12663333) and oxigraph's sparqloscope-bsbm-5000.csv.
BSBM_URL ?= https://zenodo.org/records/12663333/files
SPARQLOSCOPE_URL ?= https://raw.githubusercontent.com/oxigraph/oxigraph/de0e18f5e7c24d54bb794328746bb9a114f41281/lib/oxigraph/benches

bsbm-fetch:
	$(call fetch,BSBM,bsbm,$(SUMS_DIR)/bsbm.sha256,case "$$f" in (sparqloscope-*) url="$(SPARQLOSCOPE_URL)/$$f";; (*) url="$(BSBM_URL)/$$f";; esac)

# The Gene Ontology (https://geneontology.org/docs/download-ontology/): the releases of go.owl in go.sha256.
GO_URL ?= https://release.geneontology.org

go-fetch:
	$(call fetch,GO,go,$(SUMS_DIR)/go.sha256,url="$(GO_URL)/$${f%/go.owl}/ontology/go.owl")
