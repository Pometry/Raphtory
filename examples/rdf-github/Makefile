# Temporal RDF example: a GitHub repository's history (and a small people graph) as RDF in Raphtory,
# queried with SPARQL from YASGUI. `make serve` builds what is missing and starts everything.

PYTHON  ?= python3
REPO    ?= pometry/raphtory
PORT    ?= 1736
UI_PORT ?= 1737
DATA    ?= data
GRAPHS  ?= graphs

# fetch.sh writes this file last
GITHUB_DATA  := $(DATA)/issues.jsonl
# build markers, kept out of the graph folders that the server reads
STAMPS       := .stamps
GITHUB_GRAPH := $(STAMPS)/github
PEOPLE_GRAPH := $(STAMPS)/people

.PHONY: help check fetch graphs github people serve validate clean distclean

help:
	@echo "make serve     build the graphs if needed, then run the Raphtory server ($(PORT)) and the YASGUI pages ($(UI_PORT))"
	@echo "make fetch     download the pull requests, reviews and issues of REPO=$(REPO) (needs gh, logged in)"
	@echo "make graphs    build both graphs into $(GRAPHS)/ (github fetches first if $(DATA)/ is empty)"
	@echo "make people    build only the people graph (no download needed)"
	@echo "make validate  check the github graph against schema/github.ttl (SHACL), now and over time"
	@echo "make check     check that the installed raphtory has RDF support"
	@echo "make clean     remove the graphs; make distclean also removes the downloaded data"

check:
	@$(PYTHON) -c "from raphtory import Graph; Graph().sparql('ASK {}')" \
		|| (echo "raphtory with RDF support is needed: build it from this repository (make build-python at its root)" && false)

fetch:
	scripts/fetch.sh $(REPO) $(DATA)

$(GITHUB_DATA):
	scripts/fetch.sh $(REPO) $(DATA)

$(GITHUB_GRAPH): $(GITHUB_DATA) scripts/build_github_graph.py | check
	$(PYTHON) scripts/build_github_graph.py $(DATA) $(GRAPHS)/github
	@mkdir -p $(STAMPS) && touch $@

$(PEOPLE_GRAPH): scripts/build_people_graph.py | check
	$(PYTHON) scripts/build_people_graph.py $(GRAPHS)/people
	@mkdir -p $(STAMPS) && touch $@

github: $(GITHUB_GRAPH)
people: $(PEOPLE_GRAPH)
graphs: github people

serve: graphs
	$(PYTHON) scripts/serve.py $(GRAPHS) $(PORT) $(UI_PORT)

validate: $(GITHUB_GRAPH)
	$(PYTHON) scripts/validate.py $(GRAPHS)/github schema/github.ttl

clean:
	rm -rf $(GRAPHS) $(STAMPS)

distclean: clean
	rm -rf $(DATA)
