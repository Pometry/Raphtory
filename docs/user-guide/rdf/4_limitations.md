# Limitations

RDF support keeps one simple model: every triple is an edge, every term is a node or layer name, and the view decides
what is visible. This page lists the consequences of that model and what is not supported yet.

## The data model

- **Properties and node types are not triples.** Node and edge properties, metadata, node types and nodes without
  edges are invisible to SPARQL, `to_rdf()` and SHACL, and loading RDF never creates them. An `rdf:type` triple is an
  edge in the layer `http%3A%2F%2Fwww.w3.org%2F1999%2F02%2F22-rdf-syntax-ns#type`.
- **Layer names are escaped IRIs.** The layer of `<http://example.org/p>` is `http%3A%2F%2Fexample.org%2Fp` (see
  [layer names](1_intro.md#layer-names)); pass that name to `layers()` and the other layer views.
- **Every distinct literal is a node.** Data with many distinct values adds one node per value, which counts in node
  counts and algorithms. Run algorithms on a [layer view](../views/3_layer.md) of the predicates that link resources.
- **Terms are matched exactly.** `"042"^^xsd:integer` and `"42"^^xsd:integer` are different nodes; a `FILTER` compares
  values. Parsing lower-cases language tags and makes `xsd:string` literals plain.
- **Only the default graph is stored.** A document with triples in named graphs fails to load.
- **IRIs under `raphtory:` are reserved.** In a loaded document such an IRI must be the exact encoding of a name
  (`<raphtory:%61>` is rejected, the name `a` is `<raphtory:a>`), and a predicate under `raphtory:` must give a layer
  name safe as a file name. IRIs under `raphtory:asof:` are time graphs.
- **Graphs with integer node ids are read-only for RDF.** They can be queried and exported (the node `42` is
  `<raphtory:42>`), but writes raise an error.
- **Non-RDF edges.** A graph not loaded from RDF can have edges that are not valid RDF: from a node named like a literal,
  or in a layer named like a literal or a blank node. Patterns match them, but `CONSTRUCT` drops them, `to_rdf()` skips
  them, and `DESCRIBE` stops at the first one, so its result can be incomplete.

## Loading and retracting

- **A byte order mark is a syntax error.** Remove it first (`data.removeprefix(b"\xef\xbb\xbf")`).
- **Loads are not atomic.** If a document fails to parse or a write fails, the triples read before stay in the graph.
- **RDF 1.2 input is rejected.** Triple terms, annotations, reifiers and literals with a base direction fail with
  `... cannot be stored` in the Python package and Rust builds with `shacl`, which parse RDF 1.2. Rust builds with only
  `rdf` fail to parse them, except JSON-LD's `@direction`, which they drop.
- **Blank nodes are renamed on load.** Loading a document twice creates two copies of its blank nodes. Retractions use
  labels as written, so they must use the stored labels; `[]` in a retraction matches nothing.
- **One edge event per triple.** Writes cost about one `add_edge()` per triple and are safe while other threads query
  the graph, which may see the triples written so far. N-Triples loads faster than Turtle.
- **No GraphQL writes.** RDF is loaded in Python or Rust, and the graph is sent to the server (see
  [GraphQL](5_graphql.md#limits)).

## Exporting

- **Export is a snapshot.** `to_rdf()` writes the triples of a view, without their history. Save the graph in
  Raphtory's own format (see [saving and loading graphs](../ingestion/4_saving.md)) to keep the history.
- **RDF/XML** can only write a predicate whose IRI ends in an XML name, not in the `xmlns` namespace and not an RDF/XML
  syntax term (`rdf:li`, `rdf:about`, ...); an `rdf:type` whose object cannot name an element when it is its subject's
  only triple; and literals without control characters other than tab and line feed. Other triples are skipped, also
  in `CONSTRUCT` and `DESCRIBE` results written as RDF/XML. A blank node label starting with a digit gets an `x` in front
  (`_:1` is `x1`). Use Turtle or N-Triples to export such graphs completely.

## Queries

- **SPARQL 1.1 queries only.** No SPARQL Update and no `SERVICE`: a query that calls a service fails (`SERVICE SILENT`
  gives one empty solution), and no query ever sends a request.
- **SPARQL 1.2 syntax** (triple terms, reifiers, annotations, `TRIPLE()`, `isTRIPLE()`, `LANGDIR()`) parses in the
  Python package and Rust builds with `shacl`, but the data holds no triple terms. SPARQL Results XML and JSON-LD
  cannot write a triple term a query builds, and RDF/XML skips it.
- **Nesting limit.** Brackets nest at most 128 deep (`SPARQL syntax error: brackets nest more than 128 deep`).
- **Results are held in memory.** Use `LIMIT` for large results; only Rust's `sparql_to_writer()` streams them.
  `CONSTRUCT` and `DESCRIBE` keep every triple until the query ends, to remove duplicates.
- **Time limits.** By default a query runs until it is done. `timeout` cannot interrupt planning, which grows with the
  fourth power of the number of joined patterns, nor a single expensive step; `max_triple_patterns` bounds planning
  but not the size of expressions (a `FILTER` of 2,000 `||` takes about a second to plan). A Python query cannot be
  interrupted with Ctrl-C.
- **SPARQL Results XML and control characters.** `format="xml"` raises an error for a literal with a control character
  other than tab and line feed. JSON, CSV and TSV hold every value.
- **Cost of patterns.** A pattern with a known subject or object reads that node's edges; one with only a predicate
  reads that layer's edges; one with nothing known reads every edge of the view's layers. On a view with node or edge
  filters these two check every node of the view.
- **No snapshot isolation.** Other threads can write while a query runs, and it may or may not see their writes.

## Time travel

- **The whole graph shows every triple ever asserted or retracted.** Use `valid()` or `latest()` for the current state, `at(t)` for
  the state as of `t`. A window shows every triple that held at some point in it.
- **Time graphs follow the view.** `GRAPH <raphtory:asof:T>` is `snapshot_at(T)` of the view, so it is empty outside
  the view's window and, on `valid()`, holds only the triples that still hold now. Run time-graph queries on the whole
  graph.
- **Time graphs cannot be listed.** `GRAPH ?g` with an unbound `?g` visits only the time graphs the query names (see
  [rules for time graphs](3_time-travel.md#rules-for-time-graphs)).
- **`MINUS` inside `GRAPH ?g`** shares `?g` between both sides, so `GRAPH ?g { ?s ?p ?o MINUS { ?x ?y ?z } }` removes
  every solution. Write the `MINUS` outside the `GRAPH` pattern.
- **Several `FROM` time graphs repeat triples.** Use `DISTINCT`.
- **Validity functions cannot see `GRAPH` or `FROM`.** Pass the time graph as their fourth argument. Literal objects are
  matched by canonical form, and a literal subject or layer gives an unbound result.
- **Validity intervals ignore filters.** The functions answer only for triples the view shows, but compute the interval
  from the full history up to the end of the view.
- **`valid()` is slower than a plain view** on scans of many triples, such as `COUNT(*)` over the whole graph.

## Conformance

Queries are evaluated by spareval 0.2.7, the SPARQL engine of oxigraph 0.5.11. The W3C test suites are git submodules of
`raphtory-rdf-tests` (`make w3c-tests-init`; `RAPHTORY_RDF_TESTS` and `RAPHTORY_SHACL_TESTS` point at another
checkout), run by `make rust-test-rdf-w3c` and `make rust-test-shacl-w3c` in CI. Each evaluation test loads its data
into an `RdfGraph` with decoy history around it and runs the query on `at(T)`.

| Suite                                     | Tests run | Pass       | Known failures | Skipped |
|-------------------------------------------|-----------|------------|----------------|---------|
| SPARQL 1.1 query evaluation               | 209       | 188 (90%)  | 21             | 16      |
| SPARQL 1.0 query evaluation               | 249       | 245 (98%)  | 4              | 33      |
| Turtle (loading, and exporting N-Triples) | 145       | 145        | 0              | 4       |
| SHACL Core                                | 98        | 98         | 0              | 0       |

The skipped tests use named graphs (`qt:graphData`, `FROM`, `FROM NAMED`) or lax cardinality, or are negative Turtle
evaluation tests; the syntax tests of the suites are not run. The 22 SHACL-SPARQL tests are rejected as unsupported or
expect a failure. None of the known failures comes from Raphtory: spareval over an in-memory RDF dataset gives the same
results. Most are numbers computed by the query written in spareval's form (`"32100"^^xsd:double` where the tests expect
`"3.21E4"`), and zero-length paths that only match terms of the data.

[BEAR-B](https://aic.ai.wu.ac.at/qadlod/bear.html), the benchmark for versioned RDF, checks time travel:
`make rust-test-rdf-bear` downloads its data and compares every version (as `snapshot_at(k)` and as `<raphtory:asof:k>`) with an
in-memory RDF dataset replaying the same versions. The benchmarks in `rdf-bench` (`make bench-rdf-load`,
`bench-rdf-temporal`, `bench-rdf-sparql` and `bench-rdf-go`) compare Raphtory with oxigraph on BEAR-B, BSBM and the
Gene Ontology.

```{.python hide}
import pytest
from raphtory import Graph, RdfGraph

# properties, node types and nodes without edges are not part of the RDF view
g = Graph()
g.add_node(1, "lonely", properties={"colour": "red"}, node_type="Person")
g.add_edge(1, "Alice", "Bob", properties={"weight": 3})
rg = RdfGraph(g)
assert rg.to_rdf() == "<raphtory:Alice> <raphtory:_default> <raphtory:Bob> .\n"

# rdf:type is an ordinary layer, not a node type
t = RdfGraph()
t.load_rdf(1, b"<http://example.org/alice> a <http://example.org/Person> .")
assert t.graph.unique_layers == ["http%3A%2F%2Fwww.w3.org%2F1999%2F02%2F22-rdf-syntax-ns#type"]
assert t.graph.node("http://example.org/alice").node_type is None

# IRIs under raphtory: must encode a name exactly
with pytest.raises(Exception, match="cannot be stored"):
    t.load_rdf(1, b"<raphtory:%61> <http://example.org/p> <http://example.org/b> .", format="nt")

# non-RDF edges are matched by patterns, dropped by CONSTRUCT and skipped by to_rdf
g.add_edge(1, '"x"', "Bob")
g.add_edge(1, "Alice", "Bob", layer="_:l")
assert len(rg.sparql("SELECT * WHERE { ?s ?p ?o }")) == 3
assert rg.sparql("CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }") == [("Alice", "_default", "Bob")]

# loads are not atomic
partial = RdfGraph()
with pytest.raises(Exception, match="RDF parse error"):
    partial.load_rdf(
        1,
        b"<http://example.org/a> <http://example.org/p> <http://example.org/b> .\n<http://example.org/c> <http://example.org/p> .\n",
        format="nt",
    )
assert partial.graph.count_edges() == 1

# an anonymous blank node in a retraction never matches
anonymous = RdfGraph()
anonymous.load_rdf(1, b'[] <http://example.org/name> "x" .')
anonymous.retract_rdf(2, b'[] <http://example.org/name> "x" .')
assert len(anonymous.valid().sparql("SELECT * WHERE { ?s <http://example.org/name> ?o }")) == 1

# brackets nest 128 deep; no SPARQL Update, no SERVICE
assert t.sparql("ASK { FILTER(" + "(" * 126 + "true" + ")" * 126 + ") }") is True
with pytest.raises(Exception, match="brackets nest more than 128 deep"):
    t.sparql("ASK { FILTER(" + "(" * 127 + "true" + ")" * 127 + ") }")
with pytest.raises(Exception, match="SPARQL syntax error"):
    t.sparql("INSERT DATA { <http://example.org/a> <http://example.org/p> <http://example.org/b> }")
with pytest.raises(Exception, match="SERVICE <http://example.org/sparql> is not supported"):
    t.sparql("SELECT * WHERE { SERVICE <http://example.org/sparql> { ?s ?p ?o } }")
assert t.sparql("SELECT * WHERE { SERVICE SILENT <http://example.org/sparql> { ?s ?p ?o } }") == [{"o": None, "p": None, "s": None}]

# RDF 1.2 is parsed but not stored
with pytest.raises(Exception, match="RDF 1.2 triple terms"):
    RdfGraph().load_rdf(
        1,
        b"<http://example.org/a> <http://example.org/p> <<( <http://example.org/s> <http://example.org/p> <http://example.org/o> )>> .",
        format="nt",
    )
assert t.sparql("ASK { ?s ?p ?o FILTER(isTRIPLE(?o)) }") is False

# MINUS inside GRAPH ?g shares ?g
w = RdfGraph()
w.load_rdf(1, b"<http://example.org/a> <http://example.org/p> <http://example.org/b> .")
assert w.sparql("SELECT ?s WHERE { VALUES ?g { raphtory:asof:5 } GRAPH ?g { ?s ?p ?o MINUS { ?x ?y ?z } } }") == []

# serialized results: XML rejects control characters, JSON keeps them, RDF/XML skips them
x = RdfGraph()
x.load_rdf(1, b'<http://example.org/a> <http://example.org/p> "a\\rb", "ok" .')
with pytest.raises(Exception, match="cannot be written in SPARQL Results XML"):
    x.sparql("SELECT ?o WHERE { ?s ?p ?o }", format="xml")
assert '"a\\rb"' in x.sparql("SELECT ?o WHERE { ?s ?p ?o }", format="json")
assert RdfGraph().load_rdf(1, x.sparql("CONSTRUCT WHERE { ?s ?p ?o }", format="rdf").encode(), format="rdf") == 1

# several FROM time graphs repeat triples
assert len(w.sparql("SELECT ?s FROM raphtory:asof:5 FROM raphtory:asof:6 WHERE { ?s ?p ?o }")) == 2
assert len(w.sparql("SELECT DISTINCT ?s FROM raphtory:asof:5 FROM raphtory:asof:6 WHERE { ?s ?p ?o }")) == 1

# validity dates only cover the years 1 to 9999
far = RdfGraph()
far.load_rdf(253402300800000, b"<http://example.org/a> <http://example.org/p> <http://example.org/b> .")
args = "<http://example.org/a>, <http://example.org/p>, <http://example.org/b>"
assert far.sparql(
    "SELECT (raphtory:validFrom(" + args + ") AS ?d) (raphtory:validFromTime(" + args + ") AS ?t) {}",
    decode_literals=True,
) == [{"d": None, "t": 253402300800000}]
```
