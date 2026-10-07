# Limitations

RDF support in Raphtory deliberately keeps one simple model: every triple is an edge, every term is a node or layer
name, and the view decides what is visible. This page lists the consequences of that model and the features that are
not supported yet.

## The data model

- **Properties and node types are invisible to RDF.** Node and edge properties, metadata, node types and nodes without
  edges are not part of the RDF view of a graph: SPARQL does not see them and `to_rdf()` does not export them. In the
  other direction, loading RDF never creates properties or node types. An `rdf:type` triple is an ordinary edge in the
  layer `http://www.w3.org/1999/02/22-rdf-syntax-ns#type`, and every literal value is a node, not a property.
- **Every distinct literal is a node.** Data with many distinct values, such as timestamps or measurements, adds one node
  per value. These nodes also take part in node counts and algorithms, so run algorithms on a
  [layer view](../views/3_layer.md) of the predicates that link resources when you only want those.
- **Terms are matched exactly.** `"042"^^xsd:integer` and `"42"^^xsd:integer` are different terms, so they are different
  nodes and a pattern written with `42` does not match `"042"`. A `FILTER` such as `FILTER(?age = 42)` compares values
  and matches both. When parsing, language tags are lower-cased and `xsd:string` literals become plain literals, so
  `"Bob"@EN` is stored as `"Bob"@en`.
- **Only the default graph is stored.** A document with triples in named graphs fails to load. Before loading, merge the
  named graphs into the default graph, or split them into separate documents and load each into its own Raphtory graph.
- **IRIs starting with `raphtory:` are reserved.** In a loaded document, an IRI under `raphtory:` must be the exact
  encoding of a Raphtory name, as described in the [introduction](1_intro.md#names-and-terms). For example
  `<raphtory:%61>` is rejected with a `cannot be stored` error, because the name `a` is encoded as `<raphtory:a>`.
  IRIs starting with `raphtory:asof:` are the time graphs and never stand for a node or layer.
- **Graphs with integer node ids are read-only for RDF.** You can query and export them (the node `42` is
  `<raphtory:42>`), but loading or retracting RDF raises an error, because RDF names are strings.
- **Non-RDF edges.** A graph that was not loaded from RDF can contain edges that are not valid RDF: from a node named
  like a literal (such as `'"x"'`), or in a layer named like a literal or a blank node (such as `_:l`). SPARQL patterns
  match them like any other triple, but `CONSTRUCT` drops them, `to_rdf()` skips them, and `DESCRIBE` silently stops at
  the first one it meets, so its result can be incomplete. On such graphs, use
  `CONSTRUCT { ?s ?p ?o } WHERE { ... }` instead of `DESCRIBE`, or a view without those layers.

## Loading and retracting

- **One time per document.** `load_rdf()` and `retract_rdf()` give every triple of a document the same time. For data
  with a time per triple, call `add_edge()` and `delete_edge()` for each triple, as shown in the
  [introduction](1_intro.md#triples-with-their-own-timestamps). `load_edges()` loads a whole table of triples faster,
  but it is a bulk loader: run it only while nothing else reads or writes the graph. Unlike `add_edge()`,
  `delete_edge()`, `load_rdf()` and `retract_rdf()`, it is not safe alongside queries.
- **A byte order mark is a syntax error.** A document that starts with a UTF-8 byte order mark (the bytes `EF BB BF`,
  which some Windows tools write) fails to parse at line 1, column 1, and nothing is loaded or retracted. Remove the
  mark first, for example in Python with `data.removeprefix(b"\xef\xbb\xbf")`.
- **Loads are not atomic.** If a document fails to parse or a write fails, the triples read before the error stay in the
  graph.
- **RDF 1.2 input is rejected.** Raphtory stores RDF 1.1 triples. Triple terms (`<<( ... )>>`), and so annotations
  (`{| ... |}`) and reifiers (`<< ... ~ ex:r >>`), and literals with a base direction (`"hi"@en--ltr`, or JSON-LD's
  `@direction`) fail with `... cannot be stored: RDF 1.2 triple terms ... are not supported` in the Python package and
  in Rust builds with the `shacl` feature, which parse RDF 1.2. Other Rust builds fail to parse them, with one
  exception: their JSON-LD parser drops the direction of a value with `@direction`, so it loads as a plain
  language-tagged string (`"hi"@en`). The same JSON-LD document therefore loads in a Rust build with only the `rdf`
  feature, and fails in the Python package. As loads are not atomic, the triples before the first such term stay in
  the graph: an annotated triple `ex:a ex:p ex:b {| ex:since 2020 |}` is stored before its annotation fails. A
  `VERSION "1.2"` directive is accepted in builds that parse RDF 1.2. A node named like a directional literal
  (`"hi"@en--ltr`) is the IRI `<raphtory:%22hi%22%40en--ltr>` in every build.
- **Blank nodes change their labels on load.** Every load renames blank nodes, so the same document loaded twice
  creates two copies of its blank nodes. Retractions use the labels as written, so a retraction document must use the
  stored labels (as returned by `to_rdf()` or `sparql()`), and an anonymous blank node (`[]`) in a retraction document
  never matches anything.
- **Retracting an unknown triple creates nodes.** The retraction is recorded and its subject and object nodes are
  created, although the triple never becomes visible.
- **One edge event per triple.** `load_rdf()` and `retract_rdf()` write every triple as one edge event, in document
  order, with the same `add_edge()` and `delete_edge()` calls you would make yourself. So a load costs about as much as
  calling `add_edge()` once per triple, and it is safe to run while other threads query or write the graph; a query
  that runs during a load sees the triples written so far. N-Triples (`format="nt"`, or a `.nt` or `.nq` path) loads
  faster than Turtle, which is the default for bytes.

## Exporting

- **Export is a snapshot.** `to_rdf()` writes the triples visible in a view, without their history. To keep the history,
  save the graph in Raphtory's own format (see [saving and loading graphs](../ingestion/4_saving.md)), or export several
  snapshots.
- **Skipped edges.** Edges that are not valid RDF (see above) are left out. In Python they are skipped silently; in Rust
  `to_rdf()` returns how many triples it wrote and skipped.
- **RDF/XML** writes a predicate as an XML element, so it can only write a predicate whose IRI ends in a valid XML name:
  triples with predicates such as `http://example.org/42` or `http://example.org/` are skipped. So are triples whose
  predicate is in the namespace of XML's `xmlns` attributes (`http://www.w3.org/2000/xmlns/`), which no element can be
  in, or is one of the RDF/XML syntax terms `rdf:Description`, `rdf:li`, `rdf:RDF`, `rdf:ID`, `rdf:about`,
  `rdf:parseType`, `rdf:resource`, `rdf:nodeID` and `rdf:datatype`. An `rdf:type` triple whose object IRI does not end
  in an XML name, or is in the `xmlns` namespace, is also skipped when it is the only triple of its subject. XML cannot
  hold most control characters, and XML parsers turn a carriage return into a line feed, so a triple whose object is a
  literal with a control character other than tab and line feed (including a carriage return) is skipped too. Use
  another format, such as Turtle or N-Triples, to export such graphs completely. A blank node whose label starts with a
  digit, such as `_:1`, is written with an `x` in front (`x1`), because an `rdf:nodeID` must be an XML name. The same
  rules apply to `CONSTRUCT` and `DESCRIBE` results written as RDF/XML with `sparql(..., format="rdf")` or
  `format="xml"`, which are written grouped by subject, as `to_rdf()` writes them. The
  [SPARQL endpoint](7_sparql_endpoint.md#formats) skips nothing: it answers in another format the client accepts, or
  with status 406.

## Queries

- **SPARQL 1.1 queries only.** `SELECT`, `ASK`, `CONSTRUCT` and `DESCRIBE` are supported. SPARQL Update (`INSERT`,
  `DELETE`, `LOAD`, ...) is not: change the graph with `load_rdf()`, `retract_rdf()`, `add_edge()` and `delete_edge()`
  instead.
- **No federated queries.** A query that calls a `SERVICE` fails with an evaluation error (`SERVICE <...> is not
  supported`), and `SERVICE SILENT` gives one empty solution. No query ever sends a request to another endpoint, in any
  build.
- **SPARQL 1.2 syntax depends on the build.** The Python package, and Rust builds with the `shacl` feature, parse
  SPARQL 1.2: triple terms (`<<( ?s ?p ?o )>>`), reifiers (`<< ?s ?p ?o >>`, `~ ?r`), annotations, `TRIPLE()`,
  `isTRIPLE()`, `LANGDIR()` and `VERSION "1.2"`. The data never holds triple terms, so the patterns that read them
  match nothing and `isTRIPLE()` is false for every stored term. A triple term that a query builds, such as
  `TRIPLE(?s, ?p, ?o)` or one in a `CONSTRUCT` template, is written in SPARQL Results JSON, CSV and TSV, N-Triples and
  Turtle, but SPARQL Results XML raises an error (`... is an RDF 1.2 triple term, which Raphtory does not write in
  SPARQL Results XML`), RDF/XML skips the triple and JSON-LD fails. Rust builds with only the
  `rdf` feature reject this syntax as a SPARQL syntax error.
- **Nesting limit.** Brackets (`(`, `{` and `[`) can nest at most 128 deep. Parsing and evaluating a query recurse into
  every bracket, so thousands of nested brackets would overflow the stack and abort the process; a query that nests
  deeper than 128 fails with `SPARQL syntax error: brackets nest more than 128 deep` before it is parsed. Brackets in
  strings and comments do not count.
- **Results are held in memory.** Results are returned as lists, dictionaries and tuples or, with `format`, as one
  string holding the whole document (see [serialized results](2_sparql.md#serialized-results)). Either way the whole
  result is held in memory, so use `LIMIT` for large results; only Rust's `sparql_to_writer()` streams results to a
  writer.
- **Bounding the time of a query.** By default a query runs until it is done. `sparql(timeout=...)` stops it after a
  time and `max_triple_patterns=...` rejects a query that is too large (see
  [limiting a query](2_sparql.md#limiting-a-query)); the GraphQL server applies both by default (30 s and 100 triple
  patterns; a server can raise or remove them). The timeout cannot
  interrupt planning, whose cost grows with the fourth power of the number of triple patterns joined together (about
  1.5 s for 128 patterns, and tens of seconds for 256), and faster when `BIND` or `VALUES` bind their variables
  first, which is what `max_triple_patterns` is for. Without a pattern limit, a short query can keep a thread busy
  for minutes even with a timeout. The pattern limit does not count the size of expressions: a `FILTER` of thousands
  of `||` takes seconds to plan (about 1.3 s for 2,000 terms; an `IN (...)` list of the same values does not). Nor
  can the timeout stop a single expensive step, such as hashing a very long string, before it ends. A Python query
  cannot be interrupted with Ctrl-C; give it a timeout. A Rust build with `panic = "abort"` stops a query only between
  the triples it reads and the results it returns, not while it joins, sorts or counts results in memory, and a
  query stopped while it sorts by an `EXISTS` key can panic in the sort there, which aborts the process.
- **Removing duplicate triples holds them in memory.** `CONSTRUCT` and `DESCRIBE` results never contain the same triple
  twice, so every triple of the result is kept in memory until the query ends, also when Rust streams the result with
  `sparql_to_writer()`.
- **SPARQL Results XML and control characters.** XML cannot hold most control characters, and XML parsers turn a
  carriage return into a line feed, so `format="xml"` raises an error for a `SELECT` result with a literal that contains
  a control character other than tab and line feed (including a carriage return), instead of writing a broken document
  or dropping the row. JSON, CSV and TSV hold every value. A `CONSTRUCT` or `DESCRIBE` result written as RDF/XML follows
  the rules of `to_rdf()` instead, and skips such triples (see [exporting](#exporting)).
- **Cost of patterns.** A pattern with a known subject or object only reads that node's edges. A pattern with only a
  known predicate reads the edges of that predicate's layer, and a pattern with no known term reads every edge of the
  layers the view keeps. On a view with node or edge filters (other than a window or a layer selection), such as a
  subgraph or a property filter, these two patterns instead check every node of the view. A query that repeats the
  same pattern without subject and object (as `FILTER EXISTS` can) reads it once more and then keeps its triples (up to
  about a million) for the rest of the query.
- **No snapshot isolation.** A query holds no lock on the graph between results, so other threads can keep writing to
  the graph while it runs, and the query may or may not see those writes. Triples in layers created after the query
  started are not seen, and the validity functions read the history of a triple when they are called, so they can
  disagree with the patterns of the same query. If you need a consistent result, do not write to the graph while a query
  runs. A
  [read_only()][raphtory.Graph.read_only] handle is not a way around this: while it exists every write to the graph
  waits, and a write made with `add_edge()` or `delete_edge()` waits while holding Python's global interpreter lock,
  which freezes the whole program, including the thread that holds the handle.

## SHACL

SHACL validation has [its own page](6_shacl.md#supported-shacl): SHACL Core is supported, SHACL-SPARQL and SHACL
Advanced Features are rejected, `rdfs:subClassOf` is followed only partly, and it is not available through GraphQL.
The W3C SHACL test suite is run with `make rust-test-shacl-w3c` (see [Supported SHACL](6_shacl.md#supported-shacl)).

## Time travel

- **Event graphs ignore retractions.** On a `Graph`, queries see every triple asserted in the view. Use a
  `PersistentGraph`, or `g.persistent_graph()`, for "as of" queries.
- **Windows show the state at their end.** On a `PersistentGraph`, `g.window(start, end)` shows the triples that hold
  just before `end`, not the triples that held at some point during the window.
- **Time graphs cannot be listed.** `GRAPH ?g { ... }` with an unbound `?g` only visits the time graphs the query names:
  the ones of `FROM NAMED` or, without `FROM NAMED`, the ones it writes as constants, such as
  `VALUES ?g { raphtory:asof:2023-01-01 raphtory:asof:2024-01-01 }`. A query that names no time graph matches nothing
  there. A time graph IRI built from other values, such as `BIND(IRI(CONCAT("raphtory:asof:", ?date)) AS ?g)`, is not a
  constant: a `GRAPH ?g` pattern only reliably matches it inside `LATERAL { ... }` after the `BIND`, as shown in
  [a series of dates](3_time-travel.md#a-series-of-dates).
- **`MINUS` inside `GRAPH ?g`.** The SPARQL engine gives both sides of a `MINUS` written inside `GRAPH ?g { ... }` the
  variable `?g`, so they always share a variable: `GRAPH ?g { ?s ?p ?o MINUS { ?x ?y ?z } }` removes every solution,
  where SPARQL would remove none. Write the `MINUS` outside the `GRAPH` pattern, with its own `GRAPH ?g`, instead.
- **Time graphs are empty outside the window of the view.** On a `PersistentGraph`, `snapshot_at(t)`, `at(t)` and
  `snapshot_latest()` are windows of a single instant, and `before()`, `after()` and `window()` are windows too, so
  `GRAPH raphtory:asof:T` on such a view matches nothing for a `T` outside its window. Query time graphs on the graph
  itself, as described in the [rules for time graphs](3_time-travel.md#rules-for-time-graphs).
- **Several `FROM` time graphs repeat triples.** A triple visible in several `FROM <raphtory:asof:T>` graphs is matched
  once per graph. Use `SELECT DISTINCT` or `COUNT(DISTINCT ...)`.
- **Validity functions cannot see `GRAPH` or `FROM`.** The functions `raphtory:validFrom`, `validTo`, `validFromTime`
  and `validToTime` (see [since when?](3_time-travel.md#since-when-validity-functions)) answer for the present of the
  view unless they are given a reference time, even inside `GRAPH raphtory:asof:T { ... }` or with
  `FROM raphtory:asof:T`. Pass the time graph as the fourth argument, such as `?g` in a `BIND` after `GRAPH ?g { ... }`.
  A wrong number of arguments or a reference that is not a time gives an unbound value, not an error.
- **Literal objects are matched by their canonical form.** The validity functions receive numbers, booleans and dates
  in canonical form (`"042"^^xsd:integer` arrives as `42`), so a literal object is matched by its canonical form: `42`
  finds `"042"^^xsd:integer` or `"42"^^xsd:int`, but a value of another datatype (`1` and `1.0`) or a date-time written
  with another timezone is not matched, although `FILTER` compares them as equal. Pass the object bound by the pattern.
  If the subject has several objects with the same canonical form in the layer, such as `"42"^^xsd:int` and
  `"042"^^xsd:integer`, and the view shows more than one of them, the functions return an unbound value. The full
  history of each triple is still available through the edge API, as shown in
  [the history of a triple](3_time-travel.md#the-history-of-a-triple).
- **Literal subjects and literal-named layers give an unbound value.** A graph not loaded from RDF can have a node or a
  layer named like a literal, such as `"42"^^<http://www.w3.org/2001/XMLSchema#integer>`. When such a literal is a
  number, boolean or date, the validity functions receive it as a subject or predicate in canonical form, which does not
  tell which node or layer it stood for, so they return an unbound value.
- **Filters decide which triples are in scope, not their intervals.** The validity functions only answer for triples the
  view shows, but compute the interval from the full history of the triple up to the end of the view, ignoring the
  view's filters and the start of its window.
- **Validity dates only cover the years 1 to 9999.** `validFrom` and `validTo` return an unbound value for a time
  outside them; `validFromTime` and `validToTime` return the Raphtory time of any event.

## GraphQL

- **Only in builds with RDF.** The [`sparql` field](5_graphql.md) and the [SPARQL endpoint](7_sparql_endpoint.md) are
  part of the GraphQL server of the Python package, and so of the Python Docker images (`python.Dockerfile`). The
  `raphtory-server` Rust binary only has them when built with the `rdf` cargo feature, the Rust Docker image
  (`Dockerfile`) does not have them, and clients of other servers should check the schema for the field. A server can
  turn both off with `disable_lists`.
- **Reads only.** There is no SPARQL Update and no mutation that loads or retracts RDF. Load RDF locally with
  `load_rdf()` and `retract_rdf()`, then send the graph to the server with `send_graph()` or `upload_graph()`. The
  [SPARQL endpoint](7_sparql_endpoint.md) answers queries only, and its dataset parameters (`default-graph-uri`,
  `named-graph-uri`) can only name time graphs.
- **Time and size limits.** A query that runs longer than `sparql_timeout` (30 seconds by default) is stopped.
  Queries longer than `max_sparql_query_length` bytes (16 KiB by default) or with more than
  `max_sparql_triple_patterns` triple patterns (100 by default, counting `BIND`, the expressions `(... AS ?v)` of
  `SELECT` and `GROUP BY`, and `VALUES` too) are rejected before they are planned. A query holds its whole result in
  memory as one string until it is sent. A server can also limit how many queries run
  at once with `heavy_query_limit`, which requests that contain `sparql` and endpoint queries wait for, or disable
  SPARQL with `disable_lists`. Raising `max_sparql_triple_patterns` or removing it lets a short query keep a thread busy planning
  for minutes, which the timeout cannot interrupt.
- **Disconnecting does not stop a query.** The server does not notice that a client has disconnected while its
  request runs, so the query runs on until it ends or reaches `sparql_timeout`, keeping a thread and any
  `heavy_query_limit` slot. With `sparql_timeout` removed, a client can start queries that never stop and walk away,
  so keep a timeout on a server shared with clients you do not control.
- **No validity functions on row-filtered reads.** A caller whose read access has a row filter cannot call
  `raphtory:validFrom`, `validTo`, `validFromTime` or `validToTime`, because they read the history of a triple without
  the filter. Their queries fail with a `SPARQL evaluation error`.
- **Event graphs ignore retractions.** A graph stored as an event `Graph` shows every triple ever asserted, unless the
  request reads it with `graph(path: ..., graphType: PERSISTENT)`.

## Conformance

Queries are evaluated by spareval 0.2.7, the SPARQL engine of oxigraph 0.5.11, and Raphtory is checked against the W3C
test suites (`make rust-test-rdf-w3c`, in the `raphtory-rdf-tests` crate). The suites are the git submodule
`raphtory-rdf-tests/test-suites/rdf-tests` of the repository, a checkout of [w3c/rdf-tests](https://github.com/w3c/rdf-tests)
pinned to commit `03a6561af4a7e4ef782b00a37716ec1f8d3b76b3`, the one oxigraph 0.5.11 uses. Fetch it with
`git submodule update --init --checkout raphtory-rdf-tests/test-suites/rdf-tests` (or `make w3c-tests-init`, which also
fetches the [SHACL test suite](6_shacl.md#supported-shacl)); without it, the conformance tests print how to fetch it and pass. The
`RAPHTORY_RDF_TESTS` environment variable points the SPARQL and Turtle tests at another checkout
(`RAPHTORY_SHACL_TESTS` does the same for the SHACL tests). CI checks out both suites and runs
`make rust-test-rdf-w3c rust-test-shacl-w3c` on every pull request and every push to `master` (the "W3C conformance
tests" job), failing if a suite file is missing instead of skipping; `make rust-test-shacl-w3c` runs the SPARQL and
Turtle suites a second time on the build with the `shacl` feature, which turns on RDF 1.2 and SPARQL 1.2 as the Python
package does. Each evaluation test runs five ways, which all give the same results: on a
`Graph`, on `snapshot_at(10)` of a `PersistentGraph`, on the same snapshot with extra history that is not visible at
time 10, with `FROM raphtory:asof:10` on the graph itself, and with the results serialized and read back. Terms are
compared exactly, and the solutions of an `ORDER BY` in order. At the commit of the test suites that oxigraph 0.5.11
uses:

| Suite                                       | Tests run | Pass       | Known failures | Skipped |
|---------------------------------------------|-----------|------------|----------------|---------|
| SPARQL 1.1 query evaluation                 | 209       | 188 (90%)  | 21             | 16      |
| SPARQL 1.0 query evaluation                 | 251       | 247 (98%)  | 4              | 31      |
| SPARQL 1.1 results formats (JSON, CSV, TSV) | 10        | 9          | 1              | 0       |
| SPARQL 1.1 and 1.0 syntax                   | 302       | 301        | 1              | 0       |
| Turtle (loading, and exporting N-Triples)   | 309       | 309        | 0              | 4       |

The skipped tests use named graphs (named input graphs, `FROM` or `FROM NAMED`), which Raphtory does not store, or
are Turtle tests of a kind the harness does not run (negative evaluation tests). None of the known failures comes from
Raphtory: spareval over a plain in-memory RDF dataset gives the same results. They are:

- **Computed numbers are written in spareval's form.** `SUM`, `AVG`, `MIN`, `MAX`, casts and arithmetic return values
  such as `"32100"^^xsd:double` or `"2"^^xsd:decimal`, where the tests expect the canonical `"3.21E4"` and `"2.0"`. The
  values are equal; this accounts for 14 of the 21 SPARQL 1.1 failures. In the same way, `STR()` of a number gives
  its canonical form: `STR("01"^^xsd:integer)` is `"1"`, although the stored term is still `"01"`.
- **Zero-length paths only match terms of the data.** `?s :p* :o` gives no solution when `:o` is not in the graph;
  the tests expect `:o` itself.
- **Smaller differences.** `GROUP_CONCAT` keeps a language tag that all its values share; `BNODE(str)` returns the same
  blank node for the same string in different solutions; a date without a timezone is compared with a date with one;
  a group nested in `OPTIONAL` is simplified before its filter is scoped; the invalid query `?x<?a&&?b>?y` is
  accepted; and one test expects the data's `"1.0E6"^^xsd:double` written as `1.0e6`.

## Temporal benchmark (BEAR-B)

[BEAR-B](https://aic.ai.wu.ac.at/qadlod/bear.html) is the standard benchmark for versioned RDF: the changes to 100
of the most often edited DBpedia resources over three months of 2015, grouped by day (89 versions), by hour (1,299
versions) and one version per change (instant, 21,046 versions). It comes with 49 `?s <p> ?o` queries, 13
`?s <p> <o>` queries and 20 two-pattern joins. Raphtory loads version `k` at time `k` of a `PersistentGraph`. For
each change it retracts the deleted triples and asserts the added ones. "Version `k`" is then `snapshot_at(k)` or the
time graph `<raphtory:asof:k>`.

The versions loaded are not exactly BEAR-B's. BEAR-B's change archive holds the changes between versions, but not
version 1 itself. Version 1 is rebuilt from the changes: a triple that a change deletes before any change adds it
must have been in version 1. The rest of version 1, its static core of triples that no change mentions, is not
loaded. BEAR-B's versions hold 33,500 to 43,900 triples each, while the loaded versions differ widely in size
between granularities (see the first table below). The numbers are therefore not directly comparable with published
BEAR-B results for other systems.

`make rust-test-rdf-bear` downloads the data (about 26 MB, outside the repository) and checks Raphtory against an
in-memory RDF dataset that replays the same versions and is queried with the same SPARQL engine. Both must give the
same results for:

- the contents of each version, read from the edges and exported with `to_rdf()`;
- **Mat**: each query in one version;
- **Diff**: the results that change between two versions, using `FILTER NOT EXISTS` across two time graphs;
- **Ver**: the results in every version, using `VALUES ?g` and `FROM NAMED`, and as validity intervals with
  `raphtory:validFromTime`.

The test checks every version of day, with a Diff from version 1 to each, and a sample of hour and instant. They
all agree. BEAR's published results for day and hour are compared too, but they do not match the change archive
exactly: some rows appear or disappear between two published versions without any change recording it, and every
later version of that query then differs. The test pins how many agree. Of the day versions, 56% agree for
`?s <p> ?o` and 80% for `?s <p> <o>`. Of the pairs of consecutive day versions where either side changed, the change
agrees in 67% and 66% of cases.

`make bench-rdf-temporal` measures the same operations. The times below come from the release build on an Apple M4 Pro
laptop (14 cores). The first table is for the whole graph:

| Whole graph                                   | day (89 versions)       | hour (1,299)            | instant (21,046)       |
|-----------------------------------------------|-------------------------|-------------------------|------------------------|
| Triples written, all versions                 | 72,227                  | 224,826                 | 427,162                |
| Triples in the first, middle and last version | 1,054 / 12,405 / 31,545 | 1,199 / 33,040 / 75,958 | 3,527 / 8,502 / 13,930 |
| Loading every version                         | 0.15 s                  | 0.45 s                  | 0.86 s                 |
| ... triples per second                        | 490,000                 | 500,000                 | 495,000                |
| ... versions per second                       | 610                     | 2,900                   | 24,000                 |
| Exporting the middle version with `to_rdf()`  | 150 ms                  | 460 ms                  | 663 ms                 |
| ... reading the names of its edges and layers | 62 ms                   | 171 ms                  | 243 ms                 |

The second table gives the time per query in the middle version (45, 650 and 10,523), averaged over the queries of
each kind. Diff goes from version 1 to the middle version. Ver lists five versions spread over the whole history,
or reads every version.

| Per query                                | day      | hour     | instant  |
|------------------------------------------|----------|----------|----------|
| Mat `?s <p> ?o`: SPARQL on `snapshot_at` | 0.10 ms  | 0.13 ms  | 0.16 ms  |
| Mat `?s <p> ?o`: SPARQL on `GRAPH asof`  | 0.10 ms  | 0.13 ms  | 0.16 ms  |
| Mat `?s <p> ?o`: native (layer edges)    | 0.023 ms | 0.046 ms | 0.074 ms |
| Mat `?s <p> <o>`: SPARQL                 | 0.063 ms | 0.063 ms | 0.073 ms |
| Mat `?s <p> <o>`: native                 | 0.001 ms | 0.001 ms | 0.002 ms |
| Mat join: SPARQL                         | 0.076 ms | 0.087 ms | 0.10 ms  |
| Diff `?s <p> ?o`: SPARQL template        | 0.15 ms  | 0.21 ms  | 0.28 ms  |
| Diff `?s <p> ?o`: native (edge history)  | 0.037 ms | 0.064 ms | 0.11 ms  |
| Ver `?s <p> ?o`: SPARQL, 5 versions      | 0.25 ms  | 0.37 ms  | 0.52 ms  |
| Ver `?s <p> ?o`: native, every version   | 0.040 ms | 0.068 ms | 0.11 ms  |

What the numbers show:

- **Loading** runs at about 500,000 triples per second, however many versions the triples are spread over.
- **Exporting** a version takes 2.4 to 2.7 times as long as reading the names of its edges and layers. The rest is
  making RDF terms of the names and writing them as N-Triples.
- **Snapshots and time graphs cost the same.** The two ways of asking for a version differ by less than 1%.
- **A pattern with only a predicate reads the edges of its layer.** `?s <p> ?o` finds them in the edge storage, which
  indexes the edges of every layer, and checks each against the version (see [queries](#queries)). It costs 2 to 4.5
  times the native lookup, and grows only a little with the history: a layer's edges are found without reading the
  other nodes or edges. A pattern with a known object reads only that node's edges. It is about 60 times slower than
  the native lookup, which is mostly the fixed cost of a SPARQL query.
- **Ver costs one Mat per listed version.** It has no shortcut. Reading the validity runs from the edge history
  answers Ver for every version in a fraction of a millisecond.

## General SPARQL benchmark (BSBM)

The [Berlin SPARQL Benchmark](http://wbsg.informatik.uni-mannheim.de/bizer/berlinsparqlbenchmark/) (BSBM) models
an e-commerce site: products with features and types, offers from vendors and reviews. Its explore mix is 25 queries
from 11 templates, which look up a few products, offers or reviews each (`OPTIONAL`, `FILTER`, `ORDER BY` with
`LIMIT`, `DESCRIBE` and `CONSTRUCT`). Its business intelligence (BI) mix has 8 templates that aggregate over the whole
data. [Sparqloscope](https://github.com/ad-freiburg/sparqloscope) adds 105 queries that each exercise one SPARQL
feature: joins, `OPTIONAL`, `MINUS`, `EXISTS`, `UNION`, `GROUP BY`, property paths, string, numeric and date
functions, exports and whole-graph counts. The files are the ones oxigraph's own benchmark uses: the pre-generated
BSBM data of Zenodo record 12663333 and oxigraph's `sparqloscope-bsbm-5000.csv`.

`dataset-1000.nt` holds no products. Its 1,000 products are inserted by the 1,000 `INSERT DATA` operations of the
explore-and-update mix, which also has 1,500 `DELETE WHERE` operations that delete offers. Raphtory has no SPARQL
Update, so the harness writes the dataset at time 1 and update `k` at time `k + 2`. `INSERT DATA` becomes `load_rdf()`
and `DELETE WHERE` becomes retractions of the triples it matches. The result has 367,571 triples and 116,728 nodes.
`dataset-5000.nt` holds 4,000 of its 5,000 products: 1,620,320 triples and 476,829 nodes. Its update stream is not
used.

`make rust-test-rdf-bsbm` runs every query on Raphtory and on spareval over an in-memory RDF dataset holding the same
triples. The results must agree, as multisets of rows, in order for `ORDER BY`, and as sets of triples for
`CONSTRUCT` and `DESCRIBE`. The test takes about 95 s in the `build-fast` profile. It checks:

- the 13,750 queries of the explore-and-update mix, each on `snapshot_at()` the time of the last update before it, so
  the as-of view of the history is checked too;
- every distinct explore and BI query of the 1,000-product data (8,571 and 1,465);
- the 12,045 distinct explore queries, 35 BI queries and 104 Sparqloscope queries of the 5,000-product data;
- every Sparqloscope query on the 1,000-product data.

All of them agree. Three kinds of difference are allowed:

- **`ORDER BY` ties.** Rows with equal sort keys can come in any order. A `LIMIT` that cuts through tied rows can keep
  any of them.
- **Floating-point rounding.** `AVG` of `xsd:float` values can differ in its last digit, because the values are
  added in the order the storage returns them.
- **`SAMPLE` values.** `SAMPLE` can return any value of its group, so its value is not compared.

BI template 7 takes 10 to 25 s per query on every engine at 1,000 products, so only 3 of its queries are checked, and
it is not run on the 5,000-product data.

`make bench-rdf-sparql` compares three engines that all evaluate queries with spareval:

- Raphtory: a `PersistentGraph`, queried through the same evaluator and dataset as `sparql()`;
- oxigraph's in-memory `Store`, which encodes terms as ids and keeps sorted indexes;
- spareval over an `oxrdf::Dataset`.

Every engine parses each query and reads each result once, then drops it. `sparql()` also copies every result into
owned terms, so it costs a little more than the Raphtory column for queries with many results. The updates are timed
on the same work for Raphtory and the dataset: each `INSERT DATA` is parsed from N-Triples, and each `DELETE WHERE`
runs its `SELECT` and removes what it finds. The Store runs them as SPARQL Update.

The times below all come from one run of `make bench-rdf-sparql` (the release build, both scales) on an Apple M4 Pro
laptop (14 cores). Each figure is the mean time per query: over the first 10 distinct queries of an explore template,
the first 2 of a BI template (only the first for Q7), the last 10 runs of the explore mix, or one Sparqloscope query.
An engine whose first run of a case takes more than 1 s (0.3 s for a Sparqloscope query) is timed by that one run,
which can be slower than later runs.

| Loading                                         | Raphtory | Store   | Dataset |
|-------------------------------------------------|----------|---------|---------|
| 1,000 products: the dataset, triples per second | 504,000  | 936,000 | 593,000 |
| 1,000 products: the updates, per second         | 3,036    | 3,904   | 2,678   |
| 5,000 products: the dataset, triples per second | 463,000  | 712,000 | 354,000 |

| Explore mix (ms per query)            | Raphtory      | Store         | Dataset       |
|---------------------------------------|---------------|---------------|---------------|
| 1,000 products: each template         | 0.019 to 0.92 | 0.010 to 0.57 | 0.010 to 0.95 |
| 1,000 products: the whole mix         | 0.226         | 0.140         | 0.203         |
| 1,000 products: query mixes per hour  | 638,000       | 1,026,000     | 710,000       |
| 5,000 products: each template         | 0.018 to 1.13 | 0.009 to 0.70 | 0.009 to 1.26 |
| 5,000 products: the whole mix         | 0.356         | 0.224         | 0.360         |
| 5,000 products: query mixes per hour  | 405,000       | 642,000       | 400,000       |

| BI mix (ms per query) | Raphtory, 1,000 | Store, 1,000 | Raphtory, 5,000 | Store, 5,000 | Dataset, 5,000 |
|-----------------------|-----------------|--------------|-----------------|--------------|----------------|
| Q1                    | 5.8             | 4.0          | 60              | 60           | 102            |
| Q2                    | 67              | 56           | 103             | 87           | 77             |
| Q3                    | 26              | 9.1          | 125             | 45           | 151            |
| Q4                    | 7.5             | 4.5          | 97              | 83           | 180            |
| Q5                    | 9.9             | 3.8          | 43              | 23           | 36             |
| Q6                    | 6.8             | 2.9          | 2.4             | 1.1          | 2.0            |
| Q7                    | 12,700          | 10,500       | not run         | not run      | not run        |
| Q8                    | 15              | 5.4          | 87              | 37           | 62             |

| Sparqloscope, 5,000 products (number of queries)                                | Raphtory       | Store           | Raphtory / Store |
|---------------------------------------------------------------------------------|----------------|-----------------|------------------|
| Patterns on a small predicate, such as `?s bsbm:productPropertyNumeric6 ?o` (7) | 0.52 to 1.9 ms | 0.15 to 0.86 ms | 1.1 to 3.6       |
| Filters and string functions on `?s rdfs:label ?o` (11)                         | 9.7 to 11 ms   | 2.4 to 3.4 ms   | 3.1 to 4.1       |
| Numeric, date and other filters on one large predicate (16)                     | 30 to 63 ms    | 14 to 37 ms     | 1.4 to 4         |
| Joins, `OPTIONAL`, `MINUS`, `EXISTS` and `UNION` of large predicates (40)       | 24 to 519 ms   | 13 to 253 ms    | 1.4 to 3         |
| Aggregates, property paths and exports (24)                                     | 0.02 to 170 ms | 0.01 to 68 ms   | 1.5 to 4.4       |
| Whole-graph counts, such as `COUNT(*) { ?s ?p ?o }` (6)                         | 0.73 to 1.4 s  | 0.13 to 0.25 s  | 4.1 to 5.8       |
| `EXISTS` chain of three large predicates (1)                                    | 0.26 s         | 301 s           | 0.001            |
| All 105, geometric mean                                                         |                |                 | 2.1 (1.4 against the dataset) |

What the numbers show:

- **Selective queries cost 1.1 to 2.1 times what they cost on the Store.** The explore queries start from a known
  product, offer or review, and Raphtory reads only that node's edges. Against spareval over an in-memory dataset
  they cost 0.8 to 2.1 times as much. The explore mix runs about 405,000 times an hour on the 5,000-product data,
  against 642,000 on the Store.
- **A pattern with only a predicate reads the triples of the predicate.** `?s <p> ?o` finds the edges of the layer in
  the edge storage, which indexes the edges of every layer (see [queries](#queries)), so a small predicate costs 1.1
  to 3.6 times what it costs on the Store.
- **Scans and aggregates over many triples cost 1.4 to 5.8 times more.** Joins compare node ids, but every value that a
  filter, a function or the result needs is turned from a node name back into an RDF term. A literal is parsed from
  its N-Triples form and an IRI is validated. The Store keeps numbers and dates in a native form and short strings
  inline. Exporting 100,000 to 168,965 rows costs 2.4 to 2.5 times more, and the whole-graph counts, which read every
  triple, 4.1 to 5.8 times more.
- **Loading** runs at about 460,000 to 500,000 triples per second, 54 to 65% of the Store's bulk loader. The 2,500
  updates run at about 3,000 per second (420,000 inserted triples per second): 78% of the Store's SPARQL Update, and
  faster than the same work on the dataset.
- **Raphtory is sometimes faster.** On the Sparqloscope query
  `?a bsbm:product ?b FILTER EXISTS { ?b rdf:type ?c . ?c rdfs:comment ?d }`, spareval takes 301 s on the Store and
  58 s on the dataset, but 0.26 s on Raphtory. BI templates 1 and 4 at 5,000 products take 1.0 and 1.2 times as long
  as on the Store, and 0.5 to 0.6 times as long as on the dataset.

```{.python hide}
import pytest
from raphtory import Graph, PersistentGraph

# properties, node types and nodes without edges are not part of the RDF view
g = Graph()
g.add_node(1, "lonely", properties={"colour": "red"}, node_type="Person")
g.add_edge(1, "Alice", "Bob", properties={"weight": 3})
assert g.to_rdf() == "<raphtory:Alice> <raphtory:_default> <raphtory:Bob> .\n"
assert g.sparql("SELECT ?s ?p ?o WHERE { ?s ?p ?o }") == [{"s": "Alice", "p": "_default", "o": "Bob"}]

# rdf:type is an ordinary layer, not a node type
t = PersistentGraph()
t.load_rdf(1, b"<http://example.org/alice> a <http://example.org/Person> .")
assert t.unique_layers == ["http://www.w3.org/1999/02/22-rdf-syntax-ns#type"]
assert t.node("http://example.org/alice").node_type is None

# terms are matched exactly, FILTER compares values; language tags are lower-cased
t.load_rdf(1, b'<http://example.org/carol> <http://example.org/age> "042"^^<http://www.w3.org/2001/XMLSchema#integer> ; <http://example.org/name> "Carol"@EN .')
assert t.sparql("ASK { ?s <http://example.org/age> 42 }") is False
assert t.sparql("ASK { ?s <http://example.org/age> ?a FILTER(?a = 42) }") is True
assert t.node('"Carol"@en') is not None

# IRIs under raphtory: must encode a name exactly
with pytest.raises(Exception, match="cannot be stored"):
    t.load_rdf(1, b"<raphtory:%61> <http://example.org/p> <http://example.org/b> .", format="nt")

# graphs with integer ids can be queried and exported, but not loaded into
i = Graph()
i.add_edge(1, 42, 43)
assert i.to_rdf() == "<raphtory:42> <raphtory:_default> <raphtory:43> .\n"
assert i.sparql("ASK { raphtory:42 raphtory:_default raphtory:43 }") is True
with pytest.raises(Exception, match="string node ids"):
    i.load_rdf(1, b"<http://example.org/a> <http://example.org/p> <http://example.org/b> .")
with pytest.raises(Exception, match="string node ids"):
    i.retract_rdf(1, b"<http://example.org/a> <http://example.org/p> <http://example.org/b> .")

# non-RDF edges are matched by patterns, dropped by CONSTRUCT and skipped by to_rdf
g.add_edge(1, '"x"', "Bob")
g.add_edge(1, "Alice", "Bob", layer="_:l")
assert len(g.sparql("SELECT * WHERE { ?s ?p ?o }")) == 3
assert g.sparql("CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }") == [("Alice", "_default", "Bob")]
assert g.to_rdf() == "<raphtory:Alice> <raphtory:_default> <raphtory:Bob> .\n"

# loads are not atomic
partial = PersistentGraph()
with pytest.raises(Exception, match="RDF parse error"):
    partial.load_rdf(
        1,
        b"<http://example.org/a> <http://example.org/p> <http://example.org/b> .\n<http://example.org/c> <http://example.org/p> .\n",
        format="nt",
    )
assert partial.count_edges() == 1

# an anonymous blank node in a retraction never matches
anonymous = PersistentGraph()
anonymous.load_rdf(1, b'[] <http://example.org/name> "x" .')
anonymous.retract_rdf(2, b'[] <http://example.org/name> "x" .')
assert len(anonymous.sparql("SELECT * WHERE { ?s <http://example.org/name> ?o }")) == 1

# brackets can nest 128 deep
assert t.sparql("ASK { FILTER(" + "(" * 126 + "true" + ")" * 126 + ") }") is True
with pytest.raises(Exception, match="SPARQL syntax error: brackets nest more than 128 deep"):
    t.sparql("ASK { FILTER(" + "(" * 127 + "true" + ")" * 127 + ") }")

# no SPARQL Update, and no SERVICE
with pytest.raises(Exception, match="SPARQL syntax error"):
    t.sparql("INSERT DATA { <http://example.org/a> <http://example.org/p> <http://example.org/b> }")
with pytest.raises(Exception, match="SPARQL evaluation error: SERVICE <http://example.org/sparql> is not supported"):
    t.sparql("SELECT * WHERE { SERVICE <http://example.org/sparql> { ?s ?p ?o } }")
assert t.sparql("SELECT * WHERE { SERVICE SILENT <http://example.org/sparql> { ?s ?p ?o } }") == [{"o": None, "p": None, "s": None}]

# the package parses RDF 1.2 and SPARQL 1.2, but stores RDF 1.1 triples only
with pytest.raises(Exception, match="RDF 1.2 triple terms"):
    PersistentGraph().load_rdf(
        1,
        b"<http://example.org/a> <http://example.org/p> <<( <http://example.org/s> <http://example.org/p> <http://example.org/o> )>> .",
        format="nt",
    )
annotated = PersistentGraph()
with pytest.raises(Exception, match="cannot be stored"):
    annotated.load_rdf(1, b"@prefix ex: <http://example.org/> . ex:a ex:p ex:b {| ex:since 2020 |} .")
assert annotated.count_edges() == 1
with pytest.raises(Exception, match="directional language-tagged strings"):
    PersistentGraph().load_rdf(1, b'<http://example.org/a> <http://example.org/p> "hi"@en--ltr .', format="nt")
assert t.sparql("ASK { ?s ?p ?o FILTER(isTRIPLE(?o)) }") is False
assert t.sparql("SELECT * WHERE { ?s ?p <<( ?a ?b ?c )>> }") == []

# time graphs: an unbound ?g matches nothing, the time graphs the query names are visited wherever GRAPH ?g is,
# and a time graph built from other values needs LATERAL
w = PersistentGraph()
w.load_rdf(1, b"<http://example.org/a> <http://example.org/p> <http://example.org/b> .")
assert w.sparql("ASK { GRAPH ?g { ?s ?p ?o } }") is False
assert len(w.sparql("SELECT ?s WHERE { VALUES ?g { raphtory:asof:5 } GRAPH ?g { ?s ?p ?o } }")) == 1
assert len(w.sparql("SELECT ?s WHERE { VALUES ?g { raphtory:asof:5 } GRAPH ?g { ?s ?p ?o OPTIONAL { ?o ?q ?r } } }")) == 1
assert len(w.sparql("SELECT ?s WHERE { VALUES ?g { raphtory:asof:5 } ?s ?p ?o MINUS { GRAPH ?g { ?s ?p ?o } } }")) == 0
assert len(w.sparql("SELECT ?s WHERE { ?s ?p ?o MINUS { GRAPH raphtory:asof:5 { ?s ?p ?o } } }")) == 0
built = 'VALUES ?t { 5 } BIND(IRI(CONCAT("raphtory:asof:", STR(?t))) AS ?g)'
pattern = "GRAPH ?g { ?s ?p ?o OPTIONAL { ?o ?q ?r } }"
assert w.sparql("SELECT ?s WHERE { " + built + " " + pattern + " }") == []
assert w.sparql("SELECT ?s WHERE { " + built + " LATERAL { " + pattern + " } }") == [{"s": "http://example.org/a"}]
# MINUS inside GRAPH ?g shares ?g
assert w.sparql("SELECT ?s WHERE { VALUES ?g { raphtory:asof:5 } GRAPH ?g { ?s ?p ?o MINUS { ?x ?y ?z } } }") == []

# time graphs are empty outside the window of a time view
assert w.sparql("ASK { GRAPH raphtory:asof:3 { ?s ?p ?o } }") is True
assert w.snapshot_latest().sparql("ASK { GRAPH raphtory:asof:3 { ?s ?p ?o } }") is False
assert w.snapshot_at(5).sparql("ASK { GRAPH raphtory:asof:3 { ?s ?p ?o } }") is False
assert w.snapshot_at(5).sparql("ASK { GRAPH raphtory:asof:5 { ?s ?p ?o } }") is True

# serialized results: XML rejects control characters, JSON keeps them, RDF/XML skips them
x = PersistentGraph()
x.load_rdf(1, b'<http://example.org/a> <http://example.org/p> "a\\rb", "ok" .')
with pytest.raises(Exception, match="cannot be written in SPARQL Results XML"):
    x.sparql("SELECT ?o WHERE { ?s ?p ?o }", format="xml")
assert '"a\\rb"' in x.sparql("SELECT ?o WHERE { ?s ?p ?o }", format="json")
assert x.sparql('SELECT ?o WHERE { ?s ?p ?o FILTER(?o = "ok") }', format="xml").count("<result>") == 1
assert len(x.sparql("CONSTRUCT WHERE { ?s ?p ?o }", format="nt").splitlines()) == 2
assert PersistentGraph().load_rdf(1, x.sparql("CONSTRUCT WHERE { ?s ?p ?o }", format="rdf").encode(), format="rdf") == 1
# RDF/XML skips the predicates it cannot write, such as the syntax term rdf:li, instead of failing
y = Graph()
y.add_edge(1, "http://example.org/a", "http://example.org/b", layer="http://www.w3.org/1999/02/22-rdf-syntax-ns#li")
y.add_edge(1, "http://example.org/a", "http://example.org/b", layer="http://www.w3.org/2000/xmlns/foo")
y.add_edge(1, "http://example.org/a", "http://example.org/b", layer="http://example.org/p")
assert PersistentGraph().load_rdf(1, y.to_rdf(format="rdf").encode(), format="rdf") == 1
assert PersistentGraph().load_rdf(1, y.sparql("CONSTRUCT WHERE { ?s ?p ?o }", format="rdf").encode(), format="rdf") == 1
# CONSTRUCT results in RDF/XML are grouped by subject, so an rdf:type is only skipped when its subject has no other triple
typed = """
    PREFIX ex: <http://example.org/>
    CONSTRUCT { ?s ex:q ?o . ?o ex:r ?s . ?s a ex:42 } WHERE { ?s ex:p ?o }
"""
assert PersistentGraph().load_rdf(1, y.sparql(typed, format="rdf").encode(), format="rdf") == 3
# serialized results are one string; decode_literals does not apply to them
assert isinstance(x.sparql("SELECT ?o WHERE { ?s ?p ?o }", format="csv"), str)
with pytest.raises(ValueError):
    x.sparql("SELECT ?o WHERE { ?s ?p ?o }", format="json", decode_literals=True)

# validity functions answer for the present inside GRAPH unless given ?g; bad references give None
v = PersistentGraph()
v.load_rdf(1, b"<http://example.org/a> <http://example.org/p> <http://example.org/b> .")
assert v.sparql("""
    SELECT ?now ?then WHERE {
        GRAPH raphtory:asof:5 { ?s ?p ?o }
        BIND(raphtory:validFromTime(?s, ?p, ?o) AS ?now)
        BIND(raphtory:validFromTime(?s, ?p, ?o, raphtory:asof:5) AS ?then)
    }
""", decode_literals=True) == [{"now": 1, "then": 1}]
v.retract_rdf(7, b"<http://example.org/a> <http://example.org/p> <http://example.org/b> .")
assert v.sparql("""
    SELECT ?now ?then WHERE {
        VALUES ?g { raphtory:asof:5 }
        GRAPH ?g { ?s ?p ?o }
        BIND(raphtory:validFromTime(?s, ?p, ?o) AS ?now)
        BIND(raphtory:validToTime(?s, ?p, ?o, ?g) AS ?then)
    }
""", decode_literals=True) == [{"now": None, "then": 7}]
assert v.sparql("SELECT (raphtory:validFromTime(?s, ?p, ?o, \"yesterday\") AS ?x) WHERE { GRAPH raphtory:asof:5 { ?s ?p ?o } }") == [{"x": None}]

# equal-valued literal objects are ambiguous for the validity functions
lit = PersistentGraph()
lit.load_rdf(1, b'<http://example.org/s> <http://example.org/age> "042"^^<http://www.w3.org/2001/XMLSchema#integer> .')
since = "SELECT (raphtory:validFromTime(<http://example.org/s>, <http://example.org/age>, 42) AS ?x) {}"
assert lit.sparql(since, decode_literals=True) == [{"x": 1}]
lit.load_rdf(2, b'<http://example.org/s> <http://example.org/age> "42"^^<http://www.w3.org/2001/XMLSchema#int> .')
assert lit.sparql(since, decode_literals=True) == [{"x": None}]
assert lit.snapshot_at(1).sparql(since, decode_literals=True) == [{"x": 1}]

# validity dates only cover the years 1 to 9999
far = PersistentGraph()
far.load_rdf(253402300800000, b"<http://example.org/a> <http://example.org/p> <http://example.org/b> .")
far_args = "<http://example.org/a>, <http://example.org/p>, <http://example.org/b>"
assert far.sparql(
    "SELECT (raphtory:validFrom(" + far_args + ") AS ?d) (raphtory:validFromTime(" + far_args + ") AS ?t) {}",
    decode_literals=True,
) == [{"d": None, "t": 253402300800000}]

# several FROM time graphs repeat triples
assert len(w.sparql("SELECT ?s FROM raphtory:asof:5 FROM raphtory:asof:6 WHERE { ?s ?p ?o }")) == 2
assert len(w.sparql("SELECT DISTINCT ?s FROM raphtory:asof:5 FROM raphtory:asof:6 WHERE { ?s ?p ?o }")) == 1
```
