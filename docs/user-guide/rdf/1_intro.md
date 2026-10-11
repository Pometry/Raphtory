# RDF and SPARQL

[RDF](https://www.w3.org/TR/rdf11-primer/) is the W3C standard format for knowledge graphs. An RDF document is a set
of *triples*, each of the form `subject predicate object`, such as "alice knows bob" or "alice is 42 years old". Subjects
and predicates are IRIs (web-style identifiers such as `http://example.org/alice`), and objects are IRIs or *literals*:
values such as strings, numbers and dates. Subjects and objects can also be *blank nodes*, anonymous resources that only
have a label local to their document. [SPARQL](https://www.w3.org/TR/sparql11-query/) is the query language for RDF.

An [RdfGraph][raphtory.RdfGraph] views a Raphtory graph as RDF. It loads and retracts RDF documents, runs SPARQL 1.1
queries, exports RDF and validates against SHACL shapes. It can wrap any `Graph` or `PersistentGraph`, including one
that was never loaded from RDF. Every triple has a history: it is asserted at a time and can be retracted later, and
Raphtory's view methods (`valid()`, `at()`, `window()`, `layers()`, filters, ...) pick the triples a query sees.

The following pages cover [SPARQL queries](2_sparql.md), [time travel](3_time-travel.md), the
[limitations](4_limitations.md), [GraphQL](5_graphql.md), [SHACL validation](6_shacl.md) and the
[SPARQL endpoint](7_sparql_endpoint.md).

!!! info

    RDF support is part of the `raphtory` Python package. In Rust it is the `raphtory-rdf` crate (SHACL behind its
    `shacl` cargo feature), documented at
    [`raphtory_rdf`](https://docs.rs/raphtory-rdf/latest/raphtory_rdf/index.html)
    (`cargo doc -p raphtory-rdf --open`).

## A first example

[load_rdf()][raphtory.RdfGraph.load_rdf] asserts every triple of a document at a time and returns the number of
triples it read. [valid()][raphtory.RdfView.valid] is the current state of the graph, and
[sparql()][raphtory.RdfView.sparql] returns one dictionary per result row.

/// tab | :fontawesome-brands-python: Python
```python
from raphtory import RdfGraph

doc = b"""
@prefix ex:   <http://example.org/> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .

ex:alice foaf:knows ex:bob ;
         foaf:name  "Alice" ;
         foaf:age   42 .
ex:bob   foaf:name  "Bob"@en ;
         foaf:age   42 .
"""

rg = RdfGraph()
print(rg.load_rdf(1, doc))

rows = rg.valid().sparql("""
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    SELECT ?person ?name WHERE { ?person foaf:name ?name } ORDER BY ?person
""")
for row in rows:
    print(row)
```

!!! Output

    ```output
    5
    {'person': 'http://example.org/alice', 'name': '"Alice"'}
    {'person': 'http://example.org/bob', 'name': '"Bob"@en'}
    ```
///

/// tab | :fontawesome-brands-rust: Rust
```rust
use raphtory_rdf::{RdfFormat, RdfGraph, SparqlOptions, SparqlResults};

let doc = r#"
@prefix ex:   <http://example.org/> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .

ex:alice foaf:knows ex:bob ;
         foaf:name  "Alice" ;
         foaf:age   42 .
ex:bob   foaf:name  "Bob"@en ;
         foaf:age   42 .
"#;

let rg = RdfGraph::new();
let n = rg.load_rdf(1, doc.as_bytes(), RdfFormat::Turtle, None).unwrap();
println!("{n}");

let results = rg
    .valid()
    .sparql(
        "PREFIX foaf: <http://xmlns.com/foaf/0.1/>
         SELECT ?person ?name WHERE { ?person foaf:name ?name } ORDER BY ?person",
        &SparqlOptions::default(),
    )
    .unwrap();
if let SparqlResults::Solutions { rows, .. } = results {
    for row in rows {
        // one `Option<Term>` per variable, printed in N-Triples form
        let values: Vec<String> = row.iter().flatten().map(|term| term.to_string()).collect();
        println!("{values:?}");
    }
}
```

!!! Output

    ```output
    5
    ["<http://example.org/alice>", "\"Alice\""]
    ["<http://example.org/bob>", "\"Bob\"@en"]
    ```
///

In Python the values are the [Raphtory names](2_sparql.md#values-are-raphtory-names) of the RDF terms, as plain
strings. In Rust they are oxigraph `Term`s, printed here in N-Triples form, so IRIs have angle brackets.

```{.python continuation hide}
assert rg.graph.count_edges() == 5
assert rows == [
    {"person": "http://example.org/alice", "name": '"Alice"'},
    {"person": "http://example.org/bob", "name": '"Bob"@en'},
]
```

## How triples become a graph

1. **Every triple is an edge.** The triple `s p o` is an edge from the node `s` to the node `o` in the
   [layer](../views/3_layer.md) of `p`. Several predicates between the same two nodes are one edge with several layers.
2. **Names are RDF terms.** There is an exact, two-way mapping between node and layer names and RDF terms, described
   [below](#names-and-terms). An IRI is its own node name, and the layer of a predicate IRI is the IRI with `%`, `/`,
   `:`, `?` and `*` escaped: `<http://example.org/knows>` is the layer `http%3A%2F%2Fexample.org%2Fknows`.
3. **Literal values are nodes too.** A literal object becomes a node named by its
   [N-Triples](https://www.w3.org/TR/n-triples/) form, such as `"Alice"` (with the quotes) or
   `"42"^^<http://www.w3.org/2001/XMLSchema#integer>`. Identical literals share one node, so every subject with the age
   `42` is an in-neighbour of the node for `42`.
4. **Writes are timestamped, with persistent semantics.** Asserting a triple at `t` adds the edge at `t`, and
   retracting it at `t` deletes the edge at `t`: the triple holds from its assertion until its retraction.
5. **The view decides what is visible.** Queries, exports and SHACL see exactly the triples of the view they run on:
   on the whole graph every triple ever asserted or retracted, on `valid()` the current state, on `at(t)` the state as
   of `t`. The [time travel](3_time-travel.md) page has the details.

[graph][raphtory.RdfGraph.graph] is the same graph as a `PersistentGraph`, for the rest of Raphtory: algorithms,
nodes, edges, layers and exports to dataframes. On a view, `.graph` is the same view as a `GraphView`.

/// tab | :fontawesome-brands-python: Python
```{.python continuation}
g = rg.graph
for name in sorted(g.nodes.name):
    print(name)
print(sorted(g.unique_layers))
print(g.layer("http%3A%2F%2Fxmlns.com%2Ffoaf%2F0.1%2Fknows").has_edge("http://example.org/alice", "http://example.org/bob"))

age_42 = '"42"^^<http://www.w3.org/2001/XMLSchema#integer>'
print(sorted(g.node(age_42).in_neighbours.name))
```
///

```{.python continuation hide}
assert sorted(g.nodes.name) == [
    '"42"^^<http://www.w3.org/2001/XMLSchema#integer>',
    '"Alice"',
    '"Bob"@en',
    "http://example.org/alice",
    "http://example.org/bob",
]
assert g.node(age_42).in_degree() == 2
```

!!! Output

    ```output
    "42"^^<http://www.w3.org/2001/XMLSchema#integer>
    "Alice"
    "Bob"@en
    http://example.org/alice
    http://example.org/bob
    ['http%3A%2F%2Fxmlns.com%2Ffoaf%2F0.1%2Fage', 'http%3A%2F%2Fxmlns.com%2Ffoaf%2F0.1%2Fknows', 'http%3A%2F%2Fxmlns.com%2Ffoaf%2F0.1%2Fname']
    True
    ['http://example.org/alice', 'http://example.org/bob']
    ```

### Names and terms

Every Raphtory name stands for exactly one RDF term, and no two names stand for the same term. Every RDF term has a
name, except IRIs under `raphtory:` that are not the exact encoding of a name, such as `<raphtory:%61>` (the name `a`
is `<raphtory:a>`) and the [time graphs](3_time-travel.md#comparing-times-in-one-query) `<raphtory:asof:T>`.

| RDF term                       | Raphtory node name                                   |
|--------------------------------|------------------------------------------------------|
| IRI `<http://example.org/alice>` | `http://example.org/alice`                         |
| blank node `_:b1`              | `_:b1`                                               |
| literal `"Alice"`              | `"Alice"`, including the quotes                      |
| literal `"Bob"@en`             | `"Bob"@en`                                           |
| literal `42` (in Turtle)       | `"42"^^<http://www.w3.org/2001/XMLSchema#integer>`   |
| IRI `<raphtory:Alice>`         | `Alice`                                              |
| IRI `<raphtory:Bob%20Smith>`   | `Bob Smith`                                          |
| IRI `<raphtory:Zo%C3%AB>`      | `Zoë`                                                |
| IRI `<raphtory:42>`            | `42` (also the name of a node with integer id `42`)  |

- **IRIs** are kept as written. Any name that is a valid absolute IRI (a scheme such as `http:`, `urn:` or `user:`,
  and no character IRIs forbid, such as a space) is that IRI. Names starting with `raphtory:` are encoded like the
  names below.
- **Blank nodes** are named `_:` followed by their label (a label valid in N-Triples, so `_:a:b` is an IRI, not a blank
  node).
- **Literals** are named by their N-Triples form. Parsing lower-cases language tags (`"Bob"@EN` is `"Bob"@en`) and
  makes `"x"^^xsd:string` the plain `"x"`. Numbers keep their digits, so `"042"^^xsd:integer` and `"42"^^xsd:integer`
  are different nodes. A name added with `add_edge` is only a literal if it is in exactly this form.
- **Every other name** is the IRI `raphtory:` followed by the name with every character other than ASCII letters,
  digits, `-`, `.`, `_` and `~` percent-encoded as UTF-8. Write these IRIs in exactly this form: `raphtory:Zoë` is a
  different IRI, which matches nothing. The `raphtory:` prefix is registered in every query.

#### Layer names

Layer names must be safe as file names, so the layer of a predicate IRI is the IRI with `%` written `%25`, `/` written
`%2F`, `:` written `%3A`, `?` written `%3F` and `*` written `%2A`. IRIs under `raphtory:` follow the node rules instead:

| RDF term                                      | Layer name                                           |
|-----------------------------------------------|------------------------------------------------------|
| IRI `<http://example.org/knows>`              | `http%3A%2F%2Fexample.org%2Fknows`                   |
| IRI `<http://example.org/q?a=*>`              | `http%3A%2F%2Fexample.org%2Fq%3Fa=%2A`               |
| IRI `<urn:isbn:123>`                          | `urn%3Aisbn%3A123`                                   |
| IRI `<raphtory:knows>`                        | `knows`                                              |
| IRI `<raphtory:_default>`                     | `_default` (the default layer)                       |

Any other layer name, such as `http://example.org/p` or `ex:knows` in a graph not loaded from RDF, is the IRI
`raphtory:` followed by the percent-encoded name (`<raphtory:ex%3Aknows>`), so it never stands for the same predicate
as another layer. Loading RDF rejects a predicate under `raphtory:` whose layer name would be blank or contain a control
character or any of `/ \ : * ? " < > |`, such as `<raphtory:a%2Fb>`.

## Property graphs as RDF

`RdfGraph(g)` wraps an existing `Graph` or `PersistentGraph` without copying it: writes through either handle are seen
by both. [RdfGraph.load()][raphtory.RdfGraph.load] opens a stored graph folder of either kind. Any graph can be
queried, with the names of the [table above](#names-and-terms): the node `Alice` is `raphtory:Alice` and the layer
`knows` is `raphtory:knows`. Node and edge properties, node types and nodes without edges are not triples.

/// tab | :fontawesome-brands-python: Python
```python
from raphtory import Graph, RdfGraph

g = Graph()
g.add_edge(1, "Alice", "Bob", layer="knows")
g.add_edge(1, "Alice", "Bob Smith")

rg = RdfGraph(g)
print(rg.sparql("ASK { raphtory:Alice raphtory:knows raphtory:Bob }"))
print(rg.sparql("SELECT ?friend WHERE { raphtory:Alice raphtory:_default ?friend }"))
print(rg.to_rdf())

rg.add_triple(2, "Bob", "knows", "Carol")
print(g.has_edge("Bob", "Carol"))
```
///

```{.python continuation hide}
assert rg.sparql("ASK { raphtory:Alice raphtory:knows raphtory:Bob }") is True
assert rg.to_rdf() == (
    "<raphtory:Alice> <raphtory:knows> <raphtory:Bob> .\n"
    "<raphtory:Alice> <raphtory:_default> <raphtory:Bob%20Smith> .\n"
    "<raphtory:Bob> <raphtory:knows> <raphtory:Carol> .\n"
)
assert g.layer("knows").has_edge("Bob", "Carol")
```

!!! Output

    ```output
    True
    [{'friend': 'Bob Smith'}]
    <raphtory:Alice> <raphtory:knows> <raphtory:Bob> .
    <raphtory:Alice> <raphtory:_default> <raphtory:Bob%20Smith> .

    True
    ```

An `RdfGraph` always has persistent semantics. A retraction written through it into a wrapped event `Graph` is stored
as a deletion, which the `RdfGraph` sees. On an event `Graph` a retraction is a deletion event: it ends nothing, so
event views still show the triple, and a triple that was only retracted appears as an edge (`g.valid()` drops it).

## Loading RDF

[load_rdf()][raphtory.RdfGraph.load_rdf] takes:

- `time`: the time of the assertions, in any format `add_edge` accepts (milliseconds, a `datetime`, or a string such
  as `"2024-01-01"`).
- `source`: the document as `bytes`, or the path of a file as a `str` or `pathlib.Path`. A `str` is always a path, so
  pass a document held in a string as bytes (`doc.encode()`).
- `format` (optional): a file extension, name or media type. If not given, it comes from the extension of a path (an
  unknown extension raises an error), and is Turtle for bytes and for a path without an extension. Turtle also reads
  N-Triples.
- `base_iri` (optional): the IRI against which relative IRIs are resolved.

| Format    | Examples of `format` values               |
|-----------|-------------------------------------------|
| Turtle    | `"ttl"`, `"turtle"`, `"text/turtle"`      |
| N-Triples | `"nt"`, `"n-triples"`                     |
| N-Quads   | `"nq"`, `"n-quads"`                       |
| TriG      | `"trig"`                                  |
| RDF/XML   | `"rdf"`, `"xml"`, `"application/rdf+xml"` |
| JSON-LD   | `"jsonld"`, `"application/ld+json"`       |
| N3        | `"n3"`                                    |

- **Blank nodes are renamed.** Each load gives the blank nodes of the document fresh labels, so two documents that both
  use `_:b1` create two nodes.
- **Named graphs are rejected.** A triple in a named graph raises an error.
- **Loads are not atomic.** The triples read before an error stay in the graph.
- **String node ids.** Loading into a graph whose nodes have integer ids raises an error before anything is written.

/// tab | :fontawesome-brands-python: Python
```python
from raphtory import RdfGraph

rg = RdfGraph()

nt = b'<http://example.org/alice> <http://xmlns.com/foaf/0.1/knows> <http://example.org/bob> .'
print(rg.load_rdf(1, nt, format="nt"))

# relative IRIs are resolved against the base IRI
print(rg.load_rdf(1, b"<carol> <knows> <alice> .", base_iri="http://example.org/"))

# each load renames blank nodes, so these are two different nodes
blank = b'_:someone <http://xmlns.com/foaf/0.1/name> "Anonymous" .'
rg.load_rdf(1, blank)
rg.load_rdf(1, blank)
print(len([name for name in rg.graph.nodes.name if name.startswith("_:")]))

try:
    rg.load_rdf(1, b"<http://example.org/a> <http://example.org/p> <http://example.org/b> <http://example.org/g> .", format="nq")
except Exception as e:
    print(str(e).splitlines()[0])
```
///

```{.python continuation hide}
assert rg.graph.layer("http%3A%2F%2Fexample.org%2Fknows").has_edge("http://example.org/carol", "http://example.org/alice")
assert rg.graph.node('"Anonymous"').in_degree() == 2
assert rg.graph.node("http://example.org/g") is None
```

!!! Output

    ```output
    1
    1
    2
    RDF parse error: Named graphs are not allowed
    ```

### One triple at a time

[add_triple()][raphtory.RdfGraph.add_triple] and [retract_triple()][raphtory.RdfGraph.retract_triple] write a single
triple at its own time, for data where every triple has a time, such as a log of changes. The terms are node names, as
`sparql()` returns them: an IRI as written (`http://example.org/alice`), `_:label` for a blank node (used as written),
a literal in its N-Triples form, such as `"42"^^<http://www.w3.org/2001/XMLSchema#integer>`, and any other name as
`raphtory:` followed by the name (`Alice` is `raphtory:Alice`).

/// tab | :fontawesome-brands-python: Python
```python
from raphtory import RdfGraph

EX = "http://example.org/"
rg = RdfGraph()
rg.add_triple(1, f"{EX}alice", f"{EX}worksFor", f"{EX}acme")
rg.add_triple(2, f"{EX}alice", f"{EX}age", '"42"^^<http://www.w3.org/2001/XMLSchema#integer>')
rg.add_triple(3, f"{EX}bob", f"{EX}worksFor", f"{EX}acme")
rg.retract_triple(5, f"{EX}bob", f"{EX}worksFor", f"{EX}acme")

print(rg.valid().sparql("SELECT ?who WHERE { ?who <http://example.org/worksFor> <http://example.org/acme> }"))
```
///

```{.python continuation hide}
assert rg.valid().sparql("ASK { <http://example.org/alice> <http://example.org/age> 42 }") is True
```

!!! Output

    ```output
    [{'who': 'http://example.org/alice'}]
    ```

`load_rdf()`, `retract_rdf()`, `add_triple()` and `retract_triple()` write one edge event per triple, so they can run
while other threads or a server query the graph; a concurrent query may see part of a load.

## Retracting triples

[retract_rdf()][raphtory.RdfGraph.retract_rdf] takes the same arguments as `load_rdf()` and retracts every triple of
the document at the given time. From that time on the triple does not hold, while views of earlier times still show it:

/// tab | :fontawesome-brands-python: Python
```python
from raphtory import RdfGraph

rg = RdfGraph()
rg.load_rdf(1, b"""
    <http://example.org/alice> <http://example.org/worksFor> <http://example.org/acme> .
    <http://example.org/alice> <http://example.org/holdsSharesIn> <http://example.org/acme> .
""")
rg.retract_rdf(5, b"<http://example.org/alice> <http://example.org/worksFor> <http://example.org/acme> .")

query = "SELECT ?p ?o WHERE { <http://example.org/alice> ?p ?o } ORDER BY ?p"
print(rg.valid().sparql(query))  # now
print(rg.at(3).sparql(query))    # as of time 3
```
///

```{.python continuation hide}
assert rg.valid().sparql(query) == [{"p": "http://example.org/holdsSharesIn", "o": "http://example.org/acme"}]
assert rg.at(3).sparql(query) == rg.sparql(query)
```

!!! Output

    ```output
    [{'p': 'http://example.org/holdsSharesIn', 'o': 'http://example.org/acme'}]
    [{'p': 'http://example.org/holdsSharesIn', 'o': 'http://example.org/acme'}, {'p': 'http://example.org/worksFor', 'o': 'http://example.org/acme'}]
    ```

- **Retractions are always recorded**, even for a triple that was never asserted. The subject and object nodes are
  created, and the whole graph and any window containing the retraction show the triple, but `valid()` and `at(t)` do
  not.
- **Blank-node labels are used as written**, so a retraction must use the stored labels, as `to_rdf()` and `sparql()`
  return them. An anonymous blank node (`[]`) in a retraction never matches anything.

## Exporting RDF

[to_rdf()][raphtory.RdfView.to_rdf] writes the triples of a view, the same triples `sparql()` sees, as one triple per
visible edge and layer, without their history. It takes, all optional:

- `path`: the file to write. Without it the document is returned as a string.
- `format`: as for `load_rdf()`. Without it, it comes from the extension of `path` (an unknown extension raises an
  error), and is N-Triples otherwise.
- `prefixes`: prefix names mapped to IRIs, for the formats that use them (Turtle, TriG, RDF/XML).

/// tab | :fontawesome-brands-python: Python
```python
from raphtory import RdfGraph

rg = RdfGraph()
rg.load_rdf(1, b"""
    @prefix ex:   <http://example.org/> .
    @prefix foaf: <http://xmlns.com/foaf/0.1/> .
    ex:alice foaf:knows ex:bob ; foaf:age 42 .
""")
rg.retract_rdf(5, b"<http://example.org/alice> <http://xmlns.com/foaf/0.1/knows> <http://example.org/bob> .")

print(rg.valid().to_rdf())
print(rg.at(3).to_rdf(format="turtle", prefixes={"ex": "http://example.org/", "foaf": "http://xmlns.com/foaf/0.1/"}))
```
///

```{.python continuation hide}
assert rg.valid().to_rdf() == (
    '<http://example.org/alice> <http://xmlns.com/foaf/0.1/age> "42"^^<http://www.w3.org/2001/XMLSchema#integer> .\n'
)
ttl = rg.at(3).to_rdf(format="turtle", prefixes={"ex": "http://example.org/", "foaf": "http://xmlns.com/foaf/0.1/"})
assert "ex:alice foaf:knows ex:bob" in ttl and "foaf:age 42" in ttl
```

!!! Output

    ```output
    <http://example.org/alice> <http://xmlns.com/foaf/0.1/age> "42"^^<http://www.w3.org/2001/XMLSchema#integer> .

    @prefix foaf: <http://xmlns.com/foaf/0.1/> .
    @prefix ex: <http://example.org/> .
    ex:alice foaf:knows ex:bob ;
    	foaf:age 42 .

    ```

Exporting a graph and loading the document into a new graph gives back the same node and layer names, also for graphs
not loaded from RDF (`Alice` round-trips through `<raphtory:Alice>`). The exceptions are `_:label` names, which are
renamed blank nodes when loaded, and layer names that are not safe as file names, which `load_rdf()` rejects.

`to_rdf()` leaves out the edges that cannot be RDF triples: from a node named by a literal, or in a layer named by a
literal or a blank node. RDF/XML also leaves out the triples it cannot write (see the
[limitations](4_limitations.md#exporting)). In Rust, `to_rdf()` returns how many triples it wrote and skipped.
