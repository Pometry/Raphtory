# RDF and SPARQL

[RDF](https://www.w3.org/TR/rdf11-primer/) is the W3C standard format for knowledge graphs. An RDF document is a set
of *triples*, each of the form `subject predicate object`, such as "alice knows bob" or "alice is 42 years old". Subjects
and predicates are IRIs (web-style identifiers such as `http://example.org/alice`), and objects are IRIs or *literals*:
values such as strings, numbers and dates. Subjects and objects can also be *blank nodes*, anonymous resources that only
have a label local to their document. [SPARQL](https://www.w3.org/TR/sparql11-query/) is the query language for RDF.

Raphtory can load RDF documents into a `Graph` or `PersistentGraph`, query any graph or view with SPARQL 1.1, and
export any view back to RDF. Because Raphtory is a temporal graph, every triple also has a history: you assert triples
at a time, retract them at a later time, and can query the graph as it was at any point in time. The
[time travel](3_time-travel.md) page covers this in detail.

This page explains how triples map onto a Raphtory graph, and how to load, retract and export RDF. The following pages
cover [SPARQL queries](2_sparql.md), [time travel](3_time-travel.md), the [limitations](4_limitations.md) of the
current implementation, the [GraphQL server](5_graphql.md) and [SHACL validation](6_shacl.md).

!!! info

    RDF support is part of the `raphtory` Python package. In Rust it is behind the `rdf` cargo feature of the
    `raphtory` crate, and the API is documented in the
    [`raphtory::rdf`](https://docs.rs/raphtory/latest/raphtory/rdf/index.html) module. To build this documentation
    locally, run `cargo doc -p raphtory --features rdf --open`.

## A first example

The example below loads a small [Turtle](https://www.w3.org/TR/turtle/) document at time `1` and runs a SPARQL query
on it. [load_rdf()][raphtory.PersistentGraph.load_rdf] returns the number of triples it read, and
[sparql()][raphtory.GraphView.sparql] returns one dictionary per result row.

/// tab | :fontawesome-brands-python: Python
```python
from raphtory import PersistentGraph

doc = b"""
@prefix ex:   <http://example.org/> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .

ex:alice foaf:knows ex:bob ;
         foaf:name  "Alice" ;
         foaf:age   42 .
ex:bob   foaf:name  "Bob"@en ;
         foaf:age   42 .
"""

g = PersistentGraph()
print(g.load_rdf(1, doc))

rows = g.sparql("""
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
use raphtory::{
    prelude::*,
    rdf::{RdfFormat, SparqlResults},
};

let doc = r#"
@prefix ex:   <http://example.org/> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .

ex:alice foaf:knows ex:bob ;
         foaf:name  "Alice" ;
         foaf:age   42 .
ex:bob   foaf:name  "Bob"@en ;
         foaf:age   42 .
"#;

let g = PersistentGraph::new();
let n = g.load_rdf(1, doc.as_bytes(), RdfFormat::Turtle, None).unwrap();
println!("{n}");

let results = g
    .sparql(
        "PREFIX foaf: <http://xmlns.com/foaf/0.1/>
         SELECT ?person ?name WHERE { ?person foaf:name ?name } ORDER BY ?person",
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
assert g.count_edges() == 5
assert rows == [
    {"person": "http://example.org/alice", "name": '"Alice"'},
    {"person": "http://example.org/bob", "name": '"Bob"@en'},
]
```

## How triples become a graph

Raphtory stores RDF with a few simple rules:

1. **Every triple is an edge.** The triple `s p o` is an edge from the node `s` to the node `o` in the
   [layer](../views/3_layer.md) `p`. Several predicates between the same two nodes are one edge with several layers.
2. **Names are RDF terms.** Node names and layer names are strings, and there is an exact, two-way mapping between
   these strings and RDF terms, described [below](#names-and-terms). An IRI is simply its own name, so the IRI
   `<http://example.org/alice>` is the node named `http://example.org/alice`.
3. **Literal values are nodes too.** A literal object, such as `"Alice"` or the number `42`, becomes a node named by the
   literal written in [N-Triples](https://www.w3.org/TR/n-triples/) form, for example `"Alice"` (including the double
   quotes) or `"42"^^<http://www.w3.org/2001/XMLSchema#integer>`. Identical literals (the same lexical form, datatype
   and language tag) share one node, so you can traverse and join through values: every subject with the age `42` is
   an in-neighbour of the node for `42`. Literals that are equal in value but written differently, such as `42` and
   `"042"^^xsd:integer`, are [different nodes](#names-and-terms).
4. **Writes are timestamped.** Loading a triple at time `t` adds the edge at `t` (`add_edge`), and retracting it at `t`
   deletes the edge at `t` (`delete_edge`). This is the same on a `Graph` and a `PersistentGraph`.
5. **The view decides what is visible.** SPARQL queries and exports see the triples that are visible in the graph or
   view they are called on. On a `PersistentGraph` these are the triples whose latest assertion or retraction in the
   view is an assertion, so `g.snapshot_at(t)` is the state of the triples as of time `t`. On a `Graph` they are the
   triples asserted at least once in the view. The [time travel](3_time-travel.md) page explains both.

Continuing the example above, the five triples became five edges between five nodes, in three layers named after the
predicates:

/// tab | :fontawesome-brands-python: Python
```{.python continuation}
for name in sorted(g.nodes.name):
    print(name)
print(sorted(g.unique_layers))

FOAF = "http://xmlns.com/foaf/0.1/"
print(g.layer(FOAF + "knows").has_edge("http://example.org/alice", "http://example.org/bob"))

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
assert sorted(g.unique_layers) == [FOAF + "age", FOAF + "knows", FOAF + "name"]
assert g.node(age_42).in_degree() == 2
```

!!! Output

    ```output
    "42"^^<http://www.w3.org/2001/XMLSchema#integer>
    "Alice"
    "Bob"@en
    http://example.org/alice
    http://example.org/bob
    ['http://xmlns.com/foaf/0.1/age', 'http://xmlns.com/foaf/0.1/knows', 'http://xmlns.com/foaf/0.1/name']
    True
    ['http://example.org/alice', 'http://example.org/bob']
    ```

Everything else in Raphtory works on this graph as usual: algorithms, views, filters and exports to dataframes all see
ordinary nodes, edges and layers.

### Names and terms

Every Raphtory name stands for exactly one RDF term, and no two names stand for the same term. Every RDF term has a
name, except IRIs under `raphtory:` that are not the exact encoding of a name, such as `<raphtory:%61>` (the name `a`
is `<raphtory:a>`) and the [time graphs](3_time-travel.md#comparing-times-in-one-query) `<raphtory:asof:T>`; see
the [limitations](4_limitations.md#the-data-model). The mapping is used for both node names and layer names:

| RDF term                                                    | Raphtory name                                        |
|-------------------------------------------------------------|------------------------------------------------------|
| IRI `<http://example.org/alice>`                            | `http://example.org/alice`                           |
| blank node `_:b1`                                           | `_:b1`                                               |
| literal `"Alice"`                                           | `"Alice"`, including the quotes                      |
| literal `"Bob"@en`                                          | `"Bob"@en`                                           |
| literal `42` (in Turtle)                                    | `"42"^^<http://www.w3.org/2001/XMLSchema#integer>`   |
| IRI `<raphtory:Alice>`                                      | `Alice`                                              |
| IRI `<raphtory:Bob%20Smith>`                                | `Bob Smith`                                          |
| IRI `<raphtory:12%3A30>`                                    | `12:30`                                              |
| IRI `<raphtory:Zo%C3%AB>`                                   | `Zoë`                                                |
| IRI `<raphtory:_default>`                                   | `_default` (the default layer)                       |
| IRI `<raphtory:42>`                                         | `42` (also the name of a node with integer id `42`)  |

The rules behind the table are:

- **IRIs** are kept exactly as written. Any name that is a valid absolute IRI, meaning it starts with a scheme such
  as `http:`, `https:`, `urn:` or `mailto:` and contains no characters that IRIs forbid, such as spaces, is that IRI.
  This includes names such as `user:42`, whose `user:` looks like a scheme. Names starting with `raphtory:` are the only
  exception: they are encoded like the names below.
- **Blank nodes** are named `_:` followed by their label.
- **Literals** are named by their N-Triples form. Literals are normalised when a document is parsed: language tags are
  lower-cased (`"Bob"@EN` becomes `"Bob"@en`) and `"x"^^xsd:string` becomes the plain `"x"`. Numbers keep the digits
  they were written with, so `"042"^^xsd:integer` and `"42"^^xsd:integer` are different nodes.
- **Every other name**, such as `Alice` or the default layer `_default`, is the IRI `raphtory:` followed by the
  name with every character other than ASCII letters, digits, `-`, `.`, `_` and `~` percent-encoded as its UTF-8 bytes,
  so `Zoë` is `raphtory:Zo%C3%AB`. This lets you query graphs that were never loaded from RDF. Write these IRIs exactly
  in this form: `raphtory:Zoë` is a different IRI, which matches nothing.

The `raphtory:` prefix is registered in every SPARQL query, so you can write `raphtory:Alice` or
`raphtory:Bob%20Smith` instead of the full IRI. The example below queries and exports a graph built with `add_edge`:

/// tab | :fontawesome-brands-python: Python
```python
from raphtory import Graph

g = Graph()
g.add_edge(1, "Alice", "Bob", layer="knows")
g.add_edge(1, "Alice", "Bob Smith")

print(g.sparql("ASK { raphtory:Alice raphtory:knows raphtory:Bob }"))
print(g.sparql("SELECT ?friend WHERE { raphtory:Alice raphtory:_default ?friend }"))
print(g.to_rdf())
```
///

```{.python continuation hide}
assert g.sparql("ASK { raphtory:Alice raphtory:knows raphtory:Bob }") is True
assert g.sparql("SELECT ?friend WHERE { raphtory:Alice raphtory:_default ?friend }") == [{"friend": "Bob Smith"}]
assert g.to_rdf() == (
    "<raphtory:Alice> <raphtory:knows> <raphtory:Bob> .\n"
    "<raphtory:Alice> <raphtory:_default> <raphtory:Bob%20Smith> .\n"
)
```

!!! Output

    ```output
    True
    [{'friend': 'Bob Smith'}]
    <raphtory:Alice> <raphtory:knows> <raphtory:Bob> .
    <raphtory:Alice> <raphtory:_default> <raphtory:Bob%20Smith> .

    ```

!!! note

    A name is only a literal if it is the exact N-Triples form that the parsers produce. If you add literal nodes
    yourself with `add_edge`, write them in that form: `'"Bob"@en'` is a literal, but `'"Bob"@EN'` is not, so it is
    treated as an ordinary name and becomes the IRI `<raphtory:%22Bob%22%40EN>`.

## Loading RDF

[load_rdf()][raphtory.Graph.load_rdf] asserts every triple of a document at the given time. It is available on both
`Graph` and `PersistentGraph` and takes the following arguments:

- `time`: the time of the assertions, in any format that `add_edge` accepts (an integer number of milliseconds, a
  `datetime`, or a string such as `"2024-01-01"`).
- `source`: the document as `bytes`, or the path of a file as a `str` or `pathlib.Path`. A `str` is always read as a
  path, so pass documents held in a string as bytes, for example with `doc.encode()`.
- `format` (optional): the RDF format, as a file extension, a name or a media type. If it is not given, it is taken
  from the file extension of a path (an unknown extension raises an error), and is Turtle for bytes and for a path
  without an extension. Turtle also reads N-Triples.
- `base_iri` (optional): the IRI against which relative IRIs such as `<alice>` are resolved.

The supported formats are:

| Format    | Examples of `format` values               |
|-----------|-------------------------------------------|
| Turtle    | `"ttl"`, `"turtle"`, `"text/turtle"`      |
| N-Triples | `"nt"`, `"n-triples"`                     |
| N-Quads   | `"nq"`, `"n-quads"`                       |
| TriG      | `"trig"`                                  |
| RDF/XML   | `"rdf"`, `"xml"`, `"application/rdf+xml"` |
| JSON-LD   | `"jsonld"`, `"application/ld+json"`       |
| N3        | `"n3"`                                    |

Some details of loading:

- **Blank nodes are renamed.** Each load gives the blank nodes of the document fresh random labels, so loading two
  documents that both use `_:b1` creates two different nodes, as RDF requires.
- **Named graphs are rejected.** Raphtory stores a single RDF graph, so a triple in a named graph raises an error.
  N-Quads and TriG documents that only use the default graph load normally.
- **Loads are not atomic.** If a document fails to parse halfway through, the triples read before the error stay in the
  graph.
- **The graph needs string node ids.** RDF names are strings, so loading into a graph whose nodes were added with
  integer ids raises an error, before anything is written. An empty graph is fine.

/// tab | :fontawesome-brands-python: Python
```python
from raphtory import PersistentGraph

g = PersistentGraph()

# N-Triples, with an explicit format
nt = b'<http://example.org/alice> <http://xmlns.com/foaf/0.1/knows> <http://example.org/bob> .'
print(g.load_rdf(1, nt, format="nt"))

# relative IRIs are resolved against the base IRI
print(g.load_rdf(1, b"<carol> <knows> <alice> .", base_iri="http://example.org/"))

# each load renames blank nodes, so these are two different nodes
blank = b'_:someone <http://xmlns.com/foaf/0.1/name> "Anonymous" .'
g.load_rdf(1, blank)
g.load_rdf(1, blank)
print(len([name for name in g.nodes.name if name.startswith("_:")]))

# named graphs cannot be stored
try:
    g.load_rdf(1, b"<http://example.org/a> <http://example.org/p> <http://example.org/b> <http://example.org/g> .", format="nq")
except Exception as e:
    print(str(e).splitlines()[0])
```
///

```{.python continuation hide}
assert g.layer("http://example.org/knows").has_edge("http://example.org/carol", "http://example.org/alice")
assert len([name for name in g.nodes.name if name.startswith("_:")]) == 2
assert g.node('"Anonymous"').in_degree() == 2
assert g.node("http://example.org/g") is None
```

!!! Output

    ```output
    1
    1
    2
    RDF parse error: Named graphs are not allowed
    ```

### Triples with their own timestamps

`load_rdf()` gives every triple of a document the same time. If each triple has its own time, for example in a log of
changes, you do not need RDF documents at all: because names are plain strings, you can call `add_edge()` and
`delete_edge()` with the subject, object and predicate names directly, or load a whole table of triples with
[load_edges()][raphtory.PersistentGraph.load_edges], using the predicate column as the layer. `load_edges()` is a bulk
loader: run it only while nothing else reads or writes the graph, as below, where the graph is queried after the load.
To add triples while other threads or a server query the graph, use `add_edge()` and `delete_edge()` (or `load_rdf()`
and `retract_rdf()`), which are safe alongside queries:

/// tab | :fontawesome-brands-python: Python
```python
import pandas as pd
from raphtory import PersistentGraph

EX = "http://example.org/"
triples = pd.DataFrame(
    {
        "time": [1, 2, 3],
        "subject": [EX + "alice", EX + "alice", EX + "bob"],
        "predicate": [EX + "worksFor", EX + "age", EX + "worksFor"],
        "object": [EX + "acme", '"42"^^<http://www.w3.org/2001/XMLSchema#integer>', EX + "acme"],
    }
)

g = PersistentGraph()
g.load_edges(data=triples, time="time", src="subject", dst="object", layer_col="predicate")

# a single assertion and a single retraction
g.add_edge(4, EX + "carol", EX + "acme", layer=EX + "worksFor")
g.delete_edge(5, EX + "bob", EX + "acme", layer=EX + "worksFor")

print(g.sparql("SELECT ?who WHERE { ?who <http://example.org/worksFor> <http://example.org/acme> } ORDER BY ?who"))
```
///

```{.python continuation hide}
assert g.sparql("SELECT ?who WHERE { ?who <http://example.org/worksFor> <http://example.org/acme> } ORDER BY ?who") == [
    {"who": "http://example.org/alice"},
    {"who": "http://example.org/carol"},
]
assert g.sparql("ASK { <http://example.org/alice> <http://example.org/age> 42 }") is True
```

!!! Output

    ```output
    [{'who': 'http://example.org/alice'}, {'who': 'http://example.org/carol'}]
    ```

## Retracting triples

[retract_rdf()][raphtory.PersistentGraph.retract_rdf] takes the same arguments as `load_rdf()` and retracts every
triple of the document at the given time: for each triple it deletes the edge from the subject to the object in the
layer of the predicate, at that time. On a `PersistentGraph` the triple stops being visible from that time on, while
views of earlier times still show it:

/// tab | :fontawesome-brands-python: Python
```python
from raphtory import PersistentGraph

g = PersistentGraph()
g.load_rdf(1, b"""
    <http://example.org/alice> <http://example.org/worksFor> <http://example.org/acme> .
    <http://example.org/alice> <http://example.org/holdsSharesIn> <http://example.org/acme> .
""")
g.retract_rdf(5, b"<http://example.org/alice> <http://example.org/worksFor> <http://example.org/acme> .")

query = "SELECT ?p ?o WHERE { <http://example.org/alice> ?p ?o } ORDER BY ?p"
print(g.sparql(query))                 # now
print(g.snapshot_at(3).sparql(query))  # as of time 3
```
///

```{.python continuation hide}
assert g.sparql(query) == [{"p": "http://example.org/holdsSharesIn", "o": "http://example.org/acme"}]
assert g.snapshot_at(3).sparql(query) == [
    {"p": "http://example.org/holdsSharesIn", "o": "http://example.org/acme"},
    {"p": "http://example.org/worksFor", "o": "http://example.org/acme"},
]
assert g.count_edges() == 1
```

!!! Output

    ```output
    [{'p': 'http://example.org/holdsSharesIn', 'o': 'http://example.org/acme'}]
    [{'p': 'http://example.org/holdsSharesIn', 'o': 'http://example.org/acme'}, {'p': 'http://example.org/worksFor', 'o': 'http://example.org/acme'}]
    ```

Both triples are layers of the same edge, from `alice` to `acme`. Retracting one of them leaves the other untouched,
because each predicate is a separate layer.

A few things to keep in mind:

- **Retractions are always recorded**, even for triples that were never asserted. Such a retraction creates the
  subject and object nodes, but the triple is never visible.
- **Blank-node labels are used as written** when retracting, because the retraction has to match the labels that are
  stored in the graph. Since `load_rdf()` renames blank nodes, take the stored labels from `to_rdf()` or `sparql()`.
  An anonymous blank node (`[]`) in a retraction document never matches anything.
- **On a `Graph`, retractions are recorded but queries ignore them**: an event graph treats every assertion as an
  instantaneous event. Use a `PersistentGraph`, or `g.persistent_graph()`, to see them. The
  [time travel](3_time-travel.md#event-graphs-and-persistent-graphs) page explains the difference.

## Exporting RDF

[to_rdf()][raphtory.GraphView.to_rdf] writes the triples of a graph or view as an RDF document. It takes the following
optional arguments:

- `path`: the file to write. If it is not given, the document is returned as a string instead.
- `format`: the RDF format, as for `load_rdf()`. If it is not given, it is taken from the extension of `path` (an
  unknown extension raises an error), and is N-Triples when there is no `path` or the path has no extension.
- `prefixes`: a dictionary of prefix names to IRIs, used by formats that support prefixes (Turtle, TriG and RDF/XML).
  A prefix name that the format cannot write, such as `"a b"`, raises an error.

`to_rdf()` exports the triples that a SPARQL query on the same view sees, apart from the edges that cannot be written
as RDF (listed below), so it works on any view: for example `g.snapshot_at(t).to_rdf()` exports the state as of `t` and
`g.layer(p).to_rdf()` exports a single predicate. The output holds one triple per visible edge and layer. It is a
snapshot of the view, so the history of the triples is not exported.

/// tab | :fontawesome-brands-python: Python
```python
from raphtory import PersistentGraph

g = PersistentGraph()
g.load_rdf(1, b"""
    @prefix ex:   <http://example.org/> .
    @prefix foaf: <http://xmlns.com/foaf/0.1/> .
    ex:alice foaf:knows ex:bob ; foaf:age 42 .
""")
g.retract_rdf(5, b"<http://example.org/alice> <http://xmlns.com/foaf/0.1/knows> <http://example.org/bob> .")

print(g.to_rdf())
print(g.snapshot_at(3).to_rdf(format="turtle", prefixes={"ex": "http://example.org/", "foaf": "http://xmlns.com/foaf/0.1/"}))
```
///

```{.python continuation hide}
assert g.to_rdf() == (
    '<http://example.org/alice> <http://xmlns.com/foaf/0.1/age> "42"^^<http://www.w3.org/2001/XMLSchema#integer> .\n'
)
ttl = g.snapshot_at(3).to_rdf(format="turtle", prefixes={"ex": "http://example.org/", "foaf": "http://xmlns.com/foaf/0.1/"})
assert "@prefix ex: <http://example.org/> ." in ttl
assert "ex:alice foaf:knows ex:bob" in ttl
assert "foaf:age 42" in ttl
```

!!! Output

    ```output
    <http://example.org/alice> <http://xmlns.com/foaf/0.1/age> "42"^^<http://www.w3.org/2001/XMLSchema#integer> .

    @prefix foaf: <http://xmlns.com/foaf/0.1/> .
    @prefix ex: <http://example.org/> .
    ex:alice foaf:knows ex:bob ;
    	foaf:age 42 .

    ```

Exporting a graph and loading the document into a new graph gives back the same node names and layer names (for the
nodes that have edges), including for graphs that were not loaded from RDF: names such as `Alice` round-trip through
`<raphtory:Alice>`. The exception is names of the form `_:label`, which are blank nodes and so are renamed when they are
loaded.

Some edges cannot be written as RDF, and `to_rdf()` leaves them out:

- edges whose source node is named by a literal, because the subject of a triple cannot be a literal;
- edges in a layer named by a literal or a blank node, because a predicate must be an IRI;
- with RDF/XML only, triples whose predicate IRI does not end in a valid XML name (such as `http://example.org/42`),
  is in the namespace of XML's `xmlns` attributes or is an RDF/XML syntax term (such as `rdf:li` or `rdf:about`), an
  `rdf:type` triple whose object IRI does not end in a valid XML name or is in the `xmlns` namespace, when it is the
  only triple of its subject, and triples whose object is a literal with a control character other than tab and line
  feed, such as `"a\u0007b"` or a carriage return, because RDF/XML cannot write them, or XML parsers would read them
  back changed.

The first two kinds of edges can only exist in graphs that were not loaded from RDF. The RDF/XML cases can happen in
any graph, including one loaded from RDF, so use another format, such as Turtle or N-Triples, to export such a graph
completely. In Rust, `to_rdf()` returns the number of triples it wrote and the number it skipped.

RDF/XML also cannot write a blank node whose label starts with a digit, such as `_:1`, so it writes it with an `x` in
front, as `x1` (and `_:x1` as `xx1`, so that labels stay distinct). Blank-node labels are local to a document, so the
document has the same meaning, but a retraction document has to use the stored labels.

```{.python hide}
import os
import tempfile

import pytest
from raphtory import Graph, PersistentGraph

# the name <-> term table
names = Graph()
for name in ["http://example.org/alice", "_:b1", '"Alice"', '"Bob"@en', '"42"^^<http://www.w3.org/2001/XMLSchema#integer>',
             "Alice", "Bob Smith", "12:30", "Zoë", "user:42", "raphtory:x", '"Bob"@EN']:
    names.add_edge(1, "http://example.org/s", name, layer="http://example.org/p")
names.add_edge(1, "Alice", "Bob", layer="_default")
nt = names.to_rdf()
for term in ["<http://example.org/alice>", "_:b1", '"Alice"', '"Bob"@en', '"42"^^<http://www.w3.org/2001/XMLSchema#integer>',
             "<raphtory:Alice>", "<raphtory:Bob%20Smith>", "<raphtory:12%3A30>", "<raphtory:Zo%C3%AB>", "<user:42>",
             "<raphtory:raphtory%3Ax>", "<raphtory:%22Bob%22%40EN>", "<raphtory:_default>"]:
    assert term in nt, term
# only ASCII letters are kept: a raw non-ASCII letter makes a different IRI, which matches nothing
assert names.sparql("ASK { ?s ?p raphtory:Zo%C3%AB }") is True
assert names.sparql("ASK { ?s ?p raphtory:Zoë }") is False
# IRIs under raphtory: that encode no name have no name, and are returned in N-Triples form
assert names.sparql("SELECT ?x WHERE { BIND(<raphtory:%61> AS ?x) }") == [{"x": "<raphtory:%61>"}]
# identical literals share a node, literals equal in value but written differently do not
values = PersistentGraph()
values.load_rdf(1, b"""
    @prefix ex:  <http://example.org/> .
    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
    ex:a ex:age 42 . ex:b ex:age 42 . ex:c ex:age "042"^^xsd:integer . ex:d ex:age "42"^^xsd:int .
""")
assert sorted(values.node('"42"^^<http://www.w3.org/2001/XMLSchema#integer>').in_neighbours.name) == [
    "http://example.org/a",
    "http://example.org/b",
]
assert values.count_nodes() == 7
ids = Graph()
ids.add_edge(1, 42, 43)
assert ids.to_rdf() == "<raphtory:42> <raphtory:_default> <raphtory:43> .\n"

# parsing normalises language tags and xsd:string
norm = PersistentGraph()
norm.load_rdf(1, b'<http://example.org/a> <http://example.org/p> "Bob"@EN, "x"^^<http://www.w3.org/2001/XMLSchema#string> .')
assert sorted(norm.nodes.name) == ['"Bob"@en', '"x"', "http://example.org/a"]

# format names
doc = b"<http://example.org/a> <http://example.org/p> <http://example.org/b> ."
for fmt in ["ttl", "turtle", "text/turtle", "nt", "n-triples", "nq", "n-quads", "trig", "n3"]:
    assert PersistentGraph().load_rdf(1, doc, format=fmt) == 1, fmt
for fmt in ["rdf", "xml", "application/rdf+xml", "jsonld", "application/ld+json"]:
    exported = norm.to_rdf(format=fmt)
    assert PersistentGraph().load_rdf(1, exported.encode(), format=fmt) == 2, fmt

# a str source is a path, bytes are the document
with pytest.raises(Exception, match="pass the document as bytes"):
    PersistentGraph().load_rdf(1, doc.decode())
with tempfile.TemporaryDirectory() as tmp:
    path = os.path.join(tmp, "doc.nt")
    with open(path, "wb") as f:
        f.write(doc)
    assert PersistentGraph().load_rdf(1, path) == 1
    # a path without an extension (or a dotfile) is read as Turtle, which also reads N-Triples
    for name in ["doc", ".nt", "doc."]:
        bare = os.path.join(tmp, name)
        with open(bare, "wb") as f:
            f.write(doc)
        assert PersistentGraph().load_rdf(1, bare) == 1, name
    with pytest.raises(Exception, match="unknown RDF format 'data'"):
        PersistentGraph().load_rdf(1, os.path.join(tmp, "doc.data"))
    # to_rdf with a path writes the file, with the format taken from its extension
    out = os.path.join(tmp, "out.ttl")
    assert norm.to_rdf(out, prefixes={"ex": "http://example.org/"}) is None
    with open(out) as f:
        assert f.read() == norm.to_rdf(format="ttl", prefixes={"ex": "http://example.org/"})
    # an unknown extension raises, and a path without an extension is written as N-Triples
    for unknown in ["out.foo", "out.csv"]:
        with pytest.raises(Exception, match="unknown RDF format"):
            norm.to_rdf(os.path.join(tmp, unknown))
    no_extension = os.path.join(tmp, "out")
    assert norm.to_rdf(no_extension) is None
    with open(no_extension) as f:
        assert f.read() == norm.to_rdf(format="nt")
with pytest.raises(Exception, match="invalid prefix name"):
    norm.to_rdf(format="ttl", prefixes={"a b": "http://example.org/"})

# names round-trip through export and load, except _:label names, which are renamed blank nodes
plain = Graph()
plain.add_edge(1, "Alice", "Bob Smith", layer="knows")
plain.add_edge(1, "Alice", "12:30")
plain.add_edge(1, "Alice", "_:b1")
copy = Graph()
copy.load_rdf(1, plain.to_rdf().encode(), format="nt")
assert sorted(n for n in copy.nodes.name if not n.startswith("_:")) == ["12:30", "Alice", "Bob Smith"]
assert sorted(copy.unique_layers) == sorted(plain.unique_layers)
assert [n for n in copy.nodes.name if n.startswith("_:")] != ["_:b1"]

# edges that are not valid RDF are left out, as are predicates RDF/XML cannot write
odd = Graph()
odd.add_edge(1, '"x"', "Bob")
odd.add_edge(1, "Alice", "Bob", layer="_:l")
odd.add_edge(1, "Alice", "Bob", layer='"p"')
odd.add_edge(1, "http://example.org/a", "http://example.org/b", layer="http://example.org/42")
assert odd.to_rdf() == "<http://example.org/a> <http://example.org/42> <http://example.org/b> .\n"
assert "example.org/42" not in odd.to_rdf(format="rdf")

# RDF/XML leaves out literals with control characters, and renames blank nodes whose label starts with a digit
controls = PersistentGraph()
controls.load_rdf(1, b'<http://example.org/a> <http://example.org/p> "bell\\u0007", "a\\rb", "tab\\tnewline\\n" .', format="ttl")
xml = controls.to_rdf(format="rdf")
assert "bell" not in xml and "a\rb" not in xml and "tab\tnewline\n" in xml
assert len(controls.to_rdf().splitlines()) == 3
blanks = Graph()
blanks.add_edge(1, "_:1", "http://example.org/o", layer="http://example.org/p")
blanks.add_edge(1, "http://example.org/s", "_:42abc", layer="http://example.org/p")
blanks.add_edge(1, "http://example.org/s", "_:x1", layer="http://example.org/p")
xml = blanks.to_rdf(format="rdf")
assert 'rdf:nodeID="x1"' in xml and 'rdf:nodeID="x42abc"' in xml and 'rdf:nodeID="xx1"' in xml
copy = PersistentGraph()
assert copy.retract_rdf(1, xml.encode(), format="rdf") == 3
assert sorted(n for n in copy.nodes.name if n.startswith("_:")) == ["_:x1", "_:x42abc", "_:xx1"]

# RDF/XML also leaves out such triples of a graph loaded from RDF
loaded = PersistentGraph()
loaded.load_rdf(1, b"""
    @prefix ex: <http://example.org/> .
    ex:a ex:42 ex:b .
    ex:c a ex:42 .
""")
assert loaded.sparql("SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }", decode_literals=True) == [{"n": 2}]
assert len(loaded.to_rdf().splitlines()) == 2
assert "example.org/42" not in loaded.to_rdf(format="rdf")
# an rdf:type triple is written when its subject has another triple
loaded.load_rdf(1, b'<http://example.org/c> <http://example.org/name> "C" .')
assert '<rdf:type rdf:resource="http://example.org/42"/>' in loaded.to_rdf(format="rdf")

# retracting on a Graph is recorded, but only persistent views see it
events = Graph()
events.load_rdf(1, doc)
events.retract_rdf(2, doc)
assert events.sparql("ASK { ?s ?p ?o }") is True
assert events.persistent_graph().sparql("ASK { ?s ?p ?o }") is False

# an empty graph accepts RDF, a graph with integer ids does not
with pytest.raises(Exception, match="string node ids"):
    ids.load_rdf(1, doc)
```
