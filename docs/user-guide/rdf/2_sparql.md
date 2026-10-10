# Querying with SPARQL

[sparql()][raphtory.RdfView.sparql] runs a [SPARQL 1.1](https://www.w3.org/TR/sparql11-query/) query on the triples of
an `RdfGraph` or any view of it: one triple per visible edge and layer (see the
[introduction](1_intro.md#how-triples-become-a-graph)). `SELECT`, `ASK`, `CONSTRUCT` and `DESCRIBE` are supported, with
the rest of the SPARQL 1.1 query language (property paths, aggregates, subqueries, `OPTIONAL`, `UNION`, `MINUS`,
`FILTER`, ...). `SERVICE` is not: no query ever sends a request to another endpoint.

The examples on this page query `now = rg.valid()`, the current state:

/// tab | :fontawesome-brands-python: Python
```python
from raphtory import RdfGraph

doc = b"""
@prefix ex:   <http://example.org/> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix xsd:  <http://www.w3.org/2001/XMLSchema#> .

ex:alice foaf:knows ex:bob ;
         foaf:name   "Alice" ;
         foaf:age    42 ;
         ex:height   1.68 ;
         ex:verified true ;
         ex:joined   "2024-01-02T09:30:00Z"^^xsd:dateTime .
ex:bob   foaf:knows ex:carol ;
         foaf:name   "Bob"@en ;
         foaf:age    42 .
ex:carol foaf:name   "Carol" ;
         foaf:age    "042"^^xsd:integer .
"""

rg = RdfGraph()
print(rg.load_rdf(1, doc))
now = rg.valid()
```
///

!!! Output

    ```output
    11
    ```

## Result shapes

- `SELECT` returns a list with one dictionary per solution, mapping the variables (in the order of the `SELECT`
  clause, sorted by name for `SELECT *`) to their values. An unbound variable is `None`.
- `ASK` returns a `bool`.
- `CONSTRUCT` and `DESCRIBE` return a list of `(subject, predicate, object)` tuples, without duplicates.

/// tab | :fontawesome-brands-python: Python
```{.python continuation}
rows = now.sparql("""
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    SELECT ?person ?friend WHERE {
        ?person foaf:name ?name .
        OPTIONAL { ?person foaf:knows ?friend }
    } ORDER BY ?person
""")
for row in rows:
    print(row)

print(now.sparql("ASK { <http://example.org/carol> <http://xmlns.com/foaf/0.1/knows> ?anyone }"))

print(sorted(now.sparql("""
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    CONSTRUCT { ?b <http://example.org/knownBy> ?a } WHERE { ?a foaf:knows ?b }
""")))
```
///

```{.python continuation hide}
assert rows[2] == {"person": "http://example.org/carol", "friend": None}
```

!!! Output

    ```output
    {'person': 'http://example.org/alice', 'friend': 'http://example.org/bob'}
    {'person': 'http://example.org/bob', 'friend': 'http://example.org/carol'}
    {'person': 'http://example.org/carol', 'friend': None}
    False
    [('http://example.org/bob', 'http://example.org/knownBy', 'http://example.org/alice'), ('http://example.org/carol', 'http://example.org/knownBy', 'http://example.org/bob')]
    ```

## Values are Raphtory names

Every value is returned as the Raphtory node name of its term, a `str` (see
[names and terms](1_intro.md#names-and-terms)): IRIs without angle brackets, literals in N-Triples form. So values can
be passed straight back to Raphtory, for example to `rg.graph.node()` or `rg.retract_triple()`. A predicate is returned as its IRI; its layer is
the escaped IRI (see [layer names](1_intro.md#layer-names)). Values computed by the query, such as a `COUNT`, are
literals too.

/// tab | :fontawesome-brands-python: Python
```{.python continuation}
rows = now.sparql("SELECT ?p ?o WHERE { <http://example.org/alice> ?p ?o } ORDER BY ?p")
for row in rows:
    print(row)

height = rows[0]["o"]
print(rg.graph.node(height).in_degree())
print(rg.graph.layer("http%3A%2F%2Fexample.org%2Fheight").count_edges())
```
///

```{.python continuation hide}
assert height == "\"1.68\"^^<http://www.w3.org/2001/XMLSchema#decimal>"
```

!!! Output

    ```output
    {'p': 'http://example.org/height', 'o': '"1.68"^^<http://www.w3.org/2001/XMLSchema#decimal>'}
    {'p': 'http://example.org/joined', 'o': '"2024-01-02T09:30:00Z"^^<http://www.w3.org/2001/XMLSchema#dateTime>'}
    {'p': 'http://example.org/verified', 'o': '"true"^^<http://www.w3.org/2001/XMLSchema#boolean>'}
    {'p': 'http://xmlns.com/foaf/0.1/age', 'o': '"42"^^<http://www.w3.org/2001/XMLSchema#integer>'}
    {'p': 'http://xmlns.com/foaf/0.1/knows', 'o': 'http://example.org/bob'}
    {'p': 'http://xmlns.com/foaf/0.1/name', 'o': '"Alice"'}
    1
    1
    ```

The only values without a name are IRIs under `raphtory:` that do not encode one, such as the
[time graphs](3_time-travel.md#comparing-times-in-one-query) `<raphtory:asof:T>`. They can only come from the query
itself, and are returned in N-Triples form, with angle brackets, such as `'<raphtory:asof:2024-01-01>'`.

### Decoding literals

`decode_literals=True` returns literals as Python values. Other literals, IRIs and blank nodes are still returned as
names:

| Literal                                                    | Python value                                           |
|------------------------------------------------------------|--------------------------------------------------------|
| plain strings, `xsd:string` and language-tagged strings    | `str` (the language tag is dropped)                    |
| `xsd:boolean`                                              | `bool`                                                 |
| `xsd:integer` and its subtypes, such as `xsd:int`          | `int`, if it fits in a signed 64-bit integer           |
| `xsd:decimal`                                              | `decimal.Decimal`, with at most 18 digits after the point, 38 significant digits and an absolute value below 1.7 × 10<sup>20</sup> |
| `xsd:float` and `xsd:double`                               | `float`                                                |
| `xsd:dateTime`                                             | `datetime`: in UTC if it has a timezone, naive otherwise, years 1 to 9999 |

/// tab | :fontawesome-brands-python: Python
```{.python continuation}
rows = now.sparql("SELECT ?p ?o WHERE { <http://example.org/alice> ?p ?o } ORDER BY ?p", decode_literals=True)
print([row["o"] for row in rows])
print(now.sparql("SELECT (COUNT(*) AS ?triples) WHERE { ?s ?p ?o }", decode_literals=True))
```
///

!!! Output

    ```output
    [Decimal('1.68'), datetime.datetime(2024, 1, 2, 9, 30, tzinfo=datetime.timezone.utc), True, 42, 'http://example.org/bob', 'Alice']
    [{'triples': 11}]
    ```

## Serialized results

`format` returns the results as one string in a standard format: a SPARQL results format for `SELECT` and `ASK`, an RDF
format for `CONSTRUCT` and `DESCRIBE`.

| `format`                                               | `SELECT` and `ASK`                                                                  | `CONSTRUCT` and `DESCRIBE` |
|--------------------------------------------------------|-------------------------------------------------------------------------------------|----------------------------|
| `"json"`                                               | [SPARQL 1.1 Query Results JSON](https://www.w3.org/TR/sparql11-results-json/)       | JSON-LD                    |
| `"xml"`                                                | [SPARQL Query Results XML](https://www.w3.org/TR/rdf-sparql-XMLres/)                | RDF/XML                    |
| `"csv"`, `"tsv"`                                       | [SPARQL 1.1 Query Results CSV and TSV](https://www.w3.org/TR/sparql11-results-csv-tsv/) | not available          |
| `"nt"`, `"ttl"`, `"nq"`, `"trig"`, `"jsonld"`, `"rdf"` | not available                                                                       | N-Triples, Turtle, N-Quads, TriG, JSON-LD, RDF/XML |

A format can also be a file extension (`".srj"`) or a media type (`"application/sparql-results+json"`). A format that
does not fit the form of the query raises an error before the query runs.

/// tab | :fontawesome-brands-python: Python
```{.python continuation}
print(now.sparql("""
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    SELECT ?person ?age WHERE { ?person foaf:age ?age } ORDER BY ?person
""", format="csv"))

print(now.sparql("""
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    CONSTRUCT { ?b <http://example.org/knownBy> ?a } WHERE { ?a foaf:knows ?b }
""", format="ttl"))
```
///

!!! Output

    ```output
    person,age
    http://example.org/alice,42
    http://example.org/bob,42
    http://example.org/carol,042

    <http://example.org/bob> <http://example.org/knownBy> <http://example.org/alice> .
    <http://example.org/carol> <http://example.org/knownBy> <http://example.org/bob> .

    ```

- **Terms, not names.** Serialized results hold RDF terms, as `to_rdf()` writes them: the node `Bob Smith` of a graph
  not loaded from RDF is `raphtory:Bob%20Smith` (remove `raphtory:` and percent-decode to get the name back).
  `decode_literals` cannot be combined with `format`.
- **CSV loses information.** As the W3C specification defines it, CSV writes only values: literals lose their datatype
  and language. Use JSON, XML or TSV to keep the terms.
- **ASK in CSV and TSV** is a bare `true` or `false`.

## Joining through values

Identical literals are one node, so you can join on a value in SPARQL, or traverse through it in Raphtory. Patterns and
joins match terms exactly: Carol's age `"042"` is a different term. A `FILTER` compares numbers and dates by value:

/// tab | :fontawesome-brands-python: Python
```{.python continuation}
print(now.sparql("""
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    SELECT ?other WHERE {
        <http://example.org/alice> foaf:age ?age .
        ?other foaf:age ?age .
        FILTER(?other != <http://example.org/alice>)
    }
"""))
print(now.sparql("""
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    SELECT ?person WHERE { ?person foaf:age ?age FILTER(?age = 42) } ORDER BY ?person
"""))
```
///

!!! Output

    ```output
    [{'other': 'http://example.org/bob'}]
    [{'person': 'http://example.org/alice'}, {'person': 'http://example.org/bob'}, {'person': 'http://example.org/carol'}]
    ```

## Querying views

Every view method of an `RdfGraph` returns an [RdfView][raphtory.RdfView], and a query sees exactly the triples of the
view:

- [layers()][raphtory.RdfView.layers], [layer()][raphtory.RdfView.layer] and
  [exclude_layers()][raphtory.RdfView.exclude_layers] keep or drop predicates, named by their layer names (see
  [layer names](1_intro.md#layer-names)).
- [subgraph()][raphtory.RdfView.subgraph] and [exclude_nodes()][raphtory.RdfView.exclude_nodes] keep the triples
  between the selected nodes. Literal values are nodes too.
- [filter()][raphtory.RdfView.filter] takes the [node and edge filters](../views/6_filtering.md) of Raphtory.
- [valid()][raphtory.RdfView.valid], [at()][raphtory.RdfView.at], [window()][raphtory.RdfView.window] and the other
  time views pick the triples of a time, as described on the [time travel](3_time-travel.md) page.

/// tab | :fontawesome-brands-python: Python
```{.python continuation}
count = "SELECT (COUNT(*) AS ?triples) WHERE { ?s ?p ?o }"
names_and_friends = ["http%3A%2F%2Fxmlns.com%2Ffoaf%2F0.1%2Fname", "http%3A%2F%2Fxmlns.com%2Ffoaf%2F0.1%2Fknows"]

print(now.layers(names_and_friends).sparql(count, decode_literals=True))
print(now.exclude_nodes(["http://example.org/carol"]).sparql(count, decode_literals=True))
print(now.subgraph(["http://example.org/alice", "http://example.org/bob", "http://example.org/carol"]).sparql(
    "SELECT ?s ?o WHERE { ?s ?p ?o } ORDER BY ?s"
))
```
///

!!! Output

    ```output
    [{'triples': 5}]
    [{'triples': 8}]
    [{'s': 'http://example.org/alice', 'o': 'http://example.org/bob'}, {'s': 'http://example.org/bob', 'o': 'http://example.org/carol'}]
    ```

## Errors and limits

A query that does not parse raises an exception starting with `SPARQL syntax error`, and one that fails while it runs,
`SPARQL evaluation error`. By default a query runs until it is done. To run queries you do not control, bound them:

- **`timeout`** (seconds) stops a query once it has run that long (`SPARQL query timed out`). A single expensive step,
  such as hashing a very long string, runs to its end first.
- **`max_triple_patterns`** rejects a query that is too large, before it runs (`SPARQL query too complex`). Planning
  cannot be interrupted by `timeout`, and its cost grows with the fourth power of the number of triple patterns joined
  together (about 0.6 s for 100 patterns and 1.5 s for 128, even on an empty graph). Every triple pattern counts one
  (also those the parser makes of collections and `[ ... ]`), a property path one per predicate, a `BIND` or a
  `SELECT` or `GROUP BY` expression one, and a `VALUES` block one plus one per variable and per 100 rows.

/// tab | :fontawesome-brands-python: Python
```{.python}
from raphtory import Graph, RdfGraph

chain = Graph()
for i in range(20):
    chain.add_edge(i, str(i), str(i + 1), layer="next")
rg = RdfGraph(chain)

# 20^8 combinations of eight unrelated patterns: far too many to count
huge = "SELECT (COUNT(*) AS ?n) WHERE { " + " ".join(f"?s{i} ?p{i} ?o{i} ." for i in range(8)) + " }"
try:
    rg.sparql(huge, timeout=0.5)
except Exception as e:
    print(str(e))

try:
    rg.sparql("SELECT * WHERE { ?a raphtory:next ?b . ?b raphtory:next ?c }", max_triple_patterns=1)
except Exception as e:
    print(str(e).split(" (")[0])

print(rg.sparql("ASK { raphtory:0 raphtory:next/raphtory:next raphtory:2 }", timeout=10, max_triple_patterns=10))
```
///

!!! Output

    ```output
    SPARQL query timed out: it ran longer than its time limit of 500ms
    SPARQL query too complex: 2 triple patterns, the limit is 1
    True
    ```

The [GraphQL server](5_graphql.md#limits) applies both limits to every query, by default 30 seconds and 100 triple
patterns.

## Using SPARQL from Rust

In Rust, `sparql()` is a method of `RdfGraph` and `RdfView`, and returns a `SparqlResults` whose terms are oxigraph
`Term`s. `sparql_to_writer()` streams the results to a `std::io::Write` in a `QueryResultsFormat` (`SELECT`, `ASK`),
an `RdfFormat` (`CONSTRUCT`, `DESCRIBE`) or a `SparqlFormat` with both, which `SparqlFormat::parse()` reads from the
names of the Python `format` argument. `sparql_with()` and `sparql_to_writer_with()` also take `SparqlOptions`:

- `with_temporal_functions(false)` turns off the [validity functions](3_time-travel.md#since-when-validity-functions)
  `raphtory:validFrom`, `validTo`, `validFromTime` and `validToTime` (whose IRIs are `VALID_FROM`, `VALID_TO`,
  `VALID_FROM_TIME` and `VALID_TO_TIME`), so a query that calls them fails.
- `with_timeout(Duration)` and `with_max_triple_patterns(usize)` are the limits above, failing with
  `RdfError::Timeout` and `RdfError::TooManyPatterns`. `SparqlOptions::default()` has neither.
- `with_cancellation_token(CancellationToken)` lets another thread stop the query by calling `cancel()` on a clone of
  the token (`RdfError::Cancelled`).

A query that is stopped returns no results, but `sparql_to_writer_with()` may already have written part of the
document.

```{.python continuation hide}
from datetime import datetime, timezone
from decimal import Decimal

import pytest

zoo = RdfGraph()
zoo.load_rdf(1, b"""
    @prefix ex:  <http://example.org/> .
    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
    ex:a ex:string "x"^^xsd:string ;
         ex:lang "y"@en-GB ;
         ex:int "5"^^xsd:int ;
         ex:big 123456789012345678901234567890 ;
         ex:fraction_18 1.123456789012345678 ;
         ex:fraction_19 1.1234567890123456789 ;
         ex:float "2.5"^^xsd:float ;
         ex:offset "2024-01-02T03:04:05+02:00"^^xsd:dateTime ;
         ex:naive "2024-01-02T03:04:05"^^xsd:dateTime ;
         ex:year0 "0000-01-01T00:00:00Z"^^xsd:dateTime ;
         ex:date "2024-01-02"^^xsd:date ;
         ex:blank [] .
""")
decoded = {
    row["p"].removeprefix("http://example.org/"): row["o"]
    for row in zoo.sparql("SELECT ?p ?o WHERE { ?s ?p ?o }", decode_literals=True)
}
XSD = "http://www.w3.org/2001/XMLSchema#"
assert decoded["string"] == "x" and decoded["lang"] == "y" and decoded["int"] == 5
assert decoded["big"] == f'"123456789012345678901234567890"^^<{XSD}integer>'
assert decoded["fraction_18"] == Decimal("1.123456789012345678")
assert decoded["fraction_19"] == f'"1.1234567890123456789"^^<{XSD}decimal>'
assert decoded["float"] == 2.5
assert decoded["offset"] == datetime(2024, 1, 2, 1, 4, 5, tzinfo=timezone.utc)
assert decoded["naive"] == datetime(2024, 1, 2, 3, 4, 5)
assert decoded["year0"] == f'"0000-01-01T00:00:00Z"^^<{XSD}dateTime>'
assert decoded["date"] == f'"2024-01-02"^^<{XSD}date>'
assert zoo.graph.node(decoded["blank"]) is not None

assert zoo.sparql("SELECT ?x WHERE { BIND(raphtory:asof:2024-01-01 AS ?x) }") == [{"x": "<raphtory:asof:2024-01-01>"}]
assert zoo.sparql("SELECT (COUNT(*) AS ?n) WHERE { ?s ?p ?o }") == [{"n": f'"12"^^<{XSD}integer>'}]
described = zoo.sparql("DESCRIBE <http://example.org/a>")
assert len(described) == len(set(described)) == 12

assert zoo.sparql("ASK { ?s ?p ?o }", format="csv") == "true"
assert zoo.sparql("ASK { ?s ?p ?o }", format="json") == '{"head":{},"boolean":true}'
with pytest.raises(Exception, match="SPARQL syntax error"):
    zoo.sparql("SELECT ?x WHERE { ?x ")
with pytest.raises(ValueError):
    zoo.sparql("ASK { ?s ?p ?o }", format="json", decode_literals=True)

people = Graph()
people.add_edge(1, "Alice", "Bob Smith", layer="knows")
assert RdfGraph(people).sparql("SELECT ?s ?o WHERE { ?s raphtory:knows ?o }", format="csv") == (
    "s,o\r\nraphtory:Alice,raphtory:Bob%20Smith\r\n"
)
```
