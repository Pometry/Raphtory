# Querying with SPARQL

[sparql()][raphtory.GraphView.sparql] runs a [SPARQL 1.1](https://www.w3.org/TR/sparql11-query/) query on the RDF
triples of a graph. It is available on `Graph`, `PersistentGraph` and every view of them, so you can combine SPARQL with
Raphtory's layers, subgraphs, filters and time windows. The query sees one triple per visible edge and layer, as
described in the [introduction](1_intro.md#how-triples-become-a-graph). All four query forms are supported (`SELECT`,
`ASK`, `CONSTRUCT` and `DESCRIBE`), together with the rest of the SPARQL 1.1 query language, such as property paths,
aggregates, subqueries, `OPTIONAL`, `UNION`, `MINUS` and `FILTER`. Federated queries are not: a query that calls a
`SERVICE` fails, and no query ever sends a request to another endpoint (see the [limitations](4_limitations.md#queries)).

The examples on this page use the following graph:

/// tab | :fontawesome-brands-python: Python
```python
from raphtory import PersistentGraph

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

g = PersistentGraph()
print(g.load_rdf(1, doc))
```
///

```{.python continuation hide}
assert g.count_edges() == 11
```

!!! Output

    ```output
    11
    ```

## Result shapes

The type of the result depends on the query form:

- `SELECT` returns a list with one dictionary per solution. Each dictionary maps the variables, in the order of the
  `SELECT` clause (sorted by name for `SELECT *`), to their values. A variable without a value, for example from an
  `OPTIONAL` pattern that did not match, is `None`.
- `ASK` returns a `bool`.
- `CONSTRUCT` and `DESCRIBE` return a list of `(subject, predicate, object)` tuples, without duplicates.

To get the results as a document in a standard format instead, such as SPARQL Results JSON or Turtle, see
[serialized results](#serialized-results).

/// tab | :fontawesome-brands-python: Python
```{.python continuation}
rows = g.sparql("""
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    SELECT ?person ?friend WHERE {
        ?person foaf:name ?name .
        OPTIONAL { ?person foaf:knows ?friend }
    } ORDER BY ?person
""")
for row in rows:
    print(row)

print(g.sparql("ASK { <http://example.org/carol> <http://xmlns.com/foaf/0.1/knows> ?anyone }"))

print(g.sparql("""
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    CONSTRUCT { ?b <http://example.org/knownBy> ?a } WHERE { ?a foaf:knows ?b }
"""))
```
///

```{.python continuation hide}
assert rows == [
    {"person": "http://example.org/alice", "friend": "http://example.org/bob"},
    {"person": "http://example.org/bob", "friend": "http://example.org/carol"},
    {"person": "http://example.org/carol", "friend": None},
]
assert g.sparql("ASK { <http://example.org/carol> <http://xmlns.com/foaf/0.1/knows> ?anyone }") is False
assert sorted(
    g.sparql("""
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    CONSTRUCT { ?b <http://example.org/knownBy> ?a } WHERE { ?a foaf:knows ?b }
""")
) == [
    ("http://example.org/bob", "http://example.org/knownBy", "http://example.org/alice"),
    ("http://example.org/carol", "http://example.org/knownBy", "http://example.org/bob"),
]
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

Every value in a result is returned as the Raphtory name of its RDF term, a `str`, using the mapping described in the
[introduction](1_intro.md#names-and-terms), with one exception described below. IRIs are returned as plain strings
without angle brackets, and literals in their N-Triples form. This means you can pass the values straight back to the
Raphtory API, for example to `g.node()` or `g.layer()`:

/// tab | :fontawesome-brands-python: Python
```{.python continuation}
rows = g.sparql("""
    SELECT ?p ?o WHERE { <http://example.org/alice> ?p ?o } ORDER BY ?p
""")
for row in rows:
    print(row)

# continue in Raphtory: the value is a node, and the predicate is a layer
height = rows[0]["o"]
print(g.node(height).in_degree())
print(g.layer(rows[0]["p"]).count_edges())
```
///

```{.python continuation hide}
assert [row["p"] for row in rows] == [
    "http://example.org/height",
    "http://example.org/joined",
    "http://example.org/verified",
    "http://xmlns.com/foaf/0.1/age",
    "http://xmlns.com/foaf/0.1/knows",
    "http://xmlns.com/foaf/0.1/name",
]
assert height == "\"1.68\"^^<http://www.w3.org/2001/XMLSchema#decimal>"
assert g.node(height).in_degree() == 1
assert g.layer(rows[0]["p"]).count_edges() == 1
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

Values computed by the query, such as a `COUNT` or a `STR()`, are literals too, and are returned in the same N-Triples
form, for example `'"3"^^<http://www.w3.org/2001/XMLSchema#integer>'`.

The only values without a Raphtory name are IRIs under `raphtory:` that are not the exact encoding of a name, such as
the [time graphs](3_time-travel.md#comparing-times-in-one-query) `<raphtory:asof:T>` or a constant such as
`<raphtory:%61>` (the name `a` is `<raphtory:a>`). They can only come from the query itself, never from the graph, and
are returned in N-Triples form, with angle brackets, for example `'<raphtory:asof:2024-01-01>'`. As a name, such a
string stands for a different term, so do not pass it to `g.node()`.

### Decoding literals

Pass `decode_literals=True` to get literals as Python values instead of names. Literals of the following datatypes are
decoded, if their value fits as described; everything else, including IRIs and blank nodes, is still returned as its
name:

| Literal                                                    | Python value                                           |
|------------------------------------------------------------|--------------------------------------------------------|
| plain strings, `xsd:string` and language-tagged strings    | `str` (the language tag is dropped)                    |
| `xsd:boolean`                                              | `bool`                                                 |
| `xsd:integer` and its subtypes, such as `xsd:int`          | `int`, if it fits in a signed 64-bit integer           |
| `xsd:decimal`                                              | `decimal.Decimal`, if it has at most 18 digits after the decimal point (ignoring trailing zeros), at most 38 significant digits and an absolute value below 1.7 × 10<sup>20</sup> |
| `xsd:float` and `xsd:double`                               | `float`                                                |
| `xsd:dateTime`                                             | `datetime`: in UTC if the literal has a timezone, naive otherwise, and only for the years 1 to 9999 |

/// tab | :fontawesome-brands-python: Python
```{.python continuation}
rows = g.sparql("""
    SELECT ?p ?o WHERE { <http://example.org/alice> ?p ?o } ORDER BY ?p
""", decode_literals=True)
for row in rows:
    print(row)

print(g.sparql("SELECT (COUNT(*) AS ?triples) WHERE { ?s ?p ?o }"))
print(g.sparql("SELECT (COUNT(*) AS ?triples) WHERE { ?s ?p ?o }", decode_literals=True))
```
///

```{.python continuation hide}
from datetime import datetime, timezone
from decimal import Decimal

assert [row["o"] for row in rows] == [
    Decimal("1.68"),
    datetime(2024, 1, 2, 9, 30, tzinfo=timezone.utc),
    True,
    42,
    "http://example.org/bob",
    "Alice",
]
assert g.sparql("SELECT (COUNT(*) AS ?triples) WHERE { ?s ?p ?o }") == [
    {"triples": '"11"^^<http://www.w3.org/2001/XMLSchema#integer>'}
]
assert g.sparql("SELECT (COUNT(*) AS ?triples) WHERE { ?s ?p ?o }", decode_literals=True) == [{"triples": 11}]
```

!!! Output

    ```output
    {'p': 'http://example.org/height', 'o': Decimal('1.68')}
    {'p': 'http://example.org/joined', 'o': datetime.datetime(2024, 1, 2, 9, 30, tzinfo=datetime.timezone.utc)}
    {'p': 'http://example.org/verified', 'o': True}
    {'p': 'http://xmlns.com/foaf/0.1/age', 'o': 42}
    {'p': 'http://xmlns.com/foaf/0.1/knows', 'o': 'http://example.org/bob'}
    {'p': 'http://xmlns.com/foaf/0.1/name', 'o': 'Alice'}
    [{'triples': '"11"^^<http://www.w3.org/2001/XMLSchema#integer>'}]
    [{'triples': 11}]
    ```

Decoded values cannot be passed back to `g.node()`, so only decode literals when you no longer need the names.

## Serialized results

Pass `format` to get the results as one string in a standard format instead of Python objects, for example to save them
to a file or to hand them to another tool. `SELECT` and `ASK` results are written in a SPARQL results format, and
`CONSTRUCT` and `DESCRIBE` results in an RDF format:

| `format`                                                      | `SELECT` and `ASK`                                                                  | `CONSTRUCT` and `DESCRIBE` |
|---------------------------------------------------------------|-------------------------------------------------------------------------------------|----------------------------|
| `"json"`                                                      | [SPARQL 1.1 Query Results JSON](https://www.w3.org/TR/sparql11-results-json/)       | JSON-LD                    |
| `"xml"`                                                       | [SPARQL Query Results XML](https://www.w3.org/TR/rdf-sparql-XMLres/)                | RDF/XML                    |
| `"csv"`, `"tsv"`                                              | [SPARQL 1.1 Query Results CSV and TSV](https://www.w3.org/TR/sparql11-results-csv-tsv/) | not available          |
| `"nt"`, `"ttl"`, `"nq"`, `"trig"`, `"jsonld"`, `"rdf"`        | not available                                                                       | N-Triples, Turtle, N-Quads, TriG, JSON-LD, RDF/XML |

A format can also be given as a file extension with a dot (`".srj"`), or as a media type such as
`"application/sparql-results+json"` or `"text/turtle"`. As the table shows, `"json"` and `"xml"` mean a different format
for each form of query. A format that does not fit the form of the query, such as `"ttl"` for a `SELECT` query, raises
an error before the query runs.

/// tab | :fontawesome-brands-python: Python
```{.python continuation}
import json

csv = g.sparql("""
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    SELECT ?person ?age WHERE { ?person foaf:age ?age } ORDER BY ?person
""", format="csv")
print(csv)

results = json.loads(g.sparql("""
    SELECT ?name WHERE { <http://example.org/bob> <http://xmlns.com/foaf/0.1/name> ?name }
""", format="json"))
print(results)

print(g.sparql("""
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    CONSTRUCT { ?b <http://example.org/knownBy> ?a } WHERE { ?a foaf:knows ?b }
""", format="ttl"))
```
///

```{.python continuation hide}
assert csv == (
    "person,age\r\n"
    "http://example.org/alice,42\r\n"
    "http://example.org/bob,42\r\n"
    "http://example.org/carol,042\r\n"
)
assert results == {
    "head": {"vars": ["name"]},
    "results": {"bindings": [{"name": {"type": "literal", "value": "Bob", "xml:lang": "en"}}]},
}
assert g.sparql("ASK { ?s ?p ?o }", format="csv") == "true"
assert g.sparql("ASK { ?s ?p ?o }", format="json") == '{"head":{},"boolean":true}'
```

!!! Output

    ```output
    person,age
    http://example.org/alice,42
    http://example.org/bob,42
    http://example.org/carol,042

    {'head': {'vars': ['name']}, 'results': {'bindings': [{'name': {'type': 'literal', 'value': 'Bob', 'xml:lang': 'en'}}]}}
    <http://example.org/bob> <http://example.org/knownBy> <http://example.org/alice> .
    <http://example.org/carol> <http://example.org/knownBy> <http://example.org/bob> .

    ```

Some things to keep in mind:

- **Terms, not names.** Serialized results hold RDF terms, written as `to_rdf()` writes them, not the Raphtory names
  that `sparql()` otherwise returns. In a graph that was not loaded from RDF, the node `Bob Smith` is the IRI
  `raphtory:Bob%20Smith`. To get a name back from such an IRI, remove `raphtory:` and percent-decode the rest, for
  example with `urllib.parse.unquote`. `decode_literals` does not apply to serialized results, and passing both raises a
  `ValueError`.
- **CSV loses information.** As the W3C specification defines it, CSV writes only the value of each term: IRIs, blank
  nodes and literals look alike, and literals lose their datatype and language (Bob's name `"Bob"@en` is written `Bob`).
  Use JSON, XML or TSV to keep the full terms.
- **ASK in CSV and TSV.** The CSV and TSV formats define no form for a boolean, so an `ASK` result is written as a bare
  `true` or `false`.
- **Limits.** SPARQL Results XML cannot hold a literal with a control character, and RDF/XML skips the triples it cannot
  write, as described in the [limitations](4_limitations.md#queries).

/// tab | :fontawesome-brands-python: Python
```{.python continuation}
from urllib.parse import unquote

from raphtory import Graph

people = Graph()
people.add_edge(1, "Alice", "Bob Smith", layer="knows")
csv = people.sparql("SELECT ?s ?o WHERE { ?s raphtory:knows ?o }", format="csv")
print(csv)

iri = csv.splitlines()[1].split(",")[1]
print(unquote(iri.removeprefix("raphtory:")))
```
///

```{.python continuation hide}
assert csv == "s,o\r\nraphtory:Alice,raphtory:Bob%20Smith\r\n"
assert people.node(unquote(iri.removeprefix("raphtory:"))) is not None
assert people.sparql("SELECT ?s ?o WHERE { ?s raphtory:knows ?o }", format="tsv") == (
    "?s\t?o\n<raphtory:Alice>\t<raphtory:Bob%20Smith>\n"
)
```

!!! Output

    ```output
    s,o
    raphtory:Alice,raphtory:Bob%20Smith

    Bob Smith
    ```

## Joining through values

Because identical literals are one node, you can join on a value in SPARQL, or traverse through the value's node in
Raphtory. Here the SPARQL join finds the other people with the same age as Alice, and the in-neighbours of the node
for `42` are everyone aged 42, including Alice:

/// tab | :fontawesome-brands-python: Python
```{.python continuation}
print(g.sparql("""
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    SELECT ?other WHERE {
        <http://example.org/alice> foaf:age ?age .
        ?other foaf:age ?age .
        FILTER(?other != <http://example.org/alice>)
    }
"""))

age_42 = '"42"^^<http://www.w3.org/2001/XMLSchema#integer>'
print(sorted(g.node(age_42).in_neighbours.name))
```
///

```{.python continuation hide}
assert sorted(g.node(age_42).in_neighbours.name) == ["http://example.org/alice", "http://example.org/bob"]
```

!!! Output

    ```output
    [{'other': 'http://example.org/bob'}]
    ['http://example.org/alice', 'http://example.org/bob']
    ```

Carol is missing from both answers: her age was written as `"042"`, which is a different RDF term and therefore a
different node, even though it has the same numeric value. Patterns and joins match terms exactly. To compare values,
use a `FILTER`, which compares numbers and dates by value:

/// tab | :fontawesome-brands-python: Python
```{.python continuation}
print(g.sparql("""
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    SELECT ?person WHERE { ?person foaf:age ?age FILTER(?age = 42) } ORDER BY ?person
"""))
```
///

```{.python continuation hide}
assert g.sparql("""
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>
    SELECT ?person WHERE { ?person foaf:age ?age FILTER(?age = 42) } ORDER BY ?person
""") == [
    {"person": "http://example.org/alice"},
    {"person": "http://example.org/bob"},
    {"person": "http://example.org/carol"},
]
```

!!! Output

    ```output
    [{'person': 'http://example.org/alice'}, {'person': 'http://example.org/bob'}, {'person': 'http://example.org/carol'}]
    ```

## Querying views

A query only sees the triples of the view it runs on, so Raphtory's views work as filters on the RDF data:

- [layer()][raphtory.GraphView.layer] and [layers()][raphtory.GraphView.layers] keep only some predicates, because
  every predicate is a layer.
- [subgraph()][raphtory.GraphView.subgraph] and [exclude_nodes()][raphtory.GraphView.exclude_nodes] keep only the
  triples between the selected nodes. Remember that literal values are nodes too.
- [Node and edge filters](../views/6_filtering.md) work in the same way.
- Time views such as [snapshot_at()][raphtory.GraphView.snapshot_at] and [window()][raphtory.GraphView.window] select
  the triples that hold at a time, as described on the [time travel](3_time-travel.md) page.

/// tab | :fontawesome-brands-python: Python
```{.python continuation}
count = "SELECT (COUNT(*) AS ?triples) WHERE { ?s ?p ?o }"
FOAF = "http://xmlns.com/foaf/0.1/"

print(g.sparql(count, decode_literals=True))
print(g.layers([FOAF + "name", FOAF + "knows"]).sparql(count, decode_literals=True))
print(g.exclude_nodes(["http://example.org/carol"]).sparql(count, decode_literals=True))
print(g.subgraph(["http://example.org/alice", "http://example.org/bob", "http://example.org/carol"]).sparql(
    "SELECT ?s ?o WHERE { ?s ?p ?o } ORDER BY ?s"
))
```
///

```{.python continuation hide}
assert g.layers([FOAF + "name", FOAF + "knows"]).sparql(count, decode_literals=True) == [{"triples": 5}]
assert g.exclude_nodes(["http://example.org/carol"]).sparql(count, decode_literals=True) == [{"triples": 8}]
```

!!! Output

    ```output
    [{'triples': 11}]
    [{'triples': 5}]
    [{'triples': 8}]
    [{'s': 'http://example.org/alice', 'o': 'http://example.org/bob'}, {'s': 'http://example.org/bob', 'o': 'http://example.org/carol'}]
    ```

## Querying graphs that were not loaded from RDF

Any Raphtory graph can be queried with SPARQL, not just graphs loaded from RDF. Names that are not RDF terms are IRIs
under `raphtory:`, as described in the [introduction](1_intro.md#names-and-terms), and the `raphtory:` prefix is
registered in every query. For example, the node `Alice` is `raphtory:Alice`, the node `Bob Smith` is
`raphtory:Bob%20Smith`, and edges without a layer are in the layer `raphtory:_default`:

/// tab | :fontawesome-brands-python: Python
```python
from raphtory import Graph

g = Graph()
g.add_edge(1, "Alice", "Bob", layer="follows")
g.add_edge(2, "Bob", "Carol", layer="follows")
g.add_edge(3, "Carol", "Bob Smith", layer="follows")
g.add_edge(3, "Alice", "Carol", layer="blocks")

# who can Alice reach through one or more "follows" edges?
print(g.sparql("""
    SELECT ?who WHERE { raphtory:Alice raphtory:follows+ ?who } ORDER BY ?who
"""))
```
///

```{.python continuation hide}
assert g.sparql("SELECT ?who WHERE { raphtory:Alice raphtory:follows+ ?who } ORDER BY ?who") == [
    {"who": "Bob"},
    {"who": "Bob Smith"},
    {"who": "Carol"},
]
```

!!! Output

    ```output
    [{'who': 'Bob'}, {'who': 'Bob Smith'}, {'who': 'Carol'}]
    ```

Node and edge properties, node types and nodes without edges are not part of the RDF view of a graph, so SPARQL does not
see them.

## Errors

A query that does not parse raises an exception whose message starts with `SPARQL syntax error`, and a query that fails
while it runs raises one starting with `SPARQL evaluation error`:

/// tab | :fontawesome-brands-python: Python
```{.python continuation}
try:
    g.sparql("SELECT ?x WHERE { ?x ")
except Exception as e:
    print(str(e).split(":")[0])
```
///

!!! Output

    ```output
    SPARQL syntax error
    ```

## Limiting a query

By default a query runs until it is done, however long that takes. When you run queries you do not control, bound them
with two arguments of `sparql()`:

- **`timeout`** (seconds) stops a query once it has run that long, and raises an exception whose message starts with
  `SPARQL query timed out`. The query checks whether to stop between the triples it reads, the values it builds and
  the results it returns, so it stops soon after its deadline, also while it counts, joins or sorts results in memory.
  A single step that is expensive on its own, such as hashing a very long string, runs to its end first.
- **`max_triple_patterns`** rejects a query that is too large, with `SPARQL query too complex`, before it runs. This
  bounds the one step `timeout` cannot interrupt: planning the query, whose cost grows with the fourth power of the
  number of triple patterns joined together (about 0.1 s for 64 patterns, 0.6 s for 100 and 1.5 s for 128, even on
  an empty graph), and faster when `BIND` or `VALUES` bind their variables before they are joined. With a limit of
  100, the slowest query to plan that we found took about 0.8 s on an Apple M4 Pro.

`max_triple_patterns` counts what the cost of planning grows with:

- every triple pattern counts one, wherever it is in the query (in `OPTIONAL`, `UNION`, `FILTER EXISTS`,
  sub-queries, ...), including those the parser makes of a collection such as `(1 2 3)` (seven) or of `[ ... ]`;
- a property path counts one per predicate it names (`ex:a/ex:b` counts two);
- a `BIND`, or an expression `(... AS ?x)` of `SELECT` or `GROUP BY`, counts one;
- a `VALUES` block counts one, plus one per variable and one per 100 rows, so looking up 1,000 IRIs with
  `VALUES ?s { ... }` counts 12;
- the `CONSTRUCT` template does not count.

/// tab | :fontawesome-brands-python: Python
```{.python}
from raphtory import Graph

chain = Graph()
for i in range(20):
    chain.add_edge(i, str(i), str(i + 1), layer="next")

# 20^8 combinations of eight unrelated patterns: far too many to count
huge = "SELECT (COUNT(*) AS ?n) WHERE { " + " ".join(f"?s{i} ?p{i} ?o{i} ." for i in range(8)) + " }"
try:
    chain.sparql(huge, timeout=0.5)
except Exception as e:
    print(str(e))

try:
    chain.sparql("SELECT * WHERE { ?a raphtory:next ?b . ?b raphtory:next ?c }", max_triple_patterns=1)
except Exception as e:
    print(str(e).split(" (")[0])

print(chain.sparql("ASK { raphtory:0 raphtory:next/raphtory:next raphtory:2 }", timeout=10, max_triple_patterns=10))
```
///

!!! Output

    ```output
    SPARQL query timed out: it ran longer than its time limit of 500ms
    SPARQL query too complex: 2 triple patterns, the limit is 1
    True
    ```

The [GraphQL server](5_graphql.md#limits) applies both limits to every query, by default 30 seconds and 100 triple
patterns (a server can raise or remove them).

## Using SPARQL from Rust

In Rust, `sparql()` is a method of the `RdfViewOps` trait, which every graph view implements. It returns the collected
results as a `SparqlResults` value whose terms are oxigraph `Term`s. To serialize results, `sparql_to_writer()` streams
them to any `std::io::Write` in a format given as a `QueryResultsFormat` (for `SELECT` and `ASK`), an `RdfFormat` or
`RdfSerializer` (for `CONSTRUCT` and `DESCRIBE`), or a `SparqlFormat` with both, which `SparqlFormat::parse()` reads from
the same names as the Python `format` argument. `SparqlResults::write()` writes collected results in the same way, with
the same bytes.

Both methods register the [validity functions](3_time-travel.md#since-when-validity-functions) `raphtory:validFrom`,
`validTo`, `validFromTime` and `validToTime`, whose IRIs are the constants `VALID_FROM`, `VALID_TO`, `VALID_FROM_TIME`
and `VALID_TO_TIME`. `sparql_with()` and `sparql_to_writer_with()` also take `SparqlOptions`:

- `with_temporal_functions(false)`: the validity functions are not registered, and a query that calls them fails, for
  example on a server that should not reveal the history of the triples behind its filters.
- `with_timeout(Duration)`, `with_max_triple_patterns(usize)`: the limits [described above](#limiting-a-query), which
  fail with `RdfError::Timeout` and `RdfError::TooManyPatterns`. `SparqlOptions::default()`, which `sparql()` and
  `sparql_to_writer()` use, has neither: a library caller decides which queries to bound.
- `with_cancellation_token(CancellationToken)`: another thread stops the query by calling `cancel()` on a clone of
  the token, and the query fails with `RdfError::Cancelled` (the GraphQL server does this when it drops a request, for
  example on shutdown; it does not notice a client that disconnects, whose query runs until its timeout).
  The query only reads the token, also when it times out, so one token can stop several queries and the same options
  can run the next query.

A query that is stopped returns no results, but `sparql_to_writer_with()` may already have written part of the document.
For more control, such as custom functions, build a `RaphtoryDataset` from a view and evaluate the query with the oxigraph
evaluator returned by `raphtory::rdf::evaluator()`, adding the validity functions for that view with
`raphtory::rdf::with_temporal_functions(evaluator(), view)` if you need them. See the
[`raphtory::rdf`](https://docs.rs/raphtory/latest/raphtory/rdf/index.html) module documentation for examples, or build
it locally with `cargo doc -p raphtory --features rdf --open`.

```{.python hide}
from datetime import datetime, timezone
from decimal import Decimal

from raphtory import PersistentGraph

zoo = PersistentGraph()
zoo.load_rdf(1, b"""
    @prefix ex:  <http://example.org/> .
    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
    ex:a ex:string "x"^^xsd:string ;
         ex:lang "y"@en-GB ;
         ex:int "5"^^xsd:int ;
         ex:nni "6"^^xsd:nonNegativeInteger ;
         ex:big 123456789012345678901234567890 ;
         ex:ulong "18446744073709551615"^^xsd:unsignedLong ;
         ex:decimal 1.25 ;
         ex:fraction_18 1.123456789012345678 ;
         ex:trailing_zeros 1.50000000000000000000000 ;
         ex:digits_38 99999999999999999999.123456789012345678 ;
         ex:fraction_19 1.1234567890123456789 ;
         ex:digits_24 12345678901234567890123.5 ;
         ex:long_decimal 1.234567890123456789012345678901234567891 ;
         ex:float "2.5"^^xsd:float ;
         ex:double 1.5e0 ;
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
assert decoded["string"] == "x"
assert decoded["lang"] == "y"
assert decoded["int"] == 5 and decoded["nni"] == 6
assert decoded["big"] == f'"123456789012345678901234567890"^^<{XSD}integer>'
assert decoded["ulong"] == f'"18446744073709551615"^^<{XSD}unsignedLong>'
assert decoded["decimal"] == Decimal("1.25")
# decimals: at most 18 digits after the point (trailing zeros aside), 38 digits and below 1.7e20
assert decoded["fraction_18"] == Decimal("1.123456789012345678")
assert decoded["trailing_zeros"] == Decimal("1.5")
assert decoded["digits_38"] == Decimal("99999999999999999999.123456789012345678")
assert decoded["fraction_19"] == f'"1.1234567890123456789"^^<{XSD}decimal>'
assert decoded["digits_24"] == f'"12345678901234567890123.5"^^<{XSD}decimal>'
assert decoded["long_decimal"] == f'"1.234567890123456789012345678901234567891"^^<{XSD}decimal>'
assert decoded["float"] == 2.5 and decoded["double"] == 1.5
assert decoded["offset"] == datetime(2024, 1, 2, 1, 4, 5, tzinfo=timezone.utc)
assert decoded["naive"] == datetime(2024, 1, 2, 3, 4, 5) and decoded["naive"].tzinfo is None
assert decoded["year0"] == f'"0000-01-01T00:00:00Z"^^<{XSD}dateTime>'
assert decoded["date"] == f'"2024-01-02"^^<{XSD}date>'
assert decoded["blank"].startswith("_:")
assert zoo.node(decoded["blank"]) is not None

# IRIs under raphtory: that encode no name are returned in N-Triples form
assert zoo.sparql("SELECT ?x WHERE { BIND(<raphtory:%61> AS ?x) }") == [{"x": "<raphtory:%61>"}]
assert zoo.sparql("SELECT ?x WHERE { BIND(raphtory:asof:2024-01-01 AS ?x) }") == [{"x": "<raphtory:asof:2024-01-01>"}]
assert zoo.sparql("SELECT ?x WHERE { BIND(raphtory:a AS ?x) }") == [{"x": "a"}]

# values computed by the query are literals; CONSTRUCT and DESCRIBE results have no duplicates
assert zoo.sparql('SELECT (STR(?o) AS ?s) WHERE { ?x <http://example.org/lang> ?o }') == [{"s": '"y"'}]
twice = zoo.sparql("CONSTRUCT { ?s <http://example.org/p> <http://example.org/o> } WHERE { ?s ?p ?o }")
assert twice == [("http://example.org/a", "http://example.org/p", "http://example.org/o")]
described = zoo.sparql("DESCRIBE <http://example.org/a>")
assert len(described) == len(set(described)) == 20
```
