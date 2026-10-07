# SPARQL in GraphQL

The [GraphQL server](../graphql/1_intro.md) can run SPARQL queries too. The `Graph` type has a `sparql` field, which
runs a query on the RDF triples of the graph view it is called on and returns the results as one string:

```graphql
sparql(query: String!, format: String): String!
```

The field uses the same query engine as [sparql()](2_sparql.md) in Python, and returns the same string as
`sparql(query, format=...)` would on the same view. Without `format`, `SELECT` and `ASK` results are written as
[SPARQL 1.1 Query Results JSON](https://www.w3.org/TR/sparql11-results-json/), and `CONSTRUCT` and `DESCRIBE` results as
N-Triples.

!!! info

    The field is part of the GraphQL server of the `raphtory` Python package (`GraphServer` and the `raphtory server`
    command), so the Python Docker images, which are built from the Python package (`python.Dockerfile`), have it too.
    The `raphtory-server` Rust binary only has it when it is built with the `rdf` cargo feature, for example
    `cargo build -p raphtory-server --features rdf`, and the Rust Docker image (`Dockerfile`), built without cargo
    features, does not have it. A server can turn the field off with `disable_lists` (see [limits](#limits)). A client
    that talks to servers it does not control can look for the field in the schema first.

## Running a query

The examples on this page use the history of employment from the [time travel](3_time-travel.md) page: Alice and Bob
work for Acme from June 2021, and Alice moves to Initech in June 2023. The graph is sent to a server under the path
`people`, and the server keeps it in its working directory:

/// tab | :fontawesome-brands-python: Python
```python
import json
import tempfile

from raphtory import PersistentGraph
from raphtory.graphql import GraphServer

g = PersistentGraph()
g.load_rdf("2021-06-01", b"""
    @prefix ex: <http://example.org/> .
    ex:alice ex:worksFor ex:acme ; ex:name "Alice" .
    ex:bob   ex:worksFor ex:acme ; ex:name "Bob" .
""")
g.retract_rdf("2023-06-01", b"<http://example.org/alice> <http://example.org/worksFor> <http://example.org/acme> .")
g.load_rdf("2023-06-01", b"<http://example.org/alice> <http://example.org/worksFor> <http://example.org/initech> .")

work_dir = tempfile.mkdtemp()
with GraphServer(work_dir).start() as server:
    server.get_client().send_graph(path="people", graph=g)
```
///

Pass the SPARQL query as a GraphQL variable, rather than writing it into the GraphQL document, so that its quotes do not
need escaping. The result is a string, so parse it to read the solutions:

/// tab | :fontawesome-brands-python: Python
```{.python continuation}
staff = """
    PREFIX ex: <http://example.org/>
    SELECT ?person ?name WHERE { ?person ex:worksFor ex:acme ; ex:name ?name }
"""
with GraphServer(work_dir).start() as server:
    result = server.get_client().query(
        """
        query Staff($query: String!) {
          graph(path: "people") {
            sparql(query: $query)
          }
        }
        """,
        variables={"query": staff},
    )
results = json.loads(result["graph"]["sparql"])
print(results["results"]["bindings"])
```
///

```{.python continuation hide}
assert result["graph"]["sparql"] == g.sparql(staff, format="json")
assert results["results"]["bindings"] == [
    {"person": {"type": "uri", "value": "http://example.org/bob"}, "name": {"type": "literal", "value": "Bob"}}
]
```

!!! Output

    ```output
    [{'person': {'type': 'uri', 'value': 'http://example.org/bob'}, 'name': {'type': 'literal', 'value': 'Bob'}}]
    ```

Any GraphQL client can send the same request; standard SPARQL libraries can read the JSON. The values are RDF terms, not
Raphtory names: in a graph that was not loaded from RDF, the node `Bob Smith` is the IRI `raphtory:Bob%20Smith`, as
described in [serialized results](2_sparql.md#serialized-results).

## Formats

The `format` argument takes the same names as `format` in Python, listed in
[serialized results](2_sparql.md#serialized-results): `json`, `xml`, `csv` and `tsv` for `SELECT` and `ASK`, and RDF
formats such as `nt`, `ttl`, `jsonld` and `rdf` (RDF/XML) for `CONSTRUCT` and `DESCRIBE`. It also accepts file
extensions and media types, such as `"text/turtle"`. A format that does not fit the form of the query, such as `"ttl"`
for a `SELECT` query, is an error.

## Views

The query sees exactly the view the field is called on, so GraphQL's view fields restrict it in the same way as the
Python views described in [querying views](2_sparql.md#querying-views): `snapshotAt`, `window`, `before`, `layer`,
`layers`, `excludeLayers`, `subgraph`, `excludeNodes`, `filter` and `applyViews`. On a persistent graph, `snapshotAt`
gives the state as of a time, and one request can ask for several times by using aliases:

/// tab | :fontawesome-brands-python: Python
```{.python continuation}
employers = """
    PREFIX ex: <http://example.org/>
    SELECT ?person ?company WHERE { ?person ex:worksFor ?company } ORDER BY ?person
"""
with GraphServer(work_dir).start() as server:
    result = server.get_client().query(
        """
        query Employers($query: String!) {
          graph(path: "people") {
            now: sparql(query: $query, format: "csv")
            early2023: snapshotAt(time: "2023-01-01") {
              sparql(query: $query, format: "csv")
            }
          }
        }
        """,
        variables={"query": employers},
    )
print(result["graph"]["now"])
print(result["graph"]["early2023"]["sparql"])
```
///

```{.python continuation hide}
assert result["graph"]["now"] == g.sparql(employers, format="csv")
assert result["graph"]["early2023"]["sparql"] == g.snapshot_at("2023-01-01").sparql(employers, format="csv")
assert result["graph"]["early2023"]["sparql"] == (
    "person,company\r\n"
    "http://example.org/alice,http://example.org/acme\r\n"
    "http://example.org/bob,http://example.org/acme\r\n"
)
```

!!! Output

    ```output
    person,company
    http://example.org/alice,http://example.org/initech
    http://example.org/bob,http://example.org/acme

    person,company
    http://example.org/alice,http://example.org/acme
    http://example.org/bob,http://example.org/acme

    ```

The [time graphs](3_time-travel.md#comparing-times-in-one-query) `GRAPH <raphtory:asof:T> { ... }` and the
[validity functions](3_time-travel.md#since-when-validity-functions) work in the same way inside the query, so the
second query above could also be written with `GRAPH raphtory:asof:2023-01-01 { ... }` on the graph itself, and a query
can ask since when a triple holds:

/// tab | :fontawesome-brands-python: Python
```{.python continuation}
since = """
    PREFIX ex: <http://example.org/>
    SELECT ?person ?since WHERE {
        ?person ex:worksFor ex:acme
        BIND(raphtory:validFrom(?person, ex:worksFor, ex:acme) AS ?since)
    }
"""
with GraphServer(work_dir).start() as server:
    result = server.get_client().query(
        """
        query Since($query: String!) {
          graph(path: "people") { sparql(query: $query, format: "csv") }
        }
        """,
        variables={"query": since},
    )
print(result["graph"]["sparql"])
```
///

```{.python continuation hide}
assert result["graph"]["sparql"] == "person,since\r\nhttp://example.org/bob,2021-06-01T00:00:00Z\r\n"
as_of = """
    PREFIX ex: <http://example.org/>
    SELECT ?person ?company WHERE { GRAPH raphtory:asof:2023-01-01 { ?person ex:worksFor ?company } } ORDER BY ?person
"""
with GraphServer(work_dir).start() as server:
    in_query = server.get_client().query(
        'query Q($query: String!) { graph(path: "people") { sparql(query: $query, format: "csv") } }',
        variables={"query": as_of},
    )
assert in_query["graph"]["sparql"] == g.snapshot_at("2023-01-01").sparql(employers, format="csv")
```

!!! Output

    ```output
    person,since
    http://example.org/bob,2021-06-01T00:00:00Z

    ```

### Event graphs

A graph stored as an event `Graph` ignores retractions: its view shows every triple that was ever asserted in it, as
described in [event graphs and persistent graphs](3_time-travel.md#event-graphs-and-persistent-graphs). Read it with
`graph(path: ..., graphType: PERSISTENT)` to query it as a persistent graph, in which a retraction ends a triple:

/// tab | :fontawesome-brands-python: Python
```{.python continuation}
current = "ASK { <http://example.org/alice> <http://example.org/worksFor> <http://example.org/acme> }"
with GraphServer(work_dir).start() as server:
    client = server.get_client()
    client.send_graph(path="people_events", graph=g.event_graph())
    result = client.query(
        """
        query Current($query: String!) {
          event: graph(path: "people_events") { sparql(query: $query) }
          persistent: graph(path: "people_events", graphType: PERSISTENT) { sparql(query: $query) }
        }
        """,
        variables={"query": current},
    )
print(result["event"]["sparql"])
print(result["persistent"]["sparql"])
```
///

```{.python continuation hide}
assert result["event"]["sparql"] == '{"head":{},"boolean":true}'
assert result["persistent"]["sparql"] == '{"head":{},"boolean":false}'
```

!!! Output

    ```output
    {"head":{},"boolean":true}
    {"head":{},"boolean":false}
    ```

## Errors

A query that does not parse, fails while it runs or asks for an unknown format returns a GraphQL error, with the same
message as the exception in Python (such as `SPARQL syntax error: ...`). The field cannot be null, so on an error the
`graph` it was called on is `null` in the response. The Python client raises an exception:

/// tab | :fontawesome-brands-python: Python
```{.python continuation}
with GraphServer(work_dir).start() as server:
    try:
        server.get_client().query(
            'query Broken($query: String!) { graph(path: "people") { sparql(query: $query) } }',
            variables={"query": "SELECT ?x WHERE { ?x"},
        )
    except Exception as e:
        print("SPARQL syntax error" in str(e))
```
///

```{.python continuation hide}
import pytest

with GraphServer(work_dir).start() as server:
    client = server.get_client()
    with pytest.raises(Exception, match="SPARQL syntax error"):
        client.query(
            'query Broken($query: String!) { graph(path: "people") { sparql(query: $query) } }',
            variables={"query": "SELECT ?x WHERE { ?x"},
        )
    with pytest.raises(Exception, match="cannot be written as"):
        client.query(
            'query Broken($query: String!) { graph(path: "people") { sparql(query: $query, format: "ttl") } }',
            variables={"query": staff},
        )
```

!!! Output

    ```output
    True
    ```

## Access control

The field runs on the graph as the caller is allowed to see it:

- **Filtered reads.** For a caller whose read access to a graph has a filter, the query only sees the triples of the
  nodes, edges, layers and times the filter shows. This includes the time graphs `GRAPH <raphtory:asof:T>`, which are
  snapshots of the filtered view, so a filter that only shows a window of time also hides every other time.
- **No validity functions on filtered reads.** The [validity functions](3_time-travel.md#since-when-validity-functions)
  `raphtory:validFrom`, `validTo`, `validFromTime` and `validToTime` compute the interval of a triple from its full
  history, which a filter can hide (for example the time a triple was first asserted, before the window a filter shows).
  So they are not available to a caller whose read access has a row filter: a query that calls one fails with
  `SPARQL evaluation error: The custom function <raphtory:validFrom> is not supported`. Access that only hides
  properties or metadata does not count, because RDF does not show properties. Callers with unfiltered read access or
  write access can use them, also through `updateGraph(path: ...) { graph { sparql(...) } }`. If the server cannot
  resolve the caller's permission when the query runs, the functions are turned off too.
- **Introspection only.** A caller who may only introspect a graph gets `null` for `graph(path: ...)`, so cannot run
  queries on it.

## Limits

- **`disable_lists`.** A query such as `SELECT * WHERE { ?s ?p ?o }` lists every edge of the graph, so a server that
  disables bulk lists, for example with `GraphServer(work_dir, config={"concurrency": {"disable_lists": True}})` (see
  [GraphServer][raphtory.graphql.GraphServer]), disables the field too, which then fails with
  `SPARQL queries are disabled on this server`.
- **Query length.** The server rejects a query longer than `max_sparql_query_length` bytes, 16 KiB by default, with
  `SPARQL query too long`, before parsing it. Change it in the `concurrency` section of the configuration, for example
  `GraphServer(work_dir, config={"concurrency": {"max_sparql_query_length": 65536}})`, or with
  `--max-sparql-query-length` or `RAPHTORY_MAX_SPARQL_QUERY_LENGTH` for `raphtory server`; `None` removes the limit.
  The length does not bound how long a query takes; the next two limits do.
- **Triple patterns.** The server rejects a query with more than `max_sparql_triple_patterns` triple patterns, 100 by
  default, with `SPARQL query too complex: ... (`max_sparql_triple_patterns`)`, before planning it. Planning cannot be
  interrupted, and its cost grows with the fourth power of the number of triple patterns joined together (about 0.6 s
  for 100 patterns, 1.5 s for 128 and tens of seconds for 256, even on an empty graph), and faster when `BIND` or
  `VALUES` bind their variables first. So they are counted as described in
  [limiting a query](2_sparql.md#limiting-a-query): collections such as `(1 2 3)` and property paths count, so do
  each `BIND` and each expression `(... AS ?v)` of `SELECT` or `GROUP BY` (aggregates included), and a `VALUES` block
  counts one plus one per variable and per 100 rows (a lookup of 1,000 IRIs counts 12). With the default, the slowest
  query to plan that we found took about 0.8 s on an Apple M4 Pro. Change it with `max_sparql_triple_patterns` in the
  `concurrency` section, `--max-sparql-triple-patterns` or `RAPHTORY_MAX_SPARQL_TRIPLE_PATTERNS`; `None` removes it.
- **Timeout.** A query that runs longer than `sparql_timeout` seconds, 30 by default, is stopped and fails with
  `SPARQL query timed out: ... (`sparql_timeout`)`. Change it with `sparql_timeout` in the `concurrency` section
  (seconds, fractions allowed), `--sparql-timeout` or `RAPHTORY_SPARQL_TIMEOUT`; `None` removes it. For example,
  `GraphServer(work_dir, config={"concurrency": {"sparql_timeout": 5, "max_sparql_triple_patterns": 50}})`.
- **Disconnected clients.** The server does not notice that a client has disconnected while its request runs, so a
  query whose client went away runs on until it ends or reaches `sparql_timeout`, keeping a thread busy and, when the
  server sets `heavy_query_limit`, holding one of its slots. Without a timeout such a query is never stopped: keep
  `sparql_timeout` on a server that clients you do not control can reach.
- **Nesting.** Brackets (`(`, `{` and `[`) can nest at most 128 deep. A query that nests them deeper fails with
  `SPARQL syntax error: brackets nest more than 128 deep` before it is parsed, as it does in Python.
- **Heavy queries.** A request that contains `sparql` counts as a heavy query: when the server sets
  `heavy_query_limit`, it waits for a free slot like the traversal queries do.
- **Results in memory.** The whole result of a query is held in memory as one string until it is sent. Use `LIMIT`
  for large results.
- **Reads only.** There is no SPARQL Update and no mutation that loads RDF. Load RDF into a graph in Python with
  `load_rdf()` and `retract_rdf()`, then send it to the server with `send_graph()` or `upload_graph()`, which keeps the
  history of every triple.
- **SPARQL tools.** Tools that speak the SPARQL protocol, such as YASGUI, SPARQLWrapper and rdflib, connect to the
  [SPARQL endpoint](7_sparql_endpoint.md) of a graph, `/sparql/<graph path>`, which runs queries like this field.
