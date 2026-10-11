# RDF in GraphQL

The [GraphQL server](../graphql/1_intro.md) serves every stored graph as RDF through the `rdfGraph` field of the query
root:

```graphql
rdfGraph(path: String!): RdfGraph
```

It views a stored graph, event or persistent, with persistent semantics, as [RdfGraph][raphtory.RdfGraph] does in
Python, and is `null` if the caller cannot read the graph. The `RdfGraph` type has:

- the view fields of `Graph`, each returning an `RdfGraph`: `valid`, `at`, `latest`, `window`, `before`, `after`,
  `snapshotAt`, `snapshotLatest`, `shrinkStart`, `shrinkEnd`, `layers`, `layer`, `excludeLayers`, `excludeLayer`,
  `defaultLayer`, `subgraph`, `subgraphNodeTypes`, `excludeNodes`, `filter` and `applyViews`, and `rolling` and
  `expanding`, whose windows are `RdfGraph`s;
- `sparql(query: String!, format: SparqlFormat): String!`, which runs a SPARQL query on the view and returns the
  results as one document, by default SPARQL Results JSON for `SELECT` and `ASK` and N-Triples for `CONSTRUCT` and
  `DESCRIBE`;
- `toRdf(format: RdfFormat): String!`, the triples of the view as one document (N-Triples by default);
- `validateShacl(shapes: String!, format: RdfFormat, reportFormat: RdfFormat): String!`, the W3C
  `sh:ValidationReport` of the view against a SHACL shapes graph, with RDF terms (Turtle by default). It rejects
  [recursive shapes](6_shacl.md#recursive-shapes), which could overflow the validator's stack and end the server;
- `graph: Graph!`, the same view as a property graph, and `name`, `path`, `earliestTime`, `latestTime`, `start`,
  `end`, `windowSize`, `uniqueLayers` and `hasLayer`.

As in Python, the RDF fields see exactly the triples of the view: `valid` is the current state, `at(time: T)` the state
as of `T`, and the whole graph every triple ever asserted or retracted.

!!! info

    The field is part of the GraphQL server of the `raphtory` Python package (`GraphServer`, `raphtory server` and the
    Python Docker images). The `raphtory-server` Rust binary only has it when built with the `rdf` cargo feature
    (`validateShacl` with `shacl`).

## Running a query

The examples on this page use the history of employment from the [time travel](3_time-travel.md) page, sent to a
server under the path `people`:

/// tab | :fontawesome-brands-python: Python
```python
import json
import tempfile

from raphtory import RdfGraph
from raphtory.graphql import GraphServer

rg = RdfGraph()
rg.load_rdf("2021-06-01", b"""
    @prefix ex: <http://example.org/> .
    ex:alice ex:worksFor ex:acme ; ex:name "Alice" .
    ex:bob   ex:worksFor ex:acme ; ex:name "Bob" .
""")
rg.retract_rdf("2023-06-01", b"<http://example.org/alice> <http://example.org/worksFor> <http://example.org/acme> .")
rg.load_rdf("2023-06-01", b"<http://example.org/alice> <http://example.org/worksFor> <http://example.org/initech> .")

work_dir = tempfile.mkdtemp()
with GraphServer(work_dir).start() as server:
    server.get_client().send_graph(path="people", graph=rg.graph)
```
///

Pass the SPARQL query as a GraphQL variable, so that its quotes need no escaping, and parse the result:

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
          rdfGraph(path: "people") {
            valid { sparql(query: $query) }
          }
        }
        """,
        variables={"query": staff},
    )
results = json.loads(result["rdfGraph"]["valid"]["sparql"])
print(results["results"]["bindings"])
```
///

```{.python continuation hide}
assert result["rdfGraph"]["valid"]["sparql"] == rg.valid().sparql(staff, format="json")
```

!!! Output

    ```output
    [{'person': {'type': 'uri', 'value': 'http://example.org/bob'}, 'name': {'type': 'literal', 'value': 'Bob'}}]
    ```

The values are RDF terms, not Raphtory names: in a graph not loaded from RDF, the node `Bob Smith` is the IRI
`raphtory:Bob%20Smith` (see [serialized results](2_sparql.md#serialized-results)).

## Views and formats

One request can query several views with aliases. `format` is one of `JSON`, `XML`, `CSV` and `TSV` for `SELECT` and
`ASK`, and `TURTLE`, `N_TRIPLES`, `N_QUADS`, `TRIG`, `RDF_XML` and `JSON_LD` (or `JSON` and `XML`) for `CONSTRUCT` and
`DESCRIBE`; a format that does not fit the query is an error. The [time graphs](3_time-travel.md#comparing-times-in-one-query)
and [validity functions](3_time-travel.md#since-when-validity-functions) work inside queries as in Python.

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
          rdfGraph(path: "people") {
            now: valid { sparql(query: $query, format: CSV) }
            early2023: at(time: "2023-01-01") { sparql(query: $query, format: CSV) }
            names: layer(name: "http%3A%2F%2Fexample.org%2Fname") { toRdf }
          }
        }
        """,
        variables={"query": employers},
    )
print(result["rdfGraph"]["now"]["sparql"])
print(result["rdfGraph"]["early2023"]["sparql"])
print(result["rdfGraph"]["names"]["toRdf"])
```
///

```{.python continuation hide}
assert result["rdfGraph"]["now"]["sparql"] == rg.valid().sparql(employers, format="csv")
assert result["rdfGraph"]["early2023"]["sparql"] == rg.at("2023-01-01").sparql(employers, format="csv")
```

!!! Output

    ```output
    person,company
    http://example.org/alice,http://example.org/initech
    http://example.org/bob,http://example.org/acme

    person,company
    http://example.org/alice,http://example.org/acme
    http://example.org/bob,http://example.org/acme

    <http://example.org/alice> <http://example.org/name> "Alice" .
    <http://example.org/bob> <http://example.org/name> "Bob" .

    ```

## Errors

A query that does not parse, fails while it runs or asks for a format that does not fit returns a GraphQL error with
the message of the Python exception (such as `SPARQL syntax error: ...`). The Python client raises an exception:

/// tab | :fontawesome-brands-python: Python
```{.python continuation}
with GraphServer(work_dir).start() as server:
    try:
        server.get_client().query(
            'query Broken($query: String!) { rdfGraph(path: "people") { sparql(query: $query) } }',
            variables={"query": "SELECT ?x WHERE { ?x"},
        )
    except Exception as e:
        print("SPARQL syntax error" in str(e))
```
///

!!! Output

    ```output
    True
    ```

## Access control

The RDF fields run on the graph as the caller is allowed to see it:

- **Filtered reads.** For a caller whose read access to a graph has a filter, the RDF fields see only the triples of
  the nodes, edges, layers and times the filter shows, time graphs included.
- **No validity functions on filtered reads.** The [validity functions](3_time-travel.md#since-when-validity-functions)
  compute the interval of a triple from its full history, which a filter can hide. A caller whose read access has a row
  filter gets `SPARQL evaluation error: The custom function <raphtory:validFrom> is not supported` when calling one.
  Access that only hides properties or metadata does not count, because RDF does not show properties.
- **No access.** `rdfGraph(path: ...)` is `null` for a graph the caller cannot read, including one they may only
  introspect.

## Limits

The `concurrency` section of the server configuration (see [GraphServer][raphtory.graphql.GraphServer]) bounds the RDF
fields. The three SPARQL limits are also `raphtory server` options (`--max-sparql-query-length`,
`--max-sparql-triple-patterns`, `--max-sparql-result-bytes`) and environment variables
(`RAPHTORY_MAX_SPARQL_QUERY_LENGTH`, `RAPHTORY_MAX_SPARQL_TRIPLE_PATTERNS`, `RAPHTORY_MAX_SPARQL_RESULT_BYTES`), and
setting one to `None` in the configuration removes it.

| Setting                      | Default | What it does                                                                                     |
|------------------------------|---------|--------------------------------------------------------------------------------------------------|
| `disable_lists`              | off     | turns off `sparql`, `toRdf`, `validateShacl` and the [SPARQL endpoint](7_sparql_endpoint.md)      |
| `max_sparql_query_length`    | 16 KiB  | rejects a longer query before parsing it (`SPARQL query too long`)                                |
| `max_sparql_triple_patterns` | 100     | rejects a query with more triple patterns, counted as in [limits](2_sparql.md#errors-and-limits), before planning it |
| `max_sparql_result_bytes`    | 256 MiB | fails a larger result of `sparql` or `toRdf` (`SPARQL results larger than`)                      |
| `heavy_query_limit`          | none    | every GraphQL query and every SPARQL endpoint query takes a slot, and waits for a free one        |

For example, `GraphServer(work_dir, config={"concurrency": {"max_sparql_triple_patterns": 50}})`.

- **No time limit.** A query, export or validation runs until it ends. Bound queries with `max_sparql_triple_patterns`
  and `LIMIT`.
- **Results in memory.** The whole result is held in memory as one string, of at most `max_sparql_result_bytes`, until
  it is sent. Use `LIMIT`.
- **Reads only.** There is no SPARQL Update and no mutation that loads RDF yet. Load RDF in Python, then send the graph
  with `send_graph(path=..., graph=rg.graph)` or `upload_graph()`, which keeps the history of every triple.
- **SPARQL tools.** Tools that speak the SPARQL protocol, such as YASGUI, SPARQLWrapper and rdflib, use the
  [SPARQL endpoint](7_sparql_endpoint.md) `/sparql/<graph path>`.
