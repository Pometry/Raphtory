# SPARQL endpoint

Besides the [`sparql` field](5_graphql.md) of GraphQL, the GraphQL server serves every graph as a
[SPARQL 1.1 Protocol](https://www.w3.org/TR/sparql11-protocol/) endpoint, so standard SPARQL tools can query it directly:
query editors such as [YASGUI](https://yasgui.triply.cc/), Sparnatural and Sparklis, and client libraries such as
SPARQLWrapper, rdflib, Comunica, Apache Jena and RDF4J. The endpoint of the graph at path `people` is

```text
http://localhost:1736/sparql/people
```

and a graph in a namespace has its path after `/sparql/`, as in `http://localhost:1736/sparql/team/people` (encode
special characters, such as a space as `%20`). The endpoint runs the same query engine as the `sparql` field, with the
same permissions and limits, and answers with the same results.

!!! info

    The endpoint is part of the GraphQL server of the `raphtory` Python package (`GraphServer` and the
    `raphtory server` command) and of the Python Docker images. The `raphtory-server` Rust binary only has it when it is
    built with the `rdf` cargo feature (`cargo build -p raphtory-server --features rdf`).

## Running a query

The examples on this page use the history of employment from the [time travel](3_time-travel.md) page, sent to a
server under the path `people`. A query is an HTTP request, so any HTTP client can send it; here the `Accept` header
asks for CSV:

/// tab | :fontawesome-brands-python: Python
```python
import tempfile
import urllib.parse
import urllib.request

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

employers = """
    PREFIX ex: <http://example.org/>
    SELECT ?person ?company WHERE { ?person ex:worksFor ?company } ORDER BY ?person
"""

work_dir = tempfile.mkdtemp()
with GraphServer(work_dir).start() as server:
    server.get_client().send_graph(path="people", graph=g)
    endpoint = f"http://localhost:{server.port()}/sparql/people"
    url = endpoint + "?" + urllib.parse.urlencode({"query": employers})
    request = urllib.request.Request(url, headers={"Accept": "text/csv"})
    with urllib.request.urlopen(request) as response:
        content_type = response.headers["Content-Type"]
        csv = response.read().decode()
print(content_type)
print(csv)
```
///

```{.python continuation hide}
assert content_type == "text/csv; charset=utf-8"
assert csv == g.sparql(employers, format="csv")
```

!!! Output

    ```output
    text/csv; charset=utf-8
    person,company
    http://example.org/alice,http://example.org/initech
    http://example.org/bob,http://example.org/acme
    ```

## The protocol

The endpoint answers the query operation of the SPARQL 1.1 Protocol in its three forms:

| Request | Query | Other parameters |
|---|---|---|
| `GET /sparql/<path>?query=...` | URL parameter `query` | URL parameters |
| `POST` with `Content-Type: application/x-www-form-urlencoded` | form field `query` | form fields (or URL parameters) |
| `POST` with `Content-Type: application/sparql-query` | the request body | URL parameters |

The other parameters are `default-graph-uri` and `named-graph-uri` (see [time travel](#time-travel)) and `graph_type`;
parameters the endpoint does not know, such as the `format` and `output` that some clients add, are ignored. A `GET`
without a query returns the [SPARQL 1.1 Service Description](https://www.w3.org/TR/sparql11-service-description/) of the
endpoint, as RDF (Turtle by default): its URL, the query language, the result formats and the
[temporal functions](3_time-travel.md#since-when-validity-functions). Like a query, it needs a graph the caller can read,
and is a 404 otherwise, so a mistyped endpoint URL does not look live. The endpoint answers `OPTIONS` requests, including
the preflight requests of browsers, so query editors served from other sites can use it.

SPARQL Update is not supported: a request with an `update` parameter or of type `application/sparql-update` fails with
status 501. Load RDF into a graph in Python with `load_rdf()` and `retract_rdf()` and send it to the server, as described
for [GraphQL](5_graphql.md#limits).

## Formats

The `Accept` header picks the format of the results, with its quality values (`q=`) taken into account. Without an
`Accept` header, or with `*/*`, results are SPARQL Results JSON for `SELECT` and `ASK` and Turtle for `CONSTRUCT` and
`DESCRIBE`:

| Results | Media types |
|---|---|
| `SELECT`, `ASK` | `application/sparql-results+json` (default), `application/sparql-results+xml`, `text/csv`, `text/tab-separated-values` |
| `CONSTRUCT`, `DESCRIBE` | `text/turtle` (default), `application/n-triples`, `application/rdf+xml`, `application/ld+json`, `application/n-quads`, `application/trig` |

`application/json` is accepted for SPARQL Results JSON and JSON-LD, and `application/xml` and `text/xml` for SPARQL
Results XML and RDF/XML. The response's `Content-Type` names the format it is in, and the answers to `GET` and `POST`
requests carry `Vary: Accept`, so caches keep the formats apart (the answers to cross-origin requests from browsers carry
the `Vary: Origin` of the server's CORS layer instead). The endpoint answers 406 (Not Acceptable) when the `Accept`
header allows no format for the results of the query, for example `text/csv` for a `CONSTRUCT` query.

Two formats cannot hold every result. SPARQL Results XML cannot hold a literal with control characters, and RDF/XML
cannot hold a triple whose predicate does not end in an XML name, such as `<raphtory:2023>` for a layer named `2023`
(see [Limitations](4_limitations.md#exporting) for the details). The endpoint never sends results with something left
out: when the preferred format cannot hold them, it answers in the next format the `Accept` header allows, and with 406
when there is none. For example, `Accept: application/rdf+xml, text/turtle;q=0.5` gets RDF/XML when it holds the
results and Turtle otherwise, while `Accept: application/rdf+xml` alone gets a 406 naming the formats to accept instead.

## Graph type and time travel

A query sees the triples of the graph as the [`sparql` field](5_graphql.md) of `graph(path:)` does: on a persistent graph
the current state, on an event graph every triple asserted (retractions are ignored). The parameter `graph_type=event` or
`graph_type=persistent` reads the graph the other way, as `graphType` does in GraphQL.

The time graphs `<raphtory:asof:T>` work as in Python: `GRAPH <raphtory:asof:T> { ... }` in a query matches the triples
as of `T`. The protocol's dataset parameters can name them too. `default-graph-uri=raphtory:asof:T` makes the graph as of
`T` the default graph, so any query, including one written for the current state, runs on the past:

/// tab | :fontawesome-brands-python: Python
```{.python continuation}
with GraphServer(work_dir).start() as server:
    endpoint = f"http://localhost:{server.port()}/sparql/people"
    form = urllib.parse.urlencode(
        {"query": employers, "default-graph-uri": "raphtory:asof:2023-01-01"}
    )
    request = urllib.request.Request(
        endpoint,
        data=form.encode(),
        headers={"Content-Type": "application/x-www-form-urlencoded", "Accept": "text/csv"},
    )
    with urllib.request.urlopen(request) as response:
        early_2023 = response.read().decode()
print(early_2023)
```
///

```{.python continuation hide}
assert early_2023 == g.snapshot_at("2023-01-01").sparql(employers, format="csv")
```

!!! Output

    ```output
    person,company
    http://example.org/alice,http://example.org/acme
    http://example.org/bob,http://example.org/acme
    ```

`named-graph-uri` lists the named graphs that `GRAPH ?g { ... }` visits; both parameters can be repeated (several
default graphs are concatenated, as with several `FROM` clauses, and a graph named twice counts once). As the protocol
requires, the two parameters replace the dataset of the query: its `FROM` and `FROM NAMED` clauses are ignored, and with
only `default-graph-uri` there are no named graphs, so `GRAPH` patterns match nothing. The parameters can only name time graphs: any other IRI fails with
status 400.

## Access and limits

The endpoint runs a query as the [`sparql` field](5_graphql.md#limits) does, for the same caller:

- **Authentication.** The endpoint reads the bearer token of the `Authorization` header as the GraphQL endpoint does.
  When the server requires a token for reads, a request without a valid one fails with status 401.
- **Permissions.** The caller needs read access to the graph. A graph the caller cannot read gives the same 404 as a graph
  that does not exist. A caller whose access is row-filtered sees only the triples the filter keeps, and cannot use the
  temporal functions.
- **Limits.** `max_sparql_query_length`, `max_sparql_triple_patterns` and `sparql_timeout` apply, and a query takes one
  of the `heavy_query_limit` slots it shares with the GraphQL queries. `disable_lists` turns the endpoint off. A request
  body longer than three times `max_sparql_query_length` plus 64 KiB is not read.
- **No federation.** `SERVICE` calls are refused; the server never sends a query to another endpoint.

Errors have a plain-text body with the same message as the GraphQL error, and these status codes:

| Status | When |
|---|---|
| 400 Bad Request | a syntax or evaluation error, a query that is too long or has too many triple patterns, a dataset IRI that is not a time graph, an invalid `graph_type`, a missing or repeated `query` |
| 401 Unauthorized | no valid token when the server requires one for reads |
| 404 Not Found | the graph does not exist or the caller cannot read it |
| 405 Method Not Allowed | a method other than `GET`, `POST` and `OPTIONS` |
| 406 Not Acceptable | no format of the `Accept` header fits the results, or none of them can hold the results |
| 413 Payload Too Large | a request body far longer than any allowed query |
| 415 Unsupported Media Type | a `POST` that is neither a form nor `application/sparql-query` |
| 501 Not Implemented | SPARQL Update |
| 503 Service Unavailable | the server sets `disable_lists`, or is shutting down |
| 504 Gateway Timeout | the query ran longer than `sparql_timeout` |

## Clients

**YASGUI** and other query editors: enter the endpoint URL, such as `http://localhost:1736/sparql/people`. For time
travel in YASGUI, put `FROM <raphtory:asof:T>` in the query, or add `default-graph-uri` under the endpoint's extra
arguments: YASGUI 4.6's own "default graphs" setting is not sent.

**curl**:

```bash
# SELECT as CSV
curl -H 'Accept: text/csv' --data-urlencode 'query=SELECT * { ?s ?p ?o } LIMIT 10' \
  http://localhost:1736/sparql/people
# CONSTRUCT as Turtle, as of the start of 2023
curl -H 'Accept: text/turtle' --data-urlencode 'query=CONSTRUCT WHERE { ?s ?p ?o }' \
  --data-urlencode 'default-graph-uri=raphtory:asof:2023-01-01' http://localhost:1736/sparql/people
# the query as the body
curl -H 'Content-Type: application/sparql-query' --data 'ASK { ?s ?p ?o }' http://localhost:1736/sparql/people
# the service description
curl http://localhost:1736/sparql/people
```

**SPARQLWrapper** (`pip install sparqlwrapper`):

/// tab | :fontawesome-brands-python: Python
```{.python notest}
from SPARQLWrapper import JSON, SPARQLWrapper

sparql = SPARQLWrapper("http://localhost:1736/sparql/people")
sparql.setQuery("SELECT ?s ?o WHERE { ?s <http://example.org/worksFor> ?o }")
sparql.setReturnFormat(JSON)
sparql.addDefaultGraph("raphtory:asof:2023-01-01")  # optional: as of a time
for row in sparql.query().convert()["results"]["bindings"]:
    print(row["s"]["value"], row["o"]["value"])
```
///

**rdflib** (`pip install rdflib`), with its `SPARQLStore`. Give the graph no IRI identifier: rdflib sends an IRI
identifier, such as that of a `Dataset`'s default graph, as `default-graph-uri`, which the endpoint refuses.

/// tab | :fontawesome-brands-python: Python
```{.python notest}
import rdflib
from rdflib.plugins.stores.sparqlstore import SPARQLStore

store = SPARQLStore(query_endpoint="http://localhost:1736/sparql/people")
graph = rdflib.Graph(store=store)
for person, company in graph.query("SELECT ?p ?c WHERE { ?p <http://example.org/worksFor> ?c }"):
    print(person, company)
names = graph.query("CONSTRUCT WHERE { ?p <http://example.org/name> ?n }").graph
```
///
