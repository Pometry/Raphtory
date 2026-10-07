"""The `sparql(query:, format:)` field of `Graph` runs a SPARQL query on the
graph view it is called on and returns the serialized results, the same string
as `sparql(..., format=...)` in Python.

The graph has, from time 1, `alice knows bob`, `bob knows carol` and
`alice age 42`; `alice knows bob` is retracted at 5.
"""

import json
import socket
import tempfile
import time

import pytest
from raphtory import Graph, PersistentGraph
from raphtory.graphql import GraphServer, schema

PEOPLE = b"""
@prefix ex: <http://ex/> .
ex:alice ex:knows ex:bob ; ex:age 42 .
ex:bob ex:knows ex:carol .
"""
RETRACTED = b"<http://ex/alice> <http://ex/knows> <http://ex/bob> ."
KNOWS = "PREFIX ex: <http://ex/> SELECT ?s ?o { ?s ex:knows ?o } ORDER BY ?s ?o"


def people(graph):
    graph.load_rdf(1, PEOPLE)
    graph.retract_rdf(5, RETRACTED)
    return graph


def rows(results: str):
    results = json.loads(results)
    variables = results["head"]["vars"]
    return [
        tuple(binding[var]["value"] for var in variables)
        for binding in results["results"]["bindings"]
    ]


def sparql(client, query, format=None, path="g", view=""):
    """`{ graph(path:) { <view> { sparql(query:, format:) } } }`"""
    open_, close = (f"view: {view} {{", "}") if view else ("", "")
    document = f"""
    query Sparql($q: String!, $f: String) {{
      graph(path: "{path}") {{ {open_} sparql(query: $q, format: $f) {close} }}
    }}
    """
    variables = {"q": query} if format is None else {"q": query, "f": format}
    data = client.query(document, variables=variables)
    graph = data["graph"]
    return graph["view"]["sparql"] if view else graph["sparql"]


def test_results_are_the_same_as_in_python():
    g = people(PersistentGraph())
    with GraphServer(tempfile.mkdtemp()).start() as server:
        client = server.get_client()
        client.send_graph(path="g", graph=g)

        results = sparql(client, KNOWS)
        assert results == g.sparql(KNOWS, format="json")
        assert rows(results) == [("http://ex/bob", "http://ex/carol")]
        assert json.loads(results)["results"]["bindings"] == [
            {
                "s": {"type": "uri", "value": "http://ex/bob"},
                "o": {"type": "uri", "value": "http://ex/carol"},
            }
        ]

        for format in ["json", "xml", "csv", "tsv"]:
            assert sparql(client, KNOWS, format) == g.sparql(KNOWS, format=format)
        assert sparql(client, "ASK { ?s ?p ?o }") == '{"head":{},"boolean":true}'

        construct = (
            "CONSTRUCT { ?o <http://ex/knownBy> ?s } WHERE { ?s <http://ex/knows> ?o }"
        )
        assert sparql(client, construct) == g.sparql(construct, format="nt")
        assert sparql(client, construct) == (
            "<http://ex/carol> <http://ex/knownBy> <http://ex/bob> .\n"
        )
        assert sparql(client, construct, "ttl") == g.sparql(construct, format="ttl")


def test_views_and_time_graphs():
    g = people(PersistentGraph())
    with GraphServer(tempfile.mkdtemp()).start() as server:
        client = server.get_client()
        client.send_graph(path="g", graph=g)

        as_of_3 = [
            ("http://ex/alice", "http://ex/bob"),
            ("http://ex/bob", "http://ex/carol"),
        ]
        assert rows(sparql(client, KNOWS)) == [("http://ex/bob", "http://ex/carol")]
        assert rows(sparql(client, KNOWS, view="snapshotAt(time: 3)")) == as_of_3
        in_query = """
            PREFIX ex: <http://ex/>
            SELECT ?s ?o { GRAPH raphtory:asof:3 { ?s ex:knows ?o } } ORDER BY ?s ?o
        """
        assert sparql(client, in_query) == sparql(
            client, KNOWS, view="snapshotAt(time: 3)"
        )
        assert sparql(client, KNOWS, view="snapshotAt(time: 3)") == g.snapshot_at(
            3
        ).sparql(KNOWS, format="json")

        count = "SELECT (COUNT(*) AS ?n) { ?s ?p ?o }"
        assert rows(sparql(client, count)) == [("2",)]
        assert rows(sparql(client, count, view='layer(name: "http://ex/age")')) == [
            ("1",)
        ]

        # the temporal functions are available without access filters
        since = """
            PREFIX ex: <http://ex/>
            SELECT ?until {
                BIND(raphtory:validToTime(ex:alice, ex:knows, ex:bob, raphtory:asof:3) AS ?until)
            }
        """
        assert rows(sparql(client, since)) == [("5",)]


def test_event_graphs_ignore_retractions_unless_read_as_persistent():
    g = people(Graph())
    ask = "ASK { <http://ex/alice> <http://ex/knows> <http://ex/bob> }"
    with GraphServer(tempfile.mkdtemp()).start() as server:
        client = server.get_client()
        client.send_graph(path="g", graph=g)

        assert sparql(client, ask) == '{"head":{},"boolean":true}'
        persistent = client.query(
            """
            query Sparql($q: String!) {
              graph(path: "g", graphType: PERSISTENT) { sparql(query: $q) }
            }
            """,
            variables={"q": ask},
        )
        assert persistent["graph"]["sparql"] == '{"head":{},"boolean":false}'
        assert g.persistent_graph().sparql(ask) is False


def test_errors():
    g = people(PersistentGraph())
    with GraphServer(tempfile.mkdtemp()).start() as server:
        client = server.get_client()
        client.send_graph(path="g", graph=g)

        with pytest.raises(Exception, match="SPARQL syntax error"):
            sparql(client, "SELECT ?x WHERE { ?x")
        with pytest.raises(Exception, match="unknown SPARQL results format 'bogus'"):
            sparql(client, KNOWS, "bogus")
        with pytest.raises(Exception, match="cannot be written as"):
            sparql(client, KNOWS, "ttl")
        with pytest.raises(Exception, match="invalid time graph"):
            sparql(client, "ASK { GRAPH <raphtory:asof:nope> { ?s ?p ?o } }")


def test_disable_lists_disables_sparql():
    g = people(PersistentGraph())
    with GraphServer(
        tempfile.mkdtemp(), config={"concurrency": {"disable_lists": True}}
    ).start() as server:
        client = server.get_client()
        client.send_graph(path="g", graph=g)
        with pytest.raises(Exception, match="SPARQL queries are disabled"):
            sparql(client, KNOWS)


def nested(depth):
    return "ASK { FILTER(" + "(" * depth + "true" + ")" * depth + ") }"


def test_deep_and_long_queries_do_not_crash_the_server():
    g = people(PersistentGraph())
    with GraphServer(tempfile.mkdtemp()).start() as server:
        client = server.get_client()
        client.send_graph(path="g", graph=g)

        assert json.loads(sparql(client, nested(126)))["boolean"] is True
        # thousands of nested brackets would overflow the stack
        with pytest.raises(Exception, match="brackets nest more than 128 deep"):
            sparql(client, nested(5_000))
        # longer than the 16 KiB of `max_sparql_query_length`
        with pytest.raises(Exception, match="SPARQL query too long: 20020 bytes"):
            sparql(client, nested(10_000))
        assert rows(sparql(client, KNOWS)) == [("http://ex/bob", "http://ex/carol")]


def test_max_sparql_query_length():
    g = people(PersistentGraph())
    flat = "SELECT * { {}" + "UNION{}" * 9_000 + " }"
    with GraphServer(
        tempfile.mkdtemp(),
        config={"concurrency": {"max_sparql_query_length": len(KNOWS)}},
    ).start() as server:
        client = server.get_client()
        client.send_graph(path="g", graph=g)
        assert len(rows(sparql(client, KNOWS))) == 1
        with pytest.raises(Exception, match="the limit is 70 bytes"):
            sparql(client, KNOWS + " ")
    with GraphServer(
        tempfile.mkdtemp(), config={"concurrency": {"max_sparql_query_length": None}}
    ).start() as server:
        client = server.get_client()
        client.send_graph(path="g", graph=g)
        # a long query that nests little needs a deep stack too
        assert len(rows(sparql(client, flat))) == 9_001
        with pytest.raises(Exception, match="brackets nest more than 128 deep"):
            sparql(client, nested(100_000))


def star(n):
    """`n` triple patterns joined on `?s`."""
    return "SELECT ?s { " + " ".join(f"?s ?p{i} ?o{i} ." for i in range(n)) + " }"


# 2^40 solutions joined from the 2 triples of `people`: never finishes in time.
SLOW = (
    "SELECT (COUNT(*) AS ?n) { "
    + " ".join(f"?s{i} ?p{i} ?o{i} ." for i in range(40))
    + " }"
)


def test_sparql_timeout():
    g = people(PersistentGraph())
    with GraphServer(
        tempfile.mkdtemp(), config={"concurrency": {"sparql_timeout": 0.2}}
    ).start() as server:
        client = server.get_client()
        client.send_graph(path="g", graph=g)
        with pytest.raises(
            Exception,
            match=r"SPARQL query timed out: it ran longer than its time limit of 200ms \(`sparql_timeout`\)",
        ):
            sparql(client, SLOW)
        # the server keeps answering, and fast queries do not time out
        assert rows(sparql(client, KNOWS)) == [("http://ex/bob", "http://ex/carol")]


def test_a_disconnected_client_does_not_stop_its_query():
    """An abandoned query holds its `heavy_query_limit` slot until `sparql_timeout`."""
    g = people(PersistentGraph())
    config = {"concurrency": {"sparql_timeout": 2, "heavy_query_limit": 1}}
    with GraphServer(tempfile.mkdtemp(), config=config).start() as server:
        client = server.get_client()
        client.send_graph(path="g", graph=g)
        body = json.dumps(
            {
                "query": 'query Q($q: String!) { graph(path: "g") { sparql(query: $q) } }',
                "variables": {"q": SLOW},
            }
        ).encode()
        request = (
            b"POST / HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\n"
            + f"Content-Length: {len(body)}\r\n\r\n".encode()
            + body
        )
        with socket.create_connection(("localhost", server.port())) as abandoned:
            abandoned.sendall(request)
            start = time.monotonic()
            time.sleep(0.3)  # the query starts, then its client disconnects
        assert rows(sparql(client, KNOWS)) == [("http://ex/bob", "http://ex/carol")]
        waited = time.monotonic() - start
        # it waited for the abandoned query to time out
        assert 1.5 <= waited < 30, waited


def test_max_sparql_triple_patterns():
    g = people(PersistentGraph())
    # 100 by default
    with GraphServer(tempfile.mkdtemp()).start() as server:
        client = server.get_client()
        client.send_graph(path="g", graph=g)
        # one solution per subject of the two triples
        assert len(rows(sparql(client, star(100)))) == 2
        with pytest.raises(
            Exception,
            match=r"SPARQL query too complex: 101 triple patterns, the limit is 100",
        ):
            sparql(client, star(101))
        # BINDs count: a star whose variables are bound first is slow to plan
        binds = " ".join(f"BIND(1 AS ?o{i}) BIND(raphtory:p AS ?p{i})" for i in range(100))
        patterns = " ".join(f"?s ?p{i} ?o{i} ." for i in range(100))
        with pytest.raises(Exception, match="300 triple patterns, the limit is 100"):
            sparql(client, "SELECT * { { SELECT * { " + binds + " } } " + patterns + " }")
        # a lookup of 1000 values counts 12, and the pattern 1
        ids = " ".join(["ex:alice", "ex:bob"] + [f"ex:{i}" for i in range(998)])
        lookup = (
            "PREFIX ex: <http://ex/> SELECT ?s ?o { VALUES ?s { "
            + ids
            + " } ?s ex:knows ?o }"
        )
        assert rows(sparql(client, lookup)) == [("http://ex/bob", "http://ex/carol")]
    with GraphServer(
        tempfile.mkdtemp(),
        config={
            "concurrency": {"max_sparql_triple_patterns": 2, "sparql_timeout": None}
        },
    ).start() as server:
        client = server.get_client()
        client.send_graph(path="g", graph=g)
        assert sorted(rows(sparql(client, star(2)))) == [
            ("http://ex/alice",),
            ("http://ex/bob",),
        ]
        with pytest.raises(Exception, match="`max_sparql_triple_patterns`"):
            sparql(client, star(3))
    with GraphServer(
        tempfile.mkdtemp(), config={"concurrency": {"max_sparql_triple_patterns": None}}
    ).start() as server:
        client = server.get_client()
        client.send_graph(path="g", graph=g)
        assert len(rows(sparql(client, star(101)))) == 2


def test_invalid_sparql_timeout():
    with pytest.raises(Exception, match="concurrency.sparql_timeout"):
        GraphServer(tempfile.mkdtemp(), config={"concurrency": {"sparql_timeout": -1}})


def test_schema_has_sparql():
    sdl = schema()
    assert "sparql(" in sdl
    graph = sdl[sdl.index("\ntype Graph {") :]
    graph = graph[: graph.index("\n}")]
    assert "\tsparql(" in graph
