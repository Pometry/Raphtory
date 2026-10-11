import json
from datetime import datetime, timezone

import pytest
from raphtory import Graph, GraphView, PersistentGraph, RdfGraph, RdfView
from raphtory import filter

FOAF = "http://xmlns.com/foaf/0.1/"
XSD = "http://www.w3.org/2001/XMLSchema#"

DOC = b"""
@prefix ex: <http://ex/> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .

ex:alice foaf:knows ex:bob ;
    foaf:name "Alice"@en ;
    foaf:age 42 .
ex:bob foaf:name "Bob" .
"""

DOC_NT = b"""<http://ex/alice> <http://xmlns.com/foaf/0.1/knows> <http://ex/bob> .
<http://ex/alice> <http://xmlns.com/foaf/0.1/name> "Alice"@en .
<http://ex/alice> <http://xmlns.com/foaf/0.1/age> "42"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://ex/bob> <http://xmlns.com/foaf/0.1/name> "Bob" .
"""

A, P, B = "http://ex/a", "http://ex/p", "http://ex/b"
Q, C = "http://ex/q", "http://ex/c"


def history():
    """a p b holds from 10 to 30 and from 50; a q c holds from 5."""
    rg = RdfGraph()
    rg.add_triple(10, A, P, B)
    rg.retract_triple(30, A, P, B)
    rg.add_triple(50, A, P, B)
    rg.add_triple(5, A, Q, C)
    return rg


def test_wrapping_a_graph_shares_its_storage():
    g = Graph()
    g.add_edge(1, "Alice", "Bob", layer="knows")
    rg = RdfGraph(g)
    assert rg.sparql("ASK { raphtory:Alice raphtory:knows raphtory:Bob }") is True

    rg.add_triple(2, A, P, B)
    assert g.has_edge(A, B)
    # the values of sparql() are the names add_triple and retract_triple take
    row = rg.sparql(f"SELECT ?s ?p ?o {{ ?s ?p ?o FILTER(?s = <{A}>) }}")[0]
    rg.retract_triple(3, row["s"], row["p"], row["o"])
    assert rg.valid().sparql(f"ASK {{ <{A}> <{P}> <{B}> }}") is False

    assert isinstance(rg.graph, PersistentGraph)
    assert rg.graph.has_edge("Alice", "Bob")
    assert isinstance(rg.window(0, 2).graph, GraphView)


def test_load_stored_graph(tmp_path):
    for g in [Graph(), PersistentGraph()]:
        g.add_edge(1, "Alice", "Bob", layer="knows")
        path = tmp_path / type(g).__name__
        g.save_to_file(path)
        rg = RdfGraph.load(path)
        assert rg.sparql("ASK { raphtory:Alice raphtory:knows raphtory:Bob }") is True


def test_load_rdf_sources(tmp_path):
    (tmp_path / "doc.ttl").write_bytes(DOC)
    (tmp_path / "doc.nt").write_bytes(DOC_NT)
    for source, fmt in [
        (str(tmp_path / "doc.ttl"), None),
        (tmp_path / "doc.nt", None),
        (DOC, None),
        (DOC_NT, "nt"),
    ]:
        assert RdfGraph().load_rdf(1, source, format=fmt) == 4

    rg = RdfGraph()
    rg.load_rdf(1, DOC)
    assert rg.retract_rdf(5, DOC_NT, format="nt") == 4
    assert rg.valid().sparql("ASK { ?s ?p ?o }") is False
    assert rg.at(3).sparql("ASK { ?s ?p ?o }") is True


def test_sparql_results():
    rg = RdfGraph()
    rg.load_rdf(1, DOC)
    rows = rg.sparql(f"""
        PREFIX foaf: <{FOAF}>
        SELECT ?person ?name ?friend WHERE {{
            ?person foaf:name ?name .
            OPTIONAL {{ ?person foaf:knows ?friend }}
        }} ORDER BY ?person
        """)
    assert rows == [
        {"person": "http://ex/alice", "name": '"Alice"@en', "friend": "http://ex/bob"},
        {"person": "http://ex/bob", "name": '"Bob"', "friend": None},
    ]
    assert rg.graph.node(rows[0]["name"]) is not None

    assert rg.sparql("ASK { <http://ex/alice> ?p <http://ex/bob> }") is True
    assert rg.sparql(
        f"CONSTRUCT {{ ?s <{FOAF}knows> ?o }} WHERE {{ ?s <{FOAF}knows> ?o }}"
    ) == [("http://ex/alice", f"{FOAF}knows", "http://ex/bob")]

    results = json.loads(rg.sparql("SELECT ?s { ?s ?p ?o }", format="json"))
    assert len(results["results"]["bindings"]) == 4


def test_views():
    rg = history()
    rows = rg.at(25).sparql(
        """
        SELECT ?p ?since {
            <http://ex/a> ?p ?o .
            BIND(raphtory:validFromTime(<http://ex/a>, ?p, ?o) AS ?since)
        } ORDER BY ?p
        """,
        decode_literals=True,
    )
    # as of 25, a p b has held since 10
    assert [(row["p"], row["since"]) for row in rows] == [(P, 10), (Q, 5)]

    views = [
        rg.valid(),
        rg.subgraph(["http://ex/a", "http://ex/c"]),
        rg.subgraph_node_types(["_default"]),
        rg.exclude_nodes(["http://ex/b"]),
        rg.window(15, 35),
        rg.layers(["http%3A%2F%2Fex%2Fq"]),
        rg.filter(filter.Node.name() != "http://ex/b"),
    ]
    assert all(isinstance(view, RdfView) for view in views)


def test_decode_literals():
    rg = RdfGraph()
    rg.load_rdf(
        1,
        b"""
        @prefix ex: <http://ex/> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        ex:a ex:int 42 ;
            ex:datetime "2024-01-02T03:04:05Z"^^xsd:dateTime ;
            ex:year0 "0000-01-01T00:00:00Z"^^xsd:dateTime ;
            ex:nanos "2024-01-01T00:00:00.0000001Z"^^xsd:dateTime ;
            ex:iri ex:b .
        """,
    )
    decoded = {
        row["p"].removeprefix("http://ex/"): row["o"]
        for row in rg.sparql(
            "SELECT ?p ?o { <http://ex/a> ?p ?o }", decode_literals=True
        )
    }
    assert decoded == {
        "int": 42,
        "datetime": datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
        # a datetime holds the years 1 to 9999, in whole microseconds
        "year0": f'"0000-01-01T00:00:00Z"^^<{XSD}dateTime>',
        "nanos": f'"2024-01-01T00:00:00.0000001Z"^^<{XSD}dateTime>',
        "iri": "http://ex/b",
    }


def test_to_rdf(tmp_path):
    rg = RdfGraph()
    rg.load_rdf(1, DOC)
    nt = rg.to_rdf()
    assert sorted(nt.splitlines()) == sorted(DOC_NT.decode().splitlines())
    ttl = rg.to_rdf(format="turtle", prefixes={"foaf": FOAF})
    assert "@prefix foaf: <http://xmlns.com/foaf/0.1/> ." in ttl
    path = tmp_path / "out.ttl"
    assert rg.to_rdf(path, prefixes={"foaf": FOAF}) is None
    assert path.read_text() == ttl


def test_sparql_limits():
    g = Graph()
    for i in range(20):
        g.add_edge(i, str(i), str(i + 1), layer="p")
    rg = RdfGraph(g)
    star = "SELECT ?s { " + " ".join(f"?s ?p{i} ?o{i} ." for i in range(5)) + " }"
    assert rg.sparql(star, max_triple_patterns=5) == rg.sparql(star)
    with pytest.raises(ValueError):
        rg.sparql(star, max_triple_patterns=4)
