import json

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

    rg.add_triple(2, A, "_:p", B)
    assert g.has_edge(A, B)
    # the values of sparql() are the names add_triple and retract_triple take
    row = rg.sparql(f"SELECT ?s ?p ?o {{ ?s ?p ?o FILTER(?s = <{A}>) }}")[0]
    rg.retract_triple(3, row["s"], row["p"], row["o"])
    assert rg.valid().sparql(f"ASK {{ <{A}> ?p <{B}> }}") is False

    assert isinstance(rg.graph, PersistentGraph)
    assert rg.graph.has_edge("Alice", "Bob")
    assert isinstance(rg.window(0, 2).graph, GraphView)


def test_load_rdf_sources(tmp_path):
    (tmp_path / "doc.ttl").write_bytes(DOC)
    (tmp_path / "doc.nt").write_bytes(DOC_NT)
    for source in [str(tmp_path / "doc.ttl"), tmp_path / "doc.nt", DOC, DOC_NT]:
        assert RdfGraph().load_rdf(1, source) == 4

    rg = RdfGraph()
    rg.load_rdf(1, DOC)
    assert rg.retract_rdf(5, DOC_NT) == 4
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


def test_event_id_orders_writes_at_the_same_time():
    rg = RdfGraph()
    rg.add_triple(10, A, P, B, event_id=2)
    rg.retract_triple(timestamp=10, subject=A, predicate=P, object=B, event_id=1)
    assert rg.valid().sparql(f"ASK {{ <{A}> <{P}> <{B}> }}") is True


def test_temporal_functions():
    row = history().window(15, 35).sparql(f"""
        SELECT ?from ?to {{
            BIND(raphtory:validFromTime(<{A}>, <{P}>, <{B}>) AS ?from)
            BIND(raphtory:validToTime(<{A}>, <{P}>, <{B}>) AS ?to)
        }}
        """)[0]
    assert row == {"from": f'"15"^^<{XSD}integer>', "to": f'"30"^^<{XSD}integer>'}


def test_views():
    rg = history()
    views = [
        rg.valid(),
        rg.subgraph(["http://ex/a", "http://ex/c"]),
        rg.subgraph_node_types(["_default"]),
        rg.exclude_nodes(["http://ex/b"]),
        rg.cache_view(),
        rg.window(15, 35),
        rg.layers(["http://ex/q"]),
        rg.filter(filter.Node.name() != "http://ex/b"),
    ]
    assert all(isinstance(view, RdfView) for view in views)
    assert rg.layers(["http://ex/q"]).sparql("SELECT ?o { ?s ?p ?o }") == [{"o": C}]


def test_to_rdf(tmp_path):
    rg = RdfGraph()
    rg.load_rdf(1, DOC)
    nt = rg.to_rdf()
    assert sorted(nt.splitlines()) == sorted(DOC_NT.decode().splitlines())
    ttl = rg.to_rdf(format="turtle")
    path = tmp_path / "out.ttl"
    assert rg.to_rdf(path) is None
    assert path.read_text() == ttl
