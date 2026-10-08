import json
import os
import sys
import tempfile
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from urllib.parse import unquote

import pytest
from raphtory import Graph, PersistentGraph
from utils import with_variants

XSD = "http://www.w3.org/2001/XMLSchema#"
FOAF = "http://xmlns.com/foaf/0.1/"
INT_42 = f'"42"^^<{XSD}integer>'

DOC = b"""
@prefix ex: <http://ex/> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .

ex:alice foaf:knows ex:bob ;
    foaf:name "Alice"@en ;
    foaf:age 42 .
ex:bob foaf:name "Bob" ;
    foaf:age 42 .
"""

DOC_NT = b"""<http://ex/alice> <http://xmlns.com/foaf/0.1/knows> <http://ex/bob> .
<http://ex/alice> <http://xmlns.com/foaf/0.1/name> "Alice"@en .
<http://ex/alice> <http://xmlns.com/foaf/0.1/age> "42"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://ex/bob> <http://xmlns.com/foaf/0.1/name> "Bob" .
<http://ex/bob> <http://xmlns.com/foaf/0.1/age> "42"^^<http://www.w3.org/2001/XMLSchema#integer> .
"""

WORKS_FOR = b"<http://ex/alice> <http://ex/worksFor> <http://ex/acme> ."


def load_doc(g):
    assert g.load_rdf(1, DOC) == 5
    return g


def check_doc(g):
    assert sorted(g.nodes.name) == sorted(
        ["http://ex/alice", "http://ex/bob", '"Alice"@en', '"Bob"', INT_42]
    )
    assert sorted(g.unique_layers) == sorted(
        [f"{FOAF}knows", f"{FOAF}name", f"{FOAF}age"]
    )
    assert g.count_edges() == 5
    # equal literals are one node
    assert g.node(INT_42).in_degree() == 2
    assert g.layer(f"{FOAF}knows").has_edge("http://ex/alice", "http://ex/bob")


# Loading from a path, a PathLike and bytes; the format comes from the extension.


@pytest.mark.parametrize("graph_type", [Graph, PersistentGraph])
def test_load_rdf_sources(graph_type, tmp_path):
    ttl = tmp_path / "doc.ttl"
    ttl.write_bytes(DOC)
    nt = tmp_path / "doc.nt"
    nt.write_bytes(DOC_NT)

    g = graph_type()
    assert g.load_rdf(1, str(ttl)) == 5  # str path, format from the extension
    check_doc(g)

    g = graph_type()
    assert g.load_rdf(1, nt) == 5  # PathLike
    check_doc(g)

    g = graph_type()
    assert g.load_rdf(1, DOC) == 5  # bytes, Turtle by default
    check_doc(g)

    g = graph_type()
    assert g.load_rdf(1, DOC_NT) == 5  # Turtle also reads N-Triples
    check_doc(g)

    # an explicit format, as an extension, a name or a media type
    for fmt in ["nt", ".nt", "n-triples", "application/n-triples"]:
        g = graph_type()
        assert g.load_rdf(1, DOC_NT, format=fmt) == 5
        check_doc(g)
    g = graph_type()
    assert g.load_rdf(1, DOC, format="text/turtle") == 5
    check_doc(g)

    # the format overrides the extension
    misnamed = tmp_path / "doc.data"
    misnamed.write_bytes(DOC)
    g = graph_type()
    assert g.load_rdf(1, misnamed, format="turtle") == 5
    check_doc(g)

    # a path without an extension is Turtle, like bytes (a dotfile and an empty extension
    # have no extension)
    for name, doc in [("doc", DOC), (".nt", DOC_NT), ("doc.", DOC)]:
        bare = tmp_path / name
        bare.write_bytes(doc)
        g = graph_type()
        assert g.load_rdf(1, bare) == 5, name
        check_doc(g)
    # ... so what to_rdf writes to such a path loads back
    g2 = graph_type()
    assert g.to_rdf(tmp_path / "export") is None
    assert g2.load_rdf(1, tmp_path / "export") == 5
    check_doc(g2)


@pytest.mark.parametrize("graph_type", [Graph, PersistentGraph])
def test_load_rdf_errors(graph_type, tmp_path):
    g = graph_type()

    unknown = tmp_path / "doc.data"
    unknown.write_bytes(DOC)
    with pytest.raises(Exception, match="unknown RDF format 'data'"):
        g.load_rdf(1, unknown)
    with pytest.raises(Exception, match="unknown RDF format 'csv'"):
        g.load_rdf(1, DOC, format="csv")
    with pytest.raises(Exception, match="is a directory"):
        g.load_rdf(1, tmp_path)
    # a media-type parameter that is a lone quote
    for fmt in ['text/turtle;profile="', 'turtle;profile="']:
        with pytest.raises(Exception, match="unknown RDF format"):
            g.load_rdf(1, DOC, format=fmt)
    with pytest.raises(Exception, match="unknown RDF format"):
        g.retract_rdf(1, unknown)
    assert g.count_nodes() == 0

    with pytest.raises(Exception, match="cannot open"):
        g.load_rdf(1, tmp_path / "missing.ttl")
    # a source of the wrong type raises a TypeError, as validate_shacl does
    for source in [42, None, bytearray(DOC), memoryview(DOC)]:
        with pytest.raises(TypeError, match="RDF source must be bytes"):
            g.load_rdf(1, source)
        with pytest.raises(TypeError, match="RDF source must be bytes"):
            g.retract_rdf(1, source)
    # a document passed as a str is read as a path
    for doc in [WORKS_FOR.decode(), DOC.decode(), DOC_NT.decode()]:
        with pytest.raises(TypeError, match="pass the document as bytes"):
            g.load_rdf(1, doc)
        with pytest.raises(TypeError, match="pass the document as bytes"):
            g.retract_rdf(1, doc)
    with pytest.raises(Exception, match="RDF parse error"):
        g.load_rdf(1, b"<http://ex/a> <http://ex/p> .", format="nt")
    # named graphs cannot be stored
    with pytest.raises(Exception, match="RDF parse error"):
        g.load_rdf(
            1, b"<http://ex/a> <http://ex/p> <http://ex/b> <http://ex/g> .", "nq"
        )
    assert g.count_nodes() == 0

    # IRIs under raphtory: must be canonical names
    with pytest.raises(Exception, match="cannot be stored"):
        g.load_rdf(1, b"<raphtory:%61> <http://ex/p> <http://ex/b> .", "nt")

    # base IRIs
    assert g.load_rdf(1, b"<a> <p> <b> .", base_iri="http://ex/") == 1
    assert g.layer("http://ex/p").has_edge("http://ex/a", "http://ex/b")
    with pytest.raises(Exception, match="invalid IRI"):
        g.load_rdf(1, b"<a> <p> <b> .", base_iri="not an iri")


def test_load_rdf_needs_string_ids():
    g = Graph()
    g.add_edge(1, 1, 2)
    with pytest.raises(Exception, match="string node ids"):
        g.load_rdf(1, DOC)
    assert g.count_nodes() == 2


def test_load_rdf_times():
    pg = PersistentGraph()
    pg.load_rdf("2024-01-01", WORKS_FOR)
    pg.load_rdf(datetime(2024, 6, 1, tzinfo=timezone.utc), DOC)
    assert pg.earliest_time.dt == datetime(2024, 1, 1, tzinfo=timezone.utc)
    assert pg.latest_time.dt == datetime(2024, 6, 1, tzinfo=timezone.utc)
    assert pg.layer("http://ex/worksFor").edge("http://ex/alice", "http://ex/acme")


def test_load_rdf_renames_blank_nodes():
    g = PersistentGraph()
    doc = b'_:b1 <http://ex/name> "x" .'
    g.load_rdf(1, doc, "nt")
    g.load_rdf(1, doc, "nt")
    blanks = [name for name in g.nodes.name if name.startswith("_:")]
    assert len(blanks) == 2  # two loads never collide

    # a retraction uses the stored labels, as returned by to_rdf
    stored = g.to_rdf().splitlines()[0]
    assert g.retract_rdf(2, stored.encode(), "nt") == 1
    assert len(g.sparql("SELECT * { ?s ?p ?o }")) == 1
    assert len(g.snapshot_at(1).sparql("SELECT * { ?s ?p ?o }")) == 2


# Large documents: a load writes one edge event per triple, in document order, exactly as
# calling add_edge (or delete_edge) once per triple.


def people(n):
    """`n` N-Triples lines about `n // 4` people, and the triples as Raphtory names."""
    lines, names = [], []
    for i in range(n // 4):
        s = f"http://example.org/person/{i}"
        friend = f"http://example.org/person/{(i * 7919) % (n // 4)}"
        for p, o, term in [
            (f"{FOAF}name", f'"Person {i}"', f'"Person {i}"'),
            (f"{FOAF}knows", friend, f"<{friend}>"),
            (f"{FOAF}age", f'"{i % 90}"^^<{XSD}integer>', f'"{i % 90}"^^<{XSD}integer>'),
            (f"{FOAF}nick", f'"p{i}"@en', f'"p{i}"@en'),
        ]:
            lines.append(f"<{s}> <{p}> {term} .")
            names.append((s, p, o))
    return "\n".join(lines).encode(), names


def events(g):
    """Every addition and deletion, with its event id, per edge and layer."""
    out = []
    for e in g.edges:
        for layer in e.layer_names:
            el = e.layer(layer)
            out.append(
                (
                    e.src.name,
                    e.dst.name,
                    layer,
                    [(t.t, t.event_id) for t in el.history.collect()],
                    [(t.t, t.event_id) for t in el.deletions.collect()],
                )
            )
    return sorted(out)


@pytest.mark.parametrize("graph_type", [Graph, PersistentGraph])
def test_load_rdf_large_documents(graph_type):
    doc, names = people(100_000)
    reference = graph_type()
    for s, p, o in names:
        reference.add_edge(1, s, o, layer=p)
    for format in ["nt", "ttl"]:
        g = graph_type()
        assert g.load_rdf(1, doc, format) == 100_000
        assert events(g) == events(reference)
        assert g.count_nodes() == reference.count_nodes()
    # retractions, then assertions at the same time: the later call wins
    g = graph_type()
    assert g.retract_rdf(1, doc, "nt") == 100_000
    assert g.load_rdf(1, doc, "nt") == 100_000
    reference = graph_type()
    for s, p, o in names:
        reference.delete_edge(1, s, o, layer=p)
    for s, p, o in names:
        reference.add_edge(1, s, o, layer=p)
    assert events(g) == events(reference)
    assert len(g.persistent_graph().sparql("SELECT * { ?s ?p ?o }")) == 100_000


def test_load_rdf_shows_no_progress_bar(monkeypatch, capfd):
    # triples are written one at a time, without the progress bar of the bulk loaders
    doc, _ = people(20_000)
    monkeypatch.delenv("RAPHTORY_PROGRESS_BARS_ENABLED", raising=False)
    capfd.readouterr()
    assert PersistentGraph().load_rdf(1, doc, "nt") == 20_000
    assert PersistentGraph().retract_rdf(1, doc, "nt") == 20_000
    assert capfd.readouterr().err == ""


# Result shapes; values are Raphtory names.


@with_variants(load_doc)
def test_sparql_select():
    def check(g):
        rows = g.sparql("""
            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
            SELECT ?person ?name ?friend WHERE {
                ?person foaf:name ?name .
                OPTIONAL { ?person foaf:knows ?friend }
            } ORDER BY ?person
            """)
        assert rows == [
            {
                "person": "http://ex/alice",
                "name": '"Alice"@en',
                "friend": "http://ex/bob",
            },
            {"person": "http://ex/bob", "name": '"Bob"', "friend": None},
        ]
        # variable order is kept
        assert [list(row) for row in rows] == [["person", "name", "friend"]] * 2
        # every value is a node name
        for row in rows:
            for value in row.values():
                assert value is None or g.node(value) is not None

        # SELECT * gives the variables sorted by name
        star = g.sparql("SELECT * { ?zed ?alpha ?mid } LIMIT 1")
        assert [list(row) for row in star] == [["alpha", "mid", "zed"]]
        header = g.sparql("SELECT * { ?zed ?alpha ?mid } LIMIT 1", format="csv")
        assert header.splitlines()[0] == "alpha,mid,zed"

        # predicates are layer names
        rows = g.sparql("SELECT DISTINCT ?p { ?s ?p ?o } ORDER BY ?p")
        assert [row["p"] for row in rows] == [
            f"{FOAF}age",
            f"{FOAF}knows",
            f"{FOAF}name",
        ]
        for row in rows:
            assert row["p"] in g.unique_layers

        # literals are nodes
        rows = g.sparql(f"SELECT ?s {{ ?s <{FOAF}age> 42 }} ORDER BY ?s")
        assert rows == [{"s": "http://ex/alice"}, {"s": "http://ex/bob"}]
        assert g.sparql("SELECT ?s { ?s ?p <http://ex/nobody> }") == []

    return check


@with_variants(load_doc)
def test_sparql_ask_construct_describe():
    def check(g):
        assert g.sparql("ASK { <http://ex/alice> ?p <http://ex/bob> }") is True
        assert g.sparql("ASK { <http://ex/bob> ?p <http://ex/alice> }") is False

        triples = g.sparql(
            f"CONSTRUCT {{ ?s <{FOAF}knows> ?o }} WHERE {{ ?s <{FOAF}knows> ?o }}"
        )
        assert triples == [("http://ex/alice", f"{FOAF}knows", "http://ex/bob")]
        for s, p, o in triples:
            assert g.layer(p).edge(s, o) is not None

        triples = g.sparql("DESCRIBE <http://ex/bob>")
        assert sorted(triples) == [
            ("http://ex/bob", f"{FOAF}age", INT_42),
            ("http://ex/bob", f"{FOAF}name", '"Bob"'),
        ]
        assert all(isinstance(t, tuple) and len(t) == 3 for t in triples)

    return check


@pytest.mark.parametrize("graph_type", [Graph, PersistentGraph])
def test_sparql_construct_is_a_set(graph_type):
    g = graph_type()
    doc = b"""<http://ex/a> <http://ex/p> _:b .
        <http://ex/c> <http://ex/p> _:b .
        _:b <http://ex/q> "x" ."""
    g.load_rdf(1, doc, "nt")
    all_triples = set(g.sparql("CONSTRUCT WHERE { ?s ?p ?o }"))
    assert len(all_triples) == 3
    for query, n in [
        ("CONSTRUCT WHERE { ?s ?p ?o . ?x ?y ?o }", 3),
        (
            "CONSTRUCT { ?s <http://ex/p> ?o } WHERE { ?s ?x ?y . ?s <http://ex/p> ?o }",
            2,
        ),
        ("DESCRIBE ?o WHERE { ?s <http://ex/p> ?o }", 1),
    ]:
        triples = g.sparql(query)
        assert len(triples) == len(set(triples)) == n, query
        assert set(triples) <= all_triples


def test_sparql_errors():
    g = load_doc(PersistentGraph())
    with pytest.raises(Exception, match="SPARQL syntax error"):
        g.sparql("SELECT WHERE")
    with pytest.raises(Exception, match="SPARQL evaluation error"):
        g.sparql("SELECT * { GRAPH <raphtory:asof:garbage> { ?s ?p ?o } }")


def test_sparql_deep_and_long_queries_do_not_crash():
    g = load_doc(PersistentGraph())

    def nested(depth):
        return "ASK { FILTER(" + "(" * depth + "true" + ")" * depth + ") }"

    assert g.sparql(nested(126)) is True
    # thousands of nested brackets would overflow the stack and abort Python
    for depth in [127, 100_000]:
        with pytest.raises(
            Exception, match="SPARQL syntax error: brackets nest more than 128 deep"
        ):
            g.sparql(nested(depth))
    with pytest.raises(Exception, match="brackets nest more than 128 deep"):
        g.sparql(nested(100_000), format="json")
    # a long query that nests little runs on a thread with a deep enough stack
    flat = "SELECT * { {}" + "UNION{}" * 9_000 + " }"
    assert len(g.sparql(flat)) == 9_001
    results = json.loads(g.sparql(flat, format="json"))
    assert len(results["results"]["bindings"]) == 9_001


@with_variants(lambda g: g)
def test_sparql_plain_graph():
    def check(g):
        g.add_edge(1, "Alice", "Bob")
        g.add_edge(1, "Alice", "Bob Smith", layer="knows")
        assert g.sparql("ASK { raphtory:Alice raphtory:_default raphtory:Bob }")
        rows = g.sparql("SELECT ?who { raphtory:Alice raphtory:knows ?who }")
        assert rows == [{"who": "Bob Smith"}]
        assert g.node(rows[0]["who"]) is not None
        assert g.to_rdf() == (
            "<raphtory:Alice> <raphtory:_default> <raphtory:Bob> .\n"
            "<raphtory:Alice> <raphtory:knows> <raphtory:Bob%20Smith> .\n"
        )

    return check


def test_sparql_views():
    g = load_doc(Graph())
    query = "SELECT ?s ?o { ?s ?p ?o }"
    assert len(g.sparql(query)) == 5
    assert len(g.layer(f"{FOAF}knows").sparql(query)) == 1
    assert len(g.exclude_nodes(["http://ex/bob"]).sparql(query)) == 2
    assert len(g.subgraph(["http://ex/bob", '"Bob"']).sparql(query)) == 1
    assert g.window(5, 10).sparql(query) == []


# decode_literals


LITERALS = b"""
@prefix ex: <http://ex/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:a ex:int 42 ;
    ex:negative -7 ;
    ex:double 1.5e0 ;
    ex:float "2.5"^^xsd:float ;
    ex:bool true ;
    ex:decimal 1.25 ;
    ex:datetime "2024-01-02T03:04:05Z"^^xsd:dateTime ;
    ex:offset "2024-01-02T03:04:05+02:00"^^xsd:dateTime ;
    ex:naive "2024-01-02T03:04:05.250"^^xsd:dateTime ;
    ex:string "hello" ;
    ex:lang "bonjour"@fr ;
    ex:date "2024-01-02"^^xsd:date ;
    ex:big 123456789012345678901234567890 ;
    ex:iri ex:b .
"""


def load_literals(g):
    assert g.load_rdf(1, LITERALS) == 14
    return g


@with_variants(load_literals)
def test_decode_literals():
    def check(g):
        query = "SELECT ?p ?o { <http://ex/a> ?p ?o }"
        decoded = {
            row["p"].removeprefix("http://ex/"): row["o"]
            for row in g.sparql(query, decode_literals=True)
        }
        expected = {
            "int": 42,
            "negative": -7,
            "double": 1.5,
            "float": 2.5,
            "bool": True,
            "decimal": Decimal("1.25"),
            "datetime": datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
            "offset": datetime(2024, 1, 2, 1, 4, 5, tzinfo=timezone.utc),
            "naive": datetime(2024, 1, 2, 3, 4, 5, 250000),
            "string": "hello",
            "lang": "bonjour",
            # unsupported datatypes and IRIs stay names
            "date": f'"2024-01-02"^^<{XSD}date>',
            "big": f'"123456789012345678901234567890"^^<{XSD}integer>',
            "iri": "http://ex/b",
        }
        assert decoded == expected
        for key, value in expected.items():
            assert type(decoded[key]) is type(value), key
        assert decoded["datetime"].tzinfo is not None
        assert decoded["naive"].tzinfo is None

        # without decoding every literal is its node name
        names = {
            row["p"].removeprefix("http://ex/"): row["o"] for row in g.sparql(query)
        }
        assert names["int"] == INT_42
        assert names["string"] == '"hello"'
        assert names["lang"] == '"bonjour"@fr'
        assert all(isinstance(v, str) for v in names.values())
        for value in names.values():
            assert g.node(value) is not None

        # CONSTRUCT decodes objects too
        triples = g.sparql(
            "CONSTRUCT { ?s ?p ?o } WHERE { ?s <http://ex/int> ?o . BIND(<http://ex/int> AS ?p) }",
            decode_literals=True,
        )
        assert triples == [("http://ex/a", "http://ex/int", 42)]

        # values computed by the query
        assert g.sparql("SELECT (COUNT(*) AS ?n) { ?s ?p ?o }", True) == [{"n": 14}]
        assert g.sparql("SELECT (COUNT(*) AS ?n) { ?s ?p ?o }") == [
            {"n": f'"14"^^<{XSD}integer>'}
        ]

    return check


def test_decode_literals_out_of_range_datetimes():
    g = PersistentGraph()
    g.load_rdf(
        1,
        b"""
        @prefix ex: <http://ex/> .
        @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        ex:a ex:year0 "0000-01-01T00:00:00Z"^^xsd:dateTime ;
            ex:shifted0 "0001-01-01T00:30:00+01:00"^^xsd:dateTime ;
            ex:shifted10000 "9999-12-31T23:00:00-02:00"^^xsd:dateTime ;
            ex:naive0 "0000-06-01T00:00:00"^^xsd:dateTime ;
            ex:first "0001-01-01T00:00:00Z"^^xsd:dateTime ;
            ex:last "9999-12-31T23:59:59"^^xsd:dateTime ;
            ex:int 42 .
        """,
    )
    decoded = {
        row["p"].removeprefix("http://ex/"): row["o"]
        for row in g.sparql("SELECT ?p ?o { ?s ?p ?o }", decode_literals=True)
    }
    dt = f"^^<{XSD}dateTime>"
    # values a Python datetime cannot hold stay names
    assert decoded == {
        "year0": f'"0000-01-01T00:00:00Z"{dt}',
        "shifted0": f'"0001-01-01T00:30:00+01:00"{dt}',
        "shifted10000": f'"9999-12-31T23:00:00-02:00"{dt}',
        "naive0": f'"0000-06-01T00:00:00"{dt}',
        "first": datetime(1, 1, 1, tzinfo=timezone.utc),
        "last": datetime(9999, 12, 31, 23, 59, 59),
        "int": 42,
    }
    # in CONSTRUCT results too
    triples = g.sparql(
        "CONSTRUCT { ?s <http://ex/year0> ?o } WHERE { ?s <http://ex/year0> ?o }",
        decode_literals=True,
    )
    assert triples == [("http://ex/a", "http://ex/year0", decoded["year0"])]


# Retraction and as-of


def check_as_of(view_at, now):
    ask = "ASK { <http://ex/alice> <http://ex/worksFor> <http://ex/acme> }"
    assert view_at(0).sparql(ask) is False
    for t in range(1, 5):
        assert view_at(t).sparql(ask) is True
    for t in range(5, 8):
        assert view_at(t).sparql(ask) is False
    assert now.sparql(ask) is False


def test_retract_rdf_persistent_graph():
    pg = PersistentGraph()
    assert pg.load_rdf(1, WORKS_FOR) == 1
    assert pg.retract_rdf(5, WORKS_FOR) == 1
    check_as_of(pg.snapshot_at, pg)
    # the other predicates on the same pair stay
    pg.load_rdf(1, b"<http://ex/alice> <http://ex/owns> <http://ex/acme> .")
    assert pg.sparql("SELECT ?p { <http://ex/alice> ?p <http://ex/acme> }") == [
        {"p": "http://ex/owns"}
    ]
    # a re-assertion is visible again
    pg.load_rdf(8, WORKS_FOR)
    assert pg.sparql("ASK { <http://ex/alice> <http://ex/worksFor> ?o }") is True
    assert pg.snapshot_at(6).sparql("ASK { ?s <http://ex/worksFor> ?o }") is False


def test_retract_rdf_out_of_order():
    pg = PersistentGraph()
    pg.retract_rdf(5, WORKS_FOR)
    pg.load_rdf(1, WORKS_FOR)
    check_as_of(pg.snapshot_at, pg)


def test_retract_rdf_graph():
    g = Graph()
    g.load_rdf(1, WORKS_FOR)
    g.retract_rdf(5, WORKS_FOR)
    # event graphs ignore retractions
    assert g.sparql("ASK { ?s ?p ?o }") is True
    assert g.window(5, 10).sparql("ASK { ?s ?p ?o }") is False
    # their persistent view sees them
    pg = g.persistent_graph()
    check_as_of(pg.snapshot_at, pg)


def test_retract_rdf_from_file(tmp_path):
    path = tmp_path / "retract.nt"
    path.write_bytes(WORKS_FOR)
    pg = PersistentGraph()
    pg.load_rdf(1, path)
    assert pg.retract_rdf(5, str(path)) == 1
    check_as_of(pg.snapshot_at, pg)


# Export


@with_variants(load_doc)
def test_to_rdf_string():
    def check(g):
        doc = g.to_rdf()  # N-Triples by default
        assert isinstance(doc, str)
        assert sorted(doc.splitlines()) == sorted(DOC_NT.decode().splitlines())
        assert g.to_rdf(format="nt") == doc

        # round trip
        g2 = PersistentGraph()
        assert g2.load_rdf(1, doc.encode(), format="nt") == 5
        assert g2.to_rdf() == doc

        # views are exported as they are
        assert g.layer(f"{FOAF}knows").to_rdf() == (
            f"<http://ex/alice> <{FOAF}knows> <http://ex/bob> .\n"
        )
        assert g.before(1).to_rdf() == ""

    return check


@with_variants(load_doc)
def test_to_rdf_file():
    def check(g):
        with tempfile.TemporaryDirectory() as tmp:
            nt = os.path.join(tmp, "out.nt")
            assert g.to_rdf(nt) is None
            with open(nt) as f:
                assert f.read() == g.to_rdf()

            # the format comes from the extension
            ttl = Path(tmp) / "out.ttl"
            assert g.to_rdf(ttl, prefixes={"foaf": FOAF}) is None
            text = ttl.read_text()
            assert text == g.to_rdf(format="turtle", prefixes={"foaf": FOAF})
            assert "@prefix foaf: <http://xmlns.com/foaf/0.1/> ." in text
            g2 = Graph()
            assert g2.load_rdf(1, ttl) == 5
            assert g2.to_rdf() == g.to_rdf()

            # an explicit format overrides the extension
            data = Path(tmp) / "out.data"
            g.to_rdf(data, format="nt")
            assert data.read_text() == g.to_rdf()
            with pytest.raises(Exception, match="unknown RDF format"):
                g.to_rdf(data)
            # no extension: N-Triples
            plain = Path(tmp) / "out"
            g.to_rdf(plain)
            assert plain.read_text() == g.to_rdf()
            # an empty extension is no extension
            dot = Path(tmp) / "out."
            g.to_rdf(dot)
            assert dot.read_text() == g.to_rdf()

    return check


def test_to_rdf_prefixes():
    g = load_doc(PersistentGraph())
    ttl = g.to_rdf(format="ttl", prefixes={"foaf": FOAF, "ex": "http://ex/"})
    assert "@prefix ex: <http://ex/> ." in ttl
    assert "@prefix foaf: <http://xmlns.com/foaf/0.1/> ." in ttl
    assert "ex:alice" in ttl
    assert "foaf:knows" in ttl
    g2 = PersistentGraph()
    assert g2.load_rdf(1, ttl.encode()) == 5
    assert g2.to_rdf() == g.to_rdf()

    # prefixes are ignored by formats without them
    assert g.to_rdf(format="nt", prefixes={"ex": "http://ex/"}) == g.to_rdf()

    with pytest.raises(Exception, match="invalid IRI"):
        g.to_rdf(format="ttl", prefixes={"ex": "not an iri"})
    with pytest.raises(Exception, match="unknown RDF format"):
        g.to_rdf(format="csv")
    with pytest.raises(Exception, match="unknown RDF format"):
        g.to_rdf(format='turtle;profile="')

    # prefix names the format cannot write are rejected, instead of writing a broken document
    for fmt, name in [
        ("ttl", "a b"),
        ("ttl", "1x"),
        ("ttl", "a:b"),
        ("ttl", "ex."),
        ("trig", "ex."),
        ("rdf", "a:b"),
        ("rdf", "xmlns"),
    ]:
        with pytest.raises(Exception, match=f"invalid prefix name '{name}'"):
            g.to_rdf(format=fmt, prefixes={name: "http://ex/"})
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "out.ttl"
        with pytest.raises(Exception, match="invalid prefix name 'a b'"):
            g.to_rdf(path, prefixes={"a b": "http://ex/"})
        assert not path.exists()
    # valid names that are unusual
    for fmt, name in [("ttl", ""), ("ttl", "e.x-1"), ("trig", "e.x-1"), ("rdf", "_x")]:
        doc = g.to_rdf(format=fmt, prefixes={name: "http://ex/"})
        g2 = PersistentGraph()
        assert g2.load_rdf(1, doc.encode(), format=fmt) == 5
        assert sorted(g2.to_rdf().splitlines()) == sorted(g.to_rdf().splitlines())


def test_to_rdf_rdf_xml():
    rdf_type = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
    g = Graph()
    g.add_edge(1, "http://ex/a", "http://ex/b", layer="http://ex/p")
    # RDF/XML cannot write these predicates
    g.add_edge(1, "http://ex/a", "http://ex/b", layer="http://ex/42")
    g.add_edge(1, "http://ex/a", "http://ex/b", layer="http://ex/")
    # nor this type as the only triple of its subject
    g.add_edge(1, "http://ex/t", "http://ex/42", layer=rdf_type)
    for doc in [
        g.to_rdf(format="rdf"),
        g.to_rdf(format="rdf", prefixes={"ex": "http://ex/"}),
    ]:
        ET.fromstring(doc)  # well-formed XML
        g2 = Graph()
        assert g2.load_rdf(1, doc.encode(), format="rdf") == 1
        assert g2.to_rdf() == "<http://ex/a> <http://ex/p> <http://ex/b> .\n"
    assert len(g.to_rdf(format="nt").splitlines()) == 4


def test_to_rdf_rdf_xml_blank_nodes_and_literals():
    g = Graph()
    # blank-node labels that start with a digit are not XML names: they are renamed, without
    # merging them with other labels
    g.add_edge(1, "_:1", "http://ex/o", layer="http://ex/p")
    g.add_edge(1, "http://ex/s", "_:42abc", layer="http://ex/p")
    g.add_edge(1, "http://ex/s", "_:x1", layer="http://ex/p")
    g.add_edge(1, "http://ex/s", "_:b1", layer="http://ex/p")
    # literals with characters XML cannot hold, or that XML parsers change, are skipped
    pg = PersistentGraph()
    pg.load_rdf(
        1,
        b'<http://ex/a> <http://ex/p> "bell\\u0007 nul\\u0000", "a\\rb\\r\\nc", "tab\\tnewline\\n" .',
        format="ttl",
    )
    doc = g.to_rdf(format="rdf")
    ET.fromstring(doc)  # well-formed XML
    for node_id in ["x1", "x42abc", "xx1", "b1"]:
        assert f'rdf:nodeID="{node_id}"' in doc, node_id
    g2 = PersistentGraph()
    assert g2.load_rdf(1, doc.encode(), format="rdf") == 4
    assert len([n for n in g2.nodes.name if n.startswith("_:")]) == 4
    # the labels as written, when retracting
    g3 = PersistentGraph()
    assert g3.retract_rdf(1, doc.encode(), format="rdf") == 4
    assert sorted(n for n in g3.nodes.name if n.startswith("_:")) == [
        "_:b1",
        "_:x1",
        "_:x42abc",
        "_:xx1",
    ]

    doc = pg.to_rdf(format="rdf")
    root = ET.fromstring(doc)  # well-formed XML
    assert "\r" not in doc
    assert [e.text for e in root.iter() if e.text and e.text.strip()] == [
        "tab\tnewline\n"
    ]
    g4 = PersistentGraph()
    assert g4.load_rdf(1, doc.encode(), format="rdf") == 1
    # other formats write every triple
    assert len(pg.to_rdf(format="nt").splitlines()) == 3
    assert PersistentGraph().load_rdf(1, pg.to_rdf(format="ttl").encode()) == 3


def test_to_rdf_as_of():
    pg = PersistentGraph()
    pg.load_rdf(1, DOC)
    pg.retract_rdf(5, WORKS_FOR)
    pg.retract_rdf(5, b"<http://ex/bob> <http://xmlns.com/foaf/0.1/age> 42 .")
    assert sorted(pg.snapshot_at(3).to_rdf().splitlines()) == sorted(
        DOC_NT.decode().splitlines()
    )
    assert len(pg.to_rdf().splitlines()) == 4


# Serialized results (format=)


def binding_name(binding):
    """The Raphtory name of a term of SPARQL Results JSON (for the simple literals used here)."""
    value = binding["value"]
    if binding["type"] == "uri":
        if value.startswith("raphtory:"):
            return unquote(value.removeprefix("raphtory:"))
        return value
    if binding["type"] == "bnode":
        return f"_:{value}"
    if "xml:lang" in binding:
        return f'"{value}"@{binding["xml:lang"]}'
    if "datatype" in binding:
        return f'"{value}"^^<{binding["datatype"]}>'
    return f'"{value}"'


def test_sparql_format_json():
    g = load_doc(PersistentGraph())
    g.add_edge(1, "Alice Smith", "Bob", layer="knows")
    for query in [
        "SELECT ?s ?p ?o WHERE { ?s ?p ?o }",
        f"SELECT ?person ?friend WHERE {{ ?person <{FOAF}name> ?n OPTIONAL {{ ?person <{FOAF}knows> ?friend }} }}",
        "SELECT ?who WHERE { raphtory:Alice%20Smith raphtory:knows ?who }",
        "SELECT ?nothing WHERE { ?s ?p <http://ex/nobody> }",
    ]:
        doc = g.sparql(query, format="json")
        assert isinstance(doc, str)
        results = json.loads(doc)
        rows = g.sparql(query)
        variables = results["head"]["vars"]
        assert variables == list(rows[0]) if rows else True
        decoded = [
            {v: binding_name(b[v]) if v in b else None for v in variables}
            for b in results["results"]["bindings"]
        ]
        key = lambda row: [str(v) for v in row.values()]
        assert sorted(decoded, key=key) == sorted(rows, key=key), query
    # terms, not names
    assert json.loads(
        g.sparql("SELECT ?s WHERE { ?s raphtory:knows ?o }", format="json")
    )["results"]["bindings"] == [
        {"s": {"type": "uri", "value": "raphtory:Alice%20Smith"}}
    ]
    # ASK
    assert json.loads(g.sparql("ASK { ?s ?p ?o }", format="json")) == {
        "head": {},
        "boolean": True,
    }
    # CONSTRUCT as JSON is JSON-LD
    jsonld = json.loads(
        g.sparql(f"CONSTRUCT WHERE {{ ?s <{FOAF}knows> ?o }}", format="json")
    )
    assert jsonld == [
        {"@id": "http://ex/alice", FOAF + "knows": [{"@id": "http://ex/bob"}]}
    ]


def test_sparql_format_golden():
    g = Graph()
    g.add_edge(1, "Alice", "Bob Smith", layer="knows")
    g.add_edge(1, "Alice", '"x, \\"y\\""@en', layer="says")
    g.add_edge(1, "Alice", INT_42, layer="age")
    query = "SELECT ?p ?o WHERE { raphtory:Alice ?p ?o } ORDER BY ?p"
    csv = (
        "p,o\r\n"
        "raphtory:age,42\r\n"
        "raphtory:knows,raphtory:Bob%20Smith\r\n"
        'raphtory:says,"x, ""y"""\r\n'
    )
    tsv = (
        "?p\t?o\n"
        "<raphtory:age>\t42\n"
        "<raphtory:knows>\t<raphtory:Bob%20Smith>\n"
        '<raphtory:says>\t"x, \\"y\\""@en\n'
    )
    xml = (
        '<?xml version="1.0"?>'
        '<sparql xmlns="http://www.w3.org/2005/sparql-results#">'
        '<head><variable name="p"/><variable name="o"/></head><results>'
        '<result><binding name="p"><uri>raphtory:age</uri></binding>'
        f'<binding name="o"><literal datatype="{XSD}integer">42</literal></binding></result>'
        '<result><binding name="p"><uri>raphtory:knows</uri></binding>'
        '<binding name="o"><uri>raphtory:Bob%20Smith</uri></binding></result>'
        '<result><binding name="p"><uri>raphtory:says</uri></binding>'
        '<binding name="o"><literal xml:lang="en">x, &quot;y&quot;</literal></binding></result>'
        "</results></sparql>"
    )
    for formats, expected in [
        (["csv", ".csv", "CSV", "text/csv", "text/csv; charset=utf-8"], csv),
        (["tsv", "text/tab-separated-values"], tsv),
        (["xml", "srx", "application/sparql-results+xml"], xml),
    ]:
        for fmt in formats:
            assert g.sparql(query, format=fmt) == expected, fmt
    ns = {"r": "http://www.w3.org/2005/sparql-results#"}
    literals = ET.fromstring(xml).findall(".//r:literal", ns)
    assert [literal.text for literal in literals] == ["42", 'x, "y"']
    assert g.sparql(query, format="application/sparql-results+json") == g.sparql(
        query, format="json"
    )
    # ASK in CSV and TSV is a bare boolean
    assert g.sparql("ASK { ?s ?p ?o }", format="csv") == "true"
    assert g.sparql("ASK { ?s raphtory:nope ?o }", format="tsv") == "false"
    # CONSTRUCT
    construct = "CONSTRUCT { ?o raphtory:knownBy ?s } WHERE { ?s raphtory:knows ?o }"
    nt = "<raphtory:Bob%20Smith> <raphtory:knownBy> <raphtory:Alice> .\n"
    for fmt in ["nt", "n-triples", "application/n-triples", "txt"]:
        assert g.sparql(construct, format=fmt) == nt, fmt
    assert g.sparql(construct, format="text/turtle") == g.sparql(construct, format="ttl")


@pytest.mark.parametrize("graph_type", [Graph, PersistentGraph])
def test_sparql_format_turtle_reloads(graph_type):
    g = load_doc(graph_type())
    g.add_edge(1, "Alice Smith", "_:b1", layer="knows")
    ttl = g.sparql("CONSTRUCT WHERE { ?s ?p ?o }", format="ttl")
    g2 = graph_type()
    assert g2.load_rdf(1, ttl.encode(), format="ttl") == 6
    # blank nodes are renamed on load
    renamed = [n for n in g2.nodes.name if n.startswith("_:")]
    assert len(renamed) == 1
    expected = g.to_rdf().replace("_:b1", renamed[0])
    assert sorted(g2.to_rdf().splitlines()) == sorted(expected.splitlines())
    # ... and RDF/XML too
    rdf = g.sparql("CONSTRUCT WHERE { ?s ?p ?o }", format="rdf")
    ET.fromstring(rdf)
    assert graph_type().load_rdf(1, rdf.encode(), format="rdf") == 6


def test_sparql_format_views():
    pg = PersistentGraph()
    pg.load_rdf(1, DOC)
    pg.retract_rdf(5, b"<http://ex/bob> <http://xmlns.com/foaf/0.1/age> 42 .")
    query = f"SELECT ?s WHERE {{ ?s <{FOAF}age> ?age }} ORDER BY ?s"
    for view, n in [
        (pg, 1),
        (pg.snapshot_at(3), 2),
        (pg.event_graph(), 2),
        (pg.layer(f"{FOAF}knows"), 0),
    ]:
        bindings = json.loads(view.sparql(query, format="json"))["results"]["bindings"]
        assert [binding_name(b["s"]) for b in bindings] == [
            row["s"] for row in view.sparql(query)
        ]
        assert len(bindings) == n
    # as of a time inside the query
    assert pg.sparql(
        f"SELECT ?s WHERE {{ GRAPH raphtory:asof:3 {{ ?s <{FOAF}age> ?age }} }} ORDER BY ?s",
        format="json",
    ) == pg.snapshot_at(3).sparql(query, format="json")


def test_sparql_format_errors():
    g = load_doc(PersistentGraph())
    with pytest.raises(
        Exception, match="SELECT results cannot be written as Turtle; use a SPARQL results"
    ):
        g.sparql("SELECT * WHERE { ?s ?p ?o }", format="ttl")
    with pytest.raises(Exception, match="ASK results cannot be written as N-Triples"):
        g.sparql("ASK { ?s ?p ?o }", format="nt")
    with pytest.raises(
        Exception,
        match="CONSTRUCT results cannot be written as SPARQL Results in CSV; use an RDF format",
    ):
        g.sparql("CONSTRUCT WHERE { ?s ?p ?o }", format="csv")
    with pytest.raises(Exception, match="DESCRIBE results cannot be written"):
        g.sparql("DESCRIBE <http://ex/alice>", format="tsv")
    for fmt in ["foo", "", 'a;b="']:
        with pytest.raises(Exception, match="unknown SPARQL results format"):
            g.sparql("SELECT * WHERE { ?s ?p ?o }", format=fmt)
    with pytest.raises(ValueError, match="decode_literals cannot be combined with format"):
        g.sparql("SELECT * WHERE { ?s ?p ?o }", decode_literals=True, format="json")
    # syntax and evaluation errors as without a format
    with pytest.raises(Exception, match="SPARQL syntax error"):
        g.sparql("SELECT WHERE", format="json")
    with pytest.raises(Exception, match="SPARQL evaluation error"):
        g.sparql("SELECT * { GRAPH <raphtory:asof:garbage> { ?s ?p ?o } }", format="json")
    # SPARQL Results XML cannot hold control characters; JSON can
    g.load_rdf(1, b'<http://ex/a> <http://ex/p> "a\\rb" .')
    query = "SELECT ?o WHERE { <http://ex/a> <http://ex/p> ?o }"
    with pytest.raises(Exception, match="cannot be written in SPARQL Results XML"):
        g.sparql(query, format="xml")
    bindings = json.loads(g.sparql(query, format="json"))["results"]["bindings"]
    assert bindings == [{"o": {"type": "literal", "value": "a\rb"}}]
    # nor, in Raphtory, an RDF 1.2 triple term, which the error names as such
    triple = "SELECT ?t { ?s ?p ?o BIND(TRIPLE(?s, ?p, ?o) AS ?t) } LIMIT 1"
    with pytest.raises(Exception, match="is an RDF 1.2 triple term") as error:
        g.sparql(triple, format="xml")
    assert "control characters" not in str(error.value)
    assert len(g.sparql(triple, format="csv").splitlines()) == 2


# Bounding the work of a query

# About 20^8 solutions joined in memory from the 20 triples of `chain()`.
CUBE = " ".join(f"?s{i} ?p{i} ?o{i} ." for i in range(8))


def chain():
    g = Graph()
    for i in range(20):
        g.add_edge(i, str(i), str(i + 1), layer="p")
    return g


def star(n):
    return "SELECT ?s { " + " ".join(f"?s ?p{i} ?o{i} ." for i in range(n)) + " }"


@pytest.mark.parametrize(
    "query",
    [
        "SELECT ?s0 { " + CUBE + " }",
        "SELECT (COUNT(*) AS ?n) { " + CUBE + " }",
        "CONSTRUCT { ?s0 ?p0 ?o7 } WHERE { " + CUBE + " }",
    ],
)
def test_sparql_timeout(query):
    g = chain()
    for fmt in [None, "json" if query.startswith("SELECT") else "nt"]:
        start = time.monotonic()
        with pytest.raises(
            Exception,
            match="SPARQL query timed out: it ran longer than its time limit of 200ms",
        ):
            g.sparql(query, format=fmt, timeout=0.2)
        assert 0.2 <= time.monotonic() - start < 10


def test_sparql_timeout_not_reached():
    g = chain()
    query = "SELECT ?s ?o { ?s raphtory:p ?o } ORDER BY ?s"
    assert g.sparql(query, timeout=60) == g.sparql(query)
    assert g.sparql(query, format="csv", timeout=60.0) == g.sparql(query, format="csv")
    assert g.sparql("ASK { ?s ?p ?o }", timeout=None) is True
    # a timeout of 0 stops any query
    with pytest.raises(Exception, match="SPARQL query timed out"):
        g.sparql("ASK {}", timeout=0)
    for bad in [-1, float("nan"), float("inf"), float("-inf")]:
        with pytest.raises(ValueError, match="timeout must be a finite number"):
            g.sparql("ASK {}", timeout=bad)
    # a finite timeout too long to represent is a limit that is never reached
    for huge in [1.8e19, 1e20, sys.float_info.max]:
        assert g.sparql("ASK { ?s ?p ?o }", timeout=huge) is True
        assert g.sparql(query, format="csv", timeout=huge) == g.sparql(query, format="csv")


def test_sparql_max_triple_patterns():
    g = chain()
    assert g.sparql(star(5), max_triple_patterns=5) == g.sparql(star(5))
    with pytest.raises(
        Exception,
        match=r"SPARQL query too complex: 6 triple patterns, the limit is 5",
    ):
        g.sparql(star(6), max_triple_patterns=5)
    with pytest.raises(Exception, match="too complex"):
        g.sparql(star(6), format="json", max_triple_patterns=5)
    # collections, paths, BIND and VALUES count
    assert g.sparql("ASK { ?s ?p (1 2) }", max_triple_patterns=5) is False
    with pytest.raises(Exception, match="5 triple patterns, the limit is 4"):
        g.sparql("ASK { ?s ?p (1 2) }", max_triple_patterns=4)
    with pytest.raises(Exception, match="3 triple patterns, the limit is 2"):
        g.sparql("ASK { ?s raphtory:p/raphtory:p/raphtory:p ?o }", max_triple_patterns=2)
    with pytest.raises(Exception, match="3 triple patterns, the limit is 2"):
        g.sparql("SELECT * { BIND(1 AS ?x) BIND(2 AS ?y) ?s ?p ?o }", max_triple_patterns=2)
    # a VALUES block counts one, plus one per variable and per 100 rows
    with pytest.raises(Exception, match="3 triple patterns, the limit is 2"):
        g.sparql("SELECT * { VALUES (?x ?y) { (1 2) (3 4) } }", max_triple_patterns=2)
    ids = " ".join(f"raphtory:{i}" for i in range(1000))
    lookup = "SELECT ?s ?o { VALUES ?s { " + ids + " } ?s raphtory:p ?o }"
    assert len(g.sparql(lookup, max_triple_patterns=13)) == 20
    with pytest.raises(Exception, match="13 triple patterns, the limit is 12"):
        g.sparql(lookup, max_triple_patterns=12)
    # no limit by default
    assert len(g.sparql(star(101))) == 20
    with pytest.raises((OverflowError, TypeError)):
        g.sparql("ASK {}", max_triple_patterns=-1)


# Time graphs inside a query


def works_for(who, org):
    return f"<http://ex/{who}> <http://ex/worksFor> <http://ex/{org}> .".encode()


@pytest.fixture
def career():
    pg = PersistentGraph()
    pg.load_rdf("2021-06-01", works_for("alice", "acme"))
    pg.load_rdf("2022-03-01", works_for("bob", "acme"))
    pg.retract_rdf("2023-06-01", works_for("alice", "acme"))
    pg.load_rdf("2023-06-01", works_for("alice", "initech"))
    return pg


def test_time_graph_diff(career):
    rows = career.sparql("""
        SELECT ?who ?new {
            GRAPH raphtory:asof:2023-01-01 { ?who <http://ex/worksFor> <http://ex/acme> }
            GRAPH raphtory:asof:2024-01-01 {
                ?who <http://ex/worksFor> ?new FILTER(?new != <http://ex/acme>)
            }
        }
        """)
    assert rows == [{"who": "http://ex/alice", "new": "http://ex/initech"}]


def test_time_graph_series(career):
    rows = career.sparql(
        """
        SELECT ?year (COUNT(*) AS ?n)
        FROM NAMED raphtory:asof:2022-01-01
        FROM NAMED raphtory:asof:2023-01-01
        FROM NAMED raphtory:asof:2024-01-01
        { GRAPH ?year { ?who <http://ex/worksFor> <http://ex/acme> } }
        GROUP BY ?year ORDER BY ?year
        """,
        decode_literals=True,
    )
    # time graphs are not nodes, so they come back as their N-Triples form
    assert rows == [
        {"year": "<raphtory:asof:2022-01-01>", "n": 1},
        {"year": "<raphtory:asof:2023-01-01>", "n": 2},
        {"year": "<raphtory:asof:2024-01-01>", "n": 1},
    ]


def test_time_graph_bound_outside_graph(career):
    # GRAPH ?g visits the time graphs the query names, wherever it is and whatever binds ?g
    expected = [{"who": "http://ex/alice"}, {"who": "http://ex/bob"}]
    for query in [
        "SELECT ?who { VALUES ?g { raphtory:asof:2023-01-01 } GRAPH ?g { ?who ?p ?org } }",
        "SELECT ?who { VALUES ?g { raphtory:asof:2023-01-01 } GRAPH ?g { ?who ?p ?org OPTIONAL { ?org ?q ?r } } }",
        "SELECT ?who { VALUES ?g { raphtory:asof:2023-01-01 } GRAPH ?g { { SELECT ?who { ?who ?p ?org } } } }",
        'SELECT ?who { BIND(IRI("raphtory:asof:2023-01-01") AS ?g) GRAPH ?g { ?who ?p ?org } }',
        "SELECT ?who { GRAPH ?g { ?who ?p ?org } FILTER(?g = raphtory:asof:2023-01-01) }",
    ]:
        assert sorted(career.sparql(query), key=lambda r: r["who"]) == expected, query
    # MINUS and FILTER NOT EXISTS see the time graphs too
    dates = "VALUES ?g { raphtory:asof:2021-01-01 raphtory:asof:2023-01-01 }"
    for query in [
        "SELECT ?g { " + dates + " MINUS { GRAPH ?g { ?who ?p ?org } } }",
        "SELECT ?g { " + dates + " FILTER NOT EXISTS { GRAPH ?g { ?who ?p ?org } } }",
    ]:
        assert career.sparql(query) == [{"g": "<raphtory:asof:2021-01-01>"}], query
    # without a time graph in the query, GRAPH ?g matches nothing
    assert career.sparql("ASK { GRAPH ?g { ?s ?p ?o } }") is False
    # a time graph built from other values: LATERAL evaluates GRAPH ?g after the BIND
    built = """
        SELECT ?year (COUNT(?who) AS ?n) {
            VALUES ?year { 2022 2023 2024 }
            BIND(IRI(CONCAT("raphtory:asof:", STR(?year), "-01-01")) AS ?g)
            LATERAL { GRAPH ?g { ?who <http://ex/worksFor> <http://ex/acme> } }
        } GROUP BY ?year ORDER BY ?year
    """
    assert career.sparql(built, decode_literals=True) == [
        {"year": 2022, "n": 1},
        {"year": 2023, "n": 2},
        {"year": 2024, "n": 1},
    ]


def test_time_graph_matches_snapshot(career):
    query = "SELECT ?s ?o {{ GRAPH <raphtory:asof:{t}> {{ ?s <http://ex/worksFor> ?o }} }} ORDER BY ?s ?o"
    for t in ["2021-01-01", "2022-01-01", "2023-01-01", "2023-06-01", "2024-01-01"]:
        snapshot = career.snapshot_at(t).sparql(
            "SELECT ?s ?o { ?s <http://ex/worksFor> ?o } ORDER BY ?s ?o"
        )
        assert career.sparql(query.format(t=t)) == snapshot
    millis = int(datetime(2023, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
    assert career.sparql(query.format(t=millis)) == career.sparql(
        query.format(t="2023-01-01")
    )
    assert len(career.sparql(query.format(t="2023-01-01"))) == 2
    with pytest.raises(Exception, match="invalid time graph"):
        career.sparql(query.format(t="garbage"))


# Temporal functions

WORKS_FOR_ACME = "<http://ex/worksFor>, <http://ex/acme>"
XSD_DATE_TIME = "http://www.w3.org/2001/XMLSchema#dateTime"


def test_valid_from_since_when(career):
    since = f"""
        SELECT ?who ?since {{
            ?who <http://ex/worksFor> <http://ex/acme>
            BIND(raphtory:validFrom(?who, {WORKS_FOR_ACME}) AS ?since)
        }}
    """
    assert career.sparql(since) == [
        {"who": "http://ex/bob", "since": f'"2022-03-01T00:00:00Z"^^<{XSD_DATE_TIME}>'}
    ]
    assert career.sparql(since, decode_literals=True) == [
        {"who": "http://ex/bob", "since": datetime(2022, 3, 1, tzinfo=timezone.utc)}
    ]
    # serialized results have the functions too
    bindings = json.loads(career.sparql(since, format="json"))["results"]["bindings"]
    assert bindings == [
        {
            "who": {"type": "uri", "value": "http://ex/bob"},
            "since": {
                "type": "literal",
                "value": "2022-03-01T00:00:00Z",
                "datatype": XSD_DATE_TIME,
            },
        }
    ]
    # as of the start of 2023, with the time graph as the reference time
    rows = career.sparql(
        f"""
        SELECT ?who ?since ?until {{
            GRAPH raphtory:asof:2023-01-01 {{ ?who <http://ex/worksFor> <http://ex/acme> }}
            BIND(raphtory:validFrom(?who, {WORKS_FOR_ACME}, raphtory:asof:2023-01-01) AS ?since)
            BIND(raphtory:validTo(?who, {WORKS_FOR_ACME}, raphtory:asof:2023-01-01) AS ?until)
        }} ORDER BY ?who
        """,
        decode_literals=True,
    )
    assert rows == [
        {
            "who": "http://ex/alice",
            "since": datetime(2021, 6, 1, tzinfo=timezone.utc),
            "until": datetime(2023, 6, 1, tzinfo=timezone.utc),
        },
        {
            "who": "http://ex/bob",
            "since": datetime(2022, 3, 1, tzinfo=timezone.utc),
            "until": None,
        },
    ]


def test_valid_to_time_per_graph(career):
    rows = career.sparql(
        """
        SELECT ?g ?who ?org ?until
        FROM NAMED raphtory:asof:2022-01-01
        FROM NAMED raphtory:asof:2023-01-01
        FROM NAMED raphtory:asof:2024-01-01
        {
            GRAPH ?g { ?who <http://ex/worksFor> ?org }
            BIND(raphtory:validToTime(?who, <http://ex/worksFor>, ?org, ?g) AS ?until)
        } ORDER BY ?g ?who
        """,
        decode_literals=True,
    )
    june_2023 = int(datetime(2023, 6, 1, tzinfo=timezone.utc).timestamp() * 1000)
    assert rows == [
        {
            "g": "<raphtory:asof:2022-01-01>",
            "who": "http://ex/alice",
            "org": "http://ex/acme",
            "until": june_2023,
        },
        {
            "g": "<raphtory:asof:2023-01-01>",
            "who": "http://ex/alice",
            "org": "http://ex/acme",
            "until": june_2023,
        },
        {
            "g": "<raphtory:asof:2023-01-01>",
            "who": "http://ex/bob",
            "org": "http://ex/acme",
            "until": None,
        },
        {
            "g": "<raphtory:asof:2024-01-01>",
            "who": "http://ex/alice",
            "org": "http://ex/initech",
            "until": None,
        },
        {
            "g": "<raphtory:asof:2024-01-01>",
            "who": "http://ex/bob",
            "org": "http://ex/acme",
            "until": None,
        },
    ]


def test_valid_from_event_and_persistent_graphs():
    g = Graph()
    g.load_rdf(1, works_for("alice", "acme"))
    g.retract_rdf(5, works_for("alice", "acme"))
    g.load_rdf(9, works_for("alice", "acme"))
    alice = f"<http://ex/alice>, {WORKS_FOR_ACME}"

    def run(view, t):
        [row] = view.sparql(
            f"""SELECT (raphtory:validFromTime({alice}, {t}) AS ?from)
                       (raphtory:validToTime({alice}, {t}) AS ?to) {{}}""",
            decode_literals=True,
        )
        return row["from"], row["to"]

    # on an event graph a triple holds from its first assertion on
    assert run(g, 3) == (1, None)
    assert run(g, 6) == (1, None)
    assert run(g, 0) == (None, None)
    # on the persistent graph runs end at retractions
    pg = g.persistent_graph()
    assert run(pg, 3) == (1, 5)
    assert run(pg, 6) == (None, None)
    assert run(pg, 10) == (9, None)
    # views decide what is visible; history before a window counts
    assert run(pg.window(2, 4), 3) == (1, None)
    assert run(pg.window(4, 5), 3) == (None, None)
    # wrong arguments give None, an unknown function an error
    assert g.sparql(f"SELECT (raphtory:validFromTime({alice}, 1, 2) AS ?x) {{}}") == [
        {"x": None}
    ]
    assert g.sparql(f'SELECT (raphtory:validFromTime({alice}, "now") AS ?x) {{}}') == [
        {"x": None}
    ]
    with pytest.raises(Exception, match="SPARQL evaluation error"):
        g.sparql(f"SELECT (raphtory:validFromm({alice}) AS ?x) {{}}")
