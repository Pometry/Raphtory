import socket
import tempfile
import threading
import time
from pathlib import Path

import pytest
from raphtory import EventTime, Graph, PersistentGraph

SH = "http://www.w3.org/ns/shacl#"
XSD = "http://www.w3.org/2001/XMLSchema#"

PEOPLE = b"""
@prefix ex: <http://ex/> .
ex:alice a ex:Person ; ex:name "Alice" ; ex:age 42 ; ex:worksFor ex:acme .
ex:bob a ex:Person ; ex:name "Bob" .
ex:acme ex:name "ACME" .
"""

SHAPES = b"""
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix ex: <http://ex/> .
ex:PersonShape a sh:NodeShape ;
    sh:targetClass ex:Person ;
    sh:property ex:PersonName , ex:PersonAge , ex:PersonEmployer .
ex:PersonName sh:path ex:name ; sh:minCount 1 ; sh:datatype xsd:string .
ex:PersonAge sh:path ex:age ; sh:maxCount 1 ; sh:minInclusive 0 .
ex:PersonEmployer sh:path ( ex:worksFor ex:name ) ; sh:minLength 2 .
"""

KEYS = {
    "focus_node",
    "path",
    "value",
    "source_shape",
    "constraint_component",
    "severity",
    "messages",
}


def people():
    """alice loses her name at 5; bob gets a negative age at 7; acme's name gets too short at 9."""
    g = PersistentGraph()
    g.load_rdf(1, PEOPLE)
    g.retract_rdf(5, b'<http://ex/alice> <http://ex/name> "Alice" .')
    g.load_rdf(7, b"<http://ex/bob> <http://ex/age> -3 .")
    g.retract_rdf(9, b'<http://ex/acme> <http://ex/name> "ACME" .')
    g.load_rdf(9, b'<http://ex/acme> <http://ex/name> "A" .')
    return g


def summary(report):
    return [
        (r["focus_node"], r["constraint_component"].removeprefix(SH), r["path"], r["value"])
        for r in report["results"]
    ]


def test_report_dict():
    g = people()
    report = g.validate_shacl(SHAPES)
    assert set(report) == {"conforms", "results", "warnings"}
    assert report["conforms"] is False
    assert report["warnings"] == []
    for result in report["results"]:
        assert set(result) == KEYS
        assert result["severity"] == SH + "Violation"
        assert isinstance(result["messages"], list)
    assert summary(report) == [
        ("http://ex/alice", "MinCountConstraintComponent", "http://ex/name", None),
        (
            "http://ex/alice",
            "MinLengthConstraintComponent",
            "(<http://ex/worksFor> / <http://ex/name>)",
            '"A"',
        ),
        (
            "http://ex/bob",
            "MinInclusiveConstraintComponent",
            "http://ex/age",
            f'"-3"^^<{XSD}integer>',
        ),
    ]
    # names: focus nodes and values are nodes, single-predicate paths are layers
    for result in report["results"]:
        assert g.node(result["focus_node"]) is not None
        if result["value"] is not None:
            assert g.node(result["value"]) is not None
    assert g.layer(report["results"][0]["path"]).count_edges() > 0
    assert [r["source_shape"] for r in report["results"]] == [
        "http://ex/PersonName",
        "http://ex/PersonEmployer",
        "http://ex/PersonAge",
    ]


def test_snapshots_and_validate_at():
    g = people()
    assert g.snapshot_at(3).validate_shacl(SHAPES) == {
        "conforms": True,
        "results": [],
        "warnings": [],
    }
    reports = g.validate_shacl_at(SHAPES, [1, 5, 7, 9])
    assert [t for t, _ in reports] == [1, 5, 7, 9]
    assert [len(report["results"]) for _, report in reports] == [0, 1, 2, 3]
    for t, report in reports:
        assert report == g.snapshot_at(t).validate_shacl(SHAPES)
    first = next(t for t, r in g.validate_shacl_at(SHAPES, list(range(12))) if not r["conforms"])
    assert first == 5
    # date-time strings are times
    [(t, report)] = g.validate_shacl_at(SHAPES, ["1970-01-01T00:00:00.006Z"])
    assert t == 6 and len(report["results"]) == 1
    assert g.validate_shacl_at(SHAPES, []) == []
    # times from Raphtory's own APIs, as snapshot_at takes them
    earliest = g.node("http://ex/bob").earliest_time
    [(t, report)] = g.validate_shacl_at(SHAPES, [earliest])
    assert t == 1 and report["conforms"]
    [(t, report)] = g.validate_shacl_at(SHAPES, [EventTime(7)])
    assert t == 7 and report == g.snapshot_at(EventTime(7)).validate_shacl(SHAPES)
    # an event graph ignores retractions
    events = g.event_graph()
    assert len(events.validate_shacl(SHAPES)["results"]) == 2


def test_views():
    g = people()
    view = g.snapshot_at(3).exclude_layer("http://ex/name")
    assert [(r["focus_node"], r["path"]) for r in view.validate_shacl(SHAPES)["results"]] == [
        ("http://ex/alice", "http://ex/name"),
        ("http://ex/bob", "http://ex/name"),
    ]
    assert g.subgraph(["http://ex/acme"]).validate_shacl(SHAPES)["conforms"]


def test_plain_graph_names():
    g = Graph()
    g.add_edge(1, "Alice", "Bob", layer="manages")
    g.add_edge(2, "Alice", "Carol", layer="manages")
    shapes = b"""
        @prefix sh: <http://www.w3.org/ns/shacl#> .
        @prefix raphtory: <raphtory:> .
        <http://ex/OneReport> sh:targetSubjectsOf raphtory:manages ;
            sh:property [ sh:path raphtory:manages ; sh:maxCount 1 ] .
    """
    [result] = g.validate_shacl(shapes)["results"]
    assert (result["focus_node"], result["path"]) == ("Alice", "manages")
    assert result["source_shape"].startswith("_:")
    assert g.window(0, 2).validate_shacl(shapes)["conforms"]


def test_shapes_sources(tmp_path):
    g = people()
    expected = g.validate_shacl(SHAPES)
    path = tmp_path / "shapes.ttl"
    path.write_bytes(SHAPES)
    assert g.validate_shacl(path) == expected
    assert g.validate_shacl(str(path)) == expected
    # a path without an extension is Turtle; format overrides the extension
    other = tmp_path / "shapes"
    other.write_bytes(SHAPES)
    assert g.validate_shacl(other) == expected
    nt = tmp_path / "shapes.txt"
    nt.write_bytes(
        b"<http://ex/S> <http://www.w3.org/ns/shacl#targetNode> <http://ex/alice> .\n"
        b"<http://ex/S> <http://www.w3.org/ns/shacl#property> _:p .\n"
        b"_:p <http://www.w3.org/ns/shacl#path> <http://ex/missing> .\n"
        b'_:p <http://www.w3.org/ns/shacl#minCount> "1"^^<http://www.w3.org/2001/XMLSchema#integer> .\n'
    )
    [result] = g.validate_shacl(nt, format="nt")["results"]
    assert result["path"] == "http://ex/missing"
    # relative IRIs need a base
    relative = b"<S> <http://www.w3.org/ns/shacl#targetNode> <alice> ; <http://www.w3.org/ns/shacl#property> [ <http://www.w3.org/ns/shacl#path> <missing> ; <http://www.w3.org/ns/shacl#minCount> 1 ] ."
    [result] = g.validate_shacl(relative, base_iri="http://ex/")["results"]
    assert result["focus_node"] == "http://ex/alice"
    # a str that looks like a document is not read as a path
    with pytest.raises(TypeError, match="looks like an RDF document"):
        g.validate_shacl(SHAPES.decode())
    with pytest.raises(TypeError, match="must be bytes"):
        g.validate_shacl(42)
    with pytest.raises(Exception, match="cannot open"):
        g.validate_shacl(tmp_path / "missing.ttl")
    with pytest.raises(Exception, match="unknown RDF format"):
        g.validate_shacl(SHAPES, format="nope")


def test_report_format():
    g = people()
    for format in ["ttl", "nt", "jsonld", "rdf"]:
        document = g.validate_shacl(SHAPES, report_format=format)
        assert isinstance(document, str)
        assert SH + "ValidationReport" in document or "ValidationReport" in document
    nt = g.validate_shacl(SHAPES, report_format="nt")
    assert nt.count(f"<{SH}result>") == 3
    assert nt.count(f"<{SH}focusNode> <http://ex/alice>") == 2
    conforming = g.snapshot_at(3).validate_shacl(SHAPES, report_format="nt")
    assert len(conforming.splitlines()) == 2
    assert f'<{SH}conforms> "true"^^<{XSD}boolean>' in conforming
    with pytest.raises(Exception, match="unknown RDF format"):
        g.validate_shacl(SHAPES, report_format="nope")


def test_rdf_xml_report_with_control_characters():
    """An RDF/XML report with a carriage return raises; Turtle and N-Triples hold it."""
    g = PersistentGraph()
    g.load_rdf(1, b'<http://ex/a> <http://ex/name> "cr\\rx" .')
    shapes = b"""
        @prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <http://ex/> .
        ex:S sh:targetObjectsOf ex:name ; sh:maxLength 2 .
    """
    [result] = g.validate_shacl(shapes)["results"]
    assert result["focus_node"] == result["value"] == '"cr\\rx"'
    with pytest.raises(
        Exception,
        match="cannot be written in an RDF/XML SHACL report, which cannot hold its control "
        "characters unchanged; use Turtle, N-Triples or JSON-LD",
    ):
        g.validate_shacl(shapes, report_format="rdf")
    nt = g.validate_shacl(shapes, report_format="nt")
    assert f'<{SH}focusNode> "cr\\rx" .' in nt
    assert f'<{SH}value> "cr\\rx" .' in nt
    reloaded = PersistentGraph()
    reloaded.load_rdf(1, g.validate_shacl(shapes, report_format="ttl").encode())
    rows = reloaded.sparql(
        f"SELECT ?f ?v {{ ?r a <{SH}ValidationResult> "
        f"OPTIONAL {{ ?r <{SH}focusNode> ?f }} OPTIONAL {{ ?r <{SH}value> ?v }} }}"
    )
    assert rows == [{"f": '"cr\\rx"', "v": '"cr\\rx"'}]


def test_errors():
    g = people()
    with pytest.raises(Exception, match="RDF parse error"):
        g.validate_shacl(b"this is not turtle")
    with pytest.raises(Exception, match=r"unsupported SHACL feature: sh:sparql \(SHACL-SPARQL is not supported\)"):
        g.validate_shacl(
            b"""@prefix sh: <http://www.w3.org/ns/shacl#> .
            <http://ex/S> sh:targetNode <http://ex/a> ; sh:sparql [ sh:select "SELECT $this WHERE {}" ] ."""
        )
    with pytest.raises(Exception, match="unsupported SHACL feature: sh:rule"):
        g.validate_shacl_at(
            b"<http://ex/S> <http://www.w3.org/ns/shacl#rule> <http://ex/R> .", [1]
        )
    with pytest.raises(Exception, match="SHACL error"):
        g.validate_shacl(
            b"""@prefix sh: <http://www.w3.org/ns/shacl#> .
            <http://ex/S> a sh:NodeShape ; sh:targetClass "x" ."""
        )
    # the validator cannot evaluate the inverse of a sequence path: rejected before validating
    with pytest.raises(Exception, match=r"unsupported SHACL feature: sh:inversePath \(the validator only supports the inverse of a predicate"):
        g.validate_shacl(
            b"""@prefix sh: <http://www.w3.org/ns/shacl#> .
            <http://ex/S> sh:targetNode <http://ex/acme> ;
                sh:property [ sh:path [ sh:inversePath ( <http://ex/worksFor> <http://ex/name> ) ] ; sh:minCount 1 ] ."""
        )
    # written with inverses of predicates instead: alice works for the company named "A"
    report = g.validate_shacl(
        b"""@prefix sh: <http://www.w3.org/ns/shacl#> .
        <http://ex/S> sh:targetNode "A" ;
            sh:property [ sh:path ( [ sh:inversePath <http://ex/name> ] [ sh:inversePath <http://ex/worksFor> ] ) ;
                sh:minCount 1 ; sh:maxCount 1 ] ."""
    )
    assert report["conforms"] is True
    with pytest.raises(Exception, match="unsupported SHACL feature: sh:minListLength"):
        g.validate_shacl(
            b"""@prefix sh: <http://www.w3.org/ns/shacl#> .
            <http://ex/S> sh:targetNode <http://ex/a> ; sh:property [ sh:path <http://ex/l> ; sh:minListLength 2 ] ."""
        )
    with pytest.raises(Exception, match="unsupported SHACL feature: sh:hasValue"):
        g.validate_shacl(
            b"""@prefix sh: <http://www.w3.org/ns/shacl#> .
            <http://ex/S> sh:targetNode <http://ex/a> ;
                sh:property [ sh:path <http://ex/flag> ; sh:hasValue "1"^^<http://www.w3.org/2001/XMLSchema#boolean> ] ."""
        )
    # shapes beyond the limits of the validator are an error, not a crash
    values = " ".join(f"<http://ex/v{i}>" for i in range(100_000))
    with pytest.raises(Exception, match="an RDF list of more than 10000 members"):
        g.validate_shacl(
            f"""@prefix sh: <http://www.w3.org/ns/shacl#> .
            <http://ex/S> sh:targetNode <http://ex/a> ; sh:property [ sh:path <http://ex/p> ; sh:in ( {values} ) ] .""".encode()
        )
    depth = 100_000
    with pytest.raises(Exception, match="nests shapes and paths more than 256 deep"):
        g.validate_shacl(
            f"""@prefix sh: <http://www.w3.org/ns/shacl#> .
            <http://ex/S> sh:targetNode <http://ex/a> ;
                sh:property [ sh:path {"[ sh:zeroOrOnePath " * depth}<http://ex/p>{" ]" * depth} ; sh:maxCount 0 ] .""".encode()
        )
    assert not g.validate_shacl(SHAPES)["conforms"]


def test_subclass_warning():
    g = PersistentGraph()
    g.load_rdf(
        1,
        b"""@prefix ex: <http://ex/> . @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
        ex:Student rdfs:subClassOf ex:Person . ex:bob a ex:Student .""",
    )
    report = g.validate_shacl(SHAPES)
    assert report["conforms"]
    [warning] = report["warnings"]
    assert warning.startswith("the data has rdfs:subClassOf triples")
    assert g.exclude_layer("http://www.w3.org/2000/01/rdf-schema#subClassOf").validate_shacl(SHAPES)["warnings"] == []


def test_validation_releases_the_gil():
    g = PersistentGraph()
    nt = "".join(
        f'<http://ex/p{i}> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://ex/Person> .\n'
        f'<http://ex/p{i}> <http://ex/name> "P{i}" .\n'
        for i in range(5000)
    )
    g.load_rdf(1, nt.encode(), format="nt")
    ticks = []
    done = threading.Event()

    def tick():
        while not done.is_set():
            ticks.append(time.monotonic())
            time.sleep(0.001)

    ticker = threading.Thread(target=tick)
    ticker.start()
    validations = []
    try:
        for _ in range(3):
            start = time.monotonic()
            assert g.validate_shacl(SHAPES)["conforms"]
            validations.append((start, time.monotonic()))
    finally:
        done.set()
        ticker.join()
    # the ticker ran during validations, so the GIL was released
    inside = sum(1 for t in ticks if any(start < t < end for start, end in validations))
    assert sum(end - start for start, end in validations) >= 0.05
    assert inside >= 20, (inside, validations)


def test_reports_are_deterministic():
    """Reports from blank-node property shapes are the same on every validation."""
    g = PersistentGraph()
    g.load_rdf(1, b"@prefix ex: <http://ex/> . ex:a ex:p 5 .")
    shapes = b"""@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <http://ex/> .
    ex:S sh:targetNode ex:a ;
      sh:property [ sh:path ex:p ; sh:maxInclusive 1 ; sh:message "first" ] ;
      sh:property [ sh:path ex:p ; sh:maxInclusive 2 ; sh:message "second" ] ."""
    first = g.validate_shacl(shapes)
    assert len(first["results"]) == 2
    for _ in range(20):
        assert g.validate_shacl(shapes) == first
    assert len({r["source_shape"] for r in first["results"]}) == 2


def test_literal_values_and_recursion():
    g = PersistentGraph()
    g.load_rdf(
        1,
        b"""@prefix ex: <http://ex/> . @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        ex:a ex:v "1"^^xsd:boolean , "042"^^xsd:long , "colour"@en-gb .
        ex:alice ex:knows ex:bob . ex:bob ex:knows ex:alice .""",
    )
    report = g.validate_shacl(
        b"""@prefix sh: <http://www.w3.org/ns/shacl#> .
        <http://ex/S> sh:targetNode <http://ex/a> ; sh:property [ sh:path <http://ex/v> ; sh:in ( ) ] ."""
    )
    # the values as written in the graph, although the validator rewrites them
    assert sorted(r["value"] for r in report["results"]) == sorted(
        ['"1"^^<http://www.w3.org/2001/XMLSchema#boolean>', '"042"^^<http://www.w3.org/2001/XMLSchema#long>', '"colour"@en-gb']
    )
    assert all(g.node(r["value"]) is not None for r in report["results"])
    # recursive shapes on a cycle: alice depends on herself through bob, so she is reported,
    # with a warning
    report = g.validate_shacl(
        b"""@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <http://ex/> .
        ex:S sh:targetNode ex:alice ; sh:property [ sh:path ex:knows ; sh:node ex:S ] ."""
    )
    assert [(r["focus_node"], r["value"]) for r in report["results"]] == [
        ("http://ex/alice", "http://ex/bob"),
    ]
    [warning] = report["warnings"]
    assert warning.startswith("the shapes are recursive")


def listener():
    """A local TCP server that counts the connections it accepts."""
    server = socket.socket()
    server.bind(("127.0.0.1", 0))
    server.listen()
    server.settimeout(0.1)
    hits = []
    stop = threading.Event()

    def accept():
        while not stop.is_set():
            try:
                connection, _ = server.accept()
                hits.append(1)
                connection.close()
            except OSError:
                pass

    thread = threading.Thread(target=accept, daemon=True)
    thread.start()
    return server.getsockname()[1], hits, stop


def test_sparql_service_makes_no_request():
    """SERVICE calls never reach the network."""
    g = people()
    port, hits, stop = listener()
    service = f"<http://127.0.0.1:{port}/sparql>"
    try:
        with pytest.raises(Exception, match=f"SERVICE {service} is not supported"):
            g.sparql(f"SELECT * WHERE {{ SERVICE {service} {{ ?s ?p ?o }} }}")
        with pytest.raises(Exception, match="is not supported"):
            g.sparql(f"SELECT * WHERE {{ SERVICE {service} {{ ?s ?p ?o }} }}", format="json")
        with pytest.raises(Exception, match="is not supported"):
            g.sparql(f"CONSTRUCT {{ ?s ?p ?o }} WHERE {{ SERVICE {service} {{ ?s ?p ?o }} }}", format="nt")
        assert g.sparql(f"SELECT * WHERE {{ SERVICE SILENT {service} {{ ?s ?p ?o }} }}") == [
            {"s": None, "p": None, "o": None}
        ]
        time.sleep(0.3)
        assert hits == []
    finally:
        stop.set()
