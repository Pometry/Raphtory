from raphtory import EventTime, RdfGraph

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
    rg = RdfGraph()
    rg.load_rdf(1, PEOPLE)
    rg.retract_triple(5, "http://ex/alice", "http://ex/name", '"Alice"')
    rg.add_triple(7, "http://ex/bob", "http://ex/age", f'"-3"^^<{XSD}integer>')
    rg.retract_triple(9, "http://ex/acme", "http://ex/name", '"ACME"')
    rg.add_triple(9, "http://ex/acme", "http://ex/name", '"A"')
    return rg


def summary(report):
    return [
        (r["focus_node"], r["constraint_component"].removeprefix(SH), r["path"], r["value"])
        for r in report["results"]
    ]


def test_report_dict():
    rg = people()
    report = rg.valid().validate_shacl(SHAPES)
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
    # names: focus nodes and values are nodes
    for result in report["results"]:
        assert rg.graph.node(result["focus_node"]) is not None
        if result["value"] is not None:
            assert rg.graph.node(result["value"]) is not None
    assert [r["source_shape"] for r in report["results"]] == [
        "http://ex/PersonName",
        "http://ex/PersonEmployer",
        "http://ex/PersonAge",
    ]
    # or the report as a document
    nt = rg.valid().validate_shacl(SHAPES, report_format="nt")
    assert isinstance(nt, str)
    assert nt.count(f"<{SH}result>") == 3


def test_snapshots_and_validate_at():
    rg = people()
    # an int, a date-time string and a time from Raphtory's own APIs
    reports = rg.validate_shacl_at(
        SHAPES, [1, "1970-01-01T00:00:00.006Z", EventTime(7, 3)]
    )
    assert [t.as_tuple for t, _ in reports] == [(1, 0), (6, 0), (7, 3)]
    assert [len(report["results"]) for _, report in reports] == [0, 1, 2]
    for t, report in reports:
        assert report == rg.snapshot_at(t).validate_shacl(SHAPES)
