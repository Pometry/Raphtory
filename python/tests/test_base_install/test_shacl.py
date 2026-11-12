from raphtory import RdfGraph

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
        (
            r["focus_node"],
            r["constraint_component"].removeprefix(SH),
            r["path"],
            r["value"],
        )
        for r in report["results"]
    ]


def test_report_dict():
    rg = people()
    report = rg.valid().validate_shacl(SHAPES)
    assert report["conforms"] is False
    result = report["results"][0]
    del result["messages"]
    assert result == {
        "focus_node": "http://ex/alice",
        "path": "http://ex/name",
        "value": None,
        "source_shape": "http://ex/PersonName",
        "constraint_component": SH + "MinCountConstraintComponent",
        "severity": SH + "Violation",
    }
    assert summary(report) == [
        ("http://ex/alice", "MinCountConstraintComponent", "http://ex/name", None),
        (
            "http://ex/alice",
            "MinLengthConstraintComponent",
            "(http://ex/worksFor / http://ex/name)",
            '"A"',
        ),
        (
            "http://ex/bob",
            "MinInclusiveConstraintComponent",
            "http://ex/age",
            f'"-3"^^<{XSD}integer>',
        ),
    ]
    # or the report as a document
    nt = rg.valid().validate_shacl(SHAPES, report_format="nt")
    assert isinstance(nt, str)
    assert nt.count(f"<{SH}result>") == 3


def test_views():
    rg = people()
    reports = [rg.at(t).validate_shacl(SHAPES) for t in [1, 6, 7]]
    assert [len(report["results"]) for report in reports] == [0, 1, 2]
