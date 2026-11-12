//! The temporal functions `raphtory:validFrom`, `validTo`, `validFromTime` and `validToTime`.
use super::{
    select,
    temporal::{assert_at, retract_at, O, P, Q, S, SPO},
};
use crate::{RdfFormat, RdfGraph, RdfView};
use raphtory::{
    db::api::view::{IntoDynamic, StaticGraphViewOps},
    prelude::*,
};

const XSD: &str = "http://www.w3.org/2001/XMLSchema#";

fn int(t: i64) -> String {
    format!("\"{t}\"^^<{XSD}integer>")
}

fn date_time(lexical: &str) -> String {
    format!("\"{lexical}\"^^<{XSD}dateTime>")
}

fn parse_int(value: &str) -> Option<i64> {
    (value != "UNDEF").then(|| {
        value
            .strip_prefix('"')
            .and_then(|v| v.strip_suffix(&format!("\"^^<{XSD}integer>")))
            .unwrap_or_else(|| panic!("not an integer: {value}"))
            .parse()
            .unwrap()
    })
}

fn args(spo: [&str; 3], graph: Option<&str>) -> String {
    let mut args = spo.map(|t| format!("<{t}>")).join(", ");
    if let Some(graph) = graph {
        args.push_str(", ");
        args.push_str(graph);
    }
    args
}

fn value<G: StaticGraphViewOps + IntoDynamic>(view: &RdfView<G>, expr: &str) -> Option<String> {
    let rows = select(view, &format!("SELECT ({expr} AS ?x) {{}}"));
    assert_eq!(rows.len(), 1, "{expr}");
    Some(rows[0][0].clone()).filter(|v| v != "UNDEF")
}

fn run<G: StaticGraphViewOps + IntoDynamic>(
    view: &RdfView<G>,
    spo: [&str; 3],
    graph: Option<&str>,
) -> (Option<i64>, Option<i64>) {
    let a = args(spo, graph);
    let query = format!(
        "SELECT (raphtory:validFromTime({a}) AS ?f) (raphtory:validToTime({a}) AS ?t) {{}}"
    );
    let rows = select(view, &query);
    assert_eq!(rows.len(), 1, "{query}");
    (parse_int(&rows[0][0]), parse_int(&rows[0][1]))
}

#[test]
fn latest_run_in_the_scope() {
    let a = RdfGraph::new();
    assert_at(&a, 10, SPO);
    retract_at(&a, 30, SPO);
    assert_at(&a, 50, SPO);
    // inserted out of time order
    let b = RdfGraph::new();
    assert_at(&b, 10, SPO);
    assert_at(&b, 30, SPO);
    retract_at(&b, 20, SPO);
    assert_at(&b, 31, SPO);
    assert_at(&b, 32, SPO);
    let c = RdfGraph::new();
    assert_at(&c, 10, SPO);
    retract_at(&c, 20, SPO);
    retract_at(&c, 25, SPO);
    // event ids, not insertion order, order events at the same time
    let d = RdfGraph::new();
    assert_at(&d, 10, SPO);
    assert_at(&d, (20, 2), SPO);
    retract_at(&d, (20, 1), SPO);
    let e = RdfGraph::new();
    assert_at(&e, 10, SPO);
    retract_at(&e, (20, 2), SPO);
    assert_at(&e, (20, 1), SPO);
    // at an identical time and event id the retraction is last
    let f = RdfGraph::new();
    assert_at(&f, 10, SPO);
    retract_at(&f, (20, 1), SPO);
    assert_at(&f, (20, 1), SPO);

    assert_eq!(run(&a, SPO, None), (Some(50), None));
    assert_eq!(run(&a.window(15, 35), SPO, None), (Some(15), Some(30)));
    assert_eq!(run(&a.at(25), SPO, None), (Some(25), None));
    assert_eq!(run(&a.before(26), SPO, None), (Some(10), None));
    assert_eq!(run(&b.at(33), SPO, None), (Some(33), None));
    assert_eq!(run(&b.before(34), SPO, None), (Some(30), None));
    assert_eq!(run(&b.window(25, 34), SPO, None), (Some(30), None));
    assert_eq!(run(&b.window(31, 34), SPO, None), (Some(31), None));
    assert_eq!(run(&a, SPO, Some("raphtory:asof:25")), (Some(10), None));
    assert_eq!(run(&c, SPO, None), (Some(10), Some(20)));
    assert_eq!(run(&d, SPO, None), (Some(20), None));
    assert_eq!(run(&e, SPO, None), (Some(10), Some(20)));
    assert_eq!(run(&f, SPO, None), (Some(10), Some(20)));
}

const WORKS_FOR: &str = "http://ex/worksFor";
const ALICE: &str = "http://ex/alice";
const BOB: &str = "http://ex/bob";
const ACME: &str = "http://ex/acme";
const INITECH: &str = "http://ex/initech";

#[test]
fn time_graph_argument() {
    let rg = RdfGraph::new();
    let doc = |who: &str, org: &str| format!("<{who}> <{WORKS_FOR}> <{org}> .");
    let load = |t: &str, who, org| {
        rg.load_rdf(t, doc(who, org).as_bytes(), RdfFormat::NTriples, None)
            .unwrap()
    };
    load("2021-06-01", ALICE, ACME);
    load("2022-03-01", BOB, ACME);
    rg.retract_rdf(
        "2023-06-01",
        doc(ALICE, ACME).as_bytes(),
        RdfFormat::NTriples,
        None,
    )
    .unwrap();
    load("2023-06-01", ALICE, INITECH);

    let rows = select(
        &rg,
        "PREFIX ex: <http://ex/> SELECT ?g ?who ?since ?until
        FROM NAMED raphtory:asof:2023-01-01 FROM NAMED raphtory:asof:2024-01-01 {
            GRAPH ?g { ?who ex:worksFor ex:acme }
            BIND(raphtory:validFrom(?who, ex:worksFor, ex:acme, ?g) AS ?since)
            BIND(raphtory:validTo(?who, ex:worksFor, ex:acme, ?g) AS ?until)
        }",
    );
    let row = |date: &str, who: &str, since: &str| {
        vec![
            format!("<raphtory:asof:{date}>"),
            format!("<{who}>"),
            date_time(&format!("{since}T00:00:00Z")),
            "UNDEF".to_owned(),
        ]
    };
    assert_eq!(
        rows,
        [
            row("2023-01-01", ALICE, "2021-06-01"),
            row("2023-01-01", BOB, "2022-03-01"),
            row("2024-01-01", BOB, "2022-03-01"),
        ]
    );
}

#[test]
fn hidden_by_the_view() {
    let rg = RdfGraph::new();
    assert_at(&rg, 1, [S, P, O]);
    assert_at(&rg, 2, [S, Q, O]);
    assert_eq!(run(&rg, [S, P, O], None), (Some(1), None));
    assert_eq!(run(&rg, [S, Q, O], None), (Some(2), None));
    let only_q = rg.valid_layers(Q);
    assert_eq!(run(&only_q, [S, P, O], None), (None, None));
    assert_eq!(run(&only_q, [S, Q, O], None), (Some(2), None));
    let without_o = rg.exclude_nodes([O]);
    assert_eq!(run(&without_o, [S, P, O], None), (None, None));
}

#[test]
fn literal_objects() {
    let rg = RdfGraph::new();
    let load = |t: i64, doc: &str| {
        rg.load_rdf(t, doc.as_bytes(), RdfFormat::Turtle, None)
            .unwrap()
    };
    let integer =
        "<http://ex/s> <http://ex/age> \"042\"^^<http://www.w3.org/2001/XMLSchema#integer> .";
    load(1, integer);
    load(
        3,
        "<http://ex/s> <http://ex/age> \"42\"^^<http://www.w3.org/2001/XMLSchema#int> .",
    );
    rg.retract_rdf(5, integer.as_bytes(), RdfFormat::Turtle, None)
        .unwrap();
    load(
        1,
        r#"@prefix ex: <http://ex/> . @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
        ex:s ex:n "5"^^xsd:int ;
             ex:name "x"@EN ;
             ex:custom "x"^^<http://ex/dt> ;
             ex:bad "abc"^^xsd:integer ;
             ex:flag "1"^^xsd:boolean ;
             ex:when "2024-01-02T03:04:05+02:00"^^xsd:dateTime ;
             ex:dec "1.0"^^xsd:decimal ;
             ex:plain "x" ."#,
    );
    let prefix = format!("PREFIX ex: <http://ex/> PREFIX xsd: <{XSD}>");
    let call = |p: &str, o: &str| {
        let query =
            format!("{prefix} SELECT (raphtory:validFromTime(ex:s, ex:{p}, {o}) AS ?f) {{}}");
        parse_int(&select(&rg, &query)[0][0])
    };

    // both spellings of 42 are in the view: ambiguous
    assert_eq!(call("age", "\"042\"^^xsd:integer"), None);
    assert_eq!(call("name", "\"x\"@en"), Some(1));
    // every literal object, through a pattern
    let rows = select(
        &rg.valid(),
        &format!(
            "{prefix} SELECT ?p ?f {{ ex:s ?p ?o BIND(raphtory:validFromTime(ex:s, ?p, ?o) AS ?f) }}"
        ),
    );
    assert_eq!(rows.len(), 9);
    assert!(rows.iter().all(|row| row[1] != "UNDEF"), "{rows:?}");
}

#[test]
fn year_range() {
    for (t, expected) in [
        (-62_135_596_800_001, None), // 0000-12-31T23:59:59.999
        (-62_135_596_800_000, Some("0001-01-01T00:00:00Z")), // year 1
        (253_402_300_799_999, Some("9999-12-31T23:59:59.999Z")), // year 9999
        (253_402_300_800_000, None), // 10000-01-01
    ] {
        let rg = RdfGraph::new();
        assert_at(&rg, t, SPO);
        let a = args(SPO, None);
        assert_eq!(
            value(&rg, &format!("raphtory:validFrom({a})")),
            expected.map(date_time),
            "{t}"
        );
        assert_eq!(
            value(&rg, &format!("raphtory:validFromTime({a})")),
            Some(int(t)),
            "{t}"
        );
    }
}
