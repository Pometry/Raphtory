//! The temporal functions `raphtory:validFrom`, `validTo`, `validFromTime` and `validToTime`.
use super::{
    ask, select,
    temporal::{assert_at, retract_at, triples, Event, LOG_TRIPLES, O, P, Q, S, SPO},
    within_60s,
};
use crate::{
    db::api::view::{DynamicGraph, IntoDynamic},
    errors::GraphError,
    prelude::*,
    rdf::{
        evaluator, with_temporal_functions, QueryResults, QueryResultsFormat, RaphtoryDataset,
        RdfError, RdfFormat, RdfViewOps, SparqlOptions, SparqlResults, VALID_FROM, VALID_FROM_TIME,
        VALID_TO, VALID_TO_TIME,
    },
};
use oxigraph::sparql::SparqlEvaluator;
use proptest::prelude::*;
use raphtory_api::core::storage::timeindex::AsTime;
use std::collections::{BTreeMap, BTreeSet};

const XSD: &str = "http://www.w3.org/2001/XMLSchema#";

const JUN_2021: i64 = 1_622_505_600_000;
const MAR_2022: i64 = 1_646_092_800_000;
const JAN_2022: i64 = 1_640_995_200_000;
const JAN_2023: i64 = 1_672_531_200_000;
const JUN_2023: i64 = 1_685_577_600_000;

/// `"t"^^xsd:integer`
fn int(t: i64) -> String {
    format!("\"{t}\"^^<{XSD}integer>")
}

/// `"lexical"^^xsd:dateTime`
fn date_time(lexical: &str) -> String {
    format!("\"{lexical}\"^^<{XSD}dateTime>")
}

/// The integer of an `xsd:integer` in N-Triples form.
fn parse_int(value: &str) -> i64 {
    value
        .strip_prefix('"')
        .and_then(|v| v.strip_suffix(&format!("\"^^<{XSD}integer>")))
        .unwrap_or_else(|| panic!("not an integer: {value}"))
        .parse()
        .unwrap()
}

/// The arguments of a call on `<s> <p> <o>`, with an optional reference argument.
fn args(spo: [&str; 3], at: Option<&str>) -> String {
    let mut args = spo.map(|t| format!("<{t}>")).join(", ");
    if let Some(at) = at {
        args.push_str(", ");
        args.push_str(at);
    }
    args
}

/// The value of the expression `expr` in a query on `view`, in N-Triples form (`None`:
/// unbound).
fn value<G: RdfViewOps>(view: &G, expr: &str) -> Option<String> {
    let rows = select(view, &format!("SELECT ({expr} AS ?x) {{}}"));
    assert_eq!(rows.len(), 1, "{expr}");
    Some(rows[0][0].clone()).filter(|v| v != "UNDEF")
}

/// `(validFromTime, validToTime)` of a triple.
fn run<G: RdfViewOps>(view: &G, spo: [&str; 3], at: Option<&str>) -> (Option<i64>, Option<i64>) {
    let a = args(spo, at);
    let query = format!(
        "SELECT (raphtory:validFromTime({a}) AS ?f) (raphtory:validToTime({a}) AS ?t) {{}}"
    );
    let rows = select(view, &query);
    assert_eq!(rows.len(), 1, "{query}");
    let get = |v: &str| (v != "UNDEF").then(|| parse_int(v));
    (get(&rows[0][0]), get(&rows[0][1]))
}

/// `(validFrom, validTo)` of a triple, in N-Triples form.
fn run_dt<G: RdfViewOps>(
    view: &G,
    spo: [&str; 3],
    at: Option<&str>,
) -> (Option<String>, Option<String>) {
    let a = args(spo, at);
    (
        value(view, &format!("raphtory:validFrom({a})")),
        value(view, &format!("raphtory:validTo({a})")),
    )
}

const WORKS_FOR: &str = "http://ex/worksFor";
const ALICE: &str = "http://ex/alice";
const BOB: &str = "http://ex/bob";
const ACME: &str = "http://ex/acme";
const INITECH: &str = "http://ex/initech";

/// Alice works for acme from June 2021 to June 2023, then for initech; bob works for acme from
/// March 2022.
fn career<G: RdfMutationOps>(g: &G) {
    let doc = |who: &str, org: &str| format!("<{who}> <{WORKS_FOR}> <{org}> .");
    let load = |t: &str, who, org| {
        g.load_rdf(t, doc(who, org).as_bytes(), RdfFormat::NTriples, None)
            .unwrap()
    };
    load("2021-06-01", ALICE, ACME);
    load("2022-03-01", BOB, ACME);
    g.retract_rdf(
        "2023-06-01",
        doc(ALICE, ACME).as_bytes(),
        RdfFormat::NTriples,
        None,
    )
    .unwrap();
    load("2023-06-01", ALICE, INITECH);
}

const PREFIX: &str = "PREFIX ex: <http://ex/> PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> ";

/// The employment example.
#[test]
fn employment() {
    let pg = PersistentGraph::new();
    career(&pg);
    let alice_acme = [ALICE, WORKS_FOR, ACME];
    let bob_acme = [BOB, WORKS_FOR, ACME];
    let alice_initech = [ALICE, WORKS_FOR, INITECH];

    // now: alice no longer works for acme, bob still does
    assert_eq!(run(&pg, alice_acme, None), (None, None));
    assert_eq!(run(&pg, bob_acme, None), (Some(MAR_2022), None));
    assert_eq!(run(&pg, alice_initech, None), (Some(JUN_2023), None));
    assert_eq!(
        run_dt(&pg, bob_acme, None),
        (Some(date_time("2022-03-01T00:00:00Z")), None)
    );

    // at the start of 2023 alice worked for acme, until June
    let at = Some("raphtory:asof:2023-01-01");
    assert_eq!(run(&pg, alice_acme, at), (Some(JUN_2021), Some(JUN_2023)));
    assert_eq!(
        run_dt(&pg, alice_acme, at),
        (
            Some(date_time("2021-06-01T00:00:00Z")),
            Some(date_time("2023-06-01T00:00:00Z"))
        )
    );
    assert_eq!(run(&pg, alice_initech, at), (None, None));

    // since when?
    let rows = select(
        &pg,
        &format!(
            "{PREFIX} SELECT ?who ?since {{
                ?who ex:worksFor ex:acme
                BIND(raphtory:validFrom(?who, ex:worksFor, ex:acme) AS ?since)
            }}"
        ),
    );
    assert_eq!(
        rows,
        [[format!("<{BOB}>"), date_time("2022-03-01T00:00:00Z")]]
    );

    // the four functions are registered under their constants
    for (name, expected) in [
        (VALID_FROM, Some(date_time("2021-06-01T00:00:00Z"))),
        (VALID_TO, Some(date_time("2023-06-01T00:00:00Z"))),
        (VALID_FROM_TIME, Some(int(JUN_2021))),
        (VALID_TO_TIME, Some(int(JUN_2023))),
    ] {
        let expr = format!("<{name}>({})", args(alice_acme, at));
        assert_eq!(value(&pg, &expr), expected, "{name}");
    }
}

/// A duplicate assertion does not restart the run.
#[test]
fn duplicate_assertion() {
    let pg = PersistentGraph::new();
    assert_at(&pg, 1, SPO);
    assert_at(&pg, 3, SPO);
    assert_eq!(run(&pg, SPO, None), (Some(1), None));
    assert_eq!(run(&pg, SPO, Some("3")), (Some(1), None));
    assert_eq!(run(&pg, SPO, Some("2")), (Some(1), None));
    retract_at(&pg, 5, SPO);
    assert_eq!(run(&pg, SPO, Some("4")), (Some(1), Some(5)));
    assert_eq!(run(&pg, SPO, None), (None, None));
}

/// Assert, retract, assert again.
#[test]
fn reasserted() {
    let pg = PersistentGraph::new();
    assert_at(&pg, 1, SPO);
    retract_at(&pg, 5, SPO);
    assert_at(&pg, 9, SPO);
    assert_eq!(run(&pg, SPO, None), (Some(9), None));
    for (at, expected) in [
        (0, (None, None)),
        (1, (Some(1), Some(5))),
        (3, (Some(1), Some(5))),
        (4, (Some(1), Some(5))),
        (5, (None, None)),
        (6, (None, None)),
        (8, (None, None)),
        (9, (Some(9), None)),
        (100, (Some(9), None)),
    ] {
        assert_eq!(run(&pg, SPO, Some(&at.to_string())), expected, "at {at}");
    }
}

/// An assertion and a retraction at the same time: the one written last wins.
#[test]
fn same_time() {
    // assert then retract at 7: a zero-length flip that no snapshot sees
    let pg = PersistentGraph::new();
    assert_at(&pg, 7, SPO);
    retract_at(&pg, 7, SPO);
    assert_eq!(run(&pg, SPO, Some("7")), (None, None));
    assert_eq!(run(&pg, SPO, None), (None, None));

    // a retraction and an assertion at 7 during a run keep it going
    let pg = PersistentGraph::new();
    assert_at(&pg, 1, SPO);
    retract_at(&pg, 7, SPO);
    assert_at(&pg, 7, SPO);
    assert_eq!(run(&pg, SPO, Some("7")), (Some(1), None));
    assert_eq!(run(&pg, SPO, Some("3")), (Some(1), None));
    assert_eq!(run(&pg, SPO, None), (Some(1), None));

    // an assertion and a retraction at 7 end it at 7
    let pg = PersistentGraph::new();
    assert_at(&pg, 1, SPO);
    assert_at(&pg, 7, SPO);
    retract_at(&pg, 7, SPO);
    assert_eq!(run(&pg, SPO, Some("6")), (Some(1), Some(7)));
    assert_eq!(run(&pg, SPO, Some("7")), (None, None));
}

/// A triple that was only ever retracted never holds.
#[test]
fn only_retracted() {
    let pg = PersistentGraph::new();
    retract_at(&pg, 3, SPO);
    assert_eq!(run(&pg, SPO, None), (None, None));
    assert_eq!(run(&pg, SPO, Some("5")), (None, None));
    assert_at(&pg, 6, SPO);
    assert_eq!(run(&pg, SPO, None), (Some(6), None));
    assert_eq!(run(&pg, SPO, Some("4")), (None, None));
}

/// Every form of the reference argument; invalid ones give unbound without failing.
#[test]
fn reference_forms() {
    let pg = PersistentGraph::new();
    career(&pg);
    let alice_acme = [ALICE, WORKS_FOR, ACME];
    let expected = (Some(JUN_2021), Some(JUN_2023));
    for at in [
        "raphtory:asof:2022-01-01".to_owned(),
        "<raphtory:asof:2022-01-01>".to_owned(),
        format!("raphtory:asof:{JAN_2022}"),
        "<raphtory:asof:2022-01-01T01:00:00+01:00>".to_owned(),
        format!("\"2022-01-01T00:00:00Z\"^^<{XSD}dateTime>"),
        format!("\"2022-01-01T01:00:00+01:00\"^^<{XSD}dateTime>"),
        format!("\"2022-01-01T00:00:00\"^^<{XSD}dateTime>"),
        format!("\"2022-01-01T00:00:00Z\"^^<{XSD}dateTimeStamp>"),
        JAN_2022.to_string(),
        format!("\"{JAN_2022}\"^^<{XSD}long>"),
        format!("\"0{JAN_2022}\"^^<{XSD}integer>"),
        format!("IRI(\"raphtory:asof:{JAN_2022}\")"),
    ] {
        assert_eq!(run(&pg, alice_acme, Some(&at)), expected, "{at}");
    }
    for at in [
        "\"2022-01-01\"".to_owned(),
        format!("\"{JAN_2022}\""),
        "raphtory:asof:yesterday".to_owned(),
        "<http://ex/g>".to_owned(),
        format!("\"2022-01-01\"^^<{XSD}date>"),
        format!("{JAN_2022}.0"),
        format!("{JAN_2022}e0"),
        "true".to_owned(),
        "BNODE()".to_owned(),
        "\"99999999999999999999\"^^<http://www.w3.org/2001/XMLSchema#integer>".to_owned(),
        format!("\"x\"^^<{XSD}integer>"),
    ] {
        assert_eq!(run(&pg, alice_acme, Some(&at)), (None, None), "{at}");
    }

    // xsd:int on small times
    let pg = PersistentGraph::new();
    assert_at(&pg, 1, SPO);
    retract_at(&pg, 5, SPO);
    for at in [
        "3",
        "\"3\"^^xsd:int",
        "\"03\"^^xsd:short",
        "\"3\"^^xsd:nonNegativeInteger",
    ] {
        let a = args(SPO, Some(at));
        let query = format!("{PREFIX} SELECT (raphtory:validToTime({a}) AS ?x) {{}}");
        assert_eq!(select(&pg, &query), [[int(5)]], "{at}");
    }
    // negative times
    assert_eq!(run(&pg, SPO, Some("-3")), (None, None));
}

/// The reference from `?g`, bound by `FROM NAMED` or `VALUES`; a 3-argument call inside
/// `GRAPH` uses the present of the view.
#[test]
fn graph_as_reference() {
    let pg = PersistentGraph::new();
    career(&pg);
    let expected = |g: &str| {
        let mut rows = vec![vec![
            format!("<raphtory:asof:{g}>"),
            format!("<{BOB}>"),
            int(MAR_2022),
            "UNDEF".to_owned(),
        ]];
        if g == "2023-01-01" {
            rows.push(vec![
                format!("<raphtory:asof:{g}>"),
                format!("<{ALICE}>"),
                int(JUN_2021),
                int(JUN_2023),
            ]);
        }
        rows
    };
    let binds = "BIND(raphtory:validFromTime(?who, ex:worksFor, ex:acme, ?g) AS ?since)
                 BIND(raphtory:validToTime(?who, ex:worksFor, ex:acme, ?g) AS ?until)";
    let rows = select(
        &pg,
        &format!(
            "{PREFIX} SELECT ?g ?who ?since ?until
            FROM NAMED raphtory:asof:2023-01-01 FROM NAMED raphtory:asof:2024-01-01 {{
                GRAPH ?g {{ ?who ex:worksFor ex:acme }}
                {binds}
            }}"
        ),
    );
    let mut all = expected("2023-01-01");
    all.extend(expected("2024-01-01"));
    all.sort();
    assert_eq!(rows, all);

    let rows = select(
        &pg,
        &format!(
            "{PREFIX} SELECT ?g ?who ?since ?until {{
                VALUES ?g {{ raphtory:asof:2023-01-01 }}
                GRAPH ?g {{ ?who ex:worksFor ex:acme }}
                {binds}
            }}"
        ),
    );
    let mut one = expected("2023-01-01");
    one.sort();
    assert_eq!(rows, one);

    // inside GRAPH, a call without the reference is about the present: alice no longer works
    // for acme
    let rows = select(
        &pg,
        &format!(
            "{PREFIX} SELECT ?who ?since {{
                GRAPH raphtory:asof:2023-01-01 {{
                    ?who ex:worksFor ex:acme
                    BIND(raphtory:validFromTime(?who, ex:worksFor, ex:acme) AS ?since)
                }}
            }}"
        ),
    );
    assert_eq!(
        rows,
        [
            [format!("<{ALICE}>"), "UNDEF".to_owned()],
            [format!("<{BOB}>"), int(MAR_2022)]
        ]
    );
}

/// Time views: the reference must be in the window, history before the window counts, and
/// events after the end of the view do not.
#[test]
fn time_views() {
    let pg = PersistentGraph::new();
    assert_at(&pg, 1, SPO);
    retract_at(&pg, 5, SPO);
    assert_eq!(run(&pg, SPO, Some("2")), (Some(1), Some(5)));
    // as of 3 the retraction has not happened yet: the run is open
    let as_of_3 = pg.snapshot_at(3);
    assert_eq!(run(&as_of_3, SPO, None), (Some(1), None));
    assert_eq!(run(&as_of_3, SPO, Some("3")), (Some(1), None));
    assert_eq!(run(&as_of_3, SPO, Some("2")), (None, None));
    assert_eq!(run(&as_of_3, SPO, Some("4")), (None, None));
    // the same answers as a graph with only the events up to 3
    let until_3 = PersistentGraph::new();
    assert_at(&until_3, 1, SPO);
    assert_eq!(run(&until_3, SPO, None), (Some(1), None));
    assert_eq!(run(&until_3.snapshot_at(3), SPO, None), (Some(1), None));

    assert_eq!(run(&pg.window(0, 5), SPO, None), (Some(1), None));
    assert_eq!(run(&pg.window(0, 5), SPO, Some("4")), (Some(1), None));
    // a reference at or after the end of the window is outside it
    assert_eq!(run(&pg.window(0, 5), SPO, Some("5")), (None, None));
    assert_eq!(run(&pg.window(0, 5), SPO, Some("9")), (None, None));
    assert_eq!(run(&as_of_3, SPO, Some("9")), (None, None));
    assert_eq!(run(&pg.before(5), SPO, None), (Some(1), None));
    assert_eq!(run(&pg.window(0, 6), SPO, None), (None, None));
    assert_eq!(run(&pg.window(2, 5), SPO, Some("1")), (None, None));
    assert_eq!(run(&pg.window(2, 5), SPO, Some("2")), (Some(1), None));
    assert_eq!(run(&pg.window(2, 6), SPO, Some("4")), (Some(1), Some(5)));
    assert_eq!(run(&pg.after(2), SPO, Some("4")), (Some(1), Some(5)));
    assert_eq!(run(&pg.after(2), SPO, None), (None, None));

    // the window does not clip the history: the run started before it
    let pg = PersistentGraph::new();
    for t in [1, 3, 7, 9] {
        assert_at(&pg, t, SPO);
    }
    let window = pg.window(4, 10);
    let e = window.valid_layers(P).edge(S, O).unwrap();
    assert_eq!(e.history().t().collect(), [7, 9]);
    assert_eq!(run(&window, SPO, None), (Some(1), None));
    assert_eq!(run(&window, SPO, Some("8")), (Some(1), None));
    assert_eq!(run(&window, SPO, Some("2")), (None, None));
    assert_eq!(run(&pg.snapshot_latest(), SPO, None), (Some(1), None));
}

/// Views that hide the triple give unbound.
#[test]
fn hidden_by_the_view() {
    let pg = PersistentGraph::new();
    assert_at(&pg, 1, [S, P, O]);
    assert_at(&pg, 2, [S, Q, O]);
    assert_eq!(run(&pg, [S, P, O], None), (Some(1), None));
    assert_eq!(run(&pg, [S, Q, O], None), (Some(2), None));
    let only_q = pg.valid_layers(Q);
    assert_eq!(run(&only_q, [S, P, O], None), (None, None));
    assert_eq!(run(&only_q, [S, P, O], Some("5")), (None, None));
    assert_eq!(run(&only_q, [S, Q, O], None), (Some(2), None));
    let without_p = pg.exclude_valid_layers(P);
    assert_eq!(run(&without_p, [S, P, O], None), (None, None));
    assert_eq!(run(&without_p, [S, Q, O], None), (Some(2), None));
    let without_s = pg.subgraph([O]);
    assert_eq!(run(&without_s, [S, P, O], None), (None, None));
    assert_eq!(run(&without_s, [S, P, O], Some("5")), (None, None));
    let without_o = pg.exclude_nodes([O]);
    assert_eq!(run(&without_o, [S, P, O], None), (None, None));
    // unknown terms and layers
    assert_eq!(run(&pg, [S, "http://ex/nope", O], None), (None, None));
    assert_eq!(run(&pg, ["http://ex/nope", P, O], None), (None, None));
    assert_eq!(run(&pg, [S, P, "http://ex/nope"], None), (None, None));
    // the private layer of the graph is not a predicate
    assert_eq!(
        run(&pg, [S, "raphtory:_static_graph", O], None),
        (None, None)
    );
}

/// Event graphs: the run starts at the first assertion and never ends.
#[test]
fn event_graphs() {
    let g = Graph::new();
    assert_at(&g, 1, SPO);
    retract_at(&g, 5, SPO);
    assert_at(&g, 9, SPO);
    assert_eq!(run(&g, SPO, None), (Some(1), None));
    assert_eq!(run(&g, SPO, Some("3")), (Some(1), None));
    assert_eq!(run(&g, SPO, Some("6")), (Some(1), None));
    assert_eq!(run(&g, SPO, Some("0")), (None, None));
    assert_eq!(run(&g.snapshot_at(3), SPO, None), (Some(1), None));
    // a reference at or after the end of the window answers as of its end, like
    // `GRAPH raphtory:asof:T` on the view (on a persistent graph it gives unbound)
    assert_eq!(run(&g.snapshot_at(3), SPO, Some("9")), (Some(1), None));
    assert_eq!(run(&g.window(0, 5), SPO, Some("5")), (Some(1), None));
    assert_eq!(run(&g.window(0, 5), SPO, Some("9")), (Some(1), None));
    assert_eq!(run(&g.before(5), SPO, Some("100")), (Some(1), None));
    let [s, p, o] = SPO.map(|t| format!("<{t}>"));
    let as_of_9 = format!("ASK {{ GRAPH raphtory:asof:9 {{ {s} {p} {o} }} }}");
    assert!(ask(&g.window(0, 5), &as_of_9));
    assert!(!ask(&g.persistent_graph().window(0, 5), &as_of_9));
    // no assertion in the window: not visible
    assert_eq!(run(&g.window(3, 6), SPO, None), (None, None));
    // the run started before the window
    assert_eq!(run(&g.window(8, 10), SPO, None), (Some(1), None));
    assert_eq!(run(&g.window(8, 10), SPO, Some("7")), (None, None));
    assert_eq!(run_dt(&g, SPO, Some("6")).1, None);
    // persistent runs
    let pg = g.persistent_graph();
    assert_eq!(run(&pg, SPO, None), (Some(9), None));
    assert_eq!(run(&pg, SPO, Some("3")), (Some(1), Some(5)));
    assert_eq!(run(&pg, SPO, Some("6")), (None, None));
    // and back
    assert_eq!(run(&pg.event_graph(), SPO, Some("6")), (Some(1), None));
}

/// Literal objects are matched by their canonical form.
#[test]
fn literal_objects() {
    let pg = PersistentGraph::new();
    let load = |t: i64, doc: &str| {
        pg.load_rdf(t, doc.as_bytes(), RdfFormat::Turtle, None)
            .unwrap()
    };
    let integer =
        "<http://ex/s> <http://ex/age> \"042\"^^<http://www.w3.org/2001/XMLSchema#integer> .";
    load(1, integer);
    load(
        3,
        "<http://ex/s> <http://ex/age> \"42\"^^<http://www.w3.org/2001/XMLSchema#int> .",
    );
    pg.retract_rdf(5, integer.as_bytes(), RdfFormat::Turtle, None)
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
    let call = |view: &PersistentGraph, p: &str, o: &str, at: Option<&str>| {
        let at = at.map(|at| format!(", {at}")).unwrap_or_default();
        let query = format!(
            "{PREFIX} SELECT (raphtory:validFromTime(ex:s, ex:{p}, {o}{at}) AS ?f) \
             (raphtory:validToTime(ex:s, ex:{p}, {o}{at}) AS ?t) {{}}"
        );
        let rows = select(view, &query);
        let get = |v: &String| (v != "UNDEF").then(|| parse_int(v));
        (get(&rows[0][0]), get(&rows[0][1]))
    };

    // now only "42"^^xsd:int is visible, so every spelling of 42 finds it
    for o in [
        "42",
        "\"042\"^^xsd:integer",
        "\"42\"^^xsd:int",
        "\"+42\"^^xsd:long",
    ] {
        assert_eq!(call(&pg, "age", o, None), (Some(3), None), "{o}");
    }
    // at 2 only "042"^^xsd:integer was visible
    for o in ["42", "\"042\"^^xsd:integer", "\"42\"^^xsd:int"] {
        assert_eq!(call(&pg, "age", o, Some("2")), (Some(1), Some(5)), "{o}");
    }
    // at 4 both were: ambiguous
    for o in ["42", "\"042\"^^xsd:integer", "\"42\"^^xsd:int"] {
        assert_eq!(call(&pg, "age", o, Some("4")), (None, None), "{o}");
    }
    assert_eq!(call(&pg, "age", "43", None), (None, None));
    assert_eq!(call(&pg, "age", "\"42\"", None), (None, None));

    // bound by a pattern
    let since = |view: DynamicGraph| {
        select(
            &view,
            &format!(
                "{PREFIX} SELECT ?o ?f {{ ex:s ex:age ?o BIND(raphtory:validFromTime(ex:s, ex:age, ?o) AS ?f) }}"
            ),
        )
    };
    assert_eq!(
        since(pg.clone().into_dynamic()),
        [[format!("\"42\"^^<{XSD}int>"), int(3)]]
    );
    assert_eq!(
        since(pg.snapshot_at(4).into_dynamic()),
        [
            [format!("\"042\"^^<{XSD}integer>"), "UNDEF".to_owned()],
            [format!("\"42\"^^<{XSD}int>"), "UNDEF".to_owned()],
        ]
    );

    // other values
    for (p, o) in [
        ("n", "5"),
        ("n", "\"5\"^^xsd:int"),
        ("n", "\"05\"^^xsd:integer"),
        ("flag", "true"),
        ("flag", "\"1\"^^xsd:boolean"),
        ("when", "\"2024-01-02T03:04:05+02:00\"^^xsd:dateTime"),
        ("when", "\"2024-01-02T03:04:05.000+02:00\"^^xsd:dateTime"),
        // looked up as they are
        ("name", "\"x\"@en"),
        ("name", "\"x\"@EN"),
        ("custom", "\"x\"^^<http://ex/dt>"),
        ("bad", "\"abc\"^^xsd:integer"),
        ("plain", "\"x\""),
        ("plain", "\"x\"^^xsd:string"),
    ] {
        assert_eq!(call(&pg, p, o, None), (Some(1), None), "{p} {o}");
    }
    assert_eq!(call(&pg, "name", "\"x\"@fr", None), (None, None));
    assert_eq!(
        call(&pg, "custom", "\"y\"^^<http://ex/dt>", None),
        (None, None)
    );
    assert_eq!(call(&pg, "flag", "false", None), (None, None));
    // the canonical form of a date-time keeps its timezone, and that of a number its datatype:
    // equal values that FILTER compares as equal are not matched
    let utc = "\"2024-01-02T01:04:05Z\"^^xsd:dateTime";
    assert_eq!(call(&pg, "when", utc, None), (None, None));
    assert!(ask(
        &pg,
        &format!("{PREFIX} ASK {{ ex:s ex:when ?o FILTER(?o = {utc}) }}")
    ));
    for o in ["1.0", "\"1.00\"^^xsd:decimal", "\"01\"^^xsd:decimal"] {
        assert_eq!(call(&pg, "dec", o, None), (Some(1), None), "{o}");
    }
    assert_eq!(call(&pg, "dec", "1", None), (None, None));
    assert_eq!(call(&pg, "dec", "1.0e0", None), (None, None));
    assert!(ask(
        &pg,
        &format!("{PREFIX} ASK {{ ex:s ex:dec ?o FILTER(?o = 1) }}")
    ));
    // every literal object, through a pattern
    let rows = select(
        &pg,
        &format!(
            "{PREFIX} SELECT ?p ?f {{ ex:s ?p ?o BIND(raphtory:validFromTime(ex:s, ?p, ?o) AS ?f) }}"
        ),
    );
    assert_eq!(rows.len(), 9);
    assert!(rows.iter().all(|row| row[1] != "UNDEF"), "{rows:?}");
}

/// Graphs not loaded from RDF, u64 ids and locked views.
#[test]
fn other_graphs() {
    let g = Graph::new();
    g.add_edge(3, "Alice", "Bob Smith", NO_PROPS, Some("knows"))
        .unwrap();
    g.add_edge(4, "Alice", "Bob Smith", NO_PROPS, None).unwrap();
    let expr = |f: &str, layer: &str| {
        format!("raphtory:{f}(raphtory:Alice, {layer}, <raphtory:Bob%20Smith>)")
    };
    assert_eq!(
        value(&g, &expr("validFromTime", "raphtory:knows")),
        Some(int(3))
    );
    assert_eq!(
        value(&g, &expr("validFromTime", "raphtory:_default")),
        Some(int(4))
    );
    assert_eq!(
        value(&g, &expr("validFrom", "raphtory:knows")),
        Some(date_time("1970-01-01T00:00:00.003Z"))
    );

    let ids = Graph::new();
    ids.add_edge(3, 1, 2, NO_PROPS, Some("knows")).unwrap();
    assert_eq!(
        value(
            &ids,
            "raphtory:validFromTime(raphtory:1, raphtory:knows, raphtory:2)"
        ),
        Some(int(3))
    );
    assert_eq!(
        value(
            &ids,
            "raphtory:validFromTime(raphtory:x, raphtory:knows, raphtory:2)"
        ),
        None
    );

    let pg = PersistentGraph::new();
    assert_at(&pg, 1, SPO);
    retract_at(&pg, 5, SPO);
    let (now, at_3) = within_60s(move || {
        let locked = pg.read_only();
        (run(&locked, SPO, None), run(&locked, SPO, Some("3")))
    });
    assert_eq!(now, (None, None));
    assert_eq!(at_3, (Some(1), Some(5)));
}

/// Literal subjects and literal-named layers give unbound: the canonical form SPARQL passes
/// does not identify the node or layer.
#[test]
fn literal_subjects_and_layers() {
    let integer = |lexical: &str| format!("\"{lexical}\"^^<{XSD}integer>");
    let undef = || "UNDEF".to_owned();

    // two subjects with the same canonical form, and a string literal subject
    let g = Graph::new();
    g.add_edge(1, integer("042").as_str(), "o", NO_PROPS, Some("p"))
        .unwrap();
    g.add_edge(7, integer("42").as_str(), "o", NO_PROPS, Some("p"))
        .unwrap();
    g.add_edge(3, "\"x\"", "o", NO_PROPS, Some("p")).unwrap();
    let rows = select(
        &g,
        "SELECT ?s ?f { ?s raphtory:p raphtory:o \
         BIND(raphtory:validFromTime(?s, raphtory:p, raphtory:o) AS ?f) }",
    );
    assert_eq!(
        rows,
        [
            [integer("042"), undef()],
            [integer("42"), undef()],
            ["\"x\"".to_owned(), int(3)],
        ]
    );
    for s in [
        "42".to_owned(),
        integer("042"),
        format!("\"42\"^^<{XSD}int>"),
    ] {
        let expr = format!("raphtory:validFromTime({s}, raphtory:p, raphtory:o)");
        assert_eq!(value(&g, &expr), None, "{s}");
    }
    assert_eq!(
        value(&g, "raphtory:validFromTime(\"x\", raphtory:p, raphtory:o)"),
        Some(int(3))
    );

    // two layers with the same canonical form, and a string literal layer
    let g = Graph::new();
    g.add_edge(1, "a", "b", NO_PROPS, Some(integer("01").as_str()))
        .unwrap();
    g.add_edge(5, "a", "b", NO_PROPS, Some(integer("1").as_str()))
        .unwrap();
    g.add_edge(3, "a", "b", NO_PROPS, Some("\"x\"")).unwrap();
    let rows = select(
        &g,
        "SELECT ?p ?f { raphtory:a ?p raphtory:b \
         BIND(raphtory:validFromTime(raphtory:a, ?p, raphtory:b) AS ?f) }",
    );
    assert_eq!(
        rows,
        [
            [integer("01"), undef()],
            [integer("1"), undef()],
            ["\"x\"".to_owned(), int(3)],
        ]
    );
    assert_eq!(
        value(&g, "raphtory:validFromTime(raphtory:a, 1, raphtory:b)"),
        None
    );
}

/// Wrong calls give unbound; an unknown function fails the query.
#[test]
fn wrong_calls() {
    let pg = PersistentGraph::new();
    assert_at(&pg, 1, SPO);
    let [s, p, o] = SPO.map(|t| format!("<{t}>"));
    for call in [
        "raphtory:validFromTime()".to_owned(),
        format!("raphtory:validFromTime({s}, {p})"),
        format!("raphtory:validFromTime({s}, {p}, {o}, 1, 2)"),
        format!("raphtory:validFromTime(\"s\", {p}, {o})"),
        format!("raphtory:validFromTime({s}, \"p\", {o})"),
        format!("raphtory:validFromTime({s}, {p}, \"o\")"),
        format!("raphtory:validFromTime({o}, {p}, {s})"),
        format!("raphtory:validFromTime({s}, {p}, {o}, ?nope)"),
        format!("raphtory:validFromTime(?nope, {p}, {o})"),
        format!("raphtory:validFrom(BNODE(), {p}, {o})"),
        format!("raphtory:validFrom({s}, {p}, {o}, {o})"),
    ] {
        assert_eq!(value(&pg, &call), None, "{call}");
    }
    assert_eq!(
        value(&pg, &format!("raphtory:validFromTime({s}, {p}, {o})")),
        Some(int(1))
    );
    let error = pg
        .sparql(&format!(
            "SELECT (raphtory:validFromm({s}, {p}, {o}) AS ?x) {{}}"
        ))
        .unwrap_err();
    assert!(
        matches!(error, GraphError::Rdf(RdfError::SparqlEvaluation(_))),
        "{error}"
    );
}

/// `xsd:dateTime` results only cover the years 1 to 9999.
#[test]
fn year_range() {
    for (t, expected) in [
        (-62_167_219_200_000, None),                         // 0000-01-01
        (-62_135_596_800_001, None),                         // 0000-12-31T23:59:59.999
        (-62_135_596_800_000, Some("0001-01-01T00:00:00Z")), // year 1
        (0, Some("1970-01-01T00:00:00Z")),
        (-1, Some("1969-12-31T23:59:59.999Z")),
        (1_704_067_200_123, Some("2024-01-01T00:00:00.123Z")),
        (253_402_300_799_999, Some("9999-12-31T23:59:59.999Z")), // year 9999
        (253_402_300_800_000, None),                             // 10000-01-01
        (i64::MAX, None),
        (i64::MIN, None),
    ] {
        let pg = PersistentGraph::new();
        assert_at(&pg, t, SPO);
        let a = args(SPO, None);
        assert_eq!(
            value(&pg, &format!("raphtory:validFrom({a})")),
            expected.map(date_time),
            "{t}"
        );
        assert_eq!(
            value(&pg, &format!("raphtory:validFromTime({a})")),
            Some(int(t)),
            "{t}"
        );
    }
}

/// Use in `FILTER` and `ORDER BY`.
#[test]
fn filter_and_order_by() {
    let pg = PersistentGraph::new();
    career(&pg);
    let rows = select(
        &pg,
        &format!(
            "{PREFIX} SELECT ?who ?org {{
                ?who ex:worksFor ?org
                FILTER(raphtory:validFrom(?who, ex:worksFor, ?org) >= \"2022-01-01T00:00:00Z\"^^xsd:dateTime)
            }}"
        ),
    );
    assert_eq!(
        rows,
        [
            [format!("<{ALICE}>"), format!("<{INITECH}>")],
            [format!("<{BOB}>"), format!("<{ACME}>")]
        ]
    );
    // without ORDER BY the order is alice, bob, so ASC must reorder (an unbound key would not)
    for (dir, expected) in [("ASC", [BOB, ALICE]), ("DESC", [ALICE, BOB])] {
        let SparqlResults::Solutions { rows, .. } = pg
            .sparql(&format!(
                "{PREFIX} SELECT ?who {{ ?who ex:worksFor ?org }} \
                 ORDER BY {dir}(raphtory:validFrom(?who, ex:worksFor, ?org))"
            ))
            .unwrap()
        else {
            unreachable!()
        };
        let order: Vec<String> = rows
            .iter()
            .map(|row| row[0].as_ref().unwrap().to_string())
            .collect();
        assert_eq!(order, expected.map(|who| format!("<{who}>")), "{dir}");
    }
    // who has worked somewhere for more than a year as of 2023-01-01?
    let rows = select(
        &pg,
        &format!(
            "{PREFIX} SELECT ?who {{
                GRAPH raphtory:asof:2023-01-01 {{ ?who ex:worksFor ?org }}
                FILTER(raphtory:validFromTime(?who, ex:worksFor, ?org, raphtory:asof:2023-01-01) < {})
            }}",
            JAN_2023 - 365 * 24 * 3600 * 1000
        ),
    );
    assert_eq!(rows, [[format!("<{ALICE}>")]]);
}

/// The functions can be turned off.
#[test]
fn options() {
    assert!(SparqlOptions::default().temporal_functions);
    let pg = PersistentGraph::new();
    assert_at(&pg, 1, SPO);
    let query = format!(
        "SELECT (raphtory:validFromTime({}) AS ?x) {{}}",
        args(SPO, None)
    );
    let mut out = Vec::new();
    let options = SparqlOptions::default().with_temporal_functions(false);
    assert!(!options.temporal_functions);
    let error = pg
        .sparql_to_writer_with(&query, &mut out, QueryResultsFormat::Csv, &options)
        .unwrap_err();
    assert!(
        matches!(error, GraphError::Rdf(RdfError::SparqlEvaluation(_))),
        "{error}"
    );
    assert!(out.is_empty());
    let options = options.with_temporal_functions(true);
    pg.sparql_to_writer_with(&query, &mut out, QueryResultsFormat::Csv, &options)
        .unwrap();
    assert_eq!(String::from_utf8(out).unwrap(), "x\r\n1\r\n");

    // the plain evaluator has none, and `with_temporal_functions` adds them
    let ask = format!(
        "ASK {{ FILTER(raphtory:validFromTime({}) = 1) }}",
        args(SPO, None)
    );
    let run_with = |evaluator: SparqlEvaluator| {
        evaluator
            .parse_query(&ask)
            .unwrap()
            .on_queryable_dataset(RaphtoryDataset::new(pg.clone()))
            .execute()
            .map(|results| matches!(results, QueryResults::Boolean(true)))
    };
    assert!(run_with(evaluator()).is_err());
    assert!(run_with(with_temporal_functions(evaluator(), pg.clone())).unwrap());
}

/// Serialized results have the functions too.
#[test]
fn serialized_results() {
    let pg = PersistentGraph::new();
    career(&pg);
    let query = format!(
        "{PREFIX} SELECT ?who ?since {{
            ?who ex:worksFor ex:acme BIND(raphtory:validFrom(?who, ex:worksFor, ex:acme) AS ?since)
        }}"
    );
    let mut json = Vec::new();
    pg.sparql_to_writer(&query, &mut json, QueryResultsFormat::Json)
        .unwrap();
    assert_eq!(
        String::from_utf8(json).unwrap(),
        format!(
            r#"{{"head":{{"vars":["who","since"]}},"results":{{"bindings":[{{"who":{{"type":"uri","value":"{BOB}"}},"since":{{"type":"literal","value":"2022-03-01T00:00:00Z","datatype":"{XSD}dateTime"}}}}]}}}}"#
        )
    );
    // the collected results write the same bytes
    let mut collected = Vec::new();
    pg.sparql(&query)
        .unwrap()
        .write(&mut collected, QueryResultsFormat::Json)
        .unwrap();
    let mut streamed = Vec::new();
    pg.sparql_to_writer(&query, &mut streamed, QueryResultsFormat::Json)
        .unwrap();
    assert_eq!(collected, streamed);
    // CONSTRUCT
    let mut nt = Vec::new();
    pg.sparql_to_writer(
        &format!(
            "{PREFIX} CONSTRUCT {{ ?who ex:since ?since }} WHERE {{
                ?who ex:worksFor ex:acme BIND(raphtory:validFromTime(?who, ex:worksFor, ex:acme) AS ?since)
            }}"
        ),
        &mut nt,
        RdfFormat::NTriples,
    )
    .unwrap();
    assert_eq!(
        String::from_utf8(nt).unwrap(),
        format!("<{BOB}> <http://ex/since> {} .\n", int(MAR_2022))
    );
}

// Proptest oracle against a brute-force scan of snapshots.

/// The graph of a view's type with only the events of `log` before `end`, written in log order
/// (so events at the same time keep their order).
fn root_until(log: &[Event], persistent: bool, end: Option<i64>) -> DynamicGraph {
    let g = Graph::new();
    for e in log.iter().filter(|e| end.is_none_or(|end| e.t < end)) {
        if e.assert {
            assert_at(&g, e.t, LOG_TRIPLES[e.triple]);
        } else {
            retract_at(&g, e.t, LOG_TRIPLES[e.triple]);
        }
    }
    if persistent {
        g.persistent_graph().into_dynamic()
    } else {
        g.into_dynamic()
    }
}

/// The times scanned by the oracle. Every event is in `0..8`, so the state at `12` is the state
/// at infinity.
const SCAN: std::ops::RangeInclusive<i64> = -2..=12;

/// The expected `(from, to)` of each triple of the log on `view`, for each reference (`None`:
/// without one), from SPARQL visibility in `snapshot_at(x)` for every `x`.
fn oracle(view: &DynamicGraph, log: &[Event], persistent: bool, refs: &[Option<i64>]) -> Runs {
    let end = view.end().map(|end| end.t());
    let root = root_until(log, persistent, end);
    let holds: Vec<BTreeSet<[String; 3]>> = SCAN.map(|x| triples(&root.snapshot_at(x))).collect();
    let holds_at = |i: usize, x: i64| {
        let x = x.clamp(*SCAN.start(), *SCAN.end());
        holds[(x - SCAN.start()) as usize].contains(&nt3(i))
    };
    let mut expected = BTreeMap::new();
    for &r in refs {
        let scope = match r {
            None => triples(view),
            Some(r) => triples(&view.snapshot_at(r)),
        };
        // the reference time, capped at the last instant before the end of the view
        let t_ref = match (r, end) {
            (Some(r), Some(end)) => r.min(end - 1),
            (Some(r), None) => r,
            (None, Some(end)) => end - 1,
            (None, None) => *SCAN.end(),
        };
        for i in 0..LOG_TRIPLES.len() {
            let visible = scope.contains(&nt3(i));
            let result = if visible {
                assert!(
                    holds_at(i, t_ref),
                    "visible in the scope but not in R at {t_ref}: {log:?}"
                );
                let mut from = t_ref;
                while holds_at(i, from - 1) {
                    from -= 1;
                }
                let to = (t_ref + 1..=*SCAN.end()).find(|x| !holds_at(i, *x));
                (Some(from), to)
            } else {
                (None, None)
            };
            expected.insert((i, r), result);
        }
    }
    expected
}

/// `(from, to)` by triple of the log and reference.
type Runs = BTreeMap<(usize, Option<i64>), (Option<i64>, Option<i64>)>;

fn nt3(i: usize) -> [String; 3] {
    LOG_TRIPLES[i].map(|t| format!("<{t}>"))
}

/// The `(from, to)` of each triple of the log on `view` for each reference, from the functions.
fn actual<G: RdfViewOps>(view: &G, refs: &[Option<i64>]) -> Runs {
    let mut actual = BTreeMap::new();
    let get = |v: &String| (v != "UNDEF").then(|| parse_int(v));
    for (i, spo) in LOG_TRIPLES.into_iter().enumerate() {
        let a = args(spo, None);
        let values: Vec<String> = refs.iter().flatten().map(|r| r.to_string()).collect();
        let rows = select(
            view,
            &format!(
                "SELECT ?r ?f ?t {{
                    VALUES ?r {{ {} }}
                    BIND(raphtory:validFromTime({a}, ?r) AS ?f)
                    BIND(raphtory:validToTime({a}, ?r) AS ?t)
                }}",
                values.join(" ")
            ),
        );
        for row in rows {
            actual.insert((i, Some(parse_int(&row[0]))), (get(&row[1]), get(&row[2])));
        }
        if refs.contains(&None) {
            actual.insert((i, None), run(view, spo, None));
        }
    }
    actual
}

fn short_log() -> impl Strategy<Value = Vec<Event>> {
    prop::collection::vec(
        (0..8i64, any::<bool>(), 0..LOG_TRIPLES.len()).prop_map(|(t, assert, triple)| Event {
            t,
            assert,
            triple,
        }),
        0..=12,
    )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// The functions agree with a brute-force scan of the snapshots of the graph.
    #[test]
    fn functions_match_snapshot_scans(
        log in short_log(),
        at in -1..10i64,
        start in 0..9i64,
        len in 0..6i64,
    ) {
        let pg = PersistentGraph::new();
        let g = Graph::new();
        for e in &log {
            if e.assert {
                assert_at(&pg, e.t, LOG_TRIPLES[e.triple]);
                assert_at(&g, e.t, LOG_TRIPLES[e.triple]);
            } else {
                retract_at(&pg, e.t, LOG_TRIPLES[e.triple]);
                retract_at(&g, e.t, LOG_TRIPLES[e.triple]);
            }
        }
        let refs: Vec<Option<i64>> = std::iter::once(None).chain((-1..=10).map(Some)).collect();
        let end = start + len;
        let views: Vec<(&str, DynamicGraph, bool)> = vec![
            ("pg", pg.clone().into_dynamic(), true),
            ("pg.snapshot_at", pg.snapshot_at(at).into_dynamic(), true),
            ("pg.window", pg.window(start, end).into_dynamic(), true),
            ("g", g.clone().into_dynamic(), false),
            ("g.snapshot_at", g.snapshot_at(at).into_dynamic(), false),
            ("g.window", g.window(start, end).into_dynamic(), false),
            ("g.persistent_graph", g.persistent_graph().into_dynamic(), true),
        ];
        for (name, view, persistent) in views {
            prop_assert_eq!(
                actual(&view, &refs),
                oracle(&view, &log, persistent, &refs),
                "{} (at {}, window {}..{})", name, at, start, end
            );
        }
    }
}
