//! The temporal functions `raphtory:validFrom`, `validTo`, `validFromTime` and `validToTime`.
use super::{
    layer_of, select,
    temporal::{assert_at, retract_at, triples, Event, LOG_TRIPLES, O, P, Q, S, SPO},
    within_60s,
};
use crate::{
    db::api::view::{IntoDynamic, StaticGraphViewOps},
    prelude::*,
    rdf::{RdfFormat, VALID_FROM, VALID_FROM_TIME, VALID_TO, VALID_TO_TIME},
};
use proptest::prelude::*;
use raphtory_api::core::storage::timeindex::AsTime;
use std::collections::{BTreeMap, BTreeSet};

const XSD: &str = "http://www.w3.org/2001/XMLSchema#";

const JUN_2021: i64 = 1_622_505_600_000;
const MAR_2022: i64 = 1_646_092_800_000;
const JAN_2022: i64 = 1_640_995_200_000;
const JUN_2023: i64 = 1_685_577_600_000;
const JAN_2023: i64 = 1_672_531_200_000;
const JAN_2024: i64 = 1_704_067_200_000;

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
fn value<G: StaticGraphViewOps + IntoDynamic>(view: &RdfView<G>, expr: &str) -> Option<String> {
    let rows = select(view, &format!("SELECT ({expr} AS ?x) {{}}"));
    assert_eq!(rows.len(), 1, "{expr}");
    Some(rows[0][0].clone()).filter(|v| v != "UNDEF")
}

/// `(validFromTime, validToTime)` of a triple.
fn run<G: StaticGraphViewOps + IntoDynamic>(
    view: &RdfView<G>,
    spo: [&str; 3],
    at: Option<&str>,
) -> (Option<i64>, Option<i64>) {
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
fn run_dt<G: StaticGraphViewOps + IntoDynamic>(
    view: &RdfView<G>,
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
fn career(g: &RdfGraph) {
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
    let rg = RdfGraph::new();
    career(&rg);
    let alice_acme = [ALICE, WORKS_FOR, ACME];
    let bob_acme = [BOB, WORKS_FOR, ACME];
    let alice_initech = [ALICE, WORKS_FOR, INITECH];

    // now: alice no longer works for acme, bob still does
    assert_eq!(run(&rg, alice_acme, None), (None, None));
    assert_eq!(run(&rg, bob_acme, None), (Some(MAR_2022), None));
    assert_eq!(run(&rg, alice_initech, None), (Some(JUN_2023), None));
    assert_eq!(
        run_dt(&rg, bob_acme, None),
        (Some(date_time("2022-03-01T00:00:00Z")), None)
    );

    // at the start of 2023 alice worked for acme, until June
    let at = Some("raphtory:asof:2023-01-01");
    assert_eq!(run(&rg, alice_acme, at), (Some(JUN_2021), Some(JUN_2023)));
    assert_eq!(
        run_dt(&rg, alice_acme, at),
        (
            Some(date_time("2021-06-01T00:00:00Z")),
            Some(date_time("2023-06-01T00:00:00Z"))
        )
    );
    assert_eq!(run(&rg, alice_initech, at), (None, None));

    // since when?
    let rows = select(
        &rg.valid(),
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
        assert_eq!(value(&rg, &expr), expected, "{name}");
    }

    // the reference from `?g`
    let rows = select(
        &rg,
        &format!(
            "{PREFIX} SELECT ?g ?who ?since ?until
            FROM NAMED raphtory:asof:2023-01-01 FROM NAMED raphtory:asof:2024-01-01 {{
                GRAPH ?g {{ ?who ex:worksFor ex:acme }}
                BIND(raphtory:validFromTime(?who, ex:worksFor, ex:acme, ?g) AS ?since)
                BIND(raphtory:validToTime(?who, ex:worksFor, ex:acme, ?g) AS ?until)
            }}"
        ),
    );
    let as_of = |date: &str| format!("<raphtory:asof:{date}>");
    assert_eq!(
        rows,
        [
            [
                as_of("2023-01-01"),
                format!("<{ALICE}>"),
                int(JUN_2021),
                int(JUN_2023)
            ],
            [
                as_of("2023-01-01"),
                format!("<{BOB}>"),
                int(MAR_2022),
                "UNDEF".to_owned()
            ],
            [
                as_of("2024-01-01"),
                format!("<{BOB}>"),
                int(MAR_2022),
                "UNDEF".to_owned()
            ],
        ]
    );
}

/// Every form of the reference argument; an invalid one gives unbound without failing.
#[test]
fn reference_forms() {
    let rg = RdfGraph::new();
    career(&rg);
    let alice_acme = [ALICE, WORKS_FOR, ACME];
    let expected = (Some(JUN_2021), Some(JUN_2023));
    for at in [
        "raphtory:asof:2022-01-01".to_owned(),
        JAN_2022.to_string(),
        format!("\"2022-01-01T01:00:00+01:00\"^^<{XSD}dateTime>"),
        format!("\"2022-01-01T00:00:00\"^^<{XSD}dateTime>"),
    ] {
        assert_eq!(run(&rg, alice_acme, Some(&at)), expected, "{at}");
    }
    assert_eq!(
        run(&rg, alice_acme, Some("raphtory:asof:yesterday")),
        (None, None)
    );
}

/// SPARQL sees the triples of the view, and `validFrom` the history behind them.
#[test]
fn view_chains() {
    let rg = RdfGraph::new();
    career(&rg);
    let query = format!(
        "{PREFIX} SELECT ?who ?org ?since {{
            ?who ex:worksFor ?org
            BIND(raphtory:validFrom(?who, ex:worksFor, ?org) AS ?since)
        }}"
    );
    let row = |who: &str, org: &str, since: &str| {
        vec![
            format!("<{who}>"),
            format!("<{org}>"),
            date_time(&format!("{since}T00:00:00Z")),
        ]
    };
    assert_eq!(
        select(&rg.window(JAN_2023, JAN_2024).valid(), &query),
        [
            row(ALICE, INITECH, "2023-06-01"),
            row(BOB, ACME, "2022-03-01")
        ]
    );
    assert_eq!(
        select(&rg.at(JAN_2023), &query),
        [row(ALICE, ACME, "2021-06-01"), row(BOB, ACME, "2022-03-01")]
    );
    // alice left acme inside the window, so the triple is visible but no longer holds
    assert_eq!(
        select(&rg.window(JAN_2023, JAN_2024), &query),
        [
            vec![
                format!("<{ALICE}>"),
                format!("<{ACME}>"),
                "UNDEF".to_owned()
            ],
            row(ALICE, INITECH, "2023-06-01"),
            row(BOB, ACME, "2022-03-01")
        ]
    );
}

/// Views that hide the triple give unbound.
#[test]
fn hidden_by_the_view() {
    let rg = RdfGraph::new();
    assert_at(&rg, 1, [S, P, O]);
    assert_at(&rg, 2, [S, Q, O]);
    assert_eq!(run(&rg, [S, P, O], None), (Some(1), None));
    assert_eq!(run(&rg, [S, Q, O], None), (Some(2), None));
    let only_q = rg.valid_layers(layer_of(Q));
    assert_eq!(run(&only_q, [S, P, O], None), (None, None));
    assert_eq!(run(&only_q, [S, P, O], Some("5")), (None, None));
    assert_eq!(run(&only_q, [S, Q, O], None), (Some(2), None));
    let without_p = rg.exclude_valid_layers(layer_of(P));
    assert_eq!(run(&without_p, [S, P, O], None), (None, None));
    assert_eq!(run(&without_p, [S, Q, O], None), (Some(2), None));
    let without_s = rg.subgraph([O]);
    assert_eq!(run(&without_s, [S, P, O], None), (None, None));
    assert_eq!(run(&without_s, [S, P, O], Some("5")), (None, None));
    let without_o = rg.exclude_nodes([O]);
    assert_eq!(run(&without_o, [S, P, O], None), (None, None));
    // the private layer of the graph is not a predicate
    assert_eq!(
        run(&rg, [S, "raphtory:_static_graph", O], None),
        (None, None)
    );
}

/// Literal objects are matched by their canonical form; literal subjects are not.
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
    let call = |p: &str, o: &str, at: Option<&str>| {
        let at = at.map(|at| format!(", {at}")).unwrap_or_default();
        let query = format!(
            "{PREFIX} SELECT (raphtory:validFromTime(ex:s, ex:{p}, {o}{at}) AS ?f) \
             (raphtory:validToTime(ex:s, ex:{p}, {o}{at}) AS ?t) {{}}"
        );
        let rows = select(&rg, &query);
        let get = |v: &String| (v != "UNDEF").then(|| parse_int(v));
        (get(&rows[0][0]), get(&rows[0][1]))
    };

    assert_eq!(call("age", "\"042\"^^xsd:integer", None), (Some(3), None));
    // at 4 both spellings of 42 were visible: ambiguous
    assert_eq!(call("age", "42", Some("4")), (None, None));
    assert_eq!(call("name", "\"x\"@en", None), (Some(1), None));
    // every literal object, through a pattern
    let rows = select(
        &rg.valid(),
        &format!(
            "{PREFIX} SELECT ?p ?f {{ ex:s ?p ?o BIND(raphtory:validFromTime(ex:s, ?p, ?o) AS ?f) }}"
        ),
    );
    assert_eq!(rows.len(), 9);
    assert!(rows.iter().all(|row| row[1] != "UNDEF"), "{rows:?}");

    // the canonical form SPARQL passes for a literal subject does not identify the node
    let g = Graph::new();
    for lexical in ["042", "42"] {
        let subject = format!("\"{lexical}\"^^<{XSD}integer>");
        g.add_edge(1, subject.as_str(), "o", NO_PROPS, Some("p"))
            .unwrap();
    }
    assert_eq!(
        value(
            &RdfGraph::from(g),
            "raphtory:validFromTime(42, raphtory:p, raphtory:o)"
        ),
        None
    );
}

/// Functions on a locked view do not deadlock.
#[test]
fn locked_views() {
    let rg = RdfGraph::new();
    assert_at(&rg, 1, SPO);
    retract_at(&rg, 5, SPO);
    let (now, at_3) = within_60s(move || {
        let locked = RdfGraph::from(rg.graph().read_only());
        (run(&locked, SPO, None), run(&locked, SPO, Some("3")))
    });
    assert_eq!(now, (None, None));
    assert_eq!(at_3, (Some(1), Some(5)));
}

/// `xsd:dateTime` results only cover the years 1 to 9999.
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

// Proptest oracle against a brute-force scan of snapshots.

/// The graph with only the events of `log` before `end`, written in log order (so events at the
/// same time keep their order).
fn root_until(log: &[Event], end: Option<i64>) -> RdfGraph {
    let g = RdfGraph::new();
    for e in log.iter().filter(|e| end.is_none_or(|end| e.t < end)) {
        if e.assert {
            assert_at(&g, e.t, LOG_TRIPLES[e.triple]);
        } else {
            retract_at(&g, e.t, LOG_TRIPLES[e.triple]);
        }
    }
    g
}

/// The times scanned by the oracle. Every event is in `0..8`, so the state at `12` is the state
/// at infinity.
const SCAN: std::ops::RangeInclusive<i64> = -2..=12;

/// The expected `(from, to)` of each triple of the log on `view`, for each reference (`None`:
/// without one), from SPARQL visibility in `snapshot_at(x)` for every `x`.
fn oracle(view: &RdfView, log: &[Event], refs: &[Option<i64>]) -> Runs {
    let end = view.graph().end().map(|end| end.t());
    let root = root_until(log, end);
    let holds: Vec<BTreeSet<[String; 3]>> = SCAN
        .map(|x| triples(&root.snapshot_at(x).valid()))
        .collect();
    let holds_at = |i: usize, x: i64| {
        let x = x.clamp(*SCAN.start(), *SCAN.end());
        holds[(x - SCAN.start()) as usize].contains(&nt3(i))
    };
    let mut expected = BTreeMap::new();
    for &r in refs {
        // the functions answer for the triples valid at the end of the view
        let scope = match r {
            None => triples(&view.valid()),
            Some(r) => triples(&view.snapshot_at(r).valid()),
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

fn dynamic<G: StaticGraphViewOps + IntoDynamic>(view: RdfView<G>) -> RdfView {
    RdfView::from_view(view.graph().clone().into_dynamic()).unwrap()
}

/// The `(from, to)` of each triple of the log on `view` for each reference, from the functions.
fn actual<G: StaticGraphViewOps + IntoDynamic>(view: &RdfView<G>, refs: &[Option<i64>]) -> Runs {
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
        let rg = RdfGraph::new();
        for e in &log {
            if e.assert {
                assert_at(&rg, e.t, LOG_TRIPLES[e.triple]);
            } else {
                retract_at(&rg, e.t, LOG_TRIPLES[e.triple]);
            }
        }
        let refs: Vec<Option<i64>> = std::iter::once(None).chain((-1..=10).map(Some)).collect();
        let end = start + len;
        let views = [
            ("rg", dynamic(rg.clone())),
            ("rg.snapshot_at", dynamic(rg.snapshot_at(at))),
            ("rg.window", dynamic(rg.window(start, end))),
        ];
        for (name, view) in views {
            prop_assert_eq!(
                actual(&view, &refs),
                oracle(&view, &log, &refs),
                "{} (at {}, window {}..{})", name, at, start, end
            );
        }
    }
}
