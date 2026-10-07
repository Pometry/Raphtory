//! What SPARQL sees as of a time, through views.
use super::select;
use crate::{
    prelude::*,
    rdf::{
        model::{NamedNode, Triple},
        RdfFormat,
    },
};
use proptest::prelude::*;
use std::collections::BTreeSet;

pub(super) const S: &str = "http://ex/s";
pub(super) const P: &str = "http://ex/p";
pub(super) const Q: &str = "http://ex/q";
pub(super) const O: &str = "http://ex/o";
pub(super) const O2: &str = "http://ex/o2";

pub(super) const SPO: [&str; 3] = [S, P, O];

fn triple([s, p, o]: [&str; 3]) -> Triple {
    Triple::new(
        NamedNode::new(s).unwrap(),
        NamedNode::new(p).unwrap(),
        NamedNode::new(o).unwrap(),
    )
}

pub(super) fn assert_at<G: RdfMutationOps>(g: &G, t: i64, spo: [&str; 3]) {
    g.add_triple(t, &triple(spo)).unwrap();
}

pub(super) fn retract_at<G: RdfMutationOps>(g: &G, t: i64, spo: [&str; 3]) {
    g.delete_triple(t, &triple(spo)).unwrap();
}

/// Whether `<s> <p> <o>` is in the default graph of the view. It is checked with each of the
/// 8 binding patterns (each position is the constant or a variable), which must agree.
pub(super) fn visible<G: RdfViewOps>(view: &G, spo: [&str; 3]) -> bool {
    let vars = ["?s", "?p", "?o"];
    let answers: Vec<bool> = (0..8)
        .map(|mask| {
            let mut pattern = Vec::new();
            let mut projection = Vec::new();
            let mut expected = Vec::new();
            for i in 0..3 {
                let constant = format!("<{}>", spo[i]);
                if mask & (1 << i) != 0 {
                    pattern.push(vars[i].to_owned());
                    projection.push(vars[i]);
                    expected.push(constant);
                } else {
                    pattern.push(constant);
                }
            }
            let projection = if projection.is_empty() {
                "*".to_owned()
            } else {
                projection.join(" ")
            };
            let query = format!("SELECT {projection} {{ {} }}", pattern.join(" "));
            select(view, &query).contains(&expected)
        })
        .collect();
    assert!(
        answers.iter().all(|a| *a == answers[0]),
        "binding patterns disagree for {spo:?}: {answers:?}"
    );
    answers[0]
}

/// Every triple of the default graph of the view, in N-Triples form.
pub(super) fn triples<G: RdfViewOps>(view: &G) -> BTreeSet<[String; 3]> {
    let rows = select(view, "SELECT ?s ?p ?o { ?s ?p ?o }");
    let set: BTreeSet<[String; 3]> = rows
        .iter()
        .map(|r| [r[0].clone(), r[1].clone(), r[2].clone()])
        .collect();
    assert_eq!(set.len(), rows.len(), "duplicate rows: {rows:?}");
    set
}

pub(super) fn nt(spo: [&str; 3]) -> [String; 3] {
    spo.map(|t| format!("<{t}>"))
}

/// The answers of `asserted_then_retracted`, also expected by `out_of_order_loads`.
fn check_asserted_at_1_retracted_at_5(pg: &PersistentGraph) {
    assert!(!visible(&pg.snapshot_at(0), SPO));
    for t in 1..=4 {
        assert!(visible(&pg.snapshot_at(t), SPO), "t = {t}");
        assert_eq!(triples(&pg.snapshot_at(t)), BTreeSet::from([nt(SPO)]));
    }
    for t in 5..=8 {
        assert!(!visible(&pg.snapshot_at(t), SPO), "t = {t}");
    }
    // the unwindowed graph and the views without an end are the current state
    assert!(!visible(pg, SPO));
    assert!(!visible(&pg.snapshot_latest(), SPO));
    assert!(!visible(&pg.after(2), SPO));
    assert!(triples(pg).is_empty());
}

/// Assert at 1, retract at 5.
#[test]
fn asserted_then_retracted() {
    let pg = PersistentGraph::new();
    assert_at(&pg, 1, SPO);
    retract_at(&pg, 5, SPO);
    check_asserted_at_1_retracted_at_5(&pg);
    // `pg.at(t)` is the same view as `pg.snapshot_at(t)`
    for t in 0..=6 {
        assert_eq!(triples(&pg.at(t)), triples(&pg.snapshot_at(t)), "t = {t}");
    }
}

/// A re-assertion makes the triple visible again.
#[test]
fn reasserted_after_a_retraction() {
    let pg = PersistentGraph::new();
    assert_at(&pg, 1, SPO);
    retract_at(&pg, 5, SPO);
    assert_at(&pg, 8, SPO);
    for t in 0..=10 {
        let expected = (1..5).contains(&t) || t >= 8;
        assert_eq!(visible(&pg.snapshot_at(t), SPO), expected, "t = {t}");
    }
    assert!(visible(&pg, SPO));
    assert!(visible(&pg.snapshot_latest(), SPO));
    assert!(visible(&pg.after(2), SPO));
}

/// Assert and retract at the same time: the later call wins.
#[test]
fn same_time_call_order_decides() {
    let pg = PersistentGraph::new();
    assert_at(&pg, 3, SPO);
    retract_at(&pg, 3, SPO);
    for t in 2..=4 {
        assert!(!visible(&pg.snapshot_at(t), SPO), "t = {t}");
    }
    assert!(!visible(&pg, SPO));

    let pg = PersistentGraph::new();
    retract_at(&pg, 3, SPO);
    assert_at(&pg, 3, SPO);
    assert!(!visible(&pg.snapshot_at(2), SPO));
    assert!(visible(&pg.snapshot_at(3), SPO));
    assert!(visible(&pg.snapshot_at(4), SPO));
    assert!(visible(&pg, SPO));
}

/// Duplicate assertions give one row; a later retraction hides the triple.
#[test]
fn duplicate_assertions() {
    let pg = PersistentGraph::new();
    assert_at(&pg, 1, SPO);
    assert_at(&pg, 2, SPO);
    retract_at(&pg, 3, SPO);
    let all = "SELECT * { ?s ?p ?o }";
    assert_eq!(select(&pg.snapshot_at(1), all).len(), 1);
    assert_eq!(select(&pg.snapshot_at(2), all).len(), 1);
    assert_eq!(select(&pg.window(0, 3), all).len(), 1);
    for t in 3..=5 {
        assert!(select(&pg.snapshot_at(t), all).is_empty(), "t = {t}");
    }
    assert!(select(&pg, all).is_empty());
    // every event stays in the history
    let e = pg.edge(S, O).unwrap().layers(P).unwrap();
    assert_eq!(e.history().t().collect(), vec![1, 2]);
    assert_eq!(e.deletions().t().collect(), vec![3]);
}

/// Retracting one of two predicates on the same pair keeps the other.
#[test]
fn retracting_one_predicate_keeps_the_other() {
    let pg = PersistentGraph::new();
    assert_at(&pg, 1, [S, P, O]);
    assert_at(&pg, 1, [S, Q, O]);
    retract_at(&pg, 2, [S, P, O]);
    let query = "SELECT ?p { <http://ex/s> ?p <http://ex/o> }";
    assert_eq!(
        select(&pg.snapshot_at(1), query),
        vec![vec![format!("<{P}>")], vec![format!("<{Q}>")]]
    );
    assert_eq!(
        select(&pg.snapshot_at(2), query),
        vec![vec![format!("<{Q}>")]]
    );
    assert_eq!(select(&pg, query), vec![vec![format!("<{Q}>")]]);
    assert!(!visible(&pg, [S, P, O]));
    assert!(visible(&pg, [S, Q, O]));
}

/// Timestamps decide, not load order.
#[test]
fn out_of_order_loads() {
    let doc = format!("<{S}> <{P}> <{O}> .");
    let pg = PersistentGraph::new();
    pg.retract_rdf(5, doc.as_bytes(), RdfFormat::NTriples, None)
        .unwrap();
    pg.load_rdf(1, doc.as_bytes(), RdfFormat::NTriples, None)
        .unwrap();
    check_asserted_at_1_retracted_at_5(&pg);
}

/// A triple that was only ever retracted is never visible.
#[test]
fn orphan_retractions_are_never_visible() {
    let pg = PersistentGraph::new();
    assert_at(&pg, 1, [S, Q, O]);
    retract_at(&pg, 3, SPO);
    retract_at(&pg, 4, [O, P, O2]);
    assert!(!visible(&pg, SPO));
    assert!(!visible(&pg, [O, P, O2]));
    for t in -1..=6 {
        assert!(!visible(&pg.snapshot_at(t), SPO), "t = {t}");
        assert!(!visible(&pg.snapshot_at(t), [O, P, O2]), "t = {t}");
    }
    for a in -1..6 {
        for b in a + 1..=7 {
            assert!(!visible(&pg.window(a, b), SPO), "[{a}, {b})");
            assert!(!visible(&pg.window(a, b), [O, P, O2]), "[{a}, {b})");
        }
    }
    assert!(!visible(&pg.event_graph(), SPO));
    // the retraction did create the nodes
    assert!(pg.node(O2).is_some());
    assert_eq!(triples(&pg), BTreeSet::from([nt([S, Q, O])]));
}

/// Event graphs ignore retractions; their persistent view sees them.
#[test]
fn event_graphs_ignore_retractions() {
    let g = Graph::new();
    assert_at(&g, 1, SPO);
    retract_at(&g, 5, SPO);
    assert!(visible(&g, SPO));
    assert!(!visible(&g.snapshot_at(0), SPO));
    for t in 1..=8 {
        assert!(visible(&g.snapshot_at(t), SPO), "t = {t}");
    }
    assert!(visible(&g.window(0, 2), SPO));
    // a window that contains only the retraction
    assert!(!visible(&g.window(2, 10), SPO));
    assert!(!visible(&g.window(5, 10), SPO));

    let pg = g.persistent_graph();
    assert!(!visible(&pg, SPO));
    for t in 0..=8 {
        assert_eq!(
            visible(&pg.snapshot_at(t), SPO),
            (1..5).contains(&t),
            "t = {t}"
        );
    }
}

/// `pg.window(a, b)` is `pg.snapshot_at(b - 1)`.
#[test]
fn windows_are_as_of_their_end() {
    let pg = PersistentGraph::new();
    assert_at(&pg, 1, SPO);
    retract_at(&pg, 4, SPO);
    assert_at(&pg, 6, SPO);
    assert_at(&pg, 2, [S, Q, O]);
    retract_at(&pg, 2, [S, Q, O]);
    retract_at(&pg, 3, [S, P, O2]);
    assert_at(&pg, 5, [S, P, O2]);
    retract_at(&pg, 8, [S, P, O2]);
    assert_at(&pg, 0, [O, P, S]);
    assert_at(&pg, 7, [O, P, S]);
    let mut seen = BTreeSet::new();
    for b in 0..=10 {
        let expected = triples(&pg.snapshot_at(b - 1));
        seen.insert(expected.len());
        assert_eq!(triples(&pg.before(b)), expected, "before({b})");
        for a in -1..b {
            assert_eq!(triples(&pg.window(a, b)), expected, "[{a}, {b})");
        }
    }
    // the history is not trivial
    assert!(seen.len() >= 3, "{seen:?}");
}

/// A literal is one node; its in-degree follows the triples over time.
#[test]
fn literal_node_history() {
    let lit = "\"42\"^^<http://www.w3.org/2001/XMLSchema#integer>";
    let pg = PersistentGraph::new();
    let doc = |s: &str| format!("<{s}> <http://ex/age> {lit} .");
    pg.load_rdf(1, doc("http://ex/a").as_bytes(), RdfFormat::NTriples, None)
        .unwrap();
    // `42` in Turtle is the same literal
    pg.load_rdf(
        3,
        "<http://ex/b> <http://ex/age> 42 .".as_bytes(),
        RdfFormat::Turtle,
        None,
    )
    .unwrap();
    pg.retract_rdf(5, doc("http://ex/a").as_bytes(), RdfFormat::NTriples, None)
        .unwrap();
    assert_eq!(pg.count_nodes(), 3);

    let query = "SELECT ?s { ?s <http://ex/age> 42 }";
    for (t, expected) in [(0, 0), (1, 1), (2, 1), (3, 2), (4, 2), (5, 1), (9, 1)] {
        let view = pg.snapshot_at(t);
        assert_eq!(select(&view, query).len(), expected, "t = {t}");
        let degree = view.valid().node(lit).map_or(0, |n| n.in_degree());
        assert_eq!(degree, expected, "valid in_degree at t = {t}");
        let degree = view.node(lit).map_or(0, |n| n.in_degree());
        assert_eq!(degree, expected, "in_degree at t = {t}");
    }
    assert_eq!(select(&pg, query), vec![vec!["<http://ex/b>".to_owned()]]);
    assert_eq!(pg.node(lit).unwrap().history().t().collect(), vec![1, 3, 5]);
}

/// One event of a reference-model log: assert or retract triple `triple` at time `t`.
#[derive(Debug, Clone, Copy)]
pub(super) struct Event {
    pub(super) t: i64,
    pub(super) assert: bool,
    pub(super) triple: usize,
}

pub(super) const LOG_TRIPLES: [[&str; 3]; 3] = [[S, P, O], [S, Q, O], [O, P, S]];

/// The last event with `ev.t <= t` (ordered by time, then log order) is an assertion.
pub(super) fn model_visible_at(log: &[Event], triple: usize, t: Option<i64>) -> bool {
    log.iter()
        .enumerate()
        .filter(|(_, e)| e.triple == triple && t.is_none_or(|t| e.t <= t))
        .max_by_key(|(seq, e)| (e.t, *seq))
        .is_some_and(|(_, e)| e.assert)
}

/// Some assertion has `ev.t <= t`.
pub(super) fn model_asserted_by(log: &[Event], triple: usize, t: i64) -> bool {
    log.iter()
        .any(|e| e.triple == triple && e.assert && e.t <= t)
}

pub(super) fn model(holds: impl Fn(usize) -> bool) -> BTreeSet<[String; 3]> {
    (0..LOG_TRIPLES.len())
        .filter(|i| holds(*i))
        .map(|i| nt(LOG_TRIPLES[i]))
        .collect()
}

pub(super) fn log() -> impl Strategy<Value = Vec<Event>> {
    prop::collection::vec(
        (0..10i64, any::<bool>(), 0..LOG_TRIPLES.len()).prop_map(|(t, assert, triple)| Event {
            t,
            assert,
            triple,
        }),
        0..=30,
    )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// As-of queries match a reference model of the log.
    #[test]
    fn as_of_matches_a_reference_model(log in log()) {
        let pg = PersistentGraph::new();
        for e in &log {
            if e.assert {
                assert_at(&pg, e.t, LOG_TRIPLES[e.triple]);
            } else {
                retract_at(&pg, e.t, LOG_TRIPLES[e.triple]);
            }
        }
        let events = pg.event_graph();
        for t in -1..=11 {
            prop_assert_eq!(
                triples(&pg.snapshot_at(t)),
                model(|i| model_visible_at(&log, i, Some(t))),
                "snapshot_at({})", t
            );
            prop_assert_eq!(
                triples(&events.snapshot_at(t)),
                model(|i| model_asserted_by(&log, i, t)),
                "event_graph().snapshot_at({})", t
            );
        }
        prop_assert_eq!(triples(&pg), model(|i| model_visible_at(&log, i, None)));
    }
}
