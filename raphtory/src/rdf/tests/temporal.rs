//! Triples asserted and retracted over time, and a reference model of an assert/retract log.
use super::select;
use crate::{
    db::api::view::{IntoDynamic, StaticGraphViewOps},
    prelude::*,
    rdf::model::{NamedNode, Triple},
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

pub(super) fn assert_at(g: &RdfGraph, t: i64, spo: [&str; 3]) {
    g.add_triple(t, &triple(spo)).unwrap();
}

pub(super) fn retract_at(g: &RdfGraph, t: i64, spo: [&str; 3]) {
    g.retract_triple(t, &triple(spo)).unwrap();
}

/// Every triple of the default graph of the view, in N-Triples form.
pub(super) fn triples<G: StaticGraphViewOps + IntoDynamic>(
    view: &RdfView<G>,
) -> BTreeSet<[String; 3]> {
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
