//! Triples asserted and retracted over time.
use super::select;
use crate::{NamedNode, RdfGraph, RdfView, Triple};
use raphtory::db::api::view::{IntoDynamic, StaticGraphViewOps};
use raphtory_api::core::utils::time::TryIntoInputTime;
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

pub(super) fn assert_at(g: &RdfGraph, t: impl TryIntoInputTime, spo: [&str; 3]) {
    g.add_triple(t, &triple(spo)).unwrap();
}

pub(super) fn retract_at(g: &RdfGraph, t: impl TryIntoInputTime, spo: [&str; 3]) {
    g.retract_triple(t, &triple(spo)).unwrap();
}

/// The rows of a query that projects `?s ?p ?o`, as a set. Fails on duplicate rows.
pub(super) fn spo<G: StaticGraphViewOps + IntoDynamic>(
    view: &RdfView<G>,
    query: &str,
) -> BTreeSet<[String; 3]> {
    let rows = select(view, query);
    let set: BTreeSet<[String; 3]> = rows
        .iter()
        .map(|r| [r[0].clone(), r[1].clone(), r[2].clone()])
        .collect();
    assert_eq!(set.len(), rows.len(), "{query}: duplicate rows: {rows:?}");
    set
}

/// Every triple of the default graph of the view, in N-Triples form.
pub(super) fn triples<G: StaticGraphViewOps + IntoDynamic>(
    view: &RdfView<G>,
) -> BTreeSet<[String; 3]> {
    spo(view, "SELECT ?s ?p ?o { ?s ?p ?o }")
}

pub(super) fn nt(spo: [&str; 3]) -> [String; 3] {
    spo.map(|t| format!("<{t}>"))
}

/// One event of an assert/retract log: assert or retract `LOG_TRIPLES[triple]` at time `t`.
#[derive(Debug, Clone, Copy)]
pub(super) struct Event {
    pub(super) t: i64,
    pub(super) assert: bool,
    pub(super) triple: usize,
}

/// The triples an assert/retract log writes.
pub(super) const LOG_TRIPLES: [[&str; 3]; 3] = [[S, P, O], [S, Q, O], [O, P, S]];

/// The graph with only the events of `log` before `end`, written in log order (so events at the
/// same time keep their order).
pub(super) fn root_until(log: &[Event], end: Option<i64>) -> RdfGraph {
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
