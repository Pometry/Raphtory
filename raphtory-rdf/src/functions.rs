//! Temporal SPARQL functions: since when a triple has held, and until when.
use crate::{
    mapping::{layer_name_of, name_of, term_of},
    time_graph::TimeGraph,
};
use chrono::{DateTime, Datelike, Utc};
use itertools::Itertools;
use oxigraph::{
    model::{vocab::xsd, Literal, NamedNode, Term},
    sparql::SparqlEvaluator,
};
use raphtory::{
    db::{
        api::view::{DynamicGraph, IntoDynamic},
        graph::edge::EdgeView,
    },
    prelude::*,
};
use raphtory_api::core::storage::timeindex::AsTime;
use spareval::ExpressionTerm;

pub(crate) fn temporal_functions_of(
    evaluator: SparqlEvaluator,
    view: DynamicGraph,
) -> SparqlEvaluator {
    [
        ("raphtory:validFrom", End::From, Repr::DateTime),
        ("raphtory:validTo", End::To, Repr::DateTime),
        ("raphtory:validFromTime", End::From, Repr::Time),
        ("raphtory:validToTime", End::To, Repr::Time),
    ]
    .into_iter()
    .fold(evaluator, |evaluator, (name, end, repr)| {
        let view = view.clone();
        evaluator.with_custom_function(NamedNode::new_unchecked(name), move |args| {
            let (from, to) = latest_run(&view, args)?;
            let t = match end {
                End::From => from,
                End::To => to?,
            };
            match repr {
                Repr::Time => Some(Literal::from(t).into()),
                Repr::DateTime => date_time(t),
            }
        })
    })
}

#[derive(Clone, Copy)]
enum End {
    From,
    To,
}

#[derive(Clone, Copy)]
enum Repr {
    DateTime,
    Time,
}

fn latest_run(view: &DynamicGraph, args: &[Term]) -> Option<(i64, Option<i64>)> {
    let (s, p, o, scope) = match args {
        [s, p, o] => (s, p, o, view.clone()),
        [s, p, o, Term::NamedNode(graph)] => {
            let graph = TimeGraph::parse(graph).ok().flatten()?;
            (s, p, o, graph.view_of(view))
        }
        _ => return None,
    };
    // a canonicalised literal subject or predicate cannot be mapped back to its node or layer
    if !passed_unchanged(s) || !passed_unchanged(p) {
        return None;
    }
    let scope = scope
        .valid_layers(layer_name_of(p.as_ref())?.as_str())
        .into_dynamic();
    let s = scope.node(name_of(s.as_ref())?)?;
    let edge = if passed_unchanged(o) {
        scope.edge(s.node, scope.node(name_of(o.as_ref())?)?.node)?
    } else {
        // equal canonical forms written differently are different nodes: ambiguous
        s.out_edges()
            .into_iter()
            .filter(|e| Term::from(ExpressionTerm::from(term_of(&e.dst().name()))) == *o)
            .exactly_one()
            .ok()?
    };
    run_of(&edge)
}

fn run_of(edge: &EdgeView<DynamicGraph>) -> Option<(i64, Option<i64>)> {
    // the earliest time of an edge that held at the start of the view is that start
    let mut from = edge
        .start()
        .zip(edge.earliest_time())
        .filter(|(start, earliest)| start.t() == earliest.t())
        .map(|(start, _)| start.t());
    let mut to = None;
    let assertions = edge.history().into_iter().map(|t| (t, true));
    let retractions = edge.deletions().into_iter().map(|t| (t, false));
    // at an identical event time the retraction is last, as in Raphtory
    for (t, asserted) in assertions.merge_by(retractions, |a, b| a.0 <= b.0) {
        let holds = from.is_some() && to.is_none();
        if asserted && !holds {
            from = Some(t.t());
            to = None;
        } else if !asserted && holds {
            to = Some(t.t());
        }
    }
    Some((from?, to))
}

// SPARQL passes literals of xsd value types (numbers, booleans, date-times..) in canonical form
fn passed_unchanged(term: &Term) -> bool {
    matches!(
        ExpressionTerm::from(term.clone()),
        ExpressionTerm::NamedNode(_)
            | ExpressionTerm::BlankNode(_)
            | ExpressionTerm::StringLiteral(_)
            | ExpressionTerm::LangStringLiteral { .. }
            | ExpressionTerm::OtherTypedLiteral { .. }
    )
}

// outside the years 1 to 9999 oxsdatatypes misreads chrono's output
fn date_time(t: i64) -> Option<Term> {
    let dt = DateTime::<Utc>::from_timestamp_millis(t)?;
    if !(1..=9999).contains(&dt.year()) {
        return None;
    }
    let lexical = dt.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();
    match ExpressionTerm::from(Term::from(Literal::new_typed_literal(
        lexical,
        xsd::DATE_TIME,
    ))) {
        value @ ExpressionTerm::DateTimeLiteral(_) => Some(value.into()),
        _ => None,
    }
}
