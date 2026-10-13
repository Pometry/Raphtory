//! Temporal SPARQL functions: since when a triple has held, and until when.
//!
//! [`temporal_functions_of`] registers four functions on a [`SparqlEvaluator`], answering for
//! the triples of one view. [`RdfView::sparql`](crate::RdfView::sparql) and
//! [`RdfView::sparql_to_writer`](crate::RdfView::sparql_to_writer) register them.
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

/// `raphtory:validFrom(?s, ?p, ?o [, ?g])`: when the latest run of a triple started, as an
/// `xsd:dateTime` (see [the rules](crate#the-rules-of-the-temporal-functions)).
pub const VALID_FROM: &str = "raphtory:validFrom";

/// `raphtory:validTo(?s, ?p, ?o [, ?g])`: the retraction that ended the latest run of a triple,
/// as an `xsd:dateTime` (see [the rules](crate#the-rules-of-the-temporal-functions)).
pub const VALID_TO: &str = "raphtory:validTo";

/// `raphtory:validFromTime(?s, ?p, ?o [, ?g])`: [`VALID_FROM`] as an `xsd:integer`, the
/// Raphtory time (epoch milliseconds).
pub const VALID_FROM_TIME: &str = "raphtory:validFromTime";

/// `raphtory:validToTime(?s, ?p, ?o [, ?g])`: [`VALID_TO`] as an `xsd:integer`, the Raphtory
/// time (epoch milliseconds).
pub const VALID_TO_TIME: &str = "raphtory:validToTime";

/// Registers the temporal functions [`VALID_FROM`], [`VALID_TO`], [`VALID_FROM_TIME`] and
/// [`VALID_TO_TIME`] on `evaluator`, answering for the triples of `view` (see
/// [the rules](crate#the-rules-of-the-temporal-functions)).
pub(crate) fn temporal_functions_of(
    evaluator: SparqlEvaluator,
    view: DynamicGraph,
) -> SparqlEvaluator {
    [
        (VALID_FROM, End::From, Repr::DateTime),
        (VALID_TO, End::To, Repr::DateTime),
        (VALID_FROM_TIME, End::From, Repr::Time),
        (VALID_TO_TIME, End::To, Repr::Time),
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

/// Which end of the run a function returns.
#[derive(Clone, Copy)]
enum End {
    From,
    To,
}

/// How a function returns a time.
#[derive(Clone, Copy)]
enum Repr {
    /// An `xsd:dateTime`.
    DateTime,
    /// An `xsd:integer`.
    Time,
}

/// The latest run `(from, to)` of the triple `args[..3]` in the scope (the view, or the time
/// graph `args[3]` of it), or `None` if the triple is not visible in the scope or never held in
/// it.
fn latest_run(view: &DynamicGraph, args: &[Term]) -> Option<(i64, Option<i64>)> {
    let (s, p, o, scope) = match args {
        [s, p, o] => (s, p, o, view.clone()),
        [s, p, o, Term::NamedNode(graph)] => {
            let graph = TimeGraph::parse(graph).ok().flatten()?;
            (s, p, o, graph.view_of(view))
        }
        _ => return None,
    };
    // A canonicalised literal subject or predicate cannot be mapped back to its node or layer.
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

/// The latest run of `edge` in its view, by persistent semantics: it starts at the start of
/// the view if the edge held there, and its end is `None` if the edge holds at the end.
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

/// Whether SPARQL hands `term` to functions as it is in the graph: IRIs, blank nodes, string
/// literals and literals of other types do; a literal of an xsd value type (a number, boolean,
/// date-time, ...) arrives in canonical form instead.
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

/// The canonical `xsd:dateTime` (UTC, millisecond precision) of a time, if it is in the years 1
/// to 9999 (outside them oxsdatatypes misreads chrono's output).
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
