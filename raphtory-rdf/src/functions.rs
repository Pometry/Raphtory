//! Temporal SPARQL functions: since when a triple has held, and until when.
//!
//! [`temporal_functions_of`] registers four functions on a [`SparqlEvaluator`], answering for
//! the triples of one view. [`RdfView::sparql`](crate::RdfView::sparql) and
//! [`RdfView::sparql_to_writer`](crate::RdfView::sparql_to_writer) register them by
//! default (see [`SparqlOptions`](crate::SparqlOptions)).
use crate::{
    dataset::lookup_node,
    mapping::{layer_name_of, literal_to_prop, name_of, term_of},
    scan::in_layer,
    time_graph::TimeGraph,
};
use chrono::{DateTime, Datelike, Utc};
use itertools::Itertools;
use oxigraph::{
    model::{vocab::xsd, Literal, NamedNode, Term},
    sparql::SparqlEvaluator,
};
use raphtory::{
    db::api::view::{internal::CoreGraphOps, DynamicGraph, IntoDynamic},
    prelude::*,
};
use raphtory_api::core::{
    entities::{properties::meta::STATIC_GRAPH_LAYER_ID, VID},
    storage::timeindex::{AsTime, EventTime},
};
use spareval::ExpressionTerm;
use std::sync::Arc;

/// `raphtory:validFrom(?s, ?p, ?o [, ?ref])`: the start of the validity interval of a triple, as
/// an `xsd:dateTime` (see [the rules](crate#the-rules-of-the-temporal-functions)).
pub const VALID_FROM: &str = "raphtory:validFrom";

/// `raphtory:validTo(?s, ?p, ?o [, ?ref])`: the end of the validity interval of a triple (the
/// time it was retracted), as an `xsd:dateTime` (see
/// [the rules](crate#the-rules-of-the-temporal-functions)).
pub const VALID_TO: &str = "raphtory:validTo";

/// `raphtory:validFromTime(?s, ?p, ?o [, ?ref])`: [`VALID_FROM`] as an `xsd:integer`, the
/// Raphtory time (epoch milliseconds).
pub const VALID_FROM_TIME: &str = "raphtory:validFromTime";

/// `raphtory:validToTime(?s, ?p, ?o [, ?ref])`: [`VALID_TO`] as an `xsd:integer`, the Raphtory
/// time (epoch milliseconds).
pub const VALID_TO_TIME: &str = "raphtory:validToTime";

/// Registers the temporal functions [`VALID_FROM`], [`VALID_TO`], [`VALID_FROM_TIME`] and
/// [`VALID_TO_TIME`] on `evaluator`, answering for the triples of `view` (see
/// [the rules](crate#the-rules-of-the-temporal-functions)).
pub(crate) fn temporal_functions_of(
    evaluator: SparqlEvaluator,
    view: DynamicGraph,
) -> SparqlEvaluator {
    let temporal = Arc::new(Temporal::new(view));
    [
        (VALID_FROM, End::From, Repr::DateTime),
        (VALID_TO, End::To, Repr::DateTime),
        (VALID_FROM_TIME, End::From, Repr::Time),
        (VALID_TO_TIME, End::To, Repr::Time),
    ]
    .into_iter()
    .fold(evaluator, |evaluator, (name, end, repr)| {
        let temporal = temporal.clone();
        evaluator.with_custom_function(NamedNode::new_unchecked(name), move |args| {
            temporal.call(args, end, repr)
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

/// The run of a triple: `[from, to)`, with `to` `None` while it is open.
type Run = (i64, Option<i64>);

/// What the functions need of the view. Everything is `Send + Sync + 'static`.
struct Temporal {
    /// The view.
    base: DynamicGraph,
    /// The persistent graph of the view without its window and filters.
    root: DynamicGraph,
    /// The end of the view: only the events before it count.
    horizon: Option<EventTime>,
}

impl Temporal {
    fn new(base: DynamicGraph) -> Self {
        // cloning the storage is cheap: it is reference counted, also when it is locked
        let root = PersistentGraph::from_internal_graph(base.core_graph().clone()).into_dynamic();
        let horizon = base.end();
        Self {
            base,
            root,
            horizon,
        }
    }

    fn call(&self, args: &[Term], end: End, repr: Repr) -> Option<Term> {
        let (from, to) = self.run(args)?;
        let t = match end {
            End::From => from,
            End::To => to?,
        };
        match repr {
            Repr::Time => Some(Literal::from(t).into()),
            Repr::DateTime => date_time(t),
        }
    }

    /// The run of the triple `args[..3]` that contains the reference time (`args[3]`, if
    /// given), or `None` if the triple is not visible in the scope.
    fn run(&self, args: &[Term]) -> Option<Run> {
        let (s, p, o, at) = match args {
            [s, p, o] => (s, p, o, None),
            [s, p, o, at] => (s, p, o, Some(ref_time(at)?)),
            _ => return None,
        };
        // A canonicalised literal subject or predicate cannot be mapped back to its node or layer.
        if !passed_unchanged(s) || !passed_unchanged(p) {
            return None;
        }
        let s = lookup_node(&self.base, &name_of(s.as_ref())?)?;
        let layer = self
            .base
            .get_layer_id(&layer_name_of(p.as_ref())?)
            .filter(|l| *l != STATIC_GRAPH_LAYER_ID)?;
        // The scope is what SPARQL sees: the view (as of the reference time) in the layer.
        let scope = match at {
            None => self.base.clone(),
            Some(t) => self.base.snapshot_at(t).into_dynamic(),
        };
        let in_view = in_layer(&scope, layer);
        let valid = in_view.valid().into_dynamic();
        let root = in_layer(&self.root, layer);
        let o = self.object(&in_view, &valid, &root, s, o)?;
        // The state of `R` is constant from the end of the view on.
        let horizon = self.horizon.map(|h| h.t());
        let t_ref = match (at, horizon) {
            (Some(t), Some(h)) => t.min(h),
            (Some(t), None) => t,
            (None, Some(h)) => h,
            (None, None) => i64::MAX,
        };
        run_at(self.events(&root, s, o)?, t_ref)
    }

    /// The node of the object `o` of `s`: the one object of `s` in `in_view` that matches `o`,
    /// if the triple to it is in `valid`.
    fn object(
        &self,
        in_view: &DynamicGraph,
        valid: &DynamicGraph,
        root: &DynamicGraph,
        s: VID,
        o: &Term,
    ) -> Option<VID> {
        if passed_unchanged(o) {
            let v = lookup_node(&self.base, &name_of(o.as_ref())?)?;
            return valid.edge(s, v).is_some().then_some(v);
        }
        // Match a canonicalised literal among the objects of `s`. Collect first, so no storage
        // guard is held while names are read.
        let objects: Vec<VID> = root
            .node(s)?
            .out_edges()
            .into_iter()
            .map(|e| e.edge.dst())
            .collect();
        let mut found = objects.into_iter().filter(|v| {
            Term::from(ExpressionTerm::from(term_of(&root.node_name(*v)))) == *o
                && in_view.edge(s, *v).is_some()
        });
        let v = found.next()?;
        // equal canonical forms written differently are different nodes: ambiguous
        (found.next().is_none() && valid.edge(s, v).is_some()).then_some(v)
    }

    /// The assertions (`true`) and the retractions (`false`) of the edge `s -> o` in the layer of
    /// `root`, before the end of the view, in time order with the assertions first at an
    /// identical time (so the retraction wins), or `None` if there is no such edge.
    fn events(
        &self,
        root: &DynamicGraph,
        s: VID,
        o: VID,
    ) -> Option<impl Iterator<Item = (EventTime, bool)>> {
        let edge = root.edge(s, o)?;
        let horizon = self.horizon;
        let assertions = edge.history().into_iter().map(|t| (t, true));
        let retractions = edge.deletions().into_iter().map(|t| (t, false));
        Some(
            assertions
                .merge_by(retractions, |a, b| a.0 <= b.0)
                .take_while(move |(t, _)| horizon.is_none_or(|h| *t < h)),
        )
    }
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

/// The run that contains `t_ref`, given the events of a triple in time order, or `None` if the
/// triple does not hold at `t_ref`.
fn run_at(events: impl Iterator<Item = (EventTime, bool)>, t_ref: i64) -> Option<Run> {
    let mut events = events.peekable();
    let mut holds = false;
    let mut from = None;
    while let Some((time, mut after)) = events.next() {
        let t = time.t();
        // The state after the events of one time is the kind of its last event.
        while let Some((_, asserted)) = events.next_if(|(next, _)| next.t() == t) {
            after = asserted;
        }
        if t <= t_ref {
            if after && !holds {
                from = Some(t);
            }
            holds = after;
        } else if !holds {
            return None;
        } else if !after {
            return Some((from?, Some(t)));
        }
    }
    from.filter(|_| holds).map(|from| (from, None))
}

/// The time of a reference argument: a time graph IRI, an integer or an `xsd:dateTime` (in UTC
/// if it has no timezone).
fn ref_time(term: &Term) -> Option<i64> {
    match term {
        Term::NamedNode(iri) => TimeGraph::parse(iri).ok().flatten().map(|graph| graph.at),
        Term::Literal(literal) => match literal_to_prop(literal)? {
            Prop::I64(t) => Some(t),
            Prop::DTime(t) => Some(t.timestamp_millis()),
            Prop::NDTime(t) => Some(t.and_utc().timestamp_millis()),
            _ => None,
        },
        _ => None,
    }
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
