//! Temporal SPARQL functions: since when a triple has held, and until when.
//!
//! [`with_temporal_functions`] registers four functions on a [`SparqlEvaluator`], answering for
//! the triples of one view. [`RdfViewOps::sparql`](crate::rdf::RdfViewOps::sparql) and
//! [`RdfViewOps::sparql_to_writer`](crate::rdf::RdfViewOps::sparql_to_writer) register them by
//! default (see [`SparqlOptions`](crate::rdf::SparqlOptions)).
use crate::{
    db::api::view::{
        internal::{CoreGraphOps, InternalMaterialize},
        DynamicGraph, IntoDynamic, StaticGraphViewOps,
    },
    prelude::*,
    rdf::{
        dataset::lookup_node,
        mapping::{literal_to_prop, name_of, term_of},
        time_graph::TimeGraph,
    },
};
use chrono::{DateTime, Datelike, Utc};
use oxigraph::{
    model::{vocab::xsd, Literal, NamedNode, Term},
    sparql::SparqlEvaluator,
};
use raphtory_api::{
    core::{
        entities::{properties::meta::STATIC_GRAPH_LAYER_ID, VID},
        storage::timeindex::{AsTime, EventTime},
    },
    GraphType,
};
use spareval::ExpressionTerm;
use std::sync::Arc;

/// `raphtory:validFrom(?s, ?p, ?o [, ?ref])`: the start of the validity interval of a triple, as
/// an `xsd:dateTime` (see [`with_temporal_functions`]).
pub const VALID_FROM: &str = "raphtory:validFrom";

/// `raphtory:validTo(?s, ?p, ?o [, ?ref])`: the end of the validity interval of a triple (the
/// time it was retracted), as an `xsd:dateTime` (see [`with_temporal_functions`]).
pub const VALID_TO: &str = "raphtory:validTo";

/// `raphtory:validFromTime(?s, ?p, ?o [, ?ref])`: [`VALID_FROM`] as an `xsd:integer`, the
/// Raphtory time (epoch milliseconds).
pub const VALID_FROM_TIME: &str = "raphtory:validFromTime";

/// `raphtory:validToTime(?s, ?p, ?o [, ?ref])`: [`VALID_TO`] as an `xsd:integer`, the Raphtory
/// time (epoch milliseconds).
pub const VALID_TO_TIME: &str = "raphtory:validToTime";

/// Registers the temporal functions [`VALID_FROM`], [`VALID_TO`], [`VALID_FROM_TIME`] and
/// [`VALID_TO_TIME`] on `evaluator`, answering for the triples of `view` (the view given to
/// [`RaphtoryDataset::new`](crate::rdf::RaphtoryDataset::new)).
///
/// Each function takes a triple `?s ?p ?o` and an optional reference time `?ref`, and returns
/// one end of the *validity interval* (the run) of the triple that contains the reference time:
/// `validFrom` when it started to hold and `validTo` when it was retracted. `validFrom` and
/// `validTo` return an `xsd:dateTime` (UTC, milliseconds), `validFromTime` and `validToTime` the
/// Raphtory time as an `xsd:integer`.
///
/// ```sparql
/// SELECT ?who ?since { ?who ex:worksFor ex:acme BIND(raphtory:validFrom(?who, ex:worksFor, ex:acme) AS ?since) }
/// ```
///
/// # Arguments
///
/// - `?s ?p ?o` are the terms of the triple. SPARQL passes xsd value literals in canonical form,
///   so a literal object is matched by canonical form among the objects of `?s` in layer `?p`
///   (another datatype or timezone does not match; several matches give an unbound result).
///   Pass the object bound by the pattern. A literal subject or predicate gives an unbound
///   result.
/// - `?ref`, if given, is a time graph IRI `<raphtory:asof:T>` (see [`TimeGraph`]), an integer
///   (epoch milliseconds) or an `xsd:dateTime` (UTC if it has no timezone). Without it, the
///   reference time is the end of the view.
///
/// Any other argument, or a call with other than 3 or 4 arguments, gives an unbound result. The
/// functions never make a query fail.
///
/// # Semantics
///
/// Let `H` be the end of the view (no end for a view without a time window) and `R` the graph of
/// the view without its window and filters (with the view's [`GraphType`]), keeping only the
/// events before `H`.
///
/// 1. **Scope.** The result is unbound unless the triple is visible in the view (without
///    `?ref`) or in `view.snapshot_at(T)` (with `?ref` `T`), so hidden triples are never
///    revealed. Same window rules as `GRAPH <raphtory:asof:T>` on the view (see [`TimeGraph`]).
/// 2. **Run.** The largest interval `[from, to)` containing the reference time (capped at `H`)
///    in which the triple is in `R.snapshot_at(t)`. `validTo` is unbound if the run is still
///    open at `H` (so always without `?ref`). History before the window counts; events at or
///    after `H` do not.
/// 3. **Persistent graphs.** A triple holds at `t` if its latest event at or before `t` (by
///    time, then event id) is an assertion; a retraction and an assertion with the identical
///    time and event id count as a retraction. So a duplicate assertion does not restart a run.
/// 4. **Event graphs.** A triple holds from its first assertion on, so `validTo` is always
///    unbound (use `persistent_graph()` for runs that end at retractions).
/// 5. **Return values.** `validFrom` and `validTo` are unbound outside the years 1 to 9999;
///    `validFromTime` and `validToTime` still answer.
///
/// Functions cannot see the active `GRAPH` or `FROM`: a 3-argument call inside
/// `GRAPH <raphtory:asof:T> { .. }` uses the present of the view, not `T`. Pass the graph as
/// `?ref` instead, in a `BIND` after the `GRAPH` pattern (inside it `?g` is not bound yet):
///
/// ```sparql
/// SELECT ?who ?since ?until FROM NAMED raphtory:asof:2023-01-01 {
///   GRAPH ?g { ?who ex:worksFor ex:acme }
///   BIND(raphtory:validFrom(?who, ex:worksFor, ex:acme, ?g) AS ?since)
///   BIND(raphtory:validTo(?who, ex:worksFor, ex:acme, ?g) AS ?until)
/// }
/// ```
///
/// Results are computed when the function is called, with no snapshot isolation (query a
/// `read_only()` view for a consistent result).
///
/// # Example
/// ```
/// use raphtory::{
///     prelude::*,
///     rdf::{evaluator, model::Term, with_temporal_functions, QueryResults, RaphtoryDataset},
/// };
///
/// let pg = PersistentGraph::new();
/// pg.add_edge(1, "Alice", "Acme", NO_PROPS, Some("worksFor")).unwrap();
/// pg.delete_edge(5, "Alice", "Acme", Some("worksFor")).unwrap();
/// pg.add_edge(9, "Alice", "Acme", NO_PROPS, Some("worksFor")).unwrap();
///
/// let query = "SELECT ?from ?to {
///     VALUES ?t { 3 10 }
///     BIND(raphtory:validFromTime(raphtory:Alice, raphtory:worksFor, raphtory:Acme, ?t) AS ?from)
///     BIND(raphtory:validToTime(raphtory:Alice, raphtory:worksFor, raphtory:Acme, ?t) AS ?to)
/// }";
/// let QueryResults::Solutions(solutions) =
///     with_temporal_functions(evaluator(), pg.clone())
///         .parse_query(query)
///         .unwrap()
///         .on_queryable_dataset(RaphtoryDataset::new(pg.clone()))
///         .execute()
///         .unwrap()
/// else {
///     unreachable!()
/// };
/// let rows: Vec<Vec<Option<String>>> = solutions
///     .map(|solution| {
///         let solution = solution.unwrap();
///         ["from", "to"]
///             .map(|v| solution.get(v).map(Term::to_string))
///             .to_vec()
///     })
///     .collect();
/// let int = |t: i64| Some(format!("\"{t}\"^^<http://www.w3.org/2001/XMLSchema#integer>"));
/// // at 3 the triple holds from 1 until 5; at 10 it holds from 9 on
/// assert_eq!(rows, [[int(1), int(5)], [int(9), None]]);
/// ```
pub fn with_temporal_functions<G: StaticGraphViewOps + IntoDynamic>(
    evaluator: SparqlEvaluator,
    view: G,
) -> SparqlEvaluator {
    let temporal = Arc::new(Temporal::new(view.into_dynamic()));
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
    /// The graph of the view without window and filters, of the same graph type.
    root: DynamicGraph,
    /// The end of the view: only the events before it count.
    horizon: Option<EventTime>,
    /// Whether retractions end runs.
    persistent: bool,
}

impl Temporal {
    fn new(base: DynamicGraph) -> Self {
        // cloning the storage is cheap: it is reference counted, also when it is locked
        let root = base
            .new_base_graph(base.core_graph().clone())
            .into_dynamic();
        let horizon = base.end();
        let persistent = base.graph_type() == GraphType::PersistentGraph;
        Self {
            base,
            root,
            horizon,
            persistent,
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
            .get_layer_id(&name_of(p.as_ref())?)
            .filter(|l| *l != STATIC_GRAPH_LAYER_ID)?;
        let layer = Layer::One(self.base.get_layer_name(layer));
        // The scope is what SPARQL sees: the view (as of the reference time) in the layer.
        let scope = match at {
            None => self.base.clone(),
            Some(t) => self.base.snapshot_at(t).into_dynamic(),
        };
        let scope = scope.valid_layers(layer.clone()).valid().into_dynamic();
        let root = self.root.valid_layers(layer).into_dynamic();
        let o = self.object(&scope, &root, s, o)?;
        // The state of `R` is constant from the end of the view on.
        let horizon = self.horizon.map(|h| h.t());
        let t_ref = match (at, horizon) {
            (Some(t), Some(h)) => t.min(h),
            (Some(t), None) => t,
            (None, Some(h)) => h,
            (None, None) => i64::MAX,
        };
        run_at(self.events(&root, s, o), t_ref)
    }

    /// The node of the object `o` of `s`, among the objects of `s` visible in `scope`.
    fn object(&self, scope: &DynamicGraph, root: &DynamicGraph, s: VID, o: &Term) -> Option<VID> {
        let visible = |v: VID| scope.edge(s, v).is_some();
        if passed_unchanged(o) {
            let v = lookup_node(&self.base, &name_of(o.as_ref())?)?;
            return visible(v).then_some(v);
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
            Term::from(ExpressionTerm::from(term_of(&root.node_name(*v)))) == *o && visible(*v)
        });
        let v = found.next()?;
        // equal canonical forms written differently are different nodes: ambiguous
        found.next().is_none().then_some(v)
    }

    /// The assertions (`true`) and, on a persistent graph, the retractions (`false`) of the edge
    /// `s -> o` in the layer of `root`, before the end of the view, sorted by time with the
    /// assertions first at an identical time (so the retraction wins).
    fn events(&self, root: &DynamicGraph, s: VID, o: VID) -> Vec<(EventTime, bool)> {
        let Some(edge) = root.edge(s, o) else {
            return Vec::new();
        };
        let mut events: Vec<_> = edge.history().iter().map(|t| (t, true)).collect();
        if self.persistent {
            events.extend(edge.deletions().iter().map(|t| (t, false)));
        }
        events.retain(|(t, _)| self.horizon.is_none_or(|h| *t < h));
        events.sort_unstable_by_key(|&(t, asserted)| (t, !asserted));
        events
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

/// The run that contains `t_ref`, given the sorted events of a triple, or `None` if the triple
/// does not hold at `t_ref`.
fn run_at(events: Vec<(EventTime, bool)>, t_ref: i64) -> Option<Run> {
    let mut holds = false;
    let mut from = None;
    // The state after the events of one time is the kind of its last event.
    for group in events.chunk_by(|(a, _), (b, _)| a.t() == b.t()) {
        let t = group[0].0.t();
        let after = group[group.len() - 1].1;
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
