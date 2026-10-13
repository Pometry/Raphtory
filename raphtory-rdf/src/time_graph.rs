//! Virtual named graphs for time travel inside a SPARQL query.
use crate::{
    algebra::{walk, Node},
    RdfError,
};
use oxigraph::model::{vocab::xsd, NamedNode};
use raphtory::{
    db::api::view::{DynamicGraph, IntoDynamic},
    prelude::*,
};
use raphtory_api::core::{
    storage::timeindex::AsTime,
    utils::time::{ParseTimeError, TryIntoTime},
};
use rustc_hash::FxHashSet;
use spargebra::{
    algebra::{Expression, Function, GraphPattern},
    term::{GroundTerm, NamedNodePattern, TermPattern, TriplePattern},
    Query,
};

/// The IRI prefix of time graphs: `<raphtory:asof:T>` is the graph as of time `T`.
///
/// No node or layer name maps to an IRI under this prefix: their terms percent-encode the `:`
/// after `raphtory:` (see [`term_of`](crate::term_of)).
pub const ASOF_NS: &str = "raphtory:asof:";

/// A time graph: the virtual named graph `<raphtory:asof:T>` of the triples that hold at `T`.
///
/// In a query on a view, `GRAPH <raphtory:asof:T> { .. }` (or `FROM <raphtory:asof:T>`) matches
/// its patterns against `view.before(T + 1).valid()`, so every filter and window of the view
/// applies. On a `valid()` view a time graph holds only the triples that hold at `T` and still
/// hold at the end of the view, so compare times on a view without `valid()`.
///
/// `T` is an integer (epoch milliseconds) or a date-time Raphtory parses: RFC 3339
/// (`2024-01-01T00:00:00Z`), `%Y-%m-%d` (midnight UTC) or `%Y-%m-%dT%H:%M:%S%.3f` (UTC). `T` is
/// used as written (no percent-decoding), so different spellings of an instant are different
/// graphs with the same triples. Queries can write `raphtory:asof:2024-01-01`.
///
/// Time graphs cannot be enumerated, so with [`RdfView::sparql`](crate::RdfView::sparql)
/// `GRAPH ?g { .. }` with an unbound `?g` visits the `FROM NAMED` graphs or else the time graphs
/// the query writes as constants (as in `VALUES ?g { .. }`). A computed IRI such as
/// `IRI(CONCAT("raphtory:asof:", ?t))` only matches where `?g` is bound before the pattern,
/// e.g. `LATERAL { GRAPH ?g { .. } }` after the `BIND`.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct TimeGraph {
    /// The IRI of the graph, as written in the query.
    pub iri: NamedNode,
    /// The time `T` of the IRI, in milliseconds since the Unix epoch.
    pub at: i64,
}

impl TimeGraph {
    /// Parses a time-graph IRI.
    ///
    /// Returns `Ok(None)` if `iri` is not under [`ASOF_NS`], `Ok(Some(_))` if its time parses
    /// (as an `i64` of epoch milliseconds, or as a date-time, see [`TimeGraph`]) and
    /// [`RdfError::InvalidTimeGraph`] otherwise.
    pub fn parse(iri: &NamedNode) -> Result<Option<Self>, RdfError> {
        let Some(time) = iri.as_str().strip_prefix(ASOF_NS) else {
            return Ok(None);
        };
        let at = Self::parse_time(time).map_err(|error| RdfError::InvalidTimeGraph {
            iri: iri.clone(),
            error,
        })?;
        Ok(Some(Self {
            iri: iri.clone(),
            at,
        }))
    }

    /// Parses the time `T` of a time graph `<raphtory:asof:T>`: an `i64` of epoch milliseconds,
    /// or a date-time (see [`TimeGraph`]).
    pub fn parse_time(time: &str) -> Result<i64, ParseTimeError> {
        match time.parse::<i64>() {
            Ok(at) => Ok(at),
            Err(_) => Ok(time.try_into_time()?.t()),
        }
    }

    /// The view of this time graph in a query on `view`.
    pub(crate) fn view_of(&self, view: &DynamicGraph) -> DynamicGraph {
        view.before(self.at.saturating_add(1))
            .valid()
            .into_dynamic()
    }
}

/// The time graphs a query names: every valid IRI under [`ASOF_NS`] written as a constant
/// anywhere in it (as an IRI or `IRI("raphtory:asof:T")` of a string), each once.
pub(crate) fn time_graphs_in(query: &Query) -> Vec<TimeGraph> {
    let mut found = Found::default();
    if let Query::Construct { template, .. } = query {
        template.iter().for_each(|triple| found.triple(triple));
    }
    walk(query, |node| match node {
        Node::Pattern(pattern) => found.pattern(pattern),
        Node::Expression(expression) => found.expression(expression),
        // the IRIs of a property path are predicates, never time graphs
        Node::Path(_) => {}
    });
    found.graphs
}

/// The time graphs found so far by [`time_graphs_in`].
#[derive(Default)]
struct Found {
    graphs: Vec<TimeGraph>,
    seen: FxHashSet<NamedNode>,
}

impl Found {
    fn iri(&mut self, iri: &NamedNode) {
        if let Ok(Some(graph)) = TimeGraph::parse(iri) {
            if self.seen.insert(iri.clone()) {
                self.graphs.push(graph);
            }
        }
    }

    fn term(&mut self, term: &TermPattern) {
        if let TermPattern::NamedNode(iri) = term {
            self.iri(iri);
        }
    }

    fn named_node(&mut self, pattern: &NamedNodePattern) {
        if let NamedNodePattern::NamedNode(iri) = pattern {
            self.iri(iri);
        }
    }

    fn triple(&mut self, triple: &TriplePattern) {
        self.term(&triple.subject);
        self.named_node(&triple.predicate);
        self.term(&triple.object);
    }

    /// The constants of `pattern` itself, not of the patterns and expressions inside it.
    fn pattern(&mut self, pattern: &GraphPattern) {
        match pattern {
            GraphPattern::Bgp { patterns } => patterns.iter().for_each(|t| self.triple(t)),
            GraphPattern::Path {
                subject, object, ..
            } => {
                self.term(subject);
                self.term(object);
            }
            GraphPattern::Graph { name, .. } | GraphPattern::Service { name, .. } => {
                self.named_node(name)
            }
            GraphPattern::Values { bindings, .. } => {
                for value in bindings.iter().flatten().flatten() {
                    if let GroundTerm::NamedNode(iri) = value {
                        self.iri(iri);
                    }
                }
            }
            GraphPattern::Join { .. }
            | GraphPattern::LeftJoin { .. }
            | GraphPattern::Lateral { .. }
            | GraphPattern::Filter { .. }
            | GraphPattern::Union { .. }
            | GraphPattern::Extend { .. }
            | GraphPattern::Minus { .. }
            | GraphPattern::OrderBy { .. }
            | GraphPattern::Project { .. }
            | GraphPattern::Distinct { .. }
            | GraphPattern::Reduced { .. }
            | GraphPattern::Slice { .. }
            | GraphPattern::Group { .. } => {}
        }
    }

    /// The constants of `expression` itself, not of the expressions inside it.
    fn expression(&mut self, expression: &Expression) {
        match expression {
            Expression::NamedNode(iri) => self.iri(iri),
            // `IRI("raphtory:asof:T")` of a string is a constant too
            Expression::FunctionCall(Function::Iri, args) => {
                if let [Expression::Literal(literal)] = args.as_slice() {
                    if literal.datatype() == xsd::STRING {
                        if let Ok(iri) = NamedNode::new(literal.value()) {
                            self.iri(&iri);
                        }
                    }
                }
            }
            Expression::Literal(_)
            | Expression::Variable(_)
            | Expression::Or(..)
            | Expression::And(..)
            | Expression::Equal(..)
            | Expression::SameTerm(..)
            | Expression::Greater(..)
            | Expression::GreaterOrEqual(..)
            | Expression::Less(..)
            | Expression::LessOrEqual(..)
            | Expression::In(..)
            | Expression::Add(..)
            | Expression::Subtract(..)
            | Expression::Multiply(..)
            | Expression::Divide(..)
            | Expression::UnaryPlus(_)
            | Expression::UnaryMinus(_)
            | Expression::Not(_)
            | Expression::Exists(_)
            | Expression::Bound(_)
            | Expression::If(..)
            | Expression::Coalesce(_)
            | Expression::FunctionCall(..) => {}
        }
    }
}
