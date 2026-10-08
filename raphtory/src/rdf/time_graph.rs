//! Virtual named graphs for time travel inside a SPARQL query.
//!
//! The IRI `<raphtory:asof:T>` names the read-only graph of the triples visible as of time
//! `T`: inside `GRAPH <raphtory:asof:T> { .. }` (or with `FROM <raphtory:asof:T>`) a query sees
//! exactly what it would see on `view.snapshot_at(T)`. So one query can compare several times.
use crate::rdf::RdfError;
use oxigraph::model::{vocab::xsd, NamedNode};
use raphtory_api::core::{storage::timeindex::AsTime, utils::time::TryIntoTime};
use rustc_hash::FxHashSet;
use spargebra::{
    algebra::{AggregateExpression, Expression, Function, GraphPattern, OrderExpression},
    term::{GroundTerm, NamedNodePattern, TermPattern, TriplePattern},
    Query,
};

/// The IRI prefix of time graphs: `<raphtory:asof:T>` is the graph as of time `T`.
///
/// No node or layer name maps to an IRI under this prefix ([`term_of`](crate::rdf::term_of)
/// percent-encodes the `:` after `raphtory:`).
pub const ASOF_NS: &str = "raphtory:asof:";

/// A time graph: the virtual named graph `<raphtory:asof:T>` of the triples visible as of `T`.
///
/// In a query on a view, `GRAPH <raphtory:asof:T> { .. }` matches its patterns against
/// `view.snapshot_at(T)`: on a persistent graph the state as of `T`, on an event graph every
/// triple asserted at or before `T`. Windows intersect: on a persistent graph a time outside the
/// view's window gives an empty graph; on an event graph a time before the window's start gives
/// an empty graph, and one at or after its end every triple asserted in the window.
///
/// `T` is an integer (epoch milliseconds) or a date-time Raphtory parses: RFC 3339
/// (`2024-01-01T00:00:00Z`), `%Y-%m-%d` (midnight UTC) or `%Y-%m-%dT%H:%M:%S%.3f` (UTC). `T` is
/// used as written (no percent-decoding), so different spellings of an instant are different
/// graphs with the same triples. Queries can write `raphtory:asof:2024-01-01`.
///
/// Time graphs cannot be enumerated, so with
/// [`RdfViewOps::sparql`](crate::rdf::RdfViewOps::sparql) `GRAPH ?g { .. }` with an unbound
/// `?g` visits the `FROM NAMED` graphs or else the time graphs the query writes as constants
/// (see [`RaphtoryDataset::with_time_graphs`](crate::rdf::RaphtoryDataset::with_time_graphs)).
/// A computed IRI such as `IRI(CONCAT("raphtory:asof:", ?t))` only matches where `?g` is bound
/// before the pattern, e.g. `LATERAL { GRAPH ?g { .. } }` after the `BIND`.
///
/// Several `FROM <raphtory:asof:T>` graphs are concatenated, not merged, so use `DISTINCT` or
/// a single `FROM` / `GRAPH`.
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
    ///
    /// # Example
    /// ```
    /// use raphtory::rdf::{model::NamedNode, TimeGraph};
    ///
    /// let parse = |iri: &str| TimeGraph::parse(&NamedNode::new(iri).unwrap());
    /// let graph = parse("raphtory:asof:2024-01-01").unwrap().unwrap();
    /// assert_eq!(graph.at, 1_704_067_200_000);
    /// assert_eq!(parse("raphtory:asof:1704067200000").unwrap().unwrap().at, graph.at);
    /// assert!(parse("http://ex/g").unwrap().is_none());
    /// assert!(parse("raphtory:asof:yesterday").is_err());
    /// ```
    pub fn parse(iri: &NamedNode) -> Result<Option<Self>, RdfError> {
        let Some(time) = iri.as_str().strip_prefix(ASOF_NS) else {
            return Ok(None);
        };
        let at = match time.parse::<i64>() {
            Ok(at) => at,
            Err(_) => time
                .try_into_time()
                .map_err(|_| RdfError::InvalidTimeGraph {
                    iri: iri.as_str().to_owned(),
                    reason: format!(
                        "'{time}' is not a time: expected epoch milliseconds or a date-time \
                         such as 2024-01-01, 2024-01-01T00:00:00 or 2024-01-01T00:00:00Z"
                    ),
                })?
                .t(),
        };
        Ok(Some(Self {
            iri: iri.clone(),
            at,
        }))
    }
}

/// The time graphs a query names: every valid IRI under [`ASOF_NS`] written as a constant
/// anywhere in it (as an IRI or `IRI("raphtory:asof:T")` of a string), each once.
pub(crate) fn time_graphs_in(query: &Query) -> Vec<TimeGraph> {
    let mut found = Found::default();
    match query {
        Query::Select { pattern, .. }
        | Query::Ask { pattern, .. }
        | Query::Describe { pattern, .. } => found.pattern(pattern),
        Query::Construct {
            template, pattern, ..
        } => {
            template.iter().for_each(|triple| found.triple(triple));
            found.pattern(pattern);
        }
    }
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

    fn pattern(&mut self, pattern: &GraphPattern) {
        match pattern {
            GraphPattern::Bgp { patterns } => patterns.iter().for_each(|t| self.triple(t)),
            // the IRIs of a property path are predicates, never time graphs
            GraphPattern::Path {
                subject, object, ..
            } => {
                self.term(subject);
                self.term(object);
            }
            GraphPattern::Join { left, right }
            | GraphPattern::Lateral { left, right }
            | GraphPattern::Union { left, right }
            | GraphPattern::Minus { left, right } => {
                self.pattern(left);
                self.pattern(right);
            }
            GraphPattern::LeftJoin {
                left,
                right,
                expression,
            } => {
                self.pattern(left);
                self.pattern(right);
                if let Some(expression) = expression {
                    self.expression(expression);
                }
            }
            GraphPattern::Filter { expr, inner } => {
                self.expression(expr);
                self.pattern(inner);
            }
            GraphPattern::Graph { name, inner } | GraphPattern::Service { name, inner, .. } => {
                self.named_node(name);
                self.pattern(inner);
            }
            GraphPattern::Extend {
                inner, expression, ..
            } => {
                self.pattern(inner);
                self.expression(expression);
            }
            GraphPattern::Values { bindings, .. } => {
                for value in bindings.iter().flatten().flatten() {
                    if let GroundTerm::NamedNode(iri) = value {
                        self.iri(iri);
                    }
                }
            }
            GraphPattern::OrderBy { inner, expression } => {
                self.pattern(inner);
                for order in expression {
                    let (OrderExpression::Asc(e) | OrderExpression::Desc(e)) = order;
                    self.expression(e);
                }
            }
            GraphPattern::Group {
                inner, aggregates, ..
            } => {
                self.pattern(inner);
                for (_, aggregate) in aggregates {
                    if let AggregateExpression::FunctionCall { expr, .. } = aggregate {
                        self.expression(expr);
                    }
                }
            }
            GraphPattern::Project { inner, .. }
            | GraphPattern::Distinct { inner }
            | GraphPattern::Reduced { inner }
            | GraphPattern::Slice { inner, .. } => self.pattern(inner),
        }
    }

    fn expression(&mut self, expression: &Expression) {
        match expression {
            Expression::NamedNode(iri) => self.iri(iri),
            Expression::Literal(_) | Expression::Variable(_) | Expression::Bound(_) => {}
            // `IRI("raphtory:asof:T")` of a string is a constant too
            Expression::FunctionCall(Function::Iri, args) => {
                if let [Expression::Literal(literal)] = args.as_slice() {
                    if literal.datatype() == xsd::STRING {
                        if let Ok(iri) = NamedNode::new(literal.value()) {
                            self.iri(&iri);
                        }
                    }
                }
                args.iter().for_each(|e| self.expression(e));
            }
            Expression::Or(a, b)
            | Expression::And(a, b)
            | Expression::Equal(a, b)
            | Expression::SameTerm(a, b)
            | Expression::Greater(a, b)
            | Expression::GreaterOrEqual(a, b)
            | Expression::Less(a, b)
            | Expression::LessOrEqual(a, b)
            | Expression::Add(a, b)
            | Expression::Subtract(a, b)
            | Expression::Multiply(a, b)
            | Expression::Divide(a, b) => {
                self.expression(a);
                self.expression(b);
            }
            Expression::UnaryPlus(a) | Expression::UnaryMinus(a) | Expression::Not(a) => {
                self.expression(a)
            }
            Expression::In(a, list) => {
                self.expression(a);
                list.iter().for_each(|e| self.expression(e));
            }
            Expression::If(a, b, c) => {
                self.expression(a);
                self.expression(b);
                self.expression(c);
            }
            Expression::Coalesce(list) | Expression::FunctionCall(_, list) => {
                list.iter().for_each(|e| self.expression(e))
            }
            Expression::Exists(pattern) => self.pattern(pattern),
        }
    }
}
