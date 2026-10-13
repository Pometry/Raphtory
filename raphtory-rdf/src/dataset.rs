//! The SPARQL dataset of a graph view.
use crate::{
    mapping::{layer_name_of, layer_term, name_of, term_of},
    scan::matching,
    time_graph::TimeGraph,
    RdfError,
};
use oxigraph::model::Term;
use raphtory::{
    db::{
        api::view::{BoxedIter, DynamicGraph},
        graph::{edge::EdgeView, node::NodeView},
    },
    prelude::*,
};
use raphtory_api::core::{
    entities::{LayerId, VID},
    storage::arc_str::ArcStr,
};
use rustc_hash::FxHashMap;
use spareval::{InternalQuad, QueryableDataset};
use std::{
    cell::RefCell,
    fmt,
    hash::{Hash, Hasher},
    rc::Rc,
    sync::Arc,
};

/// Maximum number of entries in the per-dataset cache of internal terms.
const CACHE_CAP: usize = 1 << 20;

/// The internal form of an RDF term while a SPARQL query runs on a [`RaphtoryDataset`].
///
/// Every RDF term has exactly one internal value, so equality of internal terms is SPARQL
/// `sameTerm`. In order of precedence, a term is:
/// 1. [`Node`](RdfTerm::Node) if it is the term ([`term_of`]) of a node of the view,
/// 2. otherwise [`Layer`](RdfTerm::Layer) if it is the term ([`layer_term`]) of a layer of the
///    view,
/// 3. otherwise [`TimeGraph`](RdfTerm::TimeGraph) if it is an IRI under
///    [`ASOF_NS`](crate::ASOF_NS). One whose time does not parse is an error,
///    [`RdfError::InvalidTimeGraph`], which aborts the query,
/// 4. otherwise [`Other`](RdfTerm::Other).
///
/// So a predicate IRI that is also a node's term is the `Node` in every position, and the two
/// join.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) enum RdfTerm {
    /// A node of the view. Its RDF term is `term_of(node_name)`.
    Node(NodeTerm),
    /// A layer whose term is not also the term of a node. Its RDF term is
    /// `layer_term(layer_name)`.
    Layer(ArcStr),
    /// A time graph `<raphtory:asof:T>`. Its RDF term is its IRI, as written in the query.
    TimeGraph(Arc<TimeGraph>),
    /// A term that names neither a node nor a layer of the view nor a time graph, for example
    /// a constant of the query that is not in the data.
    Other(Term),
}

/// A node of the view; equal to the same node read from any view of the graph.
#[derive(Clone)]
pub(crate) struct NodeTerm(pub(crate) NodeView<'static, DynamicGraph>);

impl NodeTerm {
    fn vid(&self) -> VID {
        self.0.node
    }
}

impl PartialEq for NodeTerm {
    fn eq(&self, other: &Self) -> bool {
        self.vid() == other.vid()
    }
}

impl Eq for NodeTerm {}

impl Hash for NodeTerm {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.vid().hash(state)
    }
}

impl fmt::Debug for NodeTerm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NodeTerm({:?})", self.vid())
    }
}

/// A graph view as a SPARQL dataset, for use with an [`evaluator`](crate::query::evaluator)
/// (`evaluator().for_query(parse(q)?).on_queryable_dataset(RaphtoryDataset::new(view))`).
///
/// - The default graph holds one triple per `(edge, layer)` pair visible in the view:
///   `term_of(src) layer_term(layer) term_of(dst)`.
/// - The only named graphs are the time graphs ([`TimeGraph`]).
/// - Generalized triples (literal subject, or non-IRI layer term) are matched like any other.
///   `CONSTRUCT` drops them, and `DESCRIBE` can be incomplete on such views.
///
/// A dataset is meant for one query.
pub(crate) struct RaphtoryDataset {
    inner: Rc<Inner>,
    /// The time graphs `GRAPH ?g` visits when `?g` is unbound.
    named: Vec<Arc<TimeGraph>>,
}

/// What the scans of a [`RaphtoryDataset`] share with it.
struct Inner {
    /// The caller's view.
    base: DynamicGraph,
    /// `internalize_term`, memoised (up to [`CACHE_CAP`] entries).
    memo: RefCell<FxHashMap<Term, RdfTerm>>,
    /// The predicate term of each layer read so far.
    predicates: RefCell<FxHashMap<Option<LayerId>, RdfTerm>>,
}

/// The bound terms of a quad pattern, resolved: the subject and object nodes and the layer of
/// the predicate (`None`: unbound).
struct Bound {
    subject: Option<VID>,
    layer: Option<ArcStr>,
    object: Option<VID>,
}

impl RaphtoryDataset {
    /// Builds the dataset of `view`, listing no time graph.
    pub(crate) fn new(base: DynamicGraph) -> Self {
        Self {
            inner: Rc::new(Inner {
                base,
                memo: RefCell::default(),
                predicates: RefCell::default(),
            }),
            named: Vec::new(),
        }
    }

    /// Lists `graphs` as the named graphs `GRAPH ?g { .. }` visits with `?g` unbound when the
    /// query has no `FROM NAMED`. Unlisted time graphs still match when named explicitly.
    pub(crate) fn with_time_graphs(mut self, graphs: Vec<TimeGraph>) -> Self {
        self.named = graphs.into_iter().map(Arc::new).collect();
        self
    }

    /// The bound terms of a pattern, or `None` if it can match nothing: a bound subject or
    /// object must be a node, and a bound predicate a layer.
    fn bind(
        &self,
        subject: Option<&RdfTerm>,
        predicate: Option<&RdfTerm>,
        object: Option<&RdfTerm>,
    ) -> Option<Bound> {
        Some(Bound {
            subject: match subject {
                Some(t) => Some(node_of(t)?),
                None => None,
            },
            layer: match predicate {
                Some(t) => Some(layer_of(t)?),
                None => None,
            },
            object: match object {
                Some(t) => Some(node_of(t)?),
                None => None,
            },
        })
    }

    /// The quads matching a pattern in the default graph (`graph = None`) or in a time graph.
    fn scan(&self, bound: &Bound, graph: Option<&Arc<TimeGraph>>) -> QuadIter {
        let view = match graph {
            None => self.inner.base.clone(),
            Some(graph) => graph.view_of(&self.inner.base),
        };
        QuadIter {
            edges: matching(&view, bound.subject, bound.layer.as_deref(), bound.object),
            dataset: self.inner.clone(),
            graph_name: graph.map(|graph| RdfTerm::TimeGraph(graph.clone())),
        }
    }

    /// The quads matching a pattern: one scan per graph it reads.
    fn quads(
        &self,
        subject: Option<&RdfTerm>,
        predicate: Option<&RdfTerm>,
        object: Option<&RdfTerm>,
        graph_name: Option<Option<&RdfTerm>>,
    ) -> Vec<QuadIter> {
        let Some(bound) = self.bind(subject, predicate, object) else {
            return Vec::new();
        };
        match graph_name {
            Some(None) => vec![self.scan(&bound, None)],
            Some(Some(RdfTerm::TimeGraph(graph))) => vec![self.scan(&bound, Some(graph))],
            // Other named graphs are never stored.
            Some(Some(_)) => Vec::new(),
            // Every named graph: time graphs cannot be enumerated, so the listed ones.
            None => self
                .named
                .iter()
                .map(|graph| self.scan(&bound, Some(graph)))
                .collect(),
        }
    }
}

impl Inner {
    fn internalize(&self, term: Term) -> Result<RdfTerm, RdfError> {
        if let Some(internal) = self.memo.borrow().get(&term) {
            return Ok(internal.clone());
        }
        let internal = self.resolve(&term)?;
        let mut memo = self.memo.borrow_mut();
        if memo.len() < CACHE_CAP {
            memo.insert(term, internal.clone());
        }
        Ok(internal)
    }

    fn resolve(&self, term: &Term) -> Result<RdfTerm, RdfError> {
        if let Some(node) = name_of(term.as_ref()).and_then(|name| self.base.node(name)) {
            return Ok(RdfTerm::Node(NodeTerm(node)));
        }
        if let Some(layer) =
            layer_name_of(term.as_ref()).filter(|name| self.base.has_layer(name.as_str()))
        {
            return Ok(RdfTerm::Layer(layer.into()));
        }
        if let Term::NamedNode(iri) = term {
            if let Some(graph) = TimeGraph::parse(iri)? {
                return Ok(RdfTerm::TimeGraph(Arc::new(graph)));
            }
        }
        // Unknown terms are accepted: they are constants or computed values of the query.
        Ok(RdfTerm::Other(term.clone()))
    }

    /// The predicate term of the layer of `edge`, so the internal value of its term.
    fn predicate(&self, edge: &EdgeView<DynamicGraph>) -> Result<RdfTerm, RdfError> {
        let layer = edge.edge.layer();
        if let Some(predicate) = self.predicates.borrow().get(&layer) {
            return Ok(predicate.clone());
        }
        let predicate = self.internalize(layer_term(&edge.layer_name()?))?;
        self.predicates
            .borrow_mut()
            .insert(layer, predicate.clone());
        Ok(predicate)
    }
}

fn node_of(term: &RdfTerm) -> Option<VID> {
    match term {
        RdfTerm::Node(node) => Some(node.vid()),
        _ => None,
    }
}

/// The layer a bound predicate stands for, if any.
fn layer_of(predicate: &RdfTerm) -> Option<ArcStr> {
    match predicate {
        RdfTerm::Layer(layer) => Some(layer.clone()),
        RdfTerm::Node(node) => layer_name_of(term_of(&node.0.name()).as_ref()).map(Into::into),
        RdfTerm::TimeGraph(_) | RdfTerm::Other(_) => None,
    }
}

/// Turns the matching `(edge, layer)` pairs into quads. Owns everything it uses.
struct QuadIter {
    edges: BoxedIter<EdgeView<DynamicGraph>>,
    dataset: Rc<Inner>,
    graph_name: Option<RdfTerm>,
}

impl Iterator for QuadIter {
    type Item = Result<InternalQuad<RdfTerm>, RdfError>;

    fn next(&mut self) -> Option<Self::Item> {
        let edge = self.edges.next()?;
        Some(self.dataset.predicate(&edge).map(|predicate| InternalQuad {
            subject: RdfTerm::Node(NodeTerm(edge.src())),
            predicate,
            object: RdfTerm::Node(NodeTerm(edge.dst())),
            graph_name: self.graph_name.clone(),
        }))
    }
}

impl<'a> QueryableDataset<'a> for RaphtoryDataset {
    type InternalTerm = RdfTerm;
    type Error = RdfError;

    fn internal_quads_for_pattern(
        &self,
        subject: Option<&RdfTerm>,
        predicate: Option<&RdfTerm>,
        object: Option<&RdfTerm>,
        graph_name: Option<Option<&RdfTerm>>,
    ) -> impl Iterator<Item = Result<InternalQuad<RdfTerm>, RdfError>> + use<'a> {
        self.quads(subject, predicate, object, graph_name)
            .into_iter()
            .flatten()
    }

    /// The listed time graphs: time graphs cannot be enumerated.
    fn internal_named_graphs(&self) -> impl Iterator<Item = Result<RdfTerm, RdfError>> + use<'a> {
        let graphs: Vec<_> = self
            .named
            .iter()
            .map(|graph| Ok(RdfTerm::TimeGraph(graph.clone())))
            .collect();
        graphs.into_iter()
    }

    /// Every time graph exists (it may be empty); no other named graph does.
    fn contains_internal_graph_name(&self, graph_name: &RdfTerm) -> Result<bool, RdfError> {
        Ok(matches!(graph_name, RdfTerm::TimeGraph(_)))
    }

    fn internalize_term(&self, term: Term) -> Result<RdfTerm, RdfError> {
        self.inner.internalize(term)
    }

    fn externalize_term(&self, term: RdfTerm) -> Result<Term, RdfError> {
        Ok(match term {
            RdfTerm::Node(node) => term_of(&node.0.name()),
            RdfTerm::Layer(layer) => layer_term(&layer),
            RdfTerm::TimeGraph(graph) => graph.iri.clone().into(),
            RdfTerm::Other(t) => t,
        })
    }
}
