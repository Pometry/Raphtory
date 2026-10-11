//! The SPARQL dataset of a graph view.
use crate::{
    mapping::{layer_name_of, layer_term, name_of, term_of},
    scan::{view_layers, EdgeScan},
    time_graph::TimeGraph,
    RdfError,
};
use oxigraph::model::Term;
use raphtory::{
    core::entities::{nodes::node_ref::NodeRef, GidRef},
    db::api::view::{internal::CoreGraphOps, DynamicGraph, IntoDynamic},
    prelude::*,
};
use raphtory_api::core::entities::{properties::meta::STATIC_GRAPH_LAYER_ID, LayerId, VID};
use raphtory_storage::graph::nodes::node_storage_ops::NodeStorageOps;
use rustc_hash::FxHashMap;
use spareval::{InternalQuad, QueryableDataset};
use std::{cell::RefCell, sync::Arc};

/// Maximum number of entries in the per-dataset cache of internal terms.
const CACHE_CAP: usize = 1 << 20;

/// The internal form of an RDF term while a SPARQL query runs on a [`RaphtoryDataset`].
///
/// Every RDF term has exactly one internal value, so equality of internal terms is SPARQL
/// `sameTerm`. In order of precedence, a term is:
/// 1. [`Node`](RdfTerm::Node) if it is the term of a node name ([`term_of`]),
/// 2. otherwise [`Layer`](RdfTerm::Layer) if it is the term of a layer name ([`layer_term`]),
/// 3. otherwise [`TimeGraph`](RdfTerm::TimeGraph) if it is an IRI under
///    [`ASOF_NS`](crate::ASOF_NS). One whose time does not parse is an error,
///    [`RdfError::InvalidTimeGraph`], which aborts the query,
/// 4. otherwise [`Other`](RdfTerm::Other).
///
/// So a predicate IRI that is also a node name is the `Node` in every position, and the two join.
/// The internal value of each layer's term is fixed when the [`RaphtoryDataset`] is built.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) enum RdfTerm {
    /// A node of the graph. Its RDF term is `term_of(node_name)`.
    Node(VID),
    /// A layer whose term is not also the term of a node. Its RDF term is
    /// `layer_term(layer_name)`.
    Layer(LayerId),
    /// A time graph `<raphtory:asof:T>`. Its RDF term is its IRI, as written in the query.
    TimeGraph(Arc<TimeGraph>),
    /// A term that names neither a node nor a layer of the graph nor a time graph, for example
    /// a constant of the query that is not in the data.
    Other(Term),
}

/// A graph view as a SPARQL dataset, for use with an [`evaluator`](crate::query::evaluator)
/// (`evaluator().for_query(parse(q)?).on_queryable_dataset(RaphtoryDataset::new(view))`).
///
/// - The default graph holds one triple per `(edge, layer)` pair visible in the view:
///   `term_of(src) layer_term(layer) term_of(dst)`.
/// - The only named graphs are the time graphs ([`TimeGraph`]).
/// - Generalized triples (literal subject, or non-IRI layer term) are matched like any other.
///   `CONSTRUCT` drops them, and `DESCRIBE` can be incomplete on such views.
/// - No storage lock is held between results, so the graph can be written to during a query,
///   with no snapshot isolation: writes may or may not be seen, and triples in layers created
///   after the dataset was built are skipped. For a consistent result query a view of
///   `graph.read_only()` (writers wait until it is dropped; writing on the same thread
///   deadlocks).
///
/// A dataset is meant for one query. It caches internal terms, so a term keeps the same
/// internal value for the whole query even while the graph is written to.
pub(crate) struct RaphtoryDataset {
    /// The caller's view.
    base: DynamicGraph,
    preds: Arc<PredTable>,
    /// The time graphs `GRAPH ?g` visits when `?g` is unbound.
    named: Vec<Arc<TimeGraph>>,
    /// `internalize_term`, memoised (up to [`CACHE_CAP`] entries).
    memo: RefCell<FxHashMap<Term, RdfTerm>>,
}

/// The predicate term of every layer of the view, fixed when the dataset is built.
#[derive(Default)]
struct PredTable {
    /// `Node(v)` if a node has the layer's term, else `Layer(l)` (the same precedence as
    /// `internalize_term`).
    term: FxHashMap<LayerId, RdfTerm>,
    /// Layers whose term is a node's term, by that node.
    by_node: FxHashMap<VID, LayerId>,
}

impl PredTable {
    fn new(g: &DynamicGraph) -> Self {
        let mut table = Self::default();
        for (l, name) in view_layers(g) {
            let node = name_of(layer_term(&name).as_ref()).and_then(|node| lookup_node(g, &node));
            let term = match node {
                Some(v) => {
                    table.by_node.insert(v, l);
                    RdfTerm::Node(v)
                }
                None => RdfTerm::Layer(l),
            };
            table.term.insert(l, term);
        }
        table
    }
}

/// The node named `name`, ignoring the filters of the view.
pub(crate) fn lookup_node(g: &DynamicGraph, name: &str) -> Option<VID> {
    let v = g.internalise_node(NodeRef::External(GidRef::Str(name)))?;
    // A locked view may not hold a node whose name was resolved after it was locked (reading
    // its name would panic).
    let node = g.core_graph().try_core_node(v)?;
    // On a u64-id graph a string that is not a number is looked up by its hash, so check the
    // name of what was found.
    (node.as_ref().name() == name).then_some(v)
}

/// The bound terms of a quad pattern, resolved: the subject and object nodes and the layer of
/// the predicate (`None`: unbound).
#[derive(Clone, Copy)]
struct Bound {
    subject: Option<VID>,
    layer: Option<LayerId>,
    object: Option<VID>,
}

impl RaphtoryDataset {
    /// Builds the dataset of `view`, listing no time graph.
    pub(crate) fn new(base: DynamicGraph) -> Self {
        let preds = Arc::new(PredTable::new(&base));
        Self {
            base,
            preds,
            named: Vec::new(),
            memo: RefCell::default(),
        }
    }

    /// Lists `graphs` as the named graphs `GRAPH ?g { .. }` visits with `?g` unbound when the
    /// query has no `FROM NAMED`. Unlisted time graphs still match when named explicitly.
    pub(crate) fn with_time_graphs(mut self, graphs: Vec<TimeGraph>) -> Self {
        self.named = graphs.into_iter().map(Arc::new).collect();
        self
    }

    /// The layer a bound predicate stands for, if any.
    fn layer_of(&self, predicate: &RdfTerm) -> Option<LayerId> {
        match predicate {
            RdfTerm::Layer(l) => self.preds.term.contains_key(l).then_some(*l),
            RdfTerm::Node(v) => self.preds.by_node.get(v).copied(),
            RdfTerm::TimeGraph(_) | RdfTerm::Other(_) => None,
        }
    }

    /// The bound terms of a pattern, or `None` if it can match nothing: a bound subject or
    /// object must be a node, and a bound predicate a layer of the view.
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
                Some(t) => Some(self.layer_of(t)?),
                None => None,
            },
            object: match object {
                Some(t) => Some(node_of(t)?),
                None => None,
            },
        })
    }

    /// The quads matching a pattern in the default graph (`graph = None`) or in a time graph.
    fn scan(&self, bound: Bound, graph: Option<&Arc<TimeGraph>>) -> QuadIter {
        let Bound {
            subject,
            layer,
            object,
        } = bound;
        // `GRAPH <raphtory:asof:T>` is the view as of `T`.
        let view = match graph {
            None => self.base.clone(),
            Some(graph) => self.base.snapshot_at(graph.at).into_dynamic(),
        };
        QuadIter {
            triples: EdgeScan::matching(view, subject, layer, object),
            preds: self.preds.clone(),
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
            Some(None) => vec![self.scan(bound, None)],
            Some(Some(RdfTerm::TimeGraph(graph))) => vec![self.scan(bound, Some(graph))],
            // Other named graphs are never stored.
            Some(Some(_)) => Vec::new(),
            // Every named graph: time graphs cannot be enumerated, so the listed ones.
            None => self
                .named
                .iter()
                .map(|graph| self.scan(bound, Some(graph)))
                .collect(),
        }
    }

    fn internalize(&self, term: &Term) -> Result<RdfTerm, RdfError> {
        let layer = layer_name_of(term.as_ref())
            .and_then(|name| self.base.get_layer_id(&name))
            .filter(|l| *l != STATIC_GRAPH_LAYER_ID);
        // A layer's predicate term is fixed at build time and wins over a newer node.
        if let Some(predicate) = layer.and_then(|l| self.preds.term.get(&l)) {
            return Ok(predicate.clone());
        }
        if let Some(v) = name_of(term.as_ref()).and_then(|name| lookup_node(&self.base, &name)) {
            return Ok(RdfTerm::Node(v));
        }
        if let Some(l) = layer {
            return Ok(RdfTerm::Layer(l));
        }
        if let Term::NamedNode(iri) = term {
            if let Some(graph) = TimeGraph::parse(iri)? {
                return Ok(RdfTerm::TimeGraph(Arc::new(graph)));
            }
        }
        // Unknown terms are accepted: they are constants or computed values of the query.
        Ok(RdfTerm::Other(term.clone()))
    }
}

fn node_of(term: &RdfTerm) -> Option<VID> {
    match term {
        RdfTerm::Node(v) => Some(*v),
        _ => None,
    }
}

/// Turns scanned `(src, layer, dst)` items into quads. Owns everything it uses.
struct QuadIter {
    triples: EdgeScan,
    preds: Arc<PredTable>,
    graph_name: Option<RdfTerm>,
}

impl Iterator for QuadIter {
    type Item = Result<InternalQuad<RdfTerm>, RdfError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (s, l, o) = self.triples.next()?;
            // A layer created after the dataset was built has no predicate term: skip it.
            if let Some(predicate) = self.preds.term.get(&l) {
                return Some(Ok(InternalQuad {
                    subject: RdfTerm::Node(s),
                    predicate: predicate.clone(),
                    object: RdfTerm::Node(o),
                    graph_name: self.graph_name.clone(),
                }));
            }
        }
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
        if let Some(internal) = self.memo.borrow().get(&term) {
            return Ok(internal.clone());
        }
        let internal = self.internalize(&term)?;
        let mut memo = self.memo.borrow_mut();
        if memo.len() < CACHE_CAP {
            memo.insert(term, internal.clone());
        }
        Ok(internal)
    }

    fn externalize_term(&self, term: RdfTerm) -> Result<Term, RdfError> {
        Ok(match term {
            RdfTerm::Node(v) => term_of(&self.base.node_name(v)),
            RdfTerm::Layer(l) => layer_term(&self.base.get_layer_name(l)),
            RdfTerm::TimeGraph(graph) => graph.iri.clone().into(),
            RdfTerm::Other(t) => t,
        })
    }
}
