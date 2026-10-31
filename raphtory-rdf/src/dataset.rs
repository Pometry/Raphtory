//! The SPARQL dataset of a graph view.
use crate::{
    mapping::{iri_of, layer_name_of, name_of, term_of},
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

const CACHE_CAP: usize = 1 << 20;

// one internal value per term, so equality is sameTerm; a node's term is Node even as a predicate
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) enum RdfTerm {
    Node(NodeTerm),
    Layer(ArcStr),
    TimeGraph(Arc<TimeGraph>),
    Other(Term),
}

// compared by id, so equal to the same node read from any view of the graph
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

// the memo keeps its first answer for each term, so a dataset is meant for one query
pub(crate) struct RaphtoryDataset {
    inner: Rc<Inner>,
    named: Vec<Arc<TimeGraph>>,
}

struct Inner {
    base: DynamicGraph,
    memo: RefCell<FxHashMap<Term, RdfTerm>>,
    predicates: RefCell<FxHashMap<Option<LayerId>, RdfTerm>>,
}

struct Bound {
    subject: Option<VID>,
    layer: Option<ArcStr>,
    object: Option<VID>,
}

impl RaphtoryDataset {
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

    pub(crate) fn with_time_graphs(mut self, graphs: Vec<TimeGraph>) -> Self {
        self.named = graphs.into_iter().map(Arc::new).collect();
        self
    }

    fn bind(
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

    fn quads(
        &self,
        subject: Option<&RdfTerm>,
        predicate: Option<&RdfTerm>,
        object: Option<&RdfTerm>,
        graph_name: Option<Option<&RdfTerm>>,
    ) -> Vec<QuadIter> {
        let Some(bound) = Self::bind(subject, predicate, object) else {
            return Vec::new();
        };
        match graph_name {
            Some(None) => vec![self.scan(&bound, None)],
            Some(Some(RdfTerm::TimeGraph(graph))) => vec![self.scan(&bound, Some(graph))],
            Some(Some(_)) => Vec::new(),
            // time graphs cannot be enumerated
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
        // constants or computed values of the query
        Ok(RdfTerm::Other(term.clone()))
    }

    fn predicate(&self, edge: &EdgeView<DynamicGraph>) -> Result<RdfTerm, RdfError> {
        let layer = edge.edge.layer();
        if let Some(predicate) = self.predicates.borrow().get(&layer) {
            return Ok(predicate.clone());
        }
        let predicate = self.internalize(iri_of(&edge.layer_name()?).into())?;
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

fn layer_of(predicate: &RdfTerm) -> Option<ArcStr> {
    match predicate {
        RdfTerm::Layer(layer) => Some(layer.clone()),
        RdfTerm::Node(node) => layer_name_of(term_of(&node.0.name()).as_ref()).map(Into::into),
        RdfTerm::TimeGraph(_) | RdfTerm::Other(_) => None,
    }
}

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

    fn internal_named_graphs(&self) -> impl Iterator<Item = Result<RdfTerm, RdfError>> + use<'a> {
        let graphs: Vec<_> = self
            .named
            .iter()
            .map(|graph| Ok(RdfTerm::TimeGraph(graph.clone())))
            .collect();
        graphs.into_iter()
    }

    fn contains_internal_graph_name(&self, graph_name: &RdfTerm) -> Result<bool, RdfError> {
        Ok(matches!(graph_name, RdfTerm::TimeGraph(_)))
    }

    fn internalize_term(&self, term: Term) -> Result<RdfTerm, RdfError> {
        self.inner.internalize(term)
    }

    fn externalize_term(&self, term: RdfTerm) -> Result<Term, RdfError> {
        Ok(match term {
            RdfTerm::Node(node) => term_of(&node.0.name()),
            RdfTerm::Layer(layer) => iri_of(&layer).into(),
            RdfTerm::TimeGraph(graph) => graph.iri.clone().into(),
            RdfTerm::Other(t) => t,
        })
    }
}
