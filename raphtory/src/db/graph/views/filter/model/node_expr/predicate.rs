//! A yes/no expression applied as a filter on its entity, and what lets a node
//! predicate start somewhere smaller than every node.
//!
//! [`Predicate`] is the one place a `Bool`-typed value becomes a filter: it
//! builds the node or edge op, checks that the op answers yes/no, and wraps it
//! for the filtered graphs. Both the typed API (`NodeFilter.name().eq("x")`)
//! and a filter tree built from data end here.
//!
//! A node predicate reports how it can be narrowed through [`CreateOp::pushdown`]:
//! the nodes it names by id, or a test a property index can answer with a
//! candidate set. The expression that knows its own shape says so, the way
//! `NodeOp::const_value` lets an op say it is constant.

use crate::{
    db::{
        api::{
            state::{Index, NodeOp},
            view::internal::{DynGraphArc, GraphView, InnerFilterOps, NodeList},
        },
        graph::views::filter::{
            edge_expr_filtered_graph::EdgeExprFilteredGraph,
            exploded_edge_expr_filtered_graph::ExplodedEdgeExprFilteredGraph,
            model::{
                edge_expr::{
                    ops::{EdgeExistsOp, TruthyEdgeOp},
                    EdgeOp,
                },
                expr::{DynCreateHistory, ValueTest},
                filter_operator::{BinaryOp, StringOp},
                node_expr::{
                    ops::{gid_for_id_lookup, DomainNodeOp},
                    typing::{require_bool, truthy},
                    CreateOp, EntityExpr,
                },
                resolved_prop_type, EntityMarker,
            },
            node_filtered_graph::NodeFilteredGraph,
            CreateFilter, DynEdgeFilter,
        },
    },
    errors::GraphError,
};
use raphtory_api::core::entities::{
    properties::{
        meta::NODE_ID_PROP_ID,
        prop::{prop_hashable::HashableProp, Prop, PropType},
    },
    GID,
};
use raphtory_core::entities::nodes::node_ref::AsNodeRef;
use raphtory_storage::graph::graph::{NodePropPredicate, NodePropSemantics};
use std::{collections::HashSet, sync::Arc};

/// A yes/no expression as a filter on its entity. It is the one typed value
/// that implements `CreateFilter`: a value expression becomes a filter when a
/// builder (`gt`, `contains`, `is_in`, `any`, `is_active`, …) wraps it here, so
/// the type says which values are filters and which are not.
///
/// A filter tree uses the same type over an erased expression.
#[derive(Clone)]
pub struct Predicate<E> {
    inner: E,
}

impl<E> Predicate<E> {
    pub fn new(inner: E) -> Self {
        Predicate { inner }
    }

    /// The yes/no expression itself.
    pub fn into_inner(self) -> E {
        self.inner
    }
}

impl<E: CreateOp> Predicate<E> {
    fn entity_marker(&self) -> EntityMarker {
        self.inner.entity()
    }

    /// Where the node filter can start instead of at every node: the nodes the
    /// predicate names by id, or the candidates a property index hands over.
    /// Only a node predicate narrows; `CreateOp::pushdown` on this type forwards
    /// the expression's own answer unchanged.
    fn node_pushdown(&self) -> Option<Pushdown> {
        match self.entity_marker() {
            EntityMarker::Node => self.inner.pushdown(),
            _ => None,
        }
    }

    /// The nodes the filter starts from, resolved once against `graph`. `None`
    /// when nothing narrows it: the filter then scans every node. Whatever comes
    /// back is a superset of the matches, and `apply` still runs on each node.
    fn narrowed_domain<G: GraphView>(&self, graph: &G) -> Option<NodeList> {
        match self.node_pushdown()? {
            Pushdown::Ids(ids) => {
                let id_type = graph.id_type();
                let elems = ids
                    .iter()
                    .map(|v| gid_for_id_lookup(id_type, v))
                    .collect::<Option<Vec<GID>>>()?
                    .into_iter()
                    .filter_map(|gid| graph.internalise_node(gid.as_node_ref()))
                    .collect();
                Some(NodeList::List { elems })
            }
            Pushdown::Index(query) => query.candidates(graph),
        }
    }

    fn node_filter<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = bool> + 'graph>, GraphError> {
        let op = self.inner.create_node_op(graph.clone())?;
        let nodes = self.narrowed_domain(&graph);
        require_bool(
            &resolved_prop_type(self.inner.prop_type(), op.prop_type()),
            "a filter",
        )?;
        let filter: Arc<dyn NodeOp<Output = bool> + 'graph> = Arc::new(op.map(|v| truthy(&v)));
        Ok(match nodes {
            Some(nodes) => Arc::new(DomainNodeOp {
                nodes,
                inner: filter,
            }),
            None => filter,
        })
    }

    fn edge_filter<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = bool> + 'graph>, GraphError> {
        let op = self.inner.create_edge_op(graph)?;
        require_bool(
            &resolved_prop_type(self.inner.prop_type(), op.prop_type()),
            "a filter",
        )?;
        Ok(Arc::new(TruthyEdgeOp { inner: op }))
    }
}

impl<E: CreateOp> CreateFilter for Predicate<E> {
    type FilteredGraph<'graph, G>
        = DynGraphArc<'graph>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    type NodeFilter<'graph, G>
        = Arc<dyn NodeOp<Output = bool> + 'graph>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    type EdgeFilter<'graph, G>
        = DynEdgeFilter<'graph>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_graph_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        Ok(match self.entity_marker() {
            EntityMarker::Node => {
                let filter = self.node_filter(graph.clone())?;
                Arc::new(NodeFilteredGraph::new(graph, filter))
            }
            EntityMarker::Edge => {
                let filter = self.edge_filter(graph.clone())?;
                Arc::new(EdgeExprFilteredGraph::new(graph, filter))
            }
            EntityMarker::ExplodedEdge => {
                let filter = self.edge_filter(graph.clone())?;
                Arc::new(ExplodedEdgeExprFilteredGraph::new(graph, filter))
            }
            EntityMarker::Const => {
                return Err(GraphError::invalid_filter("a constant is not a filter"))
            }
        })
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        if !matches!(self.entity_marker(), EntityMarker::Node) {
            return Err(GraphError::NotNodeFilter);
        }
        self.node_filter(graph)
    }

    fn create_edge_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::EdgeFilter<'graph, G>, GraphError> {
        match self.entity_marker() {
            EntityMarker::Edge => self.edge_filter(graph),
            // A node or exploded-edge predicate still says which edges survive: the
            // ones the filtered graph keeps.
            EntityMarker::Node | EntityMarker::ExplodedEdge => Ok(Arc::new(EdgeExistsOp::new(
                self.create_graph_filter(graph)?,
            ))),
            EntityMarker::Const => Err(GraphError::invalid_filter("a constant is not a filter")),
        }
    }
}

impl<E: EntityExpr> EntityExpr for Predicate<E> {
    fn entity(&self) -> EntityMarker {
        self.inner.entity()
    }

    fn prop_type(&self) -> PropType {
        self.inner.prop_type()
    }

    fn nullable(&self) -> bool {
        self.inner.nullable()
    }

    fn constant(&self) -> Option<Prop> {
        self.inner.constant()
    }
}

impl<E: CreateOp> CreateOp for Predicate<E> {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        self.inner.create_node_op(graph)
    }

    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        self.inner.create_edge_op(graph)
    }

    fn history(&self) -> Option<Arc<dyn DynCreateHistory>> {
        self.inner.history()
    }

    fn index_term(&self) -> Option<IndexTerm> {
        self.inner.index_term()
    }

    fn index_query(&self) -> Option<IndexQuery> {
        self.inner.index_query()
    }

    fn pushdown(&self) -> Option<Pushdown> {
        self.inner.pushdown()
    }

    fn value_test(&self) -> Option<(Arc<dyn DynCreateHistory>, ValueTest)> {
        self.inner.value_test()
    }
}

/// What lets a node predicate start somewhere smaller than every node.
#[derive(Clone, Debug, PartialEq)]
pub enum Pushdown {
    /// `id == v` or `id in [..]` on the bare id field: those nodes.
    Ids(Vec<Prop>),
    /// A test a property index can answer with a candidate set.
    Index(IndexQuery),
}

/// A term a node index covers, as the expression reading it reports itself.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum IndexTerm {
    /// The bare node id, which names its nodes outright.
    Id,
    /// The node name: its external id, which the id index answers patterns on.
    Name,
    /// A property or metadata entry; `ever` when it is the property's history,
    /// so that any value ever held may match.
    Property {
        name: String,
        metadata: bool,
        ever: bool,
    },
}

impl IndexTerm {
    /// The latest update of a history is the property's latest value.
    pub(crate) fn latest_value(self) -> Option<IndexTerm> {
        match self {
            IndexTerm::Property {
                name,
                metadata,
                ever: true,
            } => Some(IndexTerm::Property {
                name,
                metadata,
                ever: false,
            }),
            _ => None,
        }
    }
}

/// A property test in the shape the storage's index answers: one term on one
/// side, one constant on the other, no view on the term.
#[derive(Clone, Debug, PartialEq)]
pub struct IndexQuery {
    term: IndexTerm,
    test: IndexTest,
}

impl IndexQuery {
    /// A comparison of `term` with `value`. `!=` has no candidate set, and the
    /// id index answers patterns on the name only.
    pub(crate) fn cmp(term: IndexTerm, op: BinaryOp, value: Prop) -> Option<Self> {
        if matches!(term, IndexTerm::Id | IndexTerm::Name) {
            return None;
        }
        let test = match op {
            BinaryOp::Eq => IndexTest::Eq(value),
            BinaryOp::Lt => IndexTest::Lt(value),
            BinaryOp::Le => IndexTest::Le(value),
            BinaryOp::Gt => IndexTest::Gt(value),
            BinaryOp::Ge => IndexTest::Ge(value),
            BinaryOp::Ne => return None,
        };
        Some(IndexQuery { term, test })
    }

    /// A string test of `term` against a string constant.
    pub(crate) fn string(term: IndexTerm, op: StringOp, value: Prop) -> Option<Self> {
        if matches!(term, IndexTerm::Id) {
            return None;
        }
        let Prop::Str(s) = value else {
            return None;
        };
        let test = match op {
            StringOp::StartsWith => IndexTest::StartsWith(s.to_string()),
            StringOp::EndsWith => IndexTest::EndsWith(s.to_string()),
            StringOp::Contains => IndexTest::Contains(s.to_string()),
            StringOp::NotContains | StringOp::FuzzySearch { .. } => return None,
        };
        Some(IndexQuery { term, test })
    }

    /// Membership of `term` in `values`.
    pub(crate) fn members(term: IndexTerm, values: &[Prop]) -> Option<Self> {
        if matches!(term, IndexTerm::Id | IndexTerm::Name) {
            return None;
        }
        let values = values.iter().cloned().map(HashableProp).collect();
        Some(IndexQuery {
            term,
            test: IndexTest::In(values),
        })
    }

    /// Whether the term is a property's history. Such a test narrows only under
    /// `any()`, where any value ever held may match; a latest-value test only
    /// outside it.
    pub(crate) fn is_history(&self) -> bool {
        matches!(self.term, IndexTerm::Property { ever: true, .. })
    }

    #[cfg(test)]
    pub(crate) fn term(&self) -> &IndexTerm {
        &self.term
    }

    #[cfg(test)]
    pub(crate) fn test(&self) -> &IndexTest {
        &self.test
    }

    /// The candidates the graph's index has for this test, or `None` when no
    /// index can serve it. A restricted view's latest value can differ from the
    /// global one, so under a window or layer the query asks for every value
    /// ever held, a superset, and drops the index's exactness claim.
    fn candidates<G: GraphView>(&self, graph: &G) -> Option<NodeList> {
        let plain_view = !graph.window_filtered() && !graph.is_layer_filtered();
        let (prop_id, metadata, semantics, exact_allowed) = match &self.term {
            IndexTerm::Property {
                name,
                metadata,
                ever,
            } => {
                let prop_id = graph.node_meta().get_prop_id(name, *metadata)?;
                let (semantics, exact) = match (*ever, plain_view) {
                    (true, plain) => (NodePropSemantics::Ever, plain),
                    (false, true) => (NodePropSemantics::Latest, true),
                    (false, false) => (NodePropSemantics::Ever, false),
                };
                (prop_id, *metadata, semantics, exact)
            }
            IndexTerm::Name => (NODE_ID_PROP_ID, true, NodePropSemantics::Latest, false),
            // The ids a predicate names are resolved outright, not through the index.
            IndexTerm::Id => return None,
        };
        let mut candidates = graph.core_graph().node_prop_candidates(
            prop_id,
            metadata,
            &self.test.predicate(),
            semantics,
        )?;
        candidates.exact &= exact_allowed;
        // index candidates come ascending and deduplicated, as `from_sorted` needs
        Some(NodeList::List {
            elems: Index::from_sorted(candidates.vids, candidates.exact),
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum IndexTest {
    Eq(Prop),
    Lt(Prop),
    Le(Prop),
    Gt(Prop),
    Ge(Prop),
    In(HashSet<HashableProp>),
    StartsWith(String),
    EndsWith(String),
    Contains(String),
}

impl IndexTest {
    fn predicate(&self) -> NodePropPredicate<'_> {
        match self {
            IndexTest::Eq(v) => NodePropPredicate::Eq(v),
            IndexTest::Lt(v) => NodePropPredicate::Lt(v),
            IndexTest::Le(v) => NodePropPredicate::Le(v),
            IndexTest::Gt(v) => NodePropPredicate::Gt(v),
            IndexTest::Ge(v) => NodePropPredicate::Ge(v),
            IndexTest::In(values) => NodePropPredicate::In(values),
            IndexTest::StartsWith(s) => NodePropPredicate::StartsWith(s),
            IndexTest::EndsWith(s) => NodePropPredicate::EndsWith(s),
            IndexTest::Contains(s) => NodePropPredicate::Contains(s),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::{
            api::view::internal::CoreGraphOps,
            graph::views::filter::model::{
                node_expr::{BinaryCmpExpr, DynCreateOp},
                node_filter::{NodeFilter, NodeFilterFactory},
                EntityExprFilterOps,
            },
        },
        prelude::{AdditionOps, Graph, GraphViewOps, NO_PROPS},
    };
    use raphtory_api::core::entities::VID;
    use std::sync::Mutex;

    /// `name == "x"` names its node, the way `id == x` does: the compiled
    /// filter starts from that node alone instead of scanning every node.
    #[test]
    fn a_name_equality_starts_from_its_node() {
        let g = Graph::new();
        for name in ["alice", "bob", "carol"] {
            g.add_node(0, name, NO_PROPS, None, None).unwrap();
        }
        let bob = g.node("bob").unwrap().node;
        let alice = g.node("alice").unwrap().node;

        let op = NodeFilter
            .name()
            .eq("bob")
            .create_node_filter(g.clone())
            .unwrap();
        let NodeList::List { elems } = op.domain(g.core_graph()) else {
            panic!("name equality scanned every node");
        };
        assert!(elems.index(&bob).is_some());
        assert!(elems.index(&alice).is_none());
        assert!(op.apply(g.core_graph(), bob));
        assert!(!op.apply(g.core_graph(), alice));
    }

    /// A leaf that records the address of the erased graph it is compiled against.
    #[derive(Clone, Default)]
    struct GraphProbe {
        seen: Arc<Mutex<Vec<usize>>>,
    }

    impl EntityExpr for GraphProbe {
        fn entity(&self) -> EntityMarker {
            EntityMarker::Node
        }
    }

    impl CreateOp for GraphProbe {
        fn create_node_op<'g, G: GraphView + 'g>(
            &self,
            graph: G,
        ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
            let erased = graph.clone().into_dyn_graph_arc();
            self.seen
                .lock()
                .unwrap()
                .push(Arc::as_ptr(&erased) as *const () as usize);
            Prop::I64(2).create_node_op(graph)
        }
    }

    /// `leaf > 1` as a node predicate over erased terms, as the tree builds it:
    /// the erased predicate, the comparison and the erased leaf each hand the
    /// graph on; the leaf must receive the very `Arc` the caller passed in, not
    /// a fresh box around it per level.
    #[test]
    fn erased_levels_share_one_graph_arc() {
        let g = Graph::new();
        g.add_node(0, "n", [("a", Prop::I64(2))], None, None)
            .unwrap();
        let probe = GraphProbe::default();
        let lhs: Arc<dyn DynCreateOp> = Arc::new(probe.clone());
        let rhs: Arc<dyn DynCreateOp> = Arc::new(Prop::I64(1));
        let cmp: Arc<dyn DynCreateOp> = Arc::new(BinaryCmpExpr::new(
            lhs,
            BinaryOp::Gt,
            rhs,
            EntityMarker::Node,
        ));
        let predicate = Predicate::new(cmp);

        let base: DynGraphArc<'static> = Arc::new(g.clone());
        let op = predicate.create_node_filter(base.clone()).unwrap();
        assert!(op.apply(g.core_graph(), VID(0)));

        assert_eq!(
            *probe.seen.lock().unwrap(),
            vec![Arc::as_ptr(&base) as *const () as usize]
        );
    }
}
