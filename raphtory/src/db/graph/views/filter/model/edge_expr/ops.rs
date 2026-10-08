//! Runtime edge evaluators — given an edge's storage entry, return a typed value.
//!
//! Parallel to `node_expr/ops.rs` — same design, different subject.

use crate::db::{
    api::{
        state::ops::Const,
        view::internal::{FilterOps, GraphView},
    },
    graph::edge_reads::{self, EdgeAt},
};
use raphtory_api::core::{
    entities::{
        properties::prop::{Prop, PropType},
        LayerId, ELID,
    },
    storage::timeindex::EventTime,
};
use raphtory_storage::graph::{edges::edge_storage_ops::EdgeStorageOps, graph::GraphStorage};
use storage::EdgeEntryRef;

use super::EdgeOp;
use crate::db::{
    api::state::ops::NodeOp,
    graph::views::filter::model::{
        edge_filter::Endpoint,
        node_expr::typing::{truthy, BinaryKernel, UnaryKernel},
    },
};
use std::sync::Arc;

// ─────────────────────────────────────────────────────────────────────────────
// Arc<dyn EdgeOp> — blanket impl so Arc-boxed ops satisfy EdgeOp
// ─────────────────────────────────────────────────────────────────────────────

impl<'a, V: Clone + Send + Sync> EdgeOp for Arc<dyn EdgeOp<Output = V> + 'a> {
    type Output = V;

    fn apply(&self, storage: &GraphStorage, edge: EdgeEntryRef) -> V {
        self.as_ref().apply(storage, edge)
    }

    fn apply_layer(&self, storage: &GraphStorage, edge: EdgeEntryRef, layer: LayerId) -> V {
        self.as_ref().apply_layer(storage, edge, layer)
    }

    fn apply_exploded(
        &self,
        storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
        t: EventTime,
    ) -> V {
        self.as_ref().apply_exploded(storage, edge, layer, t)
    }

    fn prop_type(&self) -> PropType {
        self.as_ref().prop_type()
    }

    fn const_value(&self) -> Option<V> {
        self.as_ref().const_value()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Const<V> — constant literal op (RHS in comparisons)
// ─────────────────────────────────────────────────────────────────────────────

impl<V: Clone + Send + Sync + 'static> EdgeOp for Const<V> {
    type Output = V;

    fn apply(&self, _storage: &GraphStorage, _edge: EdgeEntryRef) -> V {
        self.0.clone()
    }

    fn apply_layer(&self, _storage: &GraphStorage, _edge: EdgeEntryRef, _layer: LayerId) -> V {
        self.0.clone()
    }

    fn apply_exploded(
        &self,
        _storage: &GraphStorage,
        _edge: EdgeEntryRef,
        _layer: LayerId,
        _t: EventTime,
    ) -> V {
        self.0.clone()
    }

    fn const_value(&self) -> Option<V> {
        Some(self.0.clone())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// EdgePropOp<G> — latest temporal property value by pre-resolved column ID
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub(crate) struct EdgePropOp<G> {
    pub(crate) graph: G,
    pub(crate) prop_id: usize,
}

impl<G: GraphView> EdgeOp for EdgePropOp<G> {
    type Output = Option<Prop>;

    fn apply(&self, _storage: &GraphStorage, edge: EdgeEntryRef) -> Option<Prop> {
        edge_reads::shown(&self.graph, edge, EdgeAt::Whole)
            .and_then(|at| edge_reads::temporal_value(&self.graph, edge, at, self.prop_id))
    }

    fn apply_layer(
        &self,
        _storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
    ) -> Option<Prop> {
        edge_reads::shown(&self.graph, edge, EdgeAt::Layer(layer))
            .and_then(|at| edge_reads::temporal_value(&self.graph, edge, at, self.prop_id))
    }

    fn apply_exploded(
        &self,
        _storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
        t: EventTime,
    ) -> Option<Prop> {
        edge_reads::shown(&self.graph, edge, EdgeAt::Exploded(layer, t))
            .and_then(|at| edge_reads::temporal_value(&self.graph, edge, at, self.prop_id))
    }

    fn prop_type(&self) -> PropType {
        self.graph
            .edge_meta()
            .temporal_prop_mapper()
            .get_dtype(self.prop_id)
            .unwrap_or_default()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// EdgeMetaOp<G> — static metadata field by pre-resolved column ID
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub(crate) struct EdgeMetaOp<G> {
    pub(crate) graph: G,
    pub(crate) prop_id: usize,
}

impl<G: GraphView> EdgeOp for EdgeMetaOp<G> {
    type Output = Option<Prop>;

    fn apply(&self, _storage: &GraphStorage, edge: EdgeEntryRef) -> Option<Prop> {
        edge_reads::shown(&self.graph, edge, EdgeAt::Whole)
            .and_then(|at| edge_reads::metadata(&self.graph, edge, at, self.prop_id))
    }

    fn apply_layer(
        &self,
        _storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
    ) -> Option<Prop> {
        edge_reads::shown(&self.graph, edge, EdgeAt::Layer(layer))
            .and_then(|at| edge_reads::metadata(&self.graph, edge, at, self.prop_id))
    }

    fn apply_exploded(
        &self,
        _storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
        t: EventTime,
    ) -> Option<Prop> {
        edge_reads::shown(&self.graph, edge, EdgeAt::Exploded(layer, t))
            .and_then(|at| edge_reads::metadata(&self.graph, edge, at, self.prop_id))
    }

    // No declared type: the runtime shape depends on the edge's layers (a
    // multi-layer edge yields a map keyed by layer, a single-layer edge the
    // plain value), so comparisons defer to runtime coercion.
}

// ─────────────────────────────────────────────────────────────────────────────
// TemporalEdgePropOp<G> — all temporal values for a property in the view window
// ─────────────────────────────────────────────────────────────────────────────

/// Internal op produced by [`TemporalPropExpr::create_edge_op`] — not constructed directly.
///
/// Collects the property's history for the edge, one of its layers or one
/// exploded instance into a `Some(Prop::List([...]))` for a consumer that
/// needs it as one value. An aggregation or an `any()`/`all()` test written
/// directly over the history streams it through [`EdgeHistory`] instead.
///
/// [`TemporalPropExpr::create_edge_op`]: crate::db::graph::views::filter::model::node_expr::TemporalPropExpr
/// [`EdgeHistory`]: crate::db::graph::views::filter::model::expr::EdgeHistory
#[derive(Clone)]
pub(crate) struct TemporalEdgePropOp<G> {
    pub(crate) graph: G,
    pub(crate) prop_id: usize,
}

impl<G: GraphView> TemporalEdgePropOp<G> {
    fn history(&self, edge: EdgeEntryRef, at: EdgeAt) -> Option<Prop> {
        // A hidden edge reads an empty history. Every value in one property's
        // history has the property's type, so the list always unifies.
        let vals = edge_reads::shown(&self.graph, edge, at).map(|at| {
            edge_reads::temporal_hist(&self.graph, edge, at, self.prop_id).map(|(_, v)| v)
        });
        Prop::list(vals.into_iter().flatten()).ok()
    }
}

impl<G: GraphView> EdgeOp for TemporalEdgePropOp<G> {
    type Output = Option<Prop>;

    fn prop_type(&self) -> PropType {
        self.graph
            .edge_meta()
            .temporal_prop_mapper()
            .get_dtype(self.prop_id)
            .map_or(PropType::Empty, |dt| PropType::List(Box::new(dt)))
    }

    fn apply(&self, _storage: &GraphStorage, edge: EdgeEntryRef) -> Option<Prop> {
        self.history(edge, EdgeAt::Whole)
    }

    fn apply_layer(
        &self,
        _storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
    ) -> Option<Prop> {
        self.history(edge, EdgeAt::Layer(layer))
    }

    fn apply_exploded(
        &self,
        _storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
        t: EventTime,
    ) -> Option<Prop> {
        self.history(edge, EdgeAt::Exploded(layer, t))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// EdgeEndpointNodeOp — applies a node op to the src or dst VID of an edge
//
// Bridges EdgeEndpointWrapper<T: NodeExpr> into the EdgeExpr system:
// EdgeFilter::src().name().eq("Alice") compiles the name NodeOp once, then
// at evaluation time looks up the src VID and applies the node op to it.
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub(crate) struct EdgeEndpointNodeOp<'g> {
    pub(crate) node_op: Arc<dyn NodeOp<Output = Option<Prop>> + 'g>,
    pub(crate) endpoint: Endpoint,
}

impl<'g> EdgeEndpointNodeOp<'g> {
    fn at_endpoint(&self, storage: &GraphStorage, edge: EdgeEntryRef) -> Option<Prop> {
        let vid = match self.endpoint {
            Endpoint::Src => edge.src(),
            Endpoint::Dst => edge.dst(),
        };
        self.node_op.apply(storage, vid)
    }
}

impl<'g> EdgeOp for EdgeEndpointNodeOp<'g> {
    type Output = Option<Prop>;

    fn prop_type(&self) -> PropType {
        self.node_op.prop_type()
    }

    fn const_value(&self) -> Option<Self::Output> {
        self.node_op.const_value()
    }

    fn apply(&self, storage: &GraphStorage, edge: EdgeEntryRef) -> Option<Prop> {
        self.at_endpoint(storage, edge)
    }

    fn apply_layer(
        &self,
        storage: &GraphStorage,
        edge: EdgeEntryRef,
        _layer: LayerId,
    ) -> Option<Prop> {
        self.at_endpoint(storage, edge)
    }

    fn apply_exploded(
        &self,
        storage: &GraphStorage,
        edge: EdgeEntryRef,
        _layer: LayerId,
        _t: EventTime,
    ) -> Option<Prop> {
        self.at_endpoint(storage, edge)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Per-edge predicate ops — produce Some(Prop::Bool(...)) per edge.
// Used by `CreateOp::create_edge_op` for the expression-mode path of the
// structural edge predicates (IsActiveEdge, IsValidEdge, IsDeletedEdge,
// IsSelfLoopEdge).
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub(crate) struct IsActiveEdgePropOp<G> {
    pub(crate) graph: G,
}

impl<G: GraphView> EdgeOp for IsActiveEdgePropOp<G> {
    type Output = Option<Prop>;

    fn apply(&self, _storage: &GraphStorage, edge: EdgeEntryRef) -> Option<Prop> {
        Some(Prop::Bool(
            edge_reads::shown(&self.graph, edge, EdgeAt::Whole)
                .is_some_and(|at| edge_reads::is_active(&self.graph, edge, at)),
        ))
    }

    fn apply_layer(
        &self,
        _storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
    ) -> Option<Prop> {
        Some(Prop::Bool(
            edge_reads::shown(&self.graph, edge, EdgeAt::Layer(layer))
                .is_some_and(|at| edge_reads::is_active(&self.graph, edge, at)),
        ))
    }

    fn apply_exploded(
        &self,
        _storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
        t: EventTime,
    ) -> Option<Prop> {
        Some(Prop::Bool(
            edge_reads::shown(&self.graph, edge, EdgeAt::Exploded(layer, t))
                .is_some_and(|at| edge_reads::is_active(&self.graph, edge, at)),
        ))
    }

    fn prop_type(&self) -> PropType {
        PropType::Bool
    }
}

#[derive(Clone)]
pub(crate) struct IsValidEdgePropOp<G> {
    pub(crate) graph: G,
}

impl<G: GraphView> EdgeOp for IsValidEdgePropOp<G> {
    type Output = Option<Prop>;

    fn apply(&self, _storage: &GraphStorage, edge: EdgeEntryRef) -> Option<Prop> {
        Some(Prop::Bool(
            edge_reads::shown(&self.graph, edge, EdgeAt::Whole)
                .is_some_and(|at| edge_reads::is_valid(&self.graph, edge, at)),
        ))
    }

    fn apply_layer(
        &self,
        _storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
    ) -> Option<Prop> {
        Some(Prop::Bool(
            edge_reads::shown(&self.graph, edge, EdgeAt::Layer(layer))
                .is_some_and(|at| edge_reads::is_valid(&self.graph, edge, at)),
        ))
    }

    fn apply_exploded(
        &self,
        _storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
        t: EventTime,
    ) -> Option<Prop> {
        Some(Prop::Bool(
            edge_reads::shown(&self.graph, edge, EdgeAt::Exploded(layer, t))
                .is_some_and(|at| edge_reads::is_valid(&self.graph, edge, at)),
        ))
    }

    fn prop_type(&self) -> PropType {
        PropType::Bool
    }
}

#[derive(Clone)]
pub(crate) struct IsDeletedEdgePropOp<G> {
    pub(crate) graph: G,
}

impl<G: GraphView> EdgeOp for IsDeletedEdgePropOp<G> {
    type Output = Option<Prop>;

    fn apply(&self, _storage: &GraphStorage, edge: EdgeEntryRef) -> Option<Prop> {
        Some(Prop::Bool(
            edge_reads::shown(&self.graph, edge, EdgeAt::Whole)
                .is_some_and(|at| edge_reads::is_deleted(&self.graph, edge, at)),
        ))
    }

    fn apply_layer(
        &self,
        _storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
    ) -> Option<Prop> {
        Some(Prop::Bool(
            edge_reads::shown(&self.graph, edge, EdgeAt::Layer(layer))
                .is_some_and(|at| edge_reads::is_deleted(&self.graph, edge, at)),
        ))
    }

    fn apply_exploded(
        &self,
        _storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
        t: EventTime,
    ) -> Option<Prop> {
        Some(Prop::Bool(
            edge_reads::shown(&self.graph, edge, EdgeAt::Exploded(layer, t))
                .is_some_and(|at| edge_reads::is_deleted(&self.graph, edge, at)),
        ))
    }

    fn prop_type(&self) -> PropType {
        PropType::Bool
    }
}

#[derive(Clone)]
pub(crate) struct IsSelfLoopEdgePropOp;

impl EdgeOp for IsSelfLoopEdgePropOp {
    type Output = Option<Prop>;

    fn apply(&self, _storage: &GraphStorage, edge: EdgeEntryRef) -> Option<Prop> {
        Some(Prop::Bool(edge.src() == edge.dst()))
    }

    fn apply_layer(
        &self,
        _storage: &GraphStorage,
        edge: EdgeEntryRef,
        _layer: LayerId,
    ) -> Option<Prop> {
        Some(Prop::Bool(edge.src() == edge.dst()))
    }

    fn apply_exploded(
        &self,
        _storage: &GraphStorage,
        edge: EdgeEntryRef,
        _layer: LayerId,
        _t: EventTime,
    ) -> Option<Prop> {
        Some(Prop::Bool(edge.src() == edge.dst()))
    }

    fn prop_type(&self) -> PropType {
        PropType::Bool
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Edge predicates: existence in a view and the boolean combinators
// ─────────────────────────────────────────────────────────────────────────────

/// Whether the edge exists in `graph`; the edge side of `NodeExistsOp`.
#[derive(Debug, Clone)]
pub struct EdgeExistsOp<G> {
    graph: G,
}

impl<G> EdgeExistsOp<G> {
    pub(crate) fn new(graph: G) -> Self {
        Self { graph }
    }
}

impl<G: GraphView> EdgeOp for EdgeExistsOp<G> {
    type Output = bool;

    fn apply(&self, _storage: &GraphStorage, edge: EdgeEntryRef) -> bool {
        self.graph.filter_edge(edge)
    }

    fn apply_layer(&self, _storage: &GraphStorage, edge: EdgeEntryRef, layer: LayerId) -> bool {
        self.graph.filter_edge_layer(edge, layer)
    }

    fn apply_exploded(
        &self,
        _storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
        t: EventTime,
    ) -> bool {
        self.graph
            .filter_exploded_edge(ELID::new(edge.eid(), layer), t)
    }

    fn prop_type(&self) -> PropType {
        PropType::Bool
    }
}

#[derive(Debug, Clone)]
pub struct AndEdgeOp<L, R> {
    pub(crate) left: L,
    pub(crate) right: R,
}

impl<L: EdgeOp<Output = bool>, R: EdgeOp<Output = bool>> EdgeOp for AndEdgeOp<L, R> {
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, edge: EdgeEntryRef) -> bool {
        self.left.apply(storage, edge) && self.right.apply(storage, edge)
    }

    fn apply_layer(&self, storage: &GraphStorage, edge: EdgeEntryRef, layer: LayerId) -> bool {
        self.left.apply_layer(storage, edge, layer) && self.right.apply_layer(storage, edge, layer)
    }

    fn apply_exploded(
        &self,
        storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
        t: EventTime,
    ) -> bool {
        self.left.apply_exploded(storage, edge, layer, t)
            && self.right.apply_exploded(storage, edge, layer, t)
    }

    fn prop_type(&self) -> PropType {
        PropType::Bool
    }

    fn const_value(&self) -> Option<bool> {
        match (self.left.const_value(), self.right.const_value()) {
            (Some(false), _) | (_, Some(false)) => Some(false),
            (Some(true), Some(true)) => Some(true),
            _ => None,
        }
    }
}

/// The opposite of a yes/no edge op.
#[derive(Debug, Clone)]
pub struct NotEdgeOp<T>(pub(crate) T);

impl<T: EdgeOp<Output = bool>> EdgeOp for NotEdgeOp<T> {
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, edge: EdgeEntryRef) -> bool {
        !self.0.apply(storage, edge)
    }

    fn apply_layer(&self, storage: &GraphStorage, edge: EdgeEntryRef, layer: LayerId) -> bool {
        !self.0.apply_layer(storage, edge, layer)
    }

    fn apply_exploded(
        &self,
        storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
        t: EventTime,
    ) -> bool {
        !self.0.apply_exploded(storage, edge, layer, t)
    }

    fn prop_type(&self) -> PropType {
        PropType::Bool
    }

    fn const_value(&self) -> Option<bool> {
        self.0.const_value().map(|v| !v)
    }
}

#[derive(Debug, Clone)]
pub struct OrEdgeOp<L, R> {
    pub(crate) left: L,
    pub(crate) right: R,
}

impl<L: EdgeOp<Output = bool>, R: EdgeOp<Output = bool>> EdgeOp for OrEdgeOp<L, R> {
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, edge: EdgeEntryRef) -> bool {
        self.left.apply(storage, edge) || self.right.apply(storage, edge)
    }

    fn apply_layer(&self, storage: &GraphStorage, edge: EdgeEntryRef, layer: LayerId) -> bool {
        self.left.apply_layer(storage, edge, layer) || self.right.apply_layer(storage, edge, layer)
    }

    fn apply_exploded(
        &self,
        storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
        t: EventTime,
    ) -> bool {
        self.left.apply_exploded(storage, edge, layer, t)
            || self.right.apply_exploded(storage, edge, layer, t)
    }

    fn prop_type(&self) -> PropType {
        PropType::Bool
    }

    fn const_value(&self) -> Option<bool> {
        match (self.left.const_value(), self.right.const_value()) {
            (Some(true), _) | (_, Some(true)) => Some(true),
            (Some(false), Some(false)) => Some(false),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::{
            api::view::{
                filter_ops::Select,
                internal::{InternalEdgeLayerFilterOps, InternalExplodedEdgeFilterOps},
            },
            graph::{
                edges::{EdgeItem, Edges},
                views::filter::{
                    edge_expr_filtered_graph::EdgeExprFilteredGraph,
                    model::{
                        edge_filter::EdgeFilter,
                        exploded_edge_filter::ExplodedEdgeFilter,
                        graph_filter::GraphFilter,
                        node_filter::{NodeFilter, NodeFilterFactory},
                        PropertyExprFactory, ViewWrapOps,
                    },
                    CreateFilter,
                },
            },
        },
        prelude::{
            AdditionOps, EdgeViewOps, EntityExprFilterOps, Graph, GraphViewOps, NodeViewOps,
            TimeOps, NO_PROPS,
        },
    };
    use raphtory_api::core::storage::timeindex::AsTime;

    /// a→b @1 [x] w=1 · a→b @7 [y] w=7 · b→c @2 [x] w=2
    fn graph() -> Graph {
        let g = Graph::new();
        g.add_edge(1, "a", "b", [("w", 1i64)], Some("x")).unwrap();
        g.add_edge(7, "a", "b", [("w", 7i64)], Some("y")).unwrap();
        g.add_edge(2, "b", "c", [("w", 2i64)], Some("x")).unwrap();
        g
    }

    fn exploded<'graph, G: GraphViewOps<'graph>, K: EdgeItem>(
        edges: &Edges<'graph, G, K>,
    ) -> Vec<(String, String, i64)> {
        edges
            .explode()
            .iter()
            .map(|e| (e.src().name(), e.dst().name(), e.time().unwrap().t()))
            .collect()
    }

    #[test]
    fn plain_node_predicate_is_decided_per_edge() {
        let g = graph();
        let op = NodeFilter
            .name()
            .ne("c")
            .create_edge_filter(g.clone())
            .unwrap();

        let view = EdgeExprFilteredGraph::new(g.clone(), op);
        assert!(!view.internal_exploded_edge_filtered());
        assert!(!view.internal_edge_layer_filtered());
        assert!(view.internal_exploded_filter_edge_list_trusted());
        assert!(view.internal_layer_filter_edge_list_trusted());

        let selected = g
            .edges()
            .select(NodeFilter.name().ne("c"))
            .unwrap()
            .explode()
            .iter()
            .map(|e| (e.src().name(), e.dst().name(), e.time().unwrap().t()))
            .collect::<Vec<_>>();
        assert_eq!(
            selected,
            vec![
                ("a".to_string(), "b".to_string(), 1),
                ("a".to_string(), "b".to_string(), 7)
            ]
        );
    }

    /// A view used as an edge predicate decides membership once per edge: the
    /// layers and exploded instances of a kept edge all pass, so exploding
    /// after the selection lists every instance, including those outside the
    /// window. Narrowing the instances is what a graph view is for.
    #[test]
    fn a_view_predicate_decides_per_edge() {
        let g = graph();
        let windowed = GraphFilter
            .window(0, 5)
            .create_edge_filter(g.clone())
            .unwrap();
        let view = EdgeExprFilteredGraph::new(g.clone(), windowed);
        assert!(!view.internal_exploded_edge_filtered());
        assert!(!view.internal_edge_layer_filtered());
        assert!(view.internal_exploded_filter_edge_list_trusted());

        let selected = g
            .edges()
            .select(GraphFilter.window(0, 5))
            .unwrap()
            .explode()
            .iter()
            .map(|e| (e.src().name(), e.dst().name(), e.time().unwrap().t()))
            .collect::<Vec<_>>();
        assert_eq!(
            selected,
            vec![
                ("a".to_string(), "b".to_string(), 1),
                ("a".to_string(), "b".to_string(), 7),
                ("b".to_string(), "c".to_string(), 2)
            ]
        );

        let narrowed = g
            .window(0, 5)
            .edges()
            .explode()
            .iter()
            .map(|e| (e.src().name(), e.dst().name(), e.time().unwrap().t()))
            .collect::<Vec<_>>();
        assert_eq!(
            narrowed,
            vec![
                ("a".to_string(), "b".to_string(), 1),
                ("b".to_string(), "c".to_string(), 2)
            ]
        );
    }

    /// `select` asks its question of the items of the collection it is called
    /// on: once per edge on edges, once per exploded edge on exploded edges.
    /// The predicate's kind plays no part.
    #[test]
    fn select_asks_about_the_items_of_the_collection() {
        let g = graph();
        let ab = |t| ("a".to_string(), "b".to_string(), t);
        let bc = |t| ("b".to_string(), "c".to_string(), t);

        // a view on a collection of edges keeps the edges active in it, and
        // every exploded edge of a kept edge; on exploded edges it keeps the
        // exploded edges inside it
        let window = || GraphFilter.window(0, 5);
        let edges = g.edges().select(window()).unwrap();
        assert_eq!(exploded(&edges), vec![ab(1), ab(7), bc(2)]);
        let instances = g.edges().explode().select(window()).unwrap();
        assert_eq!(exploded(&instances), vec![ab(1), bc(2)]);

        // an exploded-edge predicate on a collection of edges keeps the edges
        // with a passing exploded edge, and all of their exploded edges
        let seven = || ExplodedEdgeFilter.property("w").eq(7i64);
        let edges = g.edges().select(seven()).unwrap();
        assert_eq!(exploded(&edges), vec![ab(1), ab(7)]);
        let instances = g.edges().explode().select(seven()).unwrap();
        assert_eq!(exploded(&instances), vec![ab(7)]);

        // an edge predicate reads the edge's latest value on edges, and each
        // exploded edge's own value on exploded edges
        let latest_seven = || EdgeFilter.property("w").eq(7i64);
        let edges = g.edges().select(latest_seven()).unwrap();
        assert_eq!(exploded(&edges), vec![ab(1), ab(7)]);
        let instances = g.edges().explode().select(latest_seven()).unwrap();
        assert_eq!(exploded(&instances), vec![ab(7)]);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Value ops — a yes/no (or element-wise yes/no) built from other values
// ─────────────────────────────────────────────────────────────────────────────

/// Two values combined by `kernel` with the test's `param`, producing a value
/// of type `out`. The kernel is chosen for the test's shape when the op is built.
pub struct BinaryValueEdgeOp<'g, P> {
    pub(crate) left: Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>,
    pub(crate) right: Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>,
    pub(crate) param: P,
    pub(crate) kernel: BinaryKernel<P>,
    pub(crate) out: PropType,
}

impl<'g, P: Send + Sync> EdgeOp for BinaryValueEdgeOp<'g, P> {
    type Output = Option<Prop>;

    fn prop_type(&self) -> PropType {
        self.out.clone()
    }

    fn apply(&self, storage: &GraphStorage, edge: EdgeEntryRef) -> Option<Prop> {
        (self.kernel)(
            &self.param,
            self.left.apply(storage, edge),
            self.right.apply(storage, edge),
        )
    }

    fn apply_layer(
        &self,
        storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
    ) -> Option<Prop> {
        (self.kernel)(
            &self.param,
            self.left.apply_layer(storage, edge, layer),
            self.right.apply_layer(storage, edge, layer),
        )
    }

    fn apply_exploded(
        &self,
        storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
        t: EventTime,
    ) -> Option<Prop> {
        (self.kernel)(
            &self.param,
            self.left.apply_exploded(storage, edge, layer, t),
            self.right.apply_exploded(storage, edge, layer, t),
        )
    }
}

/// One value mapped by `kernel` with the test's `param`, producing a value of
/// type `out`.
pub struct UnaryValueEdgeOp<'g, P> {
    pub(crate) inner: Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>,
    pub(crate) param: P,
    pub(crate) kernel: UnaryKernel<P>,
    pub(crate) out: PropType,
}

impl<'g, P: Send + Sync> EdgeOp for UnaryValueEdgeOp<'g, P> {
    type Output = Option<Prop>;

    fn prop_type(&self) -> PropType {
        self.out.clone()
    }

    fn apply(&self, storage: &GraphStorage, edge: EdgeEntryRef) -> Option<Prop> {
        (self.kernel)(&self.param, self.inner.apply(storage, edge))
    }

    fn apply_layer(
        &self,
        storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
    ) -> Option<Prop> {
        (self.kernel)(&self.param, self.inner.apply_layer(storage, edge, layer))
    }

    fn apply_exploded(
        &self,
        storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
        t: EventTime,
    ) -> Option<Prop> {
        (self.kernel)(
            &self.param,
            self.inner.apply_exploded(storage, edge, layer, t),
        )
    }
}

/// `and` over yes/no values, stopping at the first that does not hold.
pub struct AndValueEdgeOp<'g> {
    pub(crate) items: Vec<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>>,
}

impl<'g> EdgeOp for AndValueEdgeOp<'g> {
    type Output = Option<Prop>;

    fn prop_type(&self) -> PropType {
        PropType::Bool
    }

    fn apply(&self, storage: &GraphStorage, edge: EdgeEntryRef) -> Option<Prop> {
        let hit = self
            .items
            .iter()
            .all(|item| truthy(&item.apply(storage, edge)));
        Some(Prop::Bool(hit))
    }

    fn apply_layer(
        &self,
        storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
    ) -> Option<Prop> {
        let hit = self
            .items
            .iter()
            .all(|item| truthy(&item.apply_layer(storage, edge, layer)));
        Some(Prop::Bool(hit))
    }

    fn apply_exploded(
        &self,
        storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
        t: EventTime,
    ) -> Option<Prop> {
        let hit = self
            .items
            .iter()
            .all(|item| truthy(&item.apply_exploded(storage, edge, layer, t)));
        Some(Prop::Bool(hit))
    }
}

/// `or` over yes/no values, stopping at the first that holds.
pub struct OrValueEdgeOp<'g> {
    pub(crate) items: Vec<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>>,
}

impl<'g> EdgeOp for OrValueEdgeOp<'g> {
    type Output = Option<Prop>;

    fn prop_type(&self) -> PropType {
        PropType::Bool
    }

    fn apply(&self, storage: &GraphStorage, edge: EdgeEntryRef) -> Option<Prop> {
        let hit = self
            .items
            .iter()
            .any(|item| truthy(&item.apply(storage, edge)));
        Some(Prop::Bool(hit))
    }

    fn apply_layer(
        &self,
        storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
    ) -> Option<Prop> {
        let hit = self
            .items
            .iter()
            .any(|item| truthy(&item.apply_layer(storage, edge, layer)));
        Some(Prop::Bool(hit))
    }

    fn apply_exploded(
        &self,
        storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
        t: EventTime,
    ) -> Option<Prop> {
        let hit = self
            .items
            .iter()
            .any(|item| truthy(&item.apply_exploded(storage, edge, layer, t)));
        Some(Prop::Bool(hit))
    }
}

/// Adapts a yes/no edge value to the plain boolean the filtered graphs consume.
pub struct TruthyEdgeOp<'g> {
    pub(crate) inner: Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>,
}

impl<'g> EdgeOp for TruthyEdgeOp<'g> {
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, edge: EdgeEntryRef) -> bool {
        truthy(&self.inner.apply(storage, edge))
    }

    fn apply_layer(&self, storage: &GraphStorage, edge: EdgeEntryRef, layer: LayerId) -> bool {
        truthy(&self.inner.apply_layer(storage, edge, layer))
    }

    fn apply_exploded(
        &self,
        storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
        t: EventTime,
    ) -> bool {
        truthy(&self.inner.apply_exploded(storage, edge, layer, t))
    }
}
