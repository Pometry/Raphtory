use crate::{
    db::{
        api::{
            state::ops::{filter::NodeExistsOp, NodeFilterOp},
            view::internal::GraphView,
        },
        graph::views::filter::node_filtered_graph::NodeFilteredGraph,
    },
    errors::GraphError,
    prelude::GraphViewOps,
};

pub mod and_filtered_graph;
pub mod edge_filtered_graph;
pub mod edge_node_filtered_graph;
pub mod edge_op;
pub mod edge_property_filtered_graph;
pub mod exploded_edge_filtered_graph;
pub mod exploded_edge_node_filtered_graph;
pub mod exploded_edge_property_filter;
pub mod model;
pub mod node_filtered_graph;
pub mod not_filtered_graph;
pub mod or_filtered_graph;

use crate::db::graph::views::filter::edge_op::{EdgeExistsOp, EdgeFilterOp};
use std::sync::Arc;

pub struct Exists;

impl CreateFilter for Exists {
    crate::edge_filter_from_wrapper!();

    type EntityFiltered<'graph, G, F>
        = F
    where
        Self: 'graph,
        G: GraphView + 'graph,
        F: GraphView + 'graph;
    type NodeFilter<'graph, G, F>
        = NodeExistsOp<F>
    where
        Self: 'graph,
        G: GraphView + 'graph,
        F: GraphView + 'graph;
    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphViewOps<'graph>;

    fn create_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        _graph: G,
        filtered: F,
    ) -> Result<Self::EntityFiltered<'graph, G, F>, GraphError> {
        Ok(filtered)
    }

    fn create_node_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        _graph: G,
        filtered: F,
    ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError> {
        Ok(NodeExistsOp::new(filtered))
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        Ok(graph)
    }
}

/// Lowers a filter that holds no other filters: build the wrapper graph it
/// already produces, then ask that graph whether an edge is in its view.
///
/// Wrong for anything that wraps another filter — a composite, a view applied
/// to an inner expression, or a type that delegates to a boxed filter — because
/// it rebuilds the very wrapper composition the per-edge boolean exists to
/// avoid. Those must delegate to their inner filter instead.
#[macro_export]
macro_rules! edge_filter_from_wrapper {
    () => {
        fn create_edge_filter<
            'graph,
            G: $crate::db::api::state::ops::GraphView + 'graph,
            F: $crate::db::api::state::ops::GraphView + 'graph,
        >(
            self,
            graph: G,
            filtered: F,
        ) -> Result<
            std::sync::Arc<dyn $crate::db::graph::views::filter::edge_op::EdgeFilterOp + 'graph>,
            $crate::errors::GraphError,
        >
        where
            Self: 'graph,
        {
            Ok(std::sync::Arc::new(
                $crate::db::graph::views::filter::edge_op::EdgeExistsOp::new(
                    self.create_filter(graph, filtered)?,
                ),
            ))
        }
    };
}

pub trait CreateFilter: Sized {
    type EntityFiltered<'graph, G, F>: GraphView + 'graph
    where
        Self: 'graph,
        G: GraphView + 'graph,
        F: GraphView + 'graph;

    type NodeFilter<'graph, G, F>: NodeFilterOp + 'graph
    where
        Self: 'graph,
        G: GraphView + 'graph,
        F: GraphView + 'graph;

    type FilteredGraph<'graph, G>: GraphView + 'graph
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Self::EntityFiltered<'graph, G, F>, GraphError>;

    fn create_node_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError>;

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError>;

    /// Whether this filter tests individual edge *events* rather than edges.
    ///
    /// Exploded-edge filters do, and composites of them must keep their wrapper
    /// graphs: a per-edge boolean can only say whether an edge is in, not which
    /// of its events survive.
    fn is_exploded_edge_filter(&self) -> bool {
        false
    }

    /// Whether this filter is a boolean composite of other filters.
    ///
    /// Composites must be lowered to a per-edge boolean, because composing their
    /// operands' wrapper graphs loses a view operand's restriction. Everything
    /// else keeps its wrapper graph, which carries exact per-layer and
    /// per-event semantics that a single edge-level boolean cannot express —
    /// an exploded-edge property filter, for instance, narrows which *events*
    /// of an edge survive, not just which edges.
    fn is_edge_composite(&self) -> bool {
        false
    }

    /// Lower this filter to a per-edge boolean.
    ///
    /// Required rather than defaulted, deliberately. A filter that holds other
    /// filters has to *delegate* this — asking each inner filter for its own
    /// boolean — while a filter that stands alone builds its wrapper graph and
    /// asks whether an edge is in its view ([`edge_filter_from_wrapper!`]
    /// writes that one out). A default could only be one of the two, and every
    /// type that wanted the other would compile silently and be wrong: the
    /// composition it was supposed to fix would quietly come back. Leaving it
    /// required turns each of those into a compile error instead.
    ///
    /// Boxed rather than an associated type so an implementation is one line
    /// instead of a type definition.
    fn create_edge_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Arc<dyn EdgeFilterOp + 'graph>, GraphError>
    where
        Self: 'graph;
}

impl<T: NodeFilterOp> CreateFilter for T {
    crate::edge_filter_from_wrapper!();

    type EntityFiltered<'graph, G: GraphView + 'graph, F: GraphView + 'graph>
        = NodeFilteredGraph<G, T>
    where
        Self: 'graph;

    type NodeFilter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>
        = Self
    where
        Self: 'graph;
    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        _filtered: F,
    ) -> Result<Self::EntityFiltered<'graph, G, F>, GraphError>
    where
        Self: 'graph,
    {
        Ok(NodeFilteredGraph::new(graph, self))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        _graph: G,
        _filtered: F,
    ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError>
    where
        Self: 'graph,
    {
        Ok(self)
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError>
    where
        Self: 'graph,
    {
        Ok(graph)
    }
}
