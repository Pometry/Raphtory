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
pub mod edge_property_filtered_graph;
pub mod edge_test;
pub mod exploded_edge_filtered_graph;
pub mod exploded_edge_node_filtered_graph;
pub mod exploded_edge_property_filter;
pub mod model;
pub mod node_filtered_graph;
pub mod not_filtered_graph;
pub mod or_filtered_graph;

use crate::db::graph::views::filter::edge_test::{EdgeTest, ExistsIn};
use std::sync::Arc;

pub struct Exists;

impl CreateFilter for Exists {
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
    /// Composites must be lowered to a per-edge test, because composing their
    /// operands' wrapper graphs loses a view operand's restriction. Everything
    /// else keeps its wrapper graph, which carries exact per-layer and
    /// per-event semantics that a single edge-level boolean cannot express —
    /// an exploded-edge property filter, for instance, narrows which *events*
    /// of an edge survive, not just which edges.
    fn is_edge_composite(&self) -> bool {
        false
    }

    /// Lower this filter to a per-edge test.
    ///
    /// The default is correct for any filter that is not itself a composite:
    /// build the wrapper graph this filter already produces, then ask it
    /// whether an edge is in its view. Composites must override this to combine
    /// their operands' *tests* — combining their wrapper graphs is what loses a
    /// view operand's restriction. See [`edge_test`] for why.
    ///
    /// Boxed rather than an associated type so that adding this to the trait
    /// does not require boilerplate in every implementation; only the three
    /// composites need to say anything.
    fn create_edge_test<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Arc<dyn EdgeTest + 'graph>, GraphError>
    where
        Self: 'graph,
    {
        Ok(Arc::new(ExistsIn::new(
            self.create_filter(graph, filtered)?,
        )))
    }
}

impl<T: NodeFilterOp> CreateFilter for T {
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
