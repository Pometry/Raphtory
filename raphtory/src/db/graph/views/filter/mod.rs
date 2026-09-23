use crate::{
    db::{
        api::{
            state::ops::{filter::NodeExistsOp, NodeFilterOp},
            view::internal::GraphView,
        },
        graph::views::filter::{
            model::edge_expr::{ops::EdgeExistsOp, EdgeOp},
            node_filtered_graph::NodeFilteredGraph,
        },
    },
    errors::GraphError,
};
use std::sync::Arc;

pub mod and_filtered_graph;
pub mod edge_expr_filtered_graph;
pub mod edge_node_filtered_graph;
mod exploded_edge_expr_filtered_graph;
pub mod exploded_edge_filtered_graph;
pub mod exploded_edge_node_filtered_graph;
pub mod model;
pub mod node_filtered_graph;
pub mod not_filtered_graph;
pub mod or_filtered_graph;

pub struct Exists;

impl CreateFilter for Exists {
    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphView + 'graph;

    type NodeFilter<'graph, G>
        = NodeExistsOp<G>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    type EdgeFilter<'graph, G>
        = EdgeExistsOp<G>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_graph_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        Ok(graph)
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        Ok(NodeExistsOp::new(graph))
    }

    fn create_edge_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::EdgeFilter<'graph, G>, GraphError> {
        Ok(EdgeExistsOp::new(graph))
    }
}

/// How a filter is applied. Each entry point compiles the filter against the graph
/// it is handed and answers for one kind of entity: the graph with the failing
/// entities hidden, a yes/no operation per node, or a yes/no operation per edge.
/// Every filter has a per-edge answer, since a filtered graph always decides which
/// edges it keeps; only a node predicate has a per-node one, and an edge predicate
/// refuses that entry point.
pub trait CreateFilter: Sized {
    type FilteredGraph<'graph, G>: GraphView + 'graph
    where
        Self: 'graph,
        G: GraphView + 'graph;

    type NodeFilter<'graph, G>: NodeFilterOp + 'graph
    where
        Self: 'graph,
        G: GraphView + 'graph;

    type EdgeFilter<'graph, G>: EdgeOp<Output = bool> + Clone + 'graph
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_graph_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError>;

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError>;

    fn create_edge_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::EdgeFilter<'graph, G>, GraphError>;
}

/// An erased per-edge predicate, the edge filter of every erased or entity-agnostic
/// filter.
pub type DynEdgeFilter<'graph> = Arc<dyn EdgeOp<Output = bool> + 'graph>;

impl<T: NodeFilterOp> CreateFilter for T {
    type FilteredGraph<'graph, G>
        = NodeFilteredGraph<G, T>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    type NodeFilter<'graph, G>
        = Self
    where
        Self: 'graph,
        G: GraphView + 'graph;

    type EdgeFilter<'graph, G>
        = EdgeExistsOp<NodeFilteredGraph<G, T>>
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_graph_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError>
    where
        Self: 'graph,
    {
        Ok(NodeFilteredGraph::new(graph, self))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        _graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError>
    where
        Self: 'graph,
    {
        Ok(self)
    }

    fn create_edge_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::EdgeFilter<'graph, G>, GraphError>
    where
        Self: 'graph,
    {
        Ok(EdgeExistsOp::new(NodeFilteredGraph::new(graph, self)))
    }
}
