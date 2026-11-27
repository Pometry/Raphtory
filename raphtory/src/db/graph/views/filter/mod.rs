use crate::{
    db::{
        api::{
            state::ops::{filter::NodeExistsOp, NodeFilterOp},
            view::internal::GraphView,
        },
        graph::views::filter::{
            edge_expr_filtered_graph::EdgeExprFilteredGraph,
            model::{
                edge_expr::{
                    ops::{EdgeEndpointNodeOp, EdgeExistsOp},
                    EdgeOp,
                },
                edge_filter::Endpoint,
            },
            node_filtered_graph::NodeFilteredGraph,
        },
    },
    errors::GraphError,
};
use std::sync::Arc;

pub mod and_filtered_graph;
pub mod edge_expr_filtered_graph;
pub mod exploded_edge_expr_filtered_graph;
pub mod model;
pub mod node_filtered_graph;
mod operand;
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
    type PathFilter<'graph, G, S>
        = S
    where
        Self: 'graph,
        G: GraphView + 'graph,
        S: GraphView + 'graph;

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

    fn create_path_filter<'graph, G: GraphView + 'graph, S: GraphView + 'graph>(
        self,
        _graph: G,
        select: S,
    ) -> Result<Self::PathFilter<'graph, G, S>, GraphError> {
        Ok(select)
    }
}

/// How a filter is applied. Each entry point compiles the filter against the graph
/// it is handed and answers for one kind of entity: the graph with the failing
/// entities hidden, a boolean operation per node, or a boolean operation per edge.
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

    type PathFilter<'graph, G, S>: GraphView + 'graph
    where
        Self: 'graph,
        G: GraphView + 'graph,
        S: GraphView + 'graph;

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

    /// Create the filtered graph used for the edge filtering in a path
    ///
    /// `graph` is the base graph that should be used for constructing the filter and `select` is the
    /// current edge filter
    ///
    /// Returns a graph view for edge filtering and an additional node filter for filtering the neighbours
    fn create_path_filter<'graph, G: GraphView + 'graph, S: GraphView + 'graph>(
        self,
        graph: G,
        select: S,
    ) -> Result<Self::PathFilter<'graph, G, S>, GraphError>;
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

    type PathFilter<'graph, G, S>
        = EdgeExprFilteredGraph<S, EdgeEndpointNodeOp<'graph, bool>>
    where
        Self: 'graph,
        G: GraphView + 'graph,
        S: GraphView + 'graph;

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
    fn create_path_filter<'graph, G: GraphView + 'graph, S: GraphView + 'graph>(
        self,
        _graph: G,
        select: S,
    ) -> Result<Self::PathFilter<'graph, G, S>, GraphError>
    where
        Self: 'graph,
    {
        let filter = EdgeEndpointNodeOp {
            node_op: Arc::new(self),
            endpoint: Endpoint::Nbr,
        };
        let select = EdgeExprFilteredGraph::new(select, filter);
        Ok(select)
    }
}
