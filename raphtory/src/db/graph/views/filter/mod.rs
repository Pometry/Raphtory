use crate::{
    db::{
        api::{state::ops::NodeFilterOp, view::internal::GraphView},
        graph::views::filter::node_filtered_graph::NodeFilteredGraph,
    },
    errors::GraphError,
    prelude::GraphViewOps,
};

pub mod and_filtered_graph;
pub mod edge_node_filtered_graph;
pub mod edge_property_filtered_graph;
pub mod exploded_edge_node_filtered_graph;
pub mod exploded_edge_property_filter;
pub mod model;
pub mod node_filtered_graph;
pub mod not_filtered_graph;
pub mod or_filtered_graph;

pub trait CreateFilter: Sized {
    type EntityFiltered<'graph, G>: GraphViewOps<'graph>
    where
        Self: 'graph,
        G: GraphViewOps<'graph>;

    type NodeFilter<'graph, G>: NodeFilterOp
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError>;

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError>;
}

impl<T: NodeFilterOp> CreateFilter for T {
    type EntityFiltered<'graph, G: GraphViewOps<'graph>>
        = NodeFilteredGraph<G, T>
    where
        Self: 'graph;

    type NodeFilter<'graph, G: GraphView + 'graph>
        = Self
    where
        Self: 'graph;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError>
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
}
