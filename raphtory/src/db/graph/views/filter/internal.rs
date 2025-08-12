use crate::{errors::GraphError, prelude::GraphViewOps};

pub trait CreateEdgeFilter: Sized {
    type EdgeFiltered<'graph, G>: GraphViewOps<'graph>
    where
        G: GraphViewOps<'graph>,
        Self: 'graph;

    fn create_edge_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EdgeFiltered<'graph, G>, GraphError>;
}

pub trait CreateExplodedEdgeFilter: Sized {
    type ExplodedEdgeFiltered<'graph, G: GraphViewOps<'graph>>: GraphViewOps<'graph>
    where
        Self: 'graph;

    fn create_exploded_edge_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::ExplodedEdgeFiltered<'graph, G>, GraphError>;
}

pub trait CreateNodeFilter: Sized {
    type NodeFiltered<'graph, G>: GraphViewOps<'graph>
    where
        Self: 'graph,
        G: GraphViewOps<'graph>;

    fn create_node_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::NodeFiltered<'graph, G>, GraphError>;
}
