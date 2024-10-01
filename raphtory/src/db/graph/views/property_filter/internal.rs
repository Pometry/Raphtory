use crate::{core::utils::errors::GraphError, prelude::GraphViewOps};

pub trait InternalEdgeFilterOps: Sized {
    type EdgeFiltered<'graph, G>: GraphViewOps<'graph>
    where
        G: GraphViewOps<'graph>,
        Self: 'graph;

    fn create_edge_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EdgeFiltered<'graph, G>, GraphError>;
}

pub trait InternalExplodedEdgeFilterOps: Sized {
    type ExplodedEdgeFiltered<'graph, G: GraphViewOps<'graph>>: GraphViewOps<'graph>
    where
        Self: 'graph;

    fn create_exploded_edge_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::ExplodedEdgeFiltered<'graph, G>, GraphError>;
}
