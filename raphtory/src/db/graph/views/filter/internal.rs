use crate::{
    db::api::{state::ops::NodeFilterOp, view::internal::GraphView},
    errors::GraphError,
    prelude::GraphViewOps,
};

pub trait CreateFilter: Sized {
    type EntityFiltered<'graph, G>: GraphViewOps<'graph>
    where
        Self: 'graph,
        G: GraphViewOps<'graph>;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError>;
}

pub trait CreateNodeFilter: Sized {
    type NodeFilter<G: GraphView>: NodeFilterOp;
    fn create_node_filter<G: GraphView>(self, graph: G) -> Result<Self::NodeFilter<G>, GraphError>;
}

// Not sure if this is useful
impl<T: NodeFilterOp> CreateNodeFilter for T {
    type NodeFilter<G: GraphView> = T;

    fn create_node_filter<G: GraphView>(
        self,
        _graph: G,
    ) -> Result<Self::NodeFilter<G>, GraphError> {
        Ok(self)
    }
}
