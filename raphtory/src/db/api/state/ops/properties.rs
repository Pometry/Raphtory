use crate::{
    db::{
        api::{
            properties::{Metadata, Properties},
            state::NodeOp,
        },
        graph::node::NodeView,
    },
    prelude::GraphViewOps,
};
use raphtory_api::core::entities::VID;
use raphtory_storage::graph::graph::GraphStorage;
use std::marker::PhantomData;

#[derive(Debug, Clone)]
pub struct GetProperties<'graph, G> {
    pub(crate) graph: G,
    _marker: PhantomData<&'graph ()>,
}

impl<'graph, G> GetProperties<'graph, G> {
    pub fn new(graph: G) -> Self {
        Self {
            graph,
            _marker: PhantomData,
        }
    }
}

impl<'graph, G: GraphViewOps<'graph>> NodeOp for GetProperties<'graph, G> {
    type Output = Properties<NodeView<'graph, G, G>>;

    fn apply(&self, _storage: &GraphStorage, node: VID) -> Self::Output {
        Properties::new(NodeView::new_internal(self.graph.clone(), node))
    }
}

#[derive(Debug, Clone)]
pub struct GetMetadata<'graph, G> {
    pub(crate) graph: G,
    _marker: PhantomData<&'graph ()>,
}

impl<'graph, G> GetMetadata<'graph, G> {
    pub fn new(graph: G) -> Self {
        Self {
            graph,
            _marker: PhantomData,
        }
    }
}

impl<'graph, G: GraphViewOps<'graph>> NodeOp for GetMetadata<'graph, G> {
    type Output = Metadata<'graph, NodeView<'graph, G, G>>;

    fn apply(&self, _storage: &GraphStorage, node: VID) -> Self::Output {
        Metadata::new(NodeView::new_internal(self.graph.clone(), node))
    }
}
