use crate::errors::GraphError;
use raphtory_storage::{core_ops::CoreGraphOps, graph::graph::GraphStorage, stage::Handle};
use std::marker::PhantomData;

/// Detached copy of a graph that can buffer writes and apply them
/// as an atomic operation.
pub struct Stage<G> {
    handle: Handle,
    _graph: PhantomData<G>,
}

impl<G: CoreGraphOps + From<GraphStorage>> Stage<G> {
    pub(crate) fn new(src_graph: G) -> Result<Self, GraphError> {
        let handle = src_graph.core_graph().stage()?;

        Ok(Self {
            handle,
            _graph: PhantomData,
        })
    }

    pub fn graph(&self) -> G {
        G::from(self.handle.storage().clone())
    }

    pub fn finish(self) -> Result<G, GraphError> {
        let storage = self.handle.finish()?;

        Ok(G::from(storage))
    }

    pub fn discard(self) -> Result<(), GraphError> {
        self.handle.discard().map_err(GraphError::from)
    }
}
