use crate::errors::GraphError;
use raphtory_storage::{core_ops::CoreGraphOps, graph::graph::GraphStorage, stage::Handle};

/// Wrapper around a graph that can buffer writes and apply them
/// as an atomic operation.
pub struct Stage<G> {
    handle: Handle,

    /// The staged graph derived from `handle` that can accept writes.
    graph: G,
}

impl<G: CoreGraphOps + From<GraphStorage>> Stage<G> {
    pub(crate) fn new(src_graph: &G) -> Result<Self, GraphError> {
        let handle = src_graph.core_graph().stage()?;
        let graph = G::from(handle.storage().clone());

        Ok(Self { handle, graph })
    }

    pub fn graph(&self) -> &G {
        &self.graph
    }

    pub fn finish(self) -> Result<G, GraphError> {
        self.handle.finish()?;

        Ok(self.graph)
    }

    pub fn discard(self) -> Result<(), GraphError> {
        self.handle.discard().map_err(GraphError::from)
    }
}
