use crate::{
    core::{
        entities::{graph::tgraph::InternalGraph, VID},
        storage::timeindex::TimeIndexEntry,
        utils::errors::GraphError,
    },
    db::api::{mutation::internal::InternalDeletionOps, view::internal::HasDeletionOps},
};

impl InternalDeletionOps for InternalGraph {
    fn internal_delete_edge(
        &self,
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
        layer: usize,
    ) -> Result<(), GraphError> {
        self.inner().delete_edge(t, src, dst, layer)
    }
}

impl HasDeletionOps for InternalGraph {}
