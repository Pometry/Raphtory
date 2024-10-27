use super::GraphStorage;
use crate::{
    core::{entities::graph::tgraph::TemporalGraph, utils::errors::GraphError},
    db::api::mutation::internal::InternalDeletionOps,
};
use raphtory_api::core::{
    entities::{EID, VID},
    storage::{dict_mapper::MaybeNew, timeindex::TimeIndexEntry},
};

impl InternalDeletionOps for TemporalGraph {
    fn internal_delete_edge(
        &self,
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
        layer: usize,
    ) -> Result<MaybeNew<EID>, GraphError> {
        self.link_nodes(src, dst, t, layer, |mut new_edge| {
            new_edge.deletions_mut(layer).insert(t);
            Ok(())
        })
    }

    fn internal_delete_existing_edge(
        &self,
        t: TimeIndexEntry,
        eid: EID,
        layer: usize,
    ) -> Result<(), GraphError> {
        self.link_edge(eid, t, layer, |mut edge| {
            edge.deletions_mut(layer).insert(t);
            Ok(())
        })
    }
}

impl InternalDeletionOps for GraphStorage {
    #[inline]
    fn internal_delete_edge(
        &self,
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
        layer: usize,
    ) -> Result<MaybeNew<EID>, GraphError> {
        match self {
            GraphStorage::Unlocked(storage) => storage.internal_delete_edge(t, src, dst, layer),
            _ => Err(GraphError::AttemptToMutateImmutableGraph),
        }
    }

    #[inline]
    fn internal_delete_existing_edge(
        &self,
        t: TimeIndexEntry,
        eid: EID,
        layer: usize,
    ) -> Result<(), GraphError> {
        match self {
            GraphStorage::Unlocked(storage) => storage.internal_delete_existing_edge(t, eid, layer),
            _ => Err(GraphError::AttemptToMutateImmutableGraph),
        }
    }
}
