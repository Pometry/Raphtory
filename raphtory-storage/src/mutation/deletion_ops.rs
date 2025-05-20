use crate::{core_ops::CoreGraphOps, graph::graph::GraphStorage, mutation::MutationError};
use raphtory_api::core::{
    entities::{EID, VID},
    storage::{dict_mapper::MaybeNew, timeindex::TimeIndexEntry},
};

pub trait InternalDeletionOps: CoreGraphOps {
    fn internal_delete_edge(
        &self,
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
        layer: usize,
    ) -> Result<MaybeNew<EID>, MutationError> {
        let edge = self
            .core_graph()
            .mutable()?
            .link_nodes(src, dst, t, layer, true);
        Ok(edge.map(|mut edge| {
            let mut edge = edge.as_mut();
            edge.deletions_mut(layer).insert(t);
            edge.eid()
        }))
    }

    fn internal_delete_existing_edge(
        &self,
        t: TimeIndexEntry,
        eid: EID,
        layer: usize,
    ) -> Result<(), MutationError> {
        let mut edge = self.core_graph().mutable()?.link_edge(eid, t, layer, true);
        let mut edge = edge.as_mut();
        edge.deletions_mut(layer).insert(t);
        Ok(())
    }
}

impl InternalDeletionOps for GraphStorage {}
