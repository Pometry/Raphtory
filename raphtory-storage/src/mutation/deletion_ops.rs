use crate::{graph::graph::GraphStorage, mutation::MutationError};
use raphtory_api::{
    core::{
        entities::{EID, VID},
        storage::{dict_mapper::MaybeNew, timeindex::TimeIndexEntry},
    },
    inherit::Base,
};
use raphtory_core::entities::graph::tgraph::TemporalGraph;

pub trait InternalDeletionOps {
    type Error: From<MutationError>;
    fn internal_delete_edge(
        &self,
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
        layer: usize,
    ) -> Result<MaybeNew<EID>, Self::Error>;
    fn internal_delete_existing_edge(
        &self,
        t: TimeIndexEntry,
        eid: EID,
        layer: usize,
    ) -> Result<(), Self::Error>;
}

impl<EXT> InternalDeletionOps for db4_graph::TemporalGraph<EXT> {
    type Error = MutationError;

    fn internal_delete_edge(
        &self,
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
        layer: usize,
    ) -> Result<MaybeNew<EID>, Self::Error> {
        todo!()
    }

    fn internal_delete_existing_edge(
        &self,
        t: TimeIndexEntry,
        eid: EID,
        layer: usize,
    ) -> Result<(), Self::Error> {
        todo!()
    }
}

impl InternalDeletionOps for TemporalGraph {
    type Error = MutationError;

    fn internal_delete_edge(
        &self,
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
        layer: usize,
    ) -> Result<MaybeNew<EID>, Self::Error> {
        let edge = self.link_nodes(src, dst, t, layer, true);
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
    ) -> Result<(), Self::Error> {
        let mut edge = self.link_edge(eid, t, layer, true);
        let mut edge = edge.as_mut();
        edge.deletions_mut(layer).insert(t);
        Ok(())
    }
}

impl InternalDeletionOps for GraphStorage {
    type Error = MutationError;

    fn internal_delete_edge(
        &self,
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
        layer: usize,
    ) -> Result<MaybeNew<EID>, Self::Error> {
        self.mutable()?.internal_delete_edge(t, src, dst, layer)
    }

    fn internal_delete_existing_edge(
        &self,
        t: TimeIndexEntry,
        eid: EID,
        layer: usize,
    ) -> Result<(), Self::Error> {
        self.mutable()?.internal_delete_existing_edge(t, eid, layer)
    }
}

pub trait InheritDeletionOps: Base {}

impl<G: InheritDeletionOps> InternalDeletionOps for G
where
    G::Base: InternalDeletionOps,
{
    type Error = <G::Base as InternalDeletionOps>::Error;

    #[inline]
    fn internal_delete_edge(
        &self,
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
        layer: usize,
    ) -> Result<MaybeNew<EID>, Self::Error> {
        self.base().internal_delete_edge(t, src, dst, layer)
    }

    #[inline]
    fn internal_delete_existing_edge(
        &self,
        t: TimeIndexEntry,
        eid: EID,
        layer: usize,
    ) -> Result<(), Self::Error> {
        self.base().internal_delete_existing_edge(t, eid, layer)
    }
}
