use crate::{graph::graph::GraphStorage, mutation::MutationError};
use db4_graph::TemporalGraph;
use raphtory_api::{
    core::{
        entities::{properties::meta::STATIC_GRAPH_LAYER_ID, EID, VID},
        storage::{dict_mapper::MaybeNew, timeindex::EventTime},
    },
    inherit::Base,
};
use storage::Extension;

pub trait InternalDeletionOps {
    type Error: From<MutationError>;
    fn internal_delete_edge(
        &self,
        t: EventTime,
        src: VID,
        dst: VID,
        layer: usize,
    ) -> Result<MaybeNew<EID>, Self::Error>;

    fn internal_delete_existing_edge(
        &self,
        t: EventTime,
        eid: EID,
        layer: usize,
    ) -> Result<(), Self::Error>;
}

impl InternalDeletionOps for TemporalGraph<Extension> {
    type Error = MutationError;

    fn internal_delete_edge(
        &self,
        t: EventTime,
        src: VID,
        dst: VID,
        layer: usize,
    ) -> Result<MaybeNew<EID>, Self::Error> {
        let mut session = self.storage().write_session(src, dst, None);
        let edge = session.add_static_edge(src, dst);
        session.delete_edge_from_layer(t, src, dst, edge.map(|eid| eid.with_layer(layer)));
        Ok(edge)
    }

    fn internal_delete_existing_edge(
        &self,
        t: EventTime,
        eid: EID,
        layer: usize,
    ) -> Result<(), Self::Error> {
        let mut writer = self.storage().edge_writer(eid);
        let (_, edge_pos) = self.storage().edges().resolve_pos(eid);
        let (src, dst) = writer.get_edge(STATIC_GRAPH_LAYER_ID, edge_pos).unwrap_or_else(|| {
            panic!("Internal Error: Edge {eid:?} not found in storage");
        });
        writer.delete_edge(t, edge_pos, src, dst, layer);
        Ok(())
    }
}

impl InternalDeletionOps for GraphStorage {
    type Error = MutationError;

    fn internal_delete_edge(
        &self,
        t: EventTime,
        src: VID,
        dst: VID,
        layer: usize,
    ) -> Result<MaybeNew<EID>, Self::Error> {
        self.mutable()?.internal_delete_edge(t, src, dst, layer)
    }

    fn internal_delete_existing_edge(
        &self,
        t: EventTime,
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
        t: EventTime,
        src: VID,
        dst: VID,
        layer: usize,
    ) -> Result<MaybeNew<EID>, Self::Error> {
        self.base().internal_delete_edge(t, src, dst, layer)
    }

    #[inline]
    fn internal_delete_existing_edge(
        &self,
        t: EventTime,
        eid: EID,
        layer: usize,
    ) -> Result<(), Self::Error> {
        self.base().internal_delete_existing_edge(t, eid, layer)
    }
}
