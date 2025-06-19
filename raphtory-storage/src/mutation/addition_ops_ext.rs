use std::ops::DerefMut;

use db4_graph::TemporalGraph;
use parking_lot::RwLockWriteGuard;
use raphtory_api::core::{
    entities::properties::prop::{Prop, PropType},
    storage::dict_mapper::MaybeNew,
};
use raphtory_core::{
    entities::{nodes::node_ref::NodeRef, GidRef, EID, ELID, VID},
    storage::{raw_edges::WriteLockedEdges, timeindex::TimeIndexEntry, WriteLockedNodes},
};
use storage::{
    pages::session::WriteSession,
    persist::strategy::PersistentStrategy,
    properties::props_meta_writer::PropsMetaWriter,
    segments::{edge::MemEdgeSegment, node::MemNodeSegment},
    Layer, ES, NS,
};

use crate::{
    graph::locked::WriteLockedGraph,
    mutation::{
        addition_ops::{AtomicAdditionOps, InternalAdditionOps, SessionAdditionOps},
        MutationError,
    },
};

pub struct WriteS<
    'a,
    MNS: DerefMut<Target = MemNodeSegment>,
    MES: DerefMut<Target = MemEdgeSegment>,
    EXT: PersistentStrategy<NS = NS<EXT>, ES = ES<EXT>>,
> {
    static_session: WriteSession<'a, MNS, MES, NS<EXT>, ES<EXT>, EXT>,
    layer: Option<WriteSession<'a, MNS, MES, NS<EXT>, ES<EXT>, EXT>>,
}

pub struct UnlockedSession<'a, EXT> {
    graph: &'a TemporalGraph<EXT>,
}

impl<
        'a,
        MNS: DerefMut<Target = MemNodeSegment> + Send + Sync,
        MES: DerefMut<Target = MemEdgeSegment> + Send + Sync,
        EXT: PersistentStrategy<NS = NS<EXT>, ES = ES<EXT>>,
    > AtomicAdditionOps for WriteS<'a, MNS, MES, EXT>
{
    fn internal_add_edge(
        &mut self,
        t: TimeIndexEntry,
        src: impl Into<VID>,
        dst: impl Into<VID>,
        lsn: u64,
        layer: usize,
        props: impl IntoIterator<Item = (usize, Prop)>,
    ) -> MaybeNew<ELID> {
        let src = src.into();
        let dst = dst.into();
        let eid = self
            .static_session
            .add_static_edge(src, dst, lsn)
            .map(|eid| eid.with_layer(0));
        self.layer.as_mut().map(|layer| {
            layer.add_edge_into_layer(t, src, dst, eid, lsn, props);
        });

        eid
    }

    fn store_node_id(&self, id: NodeRef, vid: impl Into<VID>) {
        match id {
            NodeRef::External(id) => {
                let vid = vid.into();
                self.static_session.store_node_id(id, vid)
            }
            NodeRef::Internal(id) => Ok(()),
        }
    }
}

impl<'a, EXT: Send + Sync> SessionAdditionOps for UnlockedSession<'a, EXT> {
    type Error = MutationError;

    fn next_event_id(&self) -> Result<usize, Self::Error> {
        todo!()
    }

    fn reserve_event_ids(&self, num_ids: usize) -> Result<usize, Self::Error> {
        todo!()
    }

    fn set_node(&self, gid: GidRef, vid: VID) -> Result<(), Self::Error> {
        todo!()
    }

    fn resolve_graph_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, Self::Error> {
        todo!()
    }

    fn resolve_node_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, Self::Error> {
        todo!()
    }

    fn resolve_edge_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, Self::Error> {
        todo!()
    }

    fn internal_add_node(
        &self,
        t: TimeIndexEntry,
        v: VID,
        props: &[(usize, Prop)],
    ) -> Result<(), Self::Error> {
        todo!()
    }

    fn internal_add_edge(
        &self,
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
        props: &[(usize, Prop)],
        layer: usize,
    ) -> Result<MaybeNew<EID>, Self::Error> {
        todo!()
    }

    fn internal_add_edge_update(
        &self,
        t: TimeIndexEntry,
        edge: EID,
        props: &[(usize, Prop)],
        layer: usize,
    ) -> Result<(), Self::Error> {
        todo!()
    }
}

impl<EXT: PersistentStrategy<NS = NS<EXT>, ES = ES<EXT>>> InternalAdditionOps
    for TemporalGraph<EXT>
{
    type Error = MutationError;

    type WS<'a>
        = UnlockedSession<'a, EXT>
    where
        EXT: 'a;

    type AtomicAddEdge<'a> =
        WriteS<'a, RwLockWriteGuard<'a, MemNodeSegment>, RwLockWriteGuard<'a, MemEdgeSegment>, EXT>;

    fn write_lock(&self) -> Result<WriteLockedGraph, Self::Error> {
        todo!()
    }

    fn write_lock_nodes(&self) -> Result<WriteLockedNodes, Self::Error> {
        todo!()
    }

    fn write_lock_edges(&self) -> Result<WriteLockedEdges, Self::Error> {
        todo!()
    }

    fn resolve_layer(&self, layer: Option<&str>) -> Result<MaybeNew<usize>, Self::Error> {
        let id = self.edge_meta().get_or_create_layer_id(layer);

        let layer_id = id.inner();
        if self.layers().get(layer_id).is_some() {
            return Ok(id);
        }
        let count = self.layers().count();
        if count >= layer_id + 1 {
            // something has allocated the layer, wait for it to be added
            while self.layers().get(layer_id).is_none() {
                // wait for the layer to be created
                std::thread::yield_now();
            }
            return Ok(id);
        } else {
            self.layers().reserve(2);
            let layer_name = layer.unwrap_or("_default");
            loop {
                let new_layer_id = self.layers().push_with(|_| {
                    Layer::new(
                        self.graph_dir().join(format!("l_{}", layer_name)),
                        self.max_page_len_nodes(),
                        self.max_page_len_edges(),
                    )
                    .into()
                });
                if new_layer_id >= layer_id {
                    while self.layers().get(new_layer_id).is_none() {
                        // wait for the layer to be created
                        std::thread::yield_now();
                    }
                    return Ok(id);
                }
            }
        }
    }

    fn resolve_node(&self, id: NodeRef) -> Result<MaybeNew<VID>, Self::Error> {
        match id {
            NodeRef::External(id) => {
                let id = self
                    .logical_to_physical
                    .get_or_init_vid(id, || {
                        self.node_count
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                            .into()
                    })
                    .map_err(MutationError::InvalidNodeId)?;
                Ok(id)
            }
            NodeRef::Internal(id) => Ok(MaybeNew::Existing(id)),
        }
    }

    fn resolve_node_and_type(
        &self,
        id: NodeRef,
        node_type: &str,
    ) -> Result<MaybeNew<(MaybeNew<VID>, MaybeNew<usize>)>, Self::Error> {
        todo!()
    }

    fn validate_gids<'a>(
        &self,
        gids: impl IntoIterator<Item = GidRef<'a>>,
    ) -> Result<(), Self::Error> {
        Ok(self
            .logical_to_physical
            .validate_gids(gids)
            .map_err(MutationError::InvalidNodeId)?)
    }

    fn write_session(&self) -> Result<Self::WS<'_>, Self::Error> {
        todo!()
    }

    fn atomic_add_edge(
        &self,
        src: VID,
        dst: VID,
        e_id: Option<EID>,
        layer_id: usize,
    ) -> Self::AtomicAddEdge<'_> {
        let static_session = self.static_graph().write_session(src, dst, e_id);
        let layer = &self.layers()[layer_id];
        let layer = layer.write_session(src, dst, e_id);
        WriteS {
            static_session,
            layer: Some(layer),
        }
    }

    fn validate_edge_props<PN: AsRef<str>>(
        &self,
        is_static: bool,
        props: impl ExactSizeIterator<Item = (PN, Prop)>,
    ) -> Result<Vec<(usize, Prop)>, Self::Error> {
        if is_static {
            let prop_ids = PropsMetaWriter::constant(self.edge_meta(), props)
                .and_then(|pmw| pmw.into_props_const())
                .map_err(MutationError::DBV4Error)?;
            Ok(prop_ids)
        } else {
            let prop_ids = PropsMetaWriter::temporal(self.edge_meta(), props)
                .and_then(|pmw| pmw.into_props_temporal())
                .map_err(MutationError::DBV4Error)?;
            Ok(prop_ids)
        }
    }
}
