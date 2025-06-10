use std::sync::Arc;

use db4_common::error::DBV4Error;
use parking_lot::RwLockWriteGuard;
use raphtory_api::core::entities::properties::meta::Meta;
use raphtory_storage::mutation::addition_ops::{InternalAdditionOps, SessionAdditionOps};
use storage::{pages::session::WriteSession, segments::node::MemNodeSegment, Layer, ES, NS};

pub struct TemporalGraph<EXT = ()> {
    layers: boxcar::Vec<Layer<EXT>>,
    edge_meta: Arc<Meta>,
    node_meta: Arc<Meta>,
}

pub type WriteS<'a, MNS, MES, EXT> = WriteSession<'a, MNS, MES, NS<EXT>, ES<EXT>, EXT>;

pub struct UnlockedSession<'a, EXT> {
    graph: &'a TemporalGraph<EXT>,
}

impl <'a, EXT: Send + Sync> SessionAdditionOps for UnlockedSession<'a, EXT> {
    type Error = DBV4Error;
    
    fn next_event_id(&self) -> Result<usize, Self::Error> {
        todo!()
    }
    
    fn reserve_event_ids(&self, num_ids: usize) -> Result<usize, Self::Error> {
        todo!()
    }
    
    fn set_node(&self, gid: raphtory_api::core::entities::GidRef, vid: raphtory_api::core::entities::VID) -> Result<(), Self::Error> {
        todo!()
    }
    
    fn resolve_graph_property(
        &self,
        prop: &str,
        dtype: raphtory_api::core::entities::properties::prop::PropType,
        is_static: bool,
    ) -> Result<raphtory_api::core::storage::dict_mapper::MaybeNew<usize>, Self::Error> {
        todo!()
    }
    
    fn resolve_node_property(
        &self,
        prop: &str,
        dtype: raphtory_api::core::entities::properties::prop::PropType,
        is_static: bool,
    ) -> Result<raphtory_api::core::storage::dict_mapper::MaybeNew<usize>, Self::Error> {
        todo!()
    }
    
    fn resolve_edge_property(
        &self,
        prop: &str,
        dtype: raphtory_api::core::entities::properties::prop::PropType,
        is_static: bool,
    ) -> Result<raphtory_api::core::storage::dict_mapper::MaybeNew<usize>, Self::Error> {
        todo!()
    }
    
    fn internal_add_node(
        &self,
        t: raphtory_api::core::storage::timeindex::TimeIndexEntry,
        v: raphtory_api::core::entities::VID,
        props: &[(usize, raphtory_api::core::entities::properties::prop::Prop)],
    ) -> Result<(), Self::Error> {
        todo!()
    }
    
    fn internal_add_edge(
        &self,
        t: raphtory_api::core::storage::timeindex::TimeIndexEntry,
        src: raphtory_api::core::entities::VID,
        dst: raphtory_api::core::entities::VID,
        props: &[(usize, raphtory_api::core::entities::properties::prop::Prop)],
        layer: usize,
    ) -> Result<raphtory_api::core::storage::dict_mapper::MaybeNew<raphtory_api::core::entities::EID>, Self::Error> {
        todo!()
    }
    
    fn internal_add_edge_update(
        &self,
        t: raphtory_api::core::storage::timeindex::TimeIndexEntry,
        edge: raphtory_api::core::entities::EID,
        props: &[(usize, raphtory_api::core::entities::properties::prop::Prop)],
        layer: usize,
    ) -> Result<(), Self::Error> {
        todo!()
    }

}

impl <EXT : Send + Sync> InternalAdditionOps for TemporalGraph<EXT> {
    type Error = DBV4Error;

    type WS<'a> = UnlockedSession<'a, EXT> where EXT: 'a;

    type AtomicAddEdge<'a> = WriteS<RwLockWriteGuard<MemNodeSegment>, RwLockWriteGuard<MemEdgeSegment>, EXT>;

    fn write_lock(&self) -> Result<raphtory_storage::graph::locked::WriteLockedGraph, Self::Error> {
        todo!()
    }

    fn write_lock_nodes(&self) -> Result<WriteLockedNodes, Self::Error> {
        todo!()
    }

    fn write_lock_edges(&self) -> Result<WriteLockedEdges, Self::Error> {
        todo!()
    }

    fn resolve_layer(&self, layer: Option<&str>) -> Result<raphtory_api::core::storage::dict_mapper::MaybeNew<usize>, Self::Error> {
        todo!()
    }

    fn resolve_node(&self, id: NodeRef) -> Result<raphtory_api::core::storage::dict_mapper::MaybeNew<raphtory_api::core::entities::VID>, Self::Error> {
        todo!()
    }

    fn resolve_node_and_type(
        &self,
        id: NodeRef,
        node_type: &str,
    ) -> Result<raphtory_api::core::storage::dict_mapper::MaybeNew<(raphtory_api::core::storage::dict_mapper::MaybeNew<raphtory_api::core::entities::VID>, raphtory_api::core::storage::dict_mapper::MaybeNew<usize>)>, Self::Error> {
        todo!()
    }

    fn validate_gids<'a>(
        &self,
        gids: impl IntoIterator<Item = raphtory_api::core::entities::GidRef<'a>>,
    ) -> Result<(), Self::Error> {
        todo!()
    }

    fn write_session(&self) -> Result<Self::WS<'_>, Self::Error> {
        todo!()
    }

    fn atomic_add_edge(
        &self,
        src: raphtory_api::core::entities::VID,
        dst: raphtory_api::core::entities::VID,
        e_id: Option<raphtory_api::core::entities::EID>,
    ) -> Result<Self::AtomicAddEdge<'_>, Self::Error> {
        todo!()
    }

    fn validate_prop<PN: AsRef<str>>(
        &self,
        prop: impl ExactSizeIterator<Item = (PN, raphtory_api::core::entities::properties::prop::Prop)>,
    ) -> Result<Vec<(usize, raphtory_api::core::entities::properties::prop::Prop)>, Self::Error> {
        todo!()
    }
}