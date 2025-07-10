#[cfg(feature = "search")]
use crate::search::graph_index::GraphIndex;
use crate::{
    core::entities::nodes::node_ref::NodeRef,
    db::api::view::{
        internal::{InheritEdgeHistoryFilter, InheritNodeHistoryFilter, InternalStorageOps},
        Base, InheritViewOps,
    },
};
use db4_graph::{TemporalGraph, WriteLockedGraph};
use parking_lot::RwLockWriteGuard;
use raphtory_api::core::{
    entities::{properties::meta::Meta, EID, VID},
    storage::{dict_mapper::MaybeNew, timeindex::TimeIndexEntry},
};
use raphtory_storage::{
    graph::graph::GraphStorage,
    mutation::{
        addition_ops::{EdgeWriteLock, SessionAdditionOps},
        addition_ops_ext::{UnlockedSession, WriteS},
    },
};
use std::{
    fmt::{Display, Formatter},
    path::PathBuf,
    sync::Arc,
};
use storage::{
    segments::{edge::MemEdgeSegment, node::MemNodeSegment},
    Extension,
};
use tracing::info;

use crate::{db::api::view::IndexSpec, errors::GraphError};
#[cfg(feature = "search")]
use once_cell::sync::OnceCell;
use raphtory_api::core::entities::{
    properties::prop::{Prop, PropType},
    GidRef,
};
use raphtory_core::{
    entities::ELID,
    storage::{raw_edges::WriteLockedEdges, WriteLockedNodes},
};
use raphtory_storage::{
    core_ops::InheritCoreGraphOps,
    layer_ops::InheritLayerOps,
    mutation::{
        addition_ops::InternalAdditionOps, deletion_ops::InternalDeletionOps,
        property_addition_ops::InternalPropertyAdditionOps,
    },
};

#[derive(Debug, Default)]
pub struct Storage {
    graph: GraphStorage,
    #[cfg(feature = "search")]
    pub(crate) index: OnceCell<GraphIndex>,
}

impl InheritLayerOps for Storage {}
impl InheritCoreGraphOps for Storage {}

impl Display for Storage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.graph, f)
    }
}

impl Base for Storage {
    type Base = GraphStorage;

    #[inline]
    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

#[cfg(feature = "search")]
const IN_MEMORY_INDEX_NOT_PERSISTED: &str = "In-memory index not persisted. Not supported";

impl Storage {
    pub(crate) fn new(num_locks: usize) -> Self {
        Self {
            graph: GraphStorage::Unlocked(Arc::new(TemporalGraph::new(None))),
            #[cfg(feature = "search")]
            index: OnceCell::new(),
        }
    }

    pub(crate) fn from_inner(graph: GraphStorage) -> Self {
        Self {
            graph,
            #[cfg(feature = "search")]
            index: OnceCell::new(),
        }
    }

    #[cfg(feature = "search")]
    #[inline]
    fn if_index(
        &self,
        map_fn: impl FnOnce(&GraphIndex) -> Result<(), GraphError>,
    ) -> Result<(), GraphError> {
        if let Some(index) = self.index.get() {
            map_fn(index)
        } else {
            Ok(())
        }
    }
}

#[cfg(feature = "search")]
impl Storage {
    pub(crate) fn get_index_spec(&self) -> Result<IndexSpec, GraphError> {
        let index = self.index.get().ok_or(GraphError::GraphIndexIsMissing)?;
        Ok(index.index_spec.read().clone())
    }

    pub(crate) fn get_or_load_index(&self, path: PathBuf) -> Result<&GraphIndex, GraphError> {
        self.index.get_or_try_init(|| {
            let index = GraphIndex::load_from_path(&path)?;
            Ok(index)
        })
    }

    pub(crate) fn get_or_create_index(
        &self,
        index_spec: IndexSpec,
    ) -> Result<&GraphIndex, GraphError> {
        let index = self
            .index
            .get_or_try_init(|| GraphIndex::create(&self.graph, false, None))?;
        index.update(&self.graph, index_spec)?;
        Ok(index)
    }

    pub(crate) fn get_or_create_index_in_ram(
        &self,
        index_spec: IndexSpec,
    ) -> Result<&GraphIndex, GraphError> {
        let index = self
            .index
            .get_or_try_init(|| GraphIndex::create(&self.graph, true, None))?;

        if index.path.is_some() {
            return Err(GraphError::FailedToCreateIndexInRam);
        }

        index.update(&self.graph, index_spec)?;

        Ok(index)
    }

    pub(crate) fn get_index(&self) -> Option<&GraphIndex> {
        self.index.get()
    }

    pub(crate) fn persist_index_to_disk(&self, path: &PathBuf) -> Result<(), GraphError> {
        if let Some(index) = self.get_index() {
            if index.path.is_none() {
                info!("{}", IN_MEMORY_INDEX_NOT_PERSISTED);
                return Ok(());
            }
            index.persist_to_disk(path)?
        }
        Ok(())
    }

    pub(crate) fn persist_index_to_disk_zip(&self, path: &PathBuf) -> Result<(), GraphError> {
        if let Some(index) = self.get_index() {
            if index.path.is_none() {
                info!("{}", IN_MEMORY_INDEX_NOT_PERSISTED);
                return Ok(());
            }
            index.persist_to_disk_zip(path)?
        }
        Ok(())
    }
}

impl InternalStorageOps for Storage {
    fn get_storage(&self) -> Option<&Storage> {
        Some(self)
    }
}

impl InheritNodeHistoryFilter for Storage {}
impl InheritEdgeHistoryFilter for Storage {}

impl InheritViewOps for Storage {}

#[derive(Clone)]
pub struct StorageWriteSession<'a> {
    session: UnlockedSession<'a>,
    storage: &'a Storage,
}

pub struct AtomicAddEdgeSession<'a> {
    session: WriteS<'a, Extension>,
    storage: &'a Storage,
}

impl EdgeWriteLock for AtomicAddEdgeSession<'_> {
    fn internal_add_edge(
        &mut self,
        t: TimeIndexEntry,
        src: impl Into<VID>,
        dst: impl Into<VID>,
        e_id: MaybeNew<ELID>,
        lsn: u64,
        props: impl IntoIterator<Item = (usize, Prop)>,
    ) -> MaybeNew<ELID> {
        self.session
            .internal_add_edge(t, src, dst, e_id, lsn, props)
    }

    fn internal_add_static_edge(
        &mut self,
        src: impl Into<VID>,
        dst: impl Into<VID>,
        lsn: u64,
    ) -> MaybeNew<EID> {
        self.session.internal_add_static_edge(src, dst, lsn)
    }

    fn internal_delete_edge(
        &mut self,
        t: TimeIndexEntry,
        src: impl Into<VID>,
        dst: impl Into<VID>,
        lsn: u64,
        layer: usize,
    ) -> MaybeNew<ELID> {
        self.session.internal_delete_edge(t, src, dst, lsn, layer)
    }

    fn store_src_node_info(&mut self, id: impl Into<VID>, node_id: Option<GidRef>) {
        self.session.store_src_node_info(id, node_id);
    }

    fn store_dst_node_info(&mut self, id: impl Into<VID>, node_id: Option<GidRef>) {
        self.session.store_dst_node_info(id, node_id);
    }
}

impl<'a> SessionAdditionOps for StorageWriteSession<'a> {
    type Error = GraphError;

    fn next_event_id(&self) -> Result<usize, Self::Error> {
        Ok(self.session.next_event_id()?)
    }

    fn reserve_event_ids(&self, num_ids: usize) -> Result<usize, Self::Error> {
        Ok(self.session.reserve_event_ids(num_ids)?)
    }

    fn set_node(&self, gid: GidRef, vid: VID) -> Result<(), Self::Error> {
        Ok(self.session.set_node(gid, vid)?)
    }

    fn resolve_graph_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, Self::Error> {
        let id = self
            .session
            .resolve_graph_property(prop, dtype.clone(), is_static)?;

        Ok(id)
    }

    fn resolve_node_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, Self::Error> {
        let id = self
            .session
            .resolve_node_property(prop, dtype.clone(), is_static)?;

        Ok(id)
    }

    fn resolve_edge_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, Self::Error> {
        let id = self
            .session
            .resolve_edge_property(prop, dtype.clone(), is_static)?;

        Ok(id)
    }

    fn internal_add_node(
        &self,
        t: TimeIndexEntry,
        v: VID,
        props: &[(usize, Prop)],
    ) -> Result<(), Self::Error> {
        self.session.internal_add_node(t, v, props)?;

        #[cfg(feature = "search")]
        self.storage.if_index(|index| {
            index.add_node_update(&self.storage.graph, t, MaybeNew::New(v), props)
        })?;

        Ok(())
    }

    fn internal_add_edge(
        &self,
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
        props: &[(usize, Prop)],
        layer: usize,
    ) -> Result<MaybeNew<EID>, Self::Error> {
        let id = self.session.internal_add_edge(t, src, dst, props, layer)?;
        #[cfg(feature = "search")]
        self.storage
            .if_index(|index| index.add_edge_update(&self.storage.graph, id, t, layer, props))?;
        Ok(id)
    }

    fn internal_add_edge_update(
        &self,
        t: TimeIndexEntry,
        edge: EID,
        props: &[(usize, Prop)],
        layer: usize,
    ) -> Result<(), Self::Error> {
        self.session
            .internal_add_edge_update(t, edge, props, layer)?;

        #[cfg(feature = "search")]
        self.storage.if_index(|index| {
            index.add_edge_update(
                &self.storage.graph,
                MaybeNew::Existing(edge),
                t,
                layer,
                props,
            )
        })?;
        Ok(())
    }
}

impl InternalAdditionOps for Storage {
    type Error = GraphError;

    type WS<'a> = StorageWriteSession<'a>;
    type AtomicAddEdge<'a> = AtomicAddEdgeSession<'a>;

    fn write_lock(&self) -> Result<WriteLockedGraph<Extension>, Self::Error> {
        Ok(self.graph.write_lock()?)
    }

    fn write_lock_nodes(&self) -> Result<WriteLockedNodes, Self::Error> {
        Ok(self.graph.write_lock_nodes()?)
    }

    fn write_lock_edges(&self) -> Result<WriteLockedEdges, Self::Error> {
        Ok(self.graph.write_lock_edges()?)
    }

    fn resolve_layer(&self, layer: Option<&str>) -> Result<MaybeNew<usize>, Self::Error> {
        let id = self.graph.resolve_layer(layer)?;

        Ok(id)
    }

    fn resolve_node(&self, id: NodeRef) -> Result<MaybeNew<VID>, Self::Error> {
        match id {
            NodeRef::Internal(id) => Ok(MaybeNew::Existing(id)),
            NodeRef::External(gid) => {
                let id = self.graph.resolve_node(id)?;

                Ok(id)
            }
        }
    }

    fn resolve_node_and_type(
        &self,
        id: NodeRef,
        node_type: &str,
    ) -> Result<MaybeNew<(MaybeNew<VID>, MaybeNew<usize>)>, Self::Error> {
        let node_and_type = self.graph.resolve_node_and_type(id, node_type)?;

        Ok(node_and_type)
    }

    fn write_session(&self) -> Result<Self::WS<'_>, Self::Error> {
        let session = self.graph.write_session()?;
        Ok(StorageWriteSession {
            session,
            storage: self,
        })
    }

    fn atomic_add_edge(
        &self,
        src: VID,
        dst: VID,
        e_id: Option<EID>,
        layer_id: usize,
    ) -> Result<Self::AtomicAddEdge<'_>, Self::Error> {
        let session = self.graph.atomic_add_edge(src, dst, e_id, layer_id)?;
        Ok(AtomicAddEdgeSession {
            session,
            storage: self,
        })
    }

    fn internal_add_node(
        &self,
        t: TimeIndexEntry,
        v: impl Into<VID>,
        gid: Option<GidRef>,
        node_type: Option<usize>,
        props: impl IntoIterator<Item = (usize, Prop)>,
    ) -> Result<(), Self::Error> {
        Ok(self.graph.internal_add_node(t, v, gid, node_type, props)?)
    }

    fn validate_props<PN: AsRef<str>>(
        &self,
        is_static: bool,
        meta: &Meta,
        prop: impl Iterator<Item = (PN, Prop)>,
    ) -> Result<Vec<(usize, Prop)>, Self::Error> {
        Ok(self.graph.validate_props(is_static, meta, prop)?)
    }

    fn validate_gids<'a>(
        &self,
        gids: impl IntoIterator<Item = GidRef<'a>>,
    ) -> Result<(), Self::Error> {
        Ok(self.graph.validate_gids(gids)?)
    }
}

impl InternalPropertyAdditionOps for Storage {
    type Error = GraphError;
    fn internal_add_properties(
        &self,
        t: TimeIndexEntry,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        self.graph.internal_add_properties(t, props)?;

        Ok(())
    }

    fn internal_add_constant_properties(&self, props: &[(usize, Prop)]) -> Result<(), GraphError> {
        self.graph.internal_add_constant_properties(props)?;

        Ok(())
    }

    fn internal_update_constant_properties(
        &self,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        self.graph.internal_update_constant_properties(props)?;

        Ok(())
    }

    fn internal_add_constant_node_properties(
        &self,
        vid: VID,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        self.graph
            .internal_add_constant_node_properties(vid, props)?;

        #[cfg(feature = "search")]
        self.if_index(|index| index.add_node_constant_properties(vid, props))?;

        Ok(())
    }

    fn internal_update_constant_node_properties(
        &self,
        vid: VID,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        self.graph
            .internal_update_constant_node_properties(vid, props)?;

        #[cfg(feature = "search")]
        self.if_index(|index| index.update_node_constant_properties(vid, props))?;

        Ok(())
    }

    fn internal_add_constant_edge_properties(
        &self,
        eid: EID,
        layer: usize,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        self.graph
            .internal_add_constant_edge_properties(eid, layer, props)?;

        #[cfg(feature = "search")]
        self.if_index(|index| index.add_edge_constant_properties(eid, layer, props))?;

        Ok(())
    }

    fn internal_update_constant_edge_properties(
        &self,
        eid: EID,
        layer: usize,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        self.graph
            .internal_update_constant_edge_properties(eid, layer, props)?;

        #[cfg(feature = "search")]
        self.if_index(|index| index.update_edge_constant_properties(eid, layer, props))?;

        Ok(())
    }
}

impl InternalDeletionOps for Storage {
    type Error = GraphError;
    fn internal_delete_edge(
        &self,
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
        layer: usize,
    ) -> Result<MaybeNew<EID>, GraphError> {
        let eid = self.graph.internal_delete_edge(t, src, dst, layer)?;

        Ok(eid)
    }

    fn internal_delete_existing_edge(
        &self,
        t: TimeIndexEntry,
        eid: EID,
        layer: usize,
    ) -> Result<(), GraphError> {
        self.graph.internal_delete_existing_edge(t, eid, layer)?;

        Ok(())
    }
}
