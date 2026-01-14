use crate::{
    core::entities::nodes::node_ref::NodeRef,
    db::api::view::{
        internal::{InheritEdgeHistoryFilter, InheritNodeHistoryFilter, InternalStorageOps},
        Base, InheritViewOps,
    },
    errors::GraphError,
};
use db4_graph::{GraphDir, TemporalGraph, WriteLockedGraph};
use raphtory_api::core::{
    entities::{
        properties::{
            meta::Meta,
            prop::{Prop, PropType},
        },
        GidRef, EID, VID,
    },
    storage::{dict_mapper::MaybeNew, timeindex::TimeIndexEntry},
};
use raphtory_core::entities::ELID;
use raphtory_storage::{
    core_ops::InheritCoreGraphOps,
    graph::graph::GraphStorage,
    layer_ops::InheritLayerOps,
    mutation::{
        addition_ops::{EdgeWriteLock, InternalAdditionOps, SessionAdditionOps},
        addition_ops_ext::{UnlockedSession, WriteS},
        deletion_ops::InternalDeletionOps,
        durability_ops::DurabilityOps,
        property_addition_ops::InternalPropertyAdditionOps,
        EdgeWriterT, NodeWriterT,
    },
};
use std::{
    fmt::{Display, Formatter},
    path::Path,
    sync::Arc,
};
use storage::{
    transaction::TransactionManager,
    wal::{Wal, LSN},
    WalType,
};

pub use storage::{
    persist::strategy::{PersistenceConfig, PersistenceStrategy},
    Extension,
};
#[cfg(feature = "search")]
use {
    crate::{
        db::api::view::IndexSpec,
        search::graph_index::{GraphIndex, MutableGraphIndex},
        serialise::{GraphFolder, GraphPaths},
    },
    either::Either,
    parking_lot::RwLock,
    raphtory_core::entities::nodes::node_ref::AsNodeRef,
    raphtory_storage::{core_ops::CoreGraphOps, graph::nodes::node_storage_ops::NodeStorageOps},
    std::{
        io::{Seek, Write},
        ops::{Deref, DerefMut},
    },
    tracing::info,
    zip::ZipWriter,
};

#[derive(Debug, Default)]
pub struct Storage {
    graph: GraphStorage,
    #[cfg(feature = "search")]
    pub(crate) index: RwLock<GraphIndex>,
}

impl From<GraphStorage> for Storage {
    fn from(graph: GraphStorage) -> Self {
        Self::from_inner(graph)
    }
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
    pub(crate) fn new() -> Self {
        Self {
            graph: GraphStorage::Unlocked(Arc::new(TemporalGraph::default())),
            #[cfg(feature = "search")]
            index: RwLock::new(GraphIndex::Empty),
        }
    }

    pub(crate) fn new_at_path(path: impl AsRef<Path>) -> Result<Self, GraphError> {
        let config = PersistenceConfig::default();
        let graph_dir = GraphDir::from(path.as_ref());
        let wal_dir = Some(graph_dir.wal_dir());
        let wal = Arc::new(WalType::new(wal_dir)?);
        let ext = Extension::new(config, wal);
        let temporal_graph = TemporalGraph::new_with_path(path, ext)?;

        Ok(Self {
            graph: GraphStorage::Unlocked(Arc::new(temporal_graph)),
            #[cfg(feature = "search")]
            index: RwLock::new(GraphIndex::Empty),
        })
    }

    pub(crate) fn load_from(path: impl AsRef<Path>) -> Result<Self, GraphError> {
        let config = PersistenceConfig::load_from_dir(path.as_ref())
            .unwrap_or_else(|_| PersistenceConfig::default());
        let graph_dir = GraphDir::from(path.as_ref());
        let wal_dir = Some(graph_dir.wal_dir());
        let wal = Arc::new(WalType::new(wal_dir)?);
        let ext = Extension::new(config, wal);
        let temporal_graph = TemporalGraph::load_from_path(path, ext)?;

        Ok(Self {
            graph: GraphStorage::Unlocked(Arc::new(temporal_graph)),
            #[cfg(feature = "search")]
            index: RwLock::new(GraphIndex::Empty),
        })
    }

    pub(crate) fn from_inner(graph: GraphStorage) -> Self {
        Self {
            graph,
            #[cfg(feature = "search")]
            index: RwLock::new(GraphIndex::Empty),
        }
    }

    #[cfg(feature = "search")]
    #[inline]
    fn if_index(
        &self,
        map_fn: impl FnOnce(&GraphIndex) -> Result<(), GraphError>,
    ) -> Result<(), GraphError> {
        map_fn(&self.index.read_recursive())?;
        Ok(())
    }

    #[cfg(feature = "search")]
    #[inline]
    fn if_index_mut(
        &self,
        map_fn: impl FnOnce(&MutableGraphIndex) -> Result<(), GraphError>,
    ) -> Result<(), GraphError> {
        let guard = self.index.read_recursive();
        match guard.deref() {
            GraphIndex::Empty => {}
            GraphIndex::Mutable(i) => map_fn(i)?,
            GraphIndex::Immutable(_) => {
                drop(guard);
                let mut guard = self.index.write();
                guard.make_mutable_if_needed()?;
                if let GraphIndex::Mutable(m) = guard.deref_mut() {
                    map_fn(m)?
                }
            }
        }
        Ok(())
    }
}

#[cfg(feature = "search")]
impl Storage {
    pub(crate) fn get_index_spec(&self) -> Result<IndexSpec, GraphError> {
        Ok(self.index.read_recursive().index_spec())
    }

    pub(crate) fn load_index_if_empty(&self, path: &GraphFolder) -> Result<(), GraphError> {
        let guard = self.index.read_recursive();
        match guard.deref() {
            GraphIndex::Empty => {
                drop(guard);
                let mut guard = self.index.write();
                if let e @ GraphIndex::Empty = guard.deref_mut() {
                    let index = GraphIndex::load_from_path(&path)?;
                    *e = index;
                }
            }
            _ => {}
        }
        Ok(())
    }

    pub(crate) fn create_index_if_empty(&self, index_spec: IndexSpec) -> Result<(), GraphError> {
        {
            let guard = self.index.read_recursive();
            match guard.deref() {
                GraphIndex::Empty => {
                    drop(guard);
                    let mut guard = self.index.write();
                    if let e @ GraphIndex::Empty = guard.deref_mut() {
                        let index = GraphIndex::create(&self.graph, false, None)?;
                        *e = index;
                    }
                }
                _ => {}
            }
        }
        self.if_index_mut(|index| index.update(&self.graph, index_spec))?;
        Ok(())
    }

    pub(crate) fn create_index_in_ram_if_empty(
        &self,
        index_spec: IndexSpec,
    ) -> Result<(), GraphError> {
        {
            let guard = self.index.read_recursive();
            match guard.deref() {
                GraphIndex::Empty => {
                    drop(guard);
                    let mut guard = self.index.write();
                    if let e @ GraphIndex::Empty = guard.deref_mut() {
                        let index = GraphIndex::create(&self.graph, true, None)?;
                        *e = index;
                    }
                }
                _ => {}
            }
        }
        if self.index.read_recursive().path().is_some() {
            return Err(GraphError::OnDiskIndexAlreadyExists);
        }
        self.if_index_mut(|index| index.update(&self.graph, index_spec))?;
        Ok(())
    }

    pub(crate) fn get_index(&self) -> &RwLock<GraphIndex> {
        &self.index
    }

    pub(crate) fn is_indexed(&self) -> bool {
        self.index.read_recursive().is_indexed()
    }

    pub(crate) fn persist_index_to_disk(&self, path: &impl GraphPaths) -> Result<(), GraphError> {
        let guard = self.get_index().read_recursive();
        if guard.is_indexed() {
            if guard.path().is_none() {
                info!("{}", IN_MEMORY_INDEX_NOT_PERSISTED);
                return Ok(());
            }
            self.if_index(|index| index.persist_to_disk(path))?;
        }
        Ok(())
    }

    pub(crate) fn persist_index_to_disk_zip<W: Write + Seek>(
        &self,
        writer: &mut ZipWriter<W>,
        prefix: &str,
    ) -> Result<(), GraphError> {
        let guard = self.get_index().read_recursive();
        if guard.is_indexed() {
            if guard.path().is_none() {
                info!("{}", IN_MEMORY_INDEX_NOT_PERSISTED);
                return Ok(());
            }
            self.if_index(|index| index.persist_to_disk_zip(writer, prefix))?;
        }
        Ok(())
    }

    pub(crate) fn drop_index(&self) -> Result<(), GraphError> {
        let mut guard = self.index.write();
        *guard = GraphIndex::Empty;
        Ok(())
    }
}

impl InternalStorageOps for Storage {
    fn get_storage(&self) -> Option<&Storage> {
        Some(self)
    }

    fn disk_storage_path(&self) -> Option<&Path> {
        self.graph.disk_storage_path()
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
    fn internal_add_static_edge(
        &mut self,
        src: impl Into<VID>,
        dst: impl Into<VID>,
    ) -> MaybeNew<EID> {
        self.session.internal_add_static_edge(src, dst)
    }

    fn internal_add_edge(
        &mut self,
        t: TimeIndexEntry,
        src: impl Into<VID>,
        dst: impl Into<VID>,
        e_id: MaybeNew<ELID>,
        props: impl IntoIterator<Item = (usize, Prop)>,
    ) -> MaybeNew<ELID> {
        self.session.internal_add_edge(t, src, dst, e_id, props)
    }

    fn internal_delete_edge(
        &mut self,
        t: TimeIndexEntry,
        src: impl Into<VID>,
        dst: impl Into<VID>,
        layer: usize,
    ) -> MaybeNew<ELID> {
        self.session.internal_delete_edge(t, src, dst, layer)
    }

    fn store_src_node_info(&mut self, id: impl Into<VID>, node_id: Option<GidRef>) {
        self.session.store_src_node_info(id, node_id);
    }

    fn store_dst_node_info(&mut self, id: impl Into<VID>, node_id: Option<GidRef>) {
        self.session.store_dst_node_info(id, node_id);
    }

    fn set_lsn(&mut self, lsn: LSN) {
        self.session.set_lsn(lsn);
    }
}

impl<'a> SessionAdditionOps for StorageWriteSession<'a> {
    type Error = GraphError;

    fn read_event_id(&self) -> Result<usize, Self::Error> {
        Ok(self.session.read_event_id()?)
    }

    fn set_event_id(&self, event_id: usize) -> Result<(), Self::Error> {
        Ok(self.session.set_event_id(event_id)?)
    }

    fn next_event_id(&self) -> Result<usize, Self::Error> {
        Ok(self.session.next_event_id()?)
    }

    fn reserve_event_ids(&self, num_ids: usize) -> Result<usize, Self::Error> {
        Ok(self.session.reserve_event_ids(num_ids)?)
    }

    fn set_max_event_id(&self, value: usize) -> Result<usize, Self::Error> {
        Ok(self.session.set_max_event_id(value)?)
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
}

impl InternalAdditionOps for Storage {
    type Error = GraphError;

    type WS<'a> = StorageWriteSession<'a>;
    type AtomicAddEdge<'a> = AtomicAddEdgeSession<'a>;

    fn write_lock(&self) -> Result<WriteLockedGraph<'_, Extension>, Self::Error> {
        Ok(self.graph.write_lock()?)
    }

    fn resolve_layer(&self, layer: Option<&str>) -> Result<MaybeNew<usize>, Self::Error> {
        let id = self.graph.resolve_layer(layer)?;

        Ok(id)
    }

    fn resolve_node(&self, id: NodeRef) -> Result<MaybeNew<VID>, Self::Error> {
        match id {
            NodeRef::Internal(id) => Ok(MaybeNew::Existing(id)),
            NodeRef::External(_) => {
                let id = self.graph.resolve_node(id)?;

                Ok(id)
            }
        }
    }

    fn resolve_and_update_node_and_type(
        &self,
        id: NodeRef,
        node_type: Option<&str>,
    ) -> Result<MaybeNew<(MaybeNew<VID>, MaybeNew<usize>)>, Self::Error> {
        let node_and_type = self.graph.resolve_and_update_node_and_type(id, node_type)?;

        #[cfg(feature = "search")]
        node_and_type
            .if_new(|(node_id, _)| {
                let name = match id.as_gid_ref() {
                    Either::Left(gid) => gid.to_string(),
                    Either::Right(vid) => self.core_node(vid).name().to_string(),
                };
                self.if_index_mut(|index| index.add_new_node(node_id.inner(), name, node_type))
            })
            .transpose()?;

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
        v: VID,
        props: Vec<(usize, Prop)>,
    ) -> Result<(), Self::Error> {
        #[cfg(feature = "search")]
        let index_res = self.if_index_mut(|index| index.add_node_update(t, v, &props));
        // don't fail early on indexing, actually update the graph even if indexing failed
        self.graph.internal_add_node(t, v, props)?;

        #[cfg(feature = "search")]
        index_res?;

        Ok(())
    }

    fn validate_props<PN: AsRef<str>>(
        &self,
        is_static: bool,
        meta: &Meta,
        prop: impl Iterator<Item = (PN, Prop)>,
    ) -> Result<Vec<(usize, Prop)>, Self::Error> {
        Ok(self.graph.validate_props(is_static, meta, prop)?)
    }

    fn validate_props_with_status<PN: AsRef<str>>(
        &self,
        is_static: bool,
        meta: &Meta,
        props: impl Iterator<Item = (PN, Prop)>,
    ) -> Result<Vec<MaybeNew<(PN, usize, Prop)>>, Self::Error> {
        Ok(self
            .graph
            .validate_props_with_status(is_static, meta, props)?)
    }

    fn validate_gids<'a>(
        &self,
        gids: impl IntoIterator<Item = GidRef<'a>>,
    ) -> Result<(), Self::Error> {
        Ok(self.graph.validate_gids(gids)?)
    }

    fn resolve_node_and_type(
        &self,
        id: NodeRef,
        node_type: Option<&str>,
    ) -> Result<(VID, usize), Self::Error> {
        Ok(self.graph.resolve_node_and_type(id, node_type)?)
    }
}

impl DurabilityOps for Storage {
    fn transaction_manager(&self) -> &TransactionManager {
        self.graph.mutable().unwrap().transaction_manager.as_ref()
    }

    fn wal(&self) -> &WalType {
        self.graph.mutable().unwrap().wal()
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

    fn internal_add_metadata(&self, props: &[(usize, Prop)]) -> Result<(), GraphError> {
        self.graph.internal_add_metadata(props)?;

        Ok(())
    }

    fn internal_update_metadata(&self, props: &[(usize, Prop)]) -> Result<(), GraphError> {
        self.graph.internal_update_metadata(props)?;

        Ok(())
    }

    fn internal_add_node_metadata(
        &self,
        vid: VID,
        props: Vec<(usize, Prop)>,
    ) -> Result<NodeWriterT<'_>, Self::Error> {
        #[cfg(feature = "search")]
        let props_for_index = props.clone();

        let lock = self.graph.internal_add_node_metadata(vid, props)?;

        #[cfg(feature = "search")]
        self.if_index_mut(|index| index.add_node_metadata(vid, &props_for_index))?;

        Ok(lock)
    }

    fn internal_update_node_metadata(
        &self,
        vid: VID,
        props: Vec<(usize, Prop)>,
    ) -> Result<NodeWriterT<'_>, Self::Error> {
        #[cfg(feature = "search")]
        let props_for_index = props.clone();

        let lock = self.graph.internal_update_node_metadata(vid, props)?;

        #[cfg(feature = "search")]
        self.if_index_mut(|index| index.update_node_metadata(vid, &props_for_index))?;

        Ok(lock)
    }

    fn internal_add_edge_metadata(
        &self,
        eid: EID,
        layer: usize,
        props: Vec<(usize, Prop)>,
    ) -> Result<EdgeWriterT<'_>, Self::Error> {
        // FIXME: this whole thing is not great

        #[cfg(feature = "search")]
        let props_for_index = props.clone();

        let lock = self.graph.internal_add_edge_metadata(eid, layer, props)?;

        #[cfg(feature = "search")]
        self.if_index_mut(|index| index.add_edge_metadata(eid, layer, &props_for_index))?;

        Ok(lock)
    }

    fn internal_update_edge_metadata(
        &self,
        eid: EID,
        layer: usize,
        props: Vec<(usize, Prop)>,
    ) -> Result<EdgeWriterT<'_>, Self::Error> {
        // FIXME: this whole thing is not great

        #[cfg(feature = "search")]
        let props_for_index = props.clone();

        let lock = self
            .graph
            .internal_update_edge_metadata(eid, layer, props)?;

        #[cfg(feature = "search")]
        self.if_index_mut(|index| index.update_edge_metadata(eid, layer, &props_for_index))?;

        Ok(lock)
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
        Ok(self.graph.internal_delete_edge(t, src, dst, layer)?)
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
