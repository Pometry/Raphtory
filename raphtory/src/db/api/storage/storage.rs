#[cfg(feature = "search")]
use crate::search::graph_index::GraphIndex;
use crate::{
    core::entities::{graph::tgraph::TemporalGraph, nodes::node_ref::NodeRef},
    db::api::view::{
        internal::{InheritEdgeHistoryFilter, InheritNodeHistoryFilter, InternalStorageOps},
        Base, InheritViewOps,
    },
};
use parking_lot::RwLock;
use raphtory_api::core::{
    entities::{EID, VID},
    storage::{dict_mapper::MaybeNew, timeindex::TimeIndexEntry},
};
use raphtory_storage::graph::graph::GraphStorage;
use serde::{Deserialize, Serialize};
use std::{
    fmt::{Display, Formatter},
    ops::Deref,
    sync::Arc,
};
use tracing::info;

#[cfg(feature = "search")]
use crate::search::graph_index::MutableGraphIndex;
use crate::{db::api::view::IndexSpec, errors::GraphError};
use raphtory_api::core::entities::{
    properties::prop::{Prop, PropType},
    GidRef,
};
use raphtory_core::storage::{raw_edges::WriteLockedEdges, WriteLockedNodes};
use raphtory_storage::{
    core_ops::InheritCoreGraphOps,
    graph::{locked::WriteLockedGraph, nodes::node_storage_ops::NodeStorageOps},
    layer_ops::InheritLayerOps,
    mutation::{
        addition_ops::InternalAdditionOps, deletion_ops::InternalDeletionOps,
        property_addition_ops::InternalPropertyAdditionOps,
    },
};
#[cfg(feature = "proto")]
use {
    crate::serialise::incremental::{GraphWriter, InternalCache},
    crate::serialise::GraphFolder,
    once_cell::sync::OnceCell,
};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Storage {
    graph: GraphStorage,
    #[cfg(feature = "proto")]
    #[serde(skip)]
    pub(crate) cache: OnceCell<GraphWriter>,
    #[cfg(feature = "search")]
    #[serde(skip)]
    pub(crate) index: OnceCell<RwLock<GraphIndex>>,
    // vector index
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
            graph: GraphStorage::Unlocked(Arc::new(TemporalGraph::new(num_locks))),
            #[cfg(feature = "proto")]
            cache: OnceCell::new(),
            #[cfg(feature = "search")]
            index: OnceCell::new(),
        }
    }

    pub(crate) fn from_inner(graph: GraphStorage) -> Self {
        Self {
            graph,
            #[cfg(feature = "proto")]
            cache: OnceCell::new(),
            #[cfg(feature = "search")]
            index: OnceCell::new(),
        }
    }

    #[cfg(feature = "proto")]
    #[inline]
    fn if_cache(&self, map_fn: impl FnOnce(&GraphWriter)) {
        if let Some(cache) = self.cache.get() {
            map_fn(cache)
        }
    }

    #[cfg(feature = "search")]
    #[inline]
    fn if_index(
        &self,
        map_fn: impl FnOnce(&GraphIndex) -> Result<(), GraphError>,
    ) -> Result<(), GraphError> {
        if let Some(index) = self.index.get() {
            map_fn(&index.read())?
        };
        Ok(())
    }

    #[cfg(feature = "search")]
    #[inline]
    fn if_index_mut(
        &self,
        map_fn: impl FnOnce(&MutableGraphIndex) -> Result<(), GraphError>,
    ) -> Result<(), GraphError> {
        if let Some(index) = self.index.get() {
            let guard = index.read();
            if let GraphIndex::Mutable(m) = guard.deref() {
                map_fn(m)?
            } else {
                drop(guard);
                index.write().make_mutable_if_needed()?;
                let guard = index.read();
                if let GraphIndex::Mutable(m) = guard.deref() {
                    map_fn(m)?
                }
            }
        };
        Ok(())
    }
}

#[cfg(feature = "search")]
impl Storage {
    pub(crate) fn get_index_spec(&self) -> Result<IndexSpec, GraphError> {
        let index = self.index.get().ok_or(GraphError::IndexNotCreated)?;
        Ok(index.read().index_spec())
    }

    pub(crate) fn get_or_load_index(
        &self,
        path: &GraphFolder,
    ) -> Result<&RwLock<GraphIndex>, GraphError> {
        self.index.get_or_try_init(|| {
            let index = GraphIndex::load_from_path(&path)?;
            Ok(RwLock::new(index))
        })
    }

    pub(crate) fn get_or_create_index(
        &self,
        index_spec: IndexSpec,
    ) -> Result<&RwLock<GraphIndex>, GraphError> {
        let index = self.index.get_or_try_init(|| {
            let cached_graph_path = self.get_cache().map(|cache| cache.folder.clone());
            let index = GraphIndex::create(&self.graph, false, cached_graph_path)?;
            Ok::<_, GraphError>(RwLock::new(index))
        })?;

        self.if_index_mut(|index| index.update(&self.graph, index_spec))?;

        Ok(index)
    }

    pub(crate) fn get_or_create_index_in_ram(
        &self,
        index_spec: IndexSpec,
    ) -> Result<&RwLock<GraphIndex>, GraphError> {
        let index = self.index.get_or_try_init(|| {
            let index = GraphIndex::create(&self.graph, true, None)?;
            Ok::<_, GraphError>(RwLock::new(index))
        })?;

        if index.read().path().is_some() {
            return Err(GraphError::OnDiskIndexAlreadyExists);
        }

        self.if_index_mut(|index| index.update(&self.graph, index_spec))?;

        Ok(index)
    }

    pub(crate) fn get_index(&self) -> Option<&RwLock<GraphIndex>> {
        self.index.get()
    }

    pub(crate) fn persist_index_to_disk(&self, path: &GraphFolder) -> Result<(), GraphError> {
        if let Some(index) = self.get_index() {
            if index.read().path().is_none() {
                info!("{}", IN_MEMORY_INDEX_NOT_PERSISTED);
                return Ok(());
            }
            self.if_index(|index| index.persist_to_disk(path))?;
        }
        Ok(())
    }

    pub(crate) fn persist_index_to_disk_zip(&self, path: &GraphFolder) -> Result<(), GraphError> {
        if let Some(index) = self.get_index() {
            if index.read().path().is_none() {
                info!("{}", IN_MEMORY_INDEX_NOT_PERSISTED);
                return Ok(());
            }
            self.if_index(|index| index.persist_to_disk_zip(path))?;
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

impl InternalAdditionOps for Storage {
    type Error = GraphError;

    fn write_lock(&self) -> Result<WriteLockedGraph, Self::Error> {
        Ok(self.graph.write_lock()?)
    }

    fn write_lock_nodes(&self) -> Result<WriteLockedNodes, Self::Error> {
        Ok(self.graph.write_lock_nodes()?)
    }

    fn write_lock_edges(&self) -> Result<WriteLockedEdges, Self::Error> {
        Ok(self.graph.write_lock_edges()?)
    }

    fn next_event_id(&self) -> Result<usize, Self::Error> {
        Ok(self.graph.next_event_id()?)
    }

    fn reserve_event_ids(&self, num_ids: usize) -> Result<usize, Self::Error> {
        Ok(self.graph.reserve_event_ids(num_ids)?)
    }

    fn resolve_layer(&self, layer: Option<&str>) -> Result<MaybeNew<usize>, GraphError> {
        let id = self.graph.resolve_layer(layer)?;

        #[cfg(feature = "proto")]
        self.if_cache(|cache| cache.resolve_layer(layer, id));

        Ok(id)
    }

    fn resolve_node(&self, id: NodeRef) -> Result<MaybeNew<VID>, GraphError> {
        match id {
            NodeRef::Internal(id) => Ok(MaybeNew::Existing(id)),
            NodeRef::External(gid) => {
                let id = self.graph.resolve_node(id)?;

                #[cfg(feature = "proto")]
                self.if_cache(|cache| cache.resolve_node(id, gid));

                Ok(id)
            }
        }
    }

    fn set_node(&self, gid: GidRef, vid: VID) -> Result<(), Self::Error> {
        Ok(self.graph.set_node(gid, vid)?)
    }

    fn resolve_node_and_type(
        &self,
        id: NodeRef,
        node_type: &str,
    ) -> Result<MaybeNew<(MaybeNew<VID>, MaybeNew<usize>)>, GraphError> {
        let node_and_type = self.graph.resolve_node_and_type(id, node_type)?;

        #[cfg(feature = "proto")]
        self.if_cache(|cache| {
            let (vid, _) = node_and_type.inner();
            let node_entry = self.graph.core_node(vid.inner());
            cache.resolve_node_and_type(node_and_type, node_type, node_entry.id())
        });

        Ok(node_and_type)
    }

    fn resolve_graph_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, GraphError> {
        let id = self
            .graph
            .resolve_graph_property(prop, dtype.clone(), is_static)?;

        #[cfg(feature = "proto")]
        self.if_cache(|cache| cache.resolve_graph_property(prop, id, dtype, is_static));

        Ok(id)
    }

    fn resolve_node_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, GraphError> {
        let id = self
            .graph
            .resolve_node_property(prop, dtype.clone(), is_static)?;

        #[cfg(feature = "proto")]
        self.if_cache(|cache| cache.resolve_node_property(prop, id, &dtype, is_static));

        Ok(id)
    }

    fn resolve_edge_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, GraphError> {
        let id = self
            .graph
            .resolve_edge_property(prop, dtype.clone(), is_static)?;

        #[cfg(feature = "proto")]
        self.if_cache(|cache| cache.resolve_edge_property(prop, id, &dtype, is_static));

        Ok(id)
    }

    fn internal_add_node(
        &self,
        t: TimeIndexEntry,
        v: VID,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        self.graph.internal_add_node(t, v, props)?;

        #[cfg(feature = "proto")]
        self.if_cache(|cache| cache.add_node_update(t, v, props));

        #[cfg(feature = "search")]
        self.if_index_mut(|index| index.add_node_update(&self.graph, t, MaybeNew::New(v), props))?;

        Ok(())
    }

    fn internal_add_edge(
        &self,
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
        props: &[(usize, Prop)],
        layer: usize,
    ) -> Result<MaybeNew<EID>, GraphError> {
        let id = self.graph.internal_add_edge(t, src, dst, props, layer)?;

        #[cfg(feature = "proto")]
        self.if_cache(|cache| {
            cache.resolve_edge(id, src, dst);
            cache.add_edge_update(t, id.inner(), props, layer);
        });

        #[cfg(feature = "search")]
        self.if_index_mut(|index| index.add_edge_update(&self.graph, id, t, layer, props))?;

        Ok(id)
    }

    fn internal_add_edge_update(
        &self,
        t: TimeIndexEntry,
        edge: EID,
        props: &[(usize, Prop)],
        layer: usize,
    ) -> Result<(), GraphError> {
        self.graph.internal_add_edge_update(t, edge, props, layer)?;

        #[cfg(feature = "proto")]
        self.if_cache(|cache| cache.add_edge_update(t, edge, props, layer));

        #[cfg(feature = "search")]
        self.if_index_mut(|index| {
            index.add_edge_update(&self.graph, MaybeNew::Existing(edge), t, layer, props)
        })?;

        Ok(())
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

        #[cfg(feature = "proto")]
        self.if_cache(|cache| cache.add_graph_tprops(t, props));

        Ok(())
    }

    fn internal_add_constant_properties(&self, props: &[(usize, Prop)]) -> Result<(), GraphError> {
        self.graph.internal_add_constant_properties(props)?;

        #[cfg(feature = "proto")]
        self.if_cache(|cache| cache.add_graph_cprops(props));

        Ok(())
    }

    fn internal_update_constant_properties(
        &self,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        self.graph.internal_update_constant_properties(props)?;

        #[cfg(feature = "proto")]
        self.if_cache(|cache| cache.add_graph_cprops(props));

        Ok(())
    }

    fn internal_add_constant_node_properties(
        &self,
        vid: VID,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        self.graph
            .internal_add_constant_node_properties(vid, props)?;

        #[cfg(feature = "proto")]
        self.if_cache(|cache| cache.add_node_cprops(vid, props));

        #[cfg(feature = "search")]
        self.if_index_mut(|index| index.add_node_constant_properties(vid, props))?;

        Ok(())
    }

    fn internal_update_constant_node_properties(
        &self,
        vid: VID,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        self.graph
            .internal_update_constant_node_properties(vid, props)?;

        #[cfg(feature = "proto")]
        self.if_cache(|cache| cache.add_node_cprops(vid, props));

        #[cfg(feature = "search")]
        self.if_index_mut(|index| index.update_node_constant_properties(vid, props))?;

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

        #[cfg(feature = "proto")]
        self.if_cache(|cache| cache.add_edge_cprops(eid, layer, props));

        #[cfg(feature = "search")]
        self.if_index_mut(|index| index.add_edge_constant_properties(eid, layer, props))?;

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

        #[cfg(feature = "proto")]
        self.if_cache(|cache| cache.add_edge_cprops(eid, layer, props));

        #[cfg(feature = "search")]
        self.if_index_mut(|index| index.update_edge_constant_properties(eid, layer, props))?;

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

        #[cfg(feature = "proto")]
        self.if_cache(|cache| {
            cache.resolve_edge(eid, src, dst);
            cache.delete_edge(eid.inner(), t, layer);
        });

        Ok(eid)
    }

    fn internal_delete_existing_edge(
        &self,
        t: TimeIndexEntry,
        eid: EID,
        layer: usize,
    ) -> Result<(), GraphError> {
        self.graph.internal_delete_existing_edge(t, eid, layer)?;

        #[cfg(feature = "proto")]
        self.if_cache(|cache| cache.delete_edge(eid, t, layer));

        Ok(())
    }
}
