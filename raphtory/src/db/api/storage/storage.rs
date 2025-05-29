#[cfg(feature = "proto")]
use crate::db::api::storage::graph::nodes::node_storage_ops::NodeStorageOps;
#[cfg(feature = "proto")]
use crate::serialise::incremental::GraphWriter;
use crate::{
    core::{
        entities::{
            graph::tgraph::TemporalGraph,
            nodes::node_ref::{AsNodeRef, NodeRef},
        },
        storage::{raw_edges::WriteLockedEdges, WriteLockedNodes},
        utils::errors::GraphError,
        Prop, PropType,
    },
    db::api::{
        mutation::internal::{
            InternalAdditionOps, InternalDeletionOps, InternalPropertyAdditionOps,
        },
        storage::graph::{locked::WriteLockedGraph, storage_ops::GraphStorage},
        view::{Base, InheritViewOps},
    },
};

use crate::db::api::view::{
    internal::{InheritEdgeHistoryFilter, InheritNodeHistoryFilter, InternalStorageOps},
    IndexSpec,
};
#[cfg(feature = "search")]
use crate::search::graph_index::GraphIndex;
#[cfg(feature = "proto")]
use crate::serialise::GraphFolder;
#[cfg(feature = "proto")]
use once_cell::sync::OnceCell;
use raphtory_api::core::{
    entities::{GidType, EID, VID},
    storage::{
        dict_mapper::{
            MaybeNew,
            MaybeNew::{Existing, New},
        },
        timeindex::TimeIndexEntry,
    },
};
use serde::{Deserialize, Serialize};
use std::{
    fmt::{Display, Formatter},
    path::PathBuf,
    sync::Arc,
};
use tracing::info;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Storage {
    graph: GraphStorage,
    #[cfg(feature = "proto")]
    #[serde(skip)]
    pub(crate) cache: OnceCell<GraphWriter>,
    #[cfg(feature = "search")]
    #[serde(skip)]
    pub(crate) index: OnceCell<GraphIndex>,
    // vector index
}

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
            map_fn(index)
        } else {
            Ok(())
        }
    }
}

#[cfg(feature = "proto")]
impl Storage {
    /// Initialise the cache by pointing it at a proto file.
    /// Future updates will be appended to the cache.
    pub(crate) fn init_cache(&self, path: &GraphFolder) -> Result<(), GraphError> {
        self.cache
            .get_or_try_init(|| Ok::<_, GraphError>(GraphWriter::new(path.clone())?))?;
        Ok(())
    }

    /// Get the cache writer if it is initialised.
    pub(crate) fn get_cache(&self) -> Option<&GraphWriter> {
        self.cache.get()
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
            let index = GraphIndex::load_from_path(&self.graph, &path)?;
            Ok(index)
        })
    }

    pub(crate) fn get_or_create_index(
        &self,
        index_spec: IndexSpec,
    ) -> Result<&GraphIndex, GraphError> {
        if let Some(index) = self.index.get() {
            index.update(&self.graph, index_spec.clone())?;
        };
        self.index.get_or_try_init(|| {
            let cached_graph_path = self.get_cache().map(|cache| cache.folder.get_base_path());
            let index = GraphIndex::create(&self.graph, false, cached_graph_path, index_spec)?;
            Ok(index)
        })
    }

    pub(crate) fn get_or_create_index_in_ram(
        &self,
        index_spec: IndexSpec,
    ) -> Result<&GraphIndex, GraphError> {
        let index = self.index.get_or_try_init(|| {
            Ok::<_, GraphError>(GraphIndex::create(&self.graph, true, None, index_spec)?)
        })?;
        if index.path.is_some() {
            Err(GraphError::FailedToCreateIndexInRam)
        } else {
            Ok(index)
        }
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

impl InternalAdditionOps for Storage {
    #[inline]
    fn id_type(&self) -> Option<GidType> {
        self.graph.id_type()
    }

    #[inline]
    fn write_lock(&self) -> Result<WriteLockedGraph, GraphError> {
        self.graph.write_lock()
    }

    #[inline]
    fn write_lock_nodes(&self) -> Result<WriteLockedNodes, GraphError> {
        self.graph.write_lock_nodes()
    }

    #[inline]
    fn write_lock_edges(&self) -> Result<WriteLockedEdges, GraphError> {
        self.graph.write_lock_edges()
    }

    #[inline]
    fn num_shards(&self) -> Result<usize, GraphError> {
        self.graph.num_shards()
    }

    #[inline]
    fn next_event_id(&self) -> Result<usize, GraphError> {
        self.graph.next_event_id()
    }

    fn read_event_id(&self) -> usize {
        self.graph.read_event_id()
    }

    #[inline]
    fn reserve_event_ids(&self, num_ids: usize) -> Result<usize, GraphError> {
        self.graph.reserve_event_ids(num_ids)
    }

    fn resolve_layer(&self, layer: Option<&str>) -> Result<MaybeNew<usize>, GraphError> {
        let id = self.graph.resolve_layer(layer)?;

        #[cfg(feature = "proto")]
        self.if_cache(|cache| cache.resolve_layer(layer, id));

        Ok(id)
    }

    fn resolve_node<V: AsNodeRef>(&self, id: V) -> Result<MaybeNew<VID>, GraphError> {
        match id.as_node_ref() {
            NodeRef::Internal(id) => Ok(Existing(id)),
            NodeRef::External(gid) => {
                let id = self.graph.resolve_node(gid)?;

                #[cfg(feature = "proto")]
                self.if_cache(|cache| cache.resolve_node(id, gid));

                Ok(id)
            }
        }
    }

    fn resolve_node_and_type<V: AsNodeRef>(
        &self,
        id: V,
        node_type: &str,
    ) -> Result<MaybeNew<(MaybeNew<VID>, MaybeNew<usize>)>, GraphError> {
        let node_and_type = self.graph.resolve_node_and_type(id, node_type)?;

        #[cfg(feature = "proto")]
        self.if_cache(|cache| {
            let (vid, _) = node_and_type.inner();
            let node_entry = self.graph.node_entry(vid.inner());
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
        self.if_index(|index| index.add_node_update(&self.graph, t, New(v), props))?;

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
        self.if_index(|index| index.add_edge_update(&self.graph, id, t, layer, props))?;

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
        self.if_index(|index| index.add_edge_update(&self.graph, Existing(edge), t, layer, props))?;

        Ok(())
    }
}

impl InternalPropertyAdditionOps for Storage {
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

        #[cfg(feature = "proto")]
        self.if_cache(|cache| cache.add_node_cprops(vid, props));

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

        #[cfg(feature = "proto")]
        self.if_cache(|cache| cache.add_edge_cprops(eid, layer, props));

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

        #[cfg(feature = "proto")]
        self.if_cache(|cache| cache.add_edge_cprops(eid, layer, props));

        #[cfg(feature = "search")]
        self.if_index(|index| index.update_edge_constant_properties(eid, layer, props))?;

        Ok(())
    }
}

impl InternalDeletionOps for Storage {
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
