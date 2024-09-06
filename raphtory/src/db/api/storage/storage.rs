#[cfg(feature = "proto")]
use crate::serialise::incremental::GraphWriter;
use crate::{
    core::{
        entities::{
            graph::tgraph::TemporalGraph,
            nodes::node_ref::{AsNodeRef, NodeRef},
        },
        utils::errors::GraphError,
        Prop, PropType,
    },
    db::api::{
        mutation::internal::{
            InternalAdditionOps, InternalDeletionOps, InternalPropertyAdditionOps,
        },
        storage::graph::{
            locked::WriteLockedGraph, nodes::node_storage_ops::NodeStorageOps,
            storage_ops::GraphStorage,
        },
        view::{Base, InheritViewOps},
    },
};
use once_cell::sync::OnceCell;
use raphtory_api::core::{
    entities::{GidRef, GidType, EID, VID},
    storage::{dict_mapper::MaybeNew, timeindex::TimeIndexEntry},
};
use serde::{Deserialize, Serialize};
use std::{
    fmt::{Display, Formatter},
    sync::Arc,
};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Storage {
    graph: GraphStorage,
    #[cfg(feature = "proto")]
    #[serde(skip)]
    pub(crate) cache: OnceCell<GraphWriter>,
    // search index (tantivy)
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

impl Storage {
    pub(crate) fn new(num_locks: usize) -> Self {
        Self {
            graph: GraphStorage::Unlocked(Arc::new(TemporalGraph::new(num_locks))),
            #[cfg(feature = "proto")]
            cache: OnceCell::new(),
        }
    }

    pub(crate) fn from_inner(graph: GraphStorage) -> Self {
        Self {
            graph,
            #[cfg(feature = "proto")]
            cache: OnceCell::new(),
        }
    }

    #[cfg(feature = "proto")]
    #[inline]
    fn if_cache(&self, map_fn: impl FnOnce(&GraphWriter)) {
        if let Some(cache) = self.cache.get() {
            map_fn(cache)
        }
    }
}
impl InheritViewOps for Storage {}

impl InternalAdditionOps for Storage {
    #[inline]
    fn id_type(&self) -> Option<GidType> {
        self.graph.id_type()
    }

    fn write_lock(&self) -> Result<WriteLockedGraph, GraphError> {
        self.graph.write_lock()
    }

    #[inline]
    fn num_shards(&self) -> Result<usize, GraphError> {
        self.graph.num_shards()
    }

    #[inline]
    fn next_event_id(&self) -> Result<usize, GraphError> {
        self.graph.next_event_id()
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
            NodeRef::Internal(id) => Ok(MaybeNew::Existing(id)),
            NodeRef::External(gid) => {
                let id = self.graph.resolve_node(gid)?;

                #[cfg(feature = "proto")]
                self.if_cache(|cache| cache.resolve_node(id, gid));

                Ok(id)
            }
        }
    }

    fn resolve_node_no_init(&self, id: GidRef) -> Result<MaybeNew<VID>, GraphError> {
        let vid = self.graph.resolve_node_no_init(id)?;

        #[cfg(feature = "proto")]
        self.if_cache(|cache| cache.resolve_node(vid, id));

        Ok(vid)
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
        let id = self.graph.resolve_graph_property(prop, dtype, is_static)?;

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
        let id = self.graph.resolve_node_property(prop, dtype, is_static)?;

        #[cfg(feature = "proto")]
        self.if_cache(|cache| cache.resolve_node_property(prop, id, dtype, is_static));

        Ok(id)
    }

    fn resolve_edge_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, GraphError> {
        let id = self.graph.resolve_edge_property(prop, dtype, is_static)?;

        #[cfg(feature = "proto")]
        self.if_cache(|cache| cache.resolve_edge_property(prop, id, dtype, is_static));

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
