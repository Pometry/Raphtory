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
        storage::{
            cache::cached_graph::GraphWriter,
            graph::{nodes::node_storage_ops::NodeStorageOps, storage_ops::GraphStorage},
        },
        view::{Base, InheritViewOps},
    },
};
use raphtory_api::core::{
    entities::{EID, VID},
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
    cache: Option<GraphWriter>,
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
            cache: None,
        }
    }

    pub(crate) fn from_inner(graph: GraphStorage) -> Self {
        Self { graph, cache: None }
    }

    #[cfg(feature = "proto")]
    #[inline]
    fn if_cache(&self, map_fn: impl FnOnce(&GraphWriter)) {
        if let Some(cache) = self.cache.as_ref() {
            map_fn(cache)
        }
    }

    #[cfg(not(feature = "proto"))]
    #[inline]
    fn if_cache(&self, map_fn: impl FnOnce(&GraphWriter)) {}
}
impl InheritViewOps for Storage {}

impl InternalAdditionOps for Storage {
    #[inline]
    fn next_event_id(&self) -> Result<usize, GraphError> {
        self.graph.next_event_id()
    }

    fn resolve_layer(&self, layer: Option<&str>) -> Result<MaybeNew<usize>, GraphError> {
        let id = self.graph.resolve_layer(layer)?;
        self.if_cache(|cache| cache.resolve_layer(layer, id));
        Ok(id)
    }

    fn resolve_node<V: AsNodeRef>(&self, id: V) -> Result<MaybeNew<VID>, GraphError> {
        match id.as_node_ref() {
            NodeRef::Internal(id) => Ok(MaybeNew::Existing(id)),
            NodeRef::External(gid) => {
                let id = self.graph.resolve_node(gid)?;
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
        self.if_cache(|cache| cache.add_graph_tprops(t, props));
        Ok(())
    }

    fn internal_add_constant_properties(&self, props: &[(usize, Prop)]) -> Result<(), GraphError> {
        self.graph.internal_add_constant_properties(props)?;
        self.if_cache(|cache| cache.add_graph_cprops(props));
        Ok(())
    }

    fn internal_update_constant_properties(
        &self,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        self.graph.internal_update_constant_properties(props)?;
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
        self.if_cache(|cache| cache.delete_edge(eid, t, layer));
        Ok(())
    }
}
