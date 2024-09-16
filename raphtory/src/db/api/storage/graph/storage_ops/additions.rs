use super::GraphStorage;
use crate::{
    core::{
        entities::{
            graph::tgraph::TemporalGraph,
            nodes::{node_ref::AsNodeRef, node_store::NodeStore},
        },
        storage::{raw_edges::WriteLockedEdges, WriteLockedNodes},
        utils::errors::GraphError,
        PropType,
    },
    db::api::{mutation::internal::InternalAdditionOps, storage::graph::locked::WriteLockedGraph},
    prelude::{GraphViewOps, Prop},
};
use either::Either;
use raphtory_api::core::{
    entities::{GidType, EID, VID},
    storage::{dict_mapper::MaybeNew, timeindex::TimeIndexEntry},
};
use std::sync::atomic::Ordering;

impl InternalAdditionOps for TemporalGraph {
    fn id_type(&self) -> Option<GidType> {
        self.logical_to_physical.dtype()
    }

    fn write_lock(&self) -> Result<WriteLockedGraph, GraphError> {
        Ok(WriteLockedGraph::new(self))
    }

    fn write_lock_nodes(&self) -> Result<WriteLockedNodes, GraphError> {
        Ok(self.storage.nodes.write_lock())
    }

    fn write_lock_edges(&self) -> Result<WriteLockedEdges, GraphError> {
        Ok(self.storage.edges.write_lock())
    }

    fn num_shards(&self) -> Result<usize, GraphError> {
        Ok(self.storage.nodes.data.len())
    }

    fn next_event_id(&self) -> Result<usize, GraphError> {
        Ok(self.event_counter.fetch_add(1, Ordering::Relaxed))
    }

    fn read_event_id(&self) -> usize {
        self.event_counter.load(Ordering::Relaxed)
    }

    fn reserve_event_ids(&self, num_ids: usize) -> Result<usize, GraphError> {
        Ok(self.event_counter.fetch_add(num_ids, Ordering::Relaxed))
    }

    fn resolve_layer(&self, layer: Option<&str>) -> Result<MaybeNew<usize>, GraphError> {
        Ok(layer
            .map(|name| self.edge_meta.get_or_create_layer_id(name))
            .unwrap_or(MaybeNew::Existing(0)))
    }

    fn resolve_node<V: AsNodeRef>(&self, n: V) -> Result<MaybeNew<VID>, GraphError> {
        match n.as_gid_ref() {
            Either::Left(id) => {
                let ref_mut = self.logical_to_physical.get_or_init_node(id, || {
                    let node_store = NodeStore::empty(id.into());
                    self.storage.push_node(node_store)
                })?;
                Ok(ref_mut)
            }
            Either::Right(vid) => Ok(MaybeNew::Existing(vid)),
        }
    }

    fn resolve_node_and_type<V: AsNodeRef>(
        &self,
        id: V,
        node_type: &str,
    ) -> Result<MaybeNew<(MaybeNew<VID>, MaybeNew<usize>)>, GraphError> {
        if node_type == "_default" {
            return Err(GraphError::NodeTypeError(
                "_default type is not allowed to be used on nodes".to_string(),
            ));
        }
        let vid = self.resolve_node(id)?;
        let mut node_store = self.storage.get_node_mut(vid.inner());
        if node_store.node_type == 0 {
            let node_type_id = self.node_meta.get_or_create_node_type_id(node_type);
            node_store.update_node_type(node_type_id.inner());
            Ok(MaybeNew::New((vid, node_type_id)))
        } else {
            let node_type_id = self
                .node_meta
                .get_node_type_id(node_type)
                .ok_or_else(|| GraphError::NodeTypeError("Cannot change node type".to_string()))?;
            if node_type_id == node_store.node_type {
                Ok(MaybeNew::Existing((vid, MaybeNew::Existing(node_type_id))))
            } else {
                Err(GraphError::NodeTypeError(
                    "Cannot change node type".to_string(),
                ))
            }
        }
    }

    fn resolve_graph_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, GraphError> {
        self.graph_meta.resolve_property(prop, dtype, is_static)
    }

    fn resolve_node_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, GraphError> {
        self.node_meta.resolve_prop_id(prop, dtype, is_static)
    }

    fn resolve_edge_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, GraphError> {
        self.edge_meta.resolve_prop_id(prop, dtype, is_static)
    }

    fn internal_add_node(
        &self,
        t: TimeIndexEntry,
        v: VID,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        self.update_time(t);
        // get the node and update the time index
        let mut node = self.storage.get_node_mut(v);
        node.update_time(t);
        for (id, prop) in props {
            let prop = self.process_prop_value(prop);
            node.add_prop(t, *id, prop)?;
        }
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
        self.link_nodes(src, dst, t, layer, move |mut edge| {
            edge.additions_mut(layer).insert(t);
            if !props.is_empty() {
                let edge_layer = edge.layer_mut(layer);
                for (prop_id, prop) in props {
                    let prop = self.process_prop_value(prop);
                    edge_layer.add_prop(t, *prop_id, prop)?;
                }
            }
            Ok(())
        })
    }

    fn internal_add_edge_update(
        &self,
        t: TimeIndexEntry,
        edge: EID,
        props: &[(usize, Prop)],
        layer: usize,
    ) -> Result<(), GraphError> {
        self.link_edge(edge, t, layer, |mut edge| {
            edge.additions_mut(layer).insert(t);
            if !props.is_empty() {
                let edge_layer = edge.layer_mut(layer);
                for (prop_id, prop) in props {
                    let prop = self.process_prop_value(prop);
                    edge_layer.add_prop(t, *prop_id, prop)?;
                }
            }
            Ok(())
        })
    }
}

impl InternalAdditionOps for GraphStorage {
    fn id_type(&self) -> Option<GidType> {
        match self {
            GraphStorage::Unlocked(storage) => storage.id_type(),
            GraphStorage::Mem(storage) => storage.graph.id_type(),
            #[cfg(feature = "storage")]
            GraphStorage::Disk(storage) => Some(storage.inner().id_type()),
        }
    }

    fn write_lock(&self) -> Result<WriteLockedGraph, GraphError> {
        match self {
            GraphStorage::Unlocked(storage) => storage.write_lock(),
            _ => Err(GraphError::AttemptToMutateImmutableGraph),
        }
    }

    fn write_lock_nodes(&self) -> Result<WriteLockedNodes, GraphError> {
        match self {
            GraphStorage::Unlocked(storage) => storage.write_lock_nodes(),
            _ => Err(GraphError::AttemptToMutateImmutableGraph),
        }
    }

    fn write_lock_edges(&self) -> Result<WriteLockedEdges, GraphError> {
        match self {
            GraphStorage::Unlocked(storage) => storage.write_lock_edges(),
            _ => Err(GraphError::AttemptToMutateImmutableGraph),
        }
    }

    fn num_shards(&self) -> Result<usize, GraphError> {
        match self {
            GraphStorage::Unlocked(storage) => storage.num_shards(),
            _ => Err(GraphError::AttemptToMutateImmutableGraph),
        }
    }

    fn next_event_id(&self) -> Result<usize, GraphError> {
        match self {
            GraphStorage::Unlocked(storage) => storage.next_event_id(),
            _ => Err(GraphError::AttemptToMutateImmutableGraph),
        }
    }

    fn read_event_id(&self) -> usize {
        match self {
            GraphStorage::Mem(storage) => storage.graph.read_event_id(),
            GraphStorage::Unlocked(storage) => storage.read_event_id(),
            #[cfg(feature = "storage")]
            GraphStorage::Disk(storage) => storage.inner.count_temporal_edges(),
        }
    }

    fn reserve_event_ids(&self, num_ids: usize) -> Result<usize, GraphError> {
        match self {
            GraphStorage::Unlocked(storage) => storage.reserve_event_ids(num_ids),
            _ => Err(GraphError::AttemptToMutateImmutableGraph),
        }
    }

    fn resolve_layer(&self, layer: Option<&str>) -> Result<MaybeNew<usize>, GraphError> {
        match self {
            GraphStorage::Unlocked(storage) => storage.resolve_layer(layer),
            _ => Err(GraphError::AttemptToMutateImmutableGraph),
        }
    }

    fn resolve_node<V: AsNodeRef>(&self, n: V) -> Result<MaybeNew<VID>, GraphError> {
        match self {
            GraphStorage::Unlocked(storage) => storage.resolve_node(n),
            _ => Err(GraphError::AttemptToMutateImmutableGraph),
        }
    }

    fn resolve_node_and_type<V: AsNodeRef>(
        &self,
        id: V,
        node_type: &str,
    ) -> Result<MaybeNew<(MaybeNew<VID>, MaybeNew<usize>)>, GraphError> {
        match self {
            GraphStorage::Unlocked(storage) => storage.resolve_node_and_type(id, node_type),
            _ => Err(GraphError::AttemptToMutateImmutableGraph),
        }
    }

    fn resolve_graph_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, GraphError> {
        match self {
            GraphStorage::Unlocked(storage) => {
                storage.resolve_graph_property(prop, dtype, is_static)
            }
            _ => Err(GraphError::AttemptToMutateImmutableGraph),
        }
    }

    fn resolve_node_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, GraphError> {
        match self {
            GraphStorage::Unlocked(storage) => {
                storage.resolve_node_property(prop, dtype, is_static)
            }
            _ => Err(GraphError::AttemptToMutateImmutableGraph),
        }
    }

    fn resolve_edge_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, GraphError> {
        match self {
            GraphStorage::Unlocked(storage) => {
                storage.resolve_edge_property(prop, dtype, is_static)
            }
            _ => Err(GraphError::AttemptToMutateImmutableGraph),
        }
    }

    fn internal_add_node(
        &self,
        t: TimeIndexEntry,
        v: VID,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        match self {
            GraphStorage::Unlocked(storage) => storage.internal_add_node(t, v, props),
            _ => Err(GraphError::AttemptToMutateImmutableGraph),
        }
    }

    fn internal_add_edge(
        &self,
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
        props: &[(usize, Prop)],
        layer: usize,
    ) -> Result<MaybeNew<EID>, GraphError> {
        match self {
            GraphStorage::Unlocked(storage) => storage.internal_add_edge(t, src, dst, props, layer),
            _ => Err(GraphError::AttemptToMutateImmutableGraph),
        }
    }

    fn internal_add_edge_update(
        &self,
        t: TimeIndexEntry,
        edge: EID,
        props: &[(usize, Prop)],
        layer: usize,
    ) -> Result<(), GraphError> {
        match self {
            GraphStorage::Unlocked(storage) => {
                storage.internal_add_edge_update(t, edge, props, layer)
            }
            _ => Err(GraphError::AttemptToMutateImmutableGraph),
        }
    }
}
