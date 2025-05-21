use crate::{
    core_ops::CoreGraphOps,
    graph::{
        graph::GraphStorage,
        locked::{LockedGraph, WriteLockedGraph},
    },
    mutation::MutationError,
};
use raphtory_api::core::{
    entities::{
        properties::prop::{Prop, PropType},
        GidType, EID, MAX_LAYER, VID,
    },
    storage::{dict_mapper::MaybeNew, timeindex::TimeIndexEntry},
};
use raphtory_core::{
    entities::nodes::node_ref::AsNodeRef,
    storage::{raw_edges::WriteLockedEdges, WriteLockedNodes},
};
use std::sync::atomic::Ordering;

pub trait InternalAdditionOps: CoreGraphOps {
    fn id_type(&self) -> Option<GidType> {
        match self.core_graph() {
            GraphStorage::Mem(LockedGraph { graph, .. }) | GraphStorage::Unlocked(graph) => {
                graph.logical_to_physical.dtype()
            }
            #[cfg(feature = "storage")]
            GraphStorage::Disk(storage) => Some(storage.inner().id_type()),
        }
    }
    fn write_lock(&self) -> Result<WriteLockedGraph, MutationError> {
        Ok(WriteLockedGraph::new(self.core_graph().mutable()?))
    }

    fn write_lock_nodes(&self) -> Result<WriteLockedNodes, MutationError> {
        Ok(self.core_graph().mutable()?.storage.nodes.write_lock())
    }

    fn write_lock_edges(&self) -> Result<WriteLockedEdges, MutationError> {
        Ok(self.core_graph().mutable()?.storage.edges.write_lock())
    }
    fn num_shards(&self) -> usize {
        match self.core_graph() {
            GraphStorage::Mem(LockedGraph { graph, .. }) | GraphStorage::Unlocked(graph) => {
                graph.storage.num_shards()
            }
            #[cfg(feature = "storage")]
            GraphStorage::Disk(_) => 1,
        }
    }
    /// get the sequence id for the next event
    fn next_event_id(&self) -> Result<usize, MutationError> {
        Ok(self
            .core_graph()
            .mutable()?
            .event_counter
            .fetch_add(1, Ordering::Relaxed))
    }

    /// get the current sequence id without incrementing the counter
    fn read_event_id(&self) -> usize {
        match self.core_graph() {
            GraphStorage::Unlocked(graph) | GraphStorage::Mem(LockedGraph { graph, .. }) => {
                graph.event_counter.load(Ordering::Relaxed)
            }
            #[cfg(feature = "storage")]
            GraphStorage::Disk(storage) => storage.inner.count_temporal_edges(),
        }
    }

    fn reserve_event_ids(&self, num_ids: usize) -> Result<usize, MutationError> {
        Ok(self
            .core_graph()
            .mutable()?
            .event_counter
            .fetch_add(num_ids, Ordering::Relaxed))
    }

    /// map layer name to id and allocate a new layer if needed
    fn resolve_layer(&self, layer: Option<&str>) -> Result<MaybeNew<usize>, MutationError> {
        let id = self
            .core_graph()
            .mutable()?
            .edge_meta
            .get_or_create_layer_id(layer);
        if let MaybeNew::New(id) = id {
            if id > MAX_LAYER {
                return Err(MutationError::TooManyLayers);
            }
        }
        Ok(id)
    }

    /// map external node id to internal id, allocating a new empty node if needed
    fn resolve_node<V: AsNodeRef>(&self, id: V) -> Result<MaybeNew<VID>, MutationError> {
        Ok(self.core_graph().mutable()?.resolve_node(id)?)
    }

    /// resolve a node and corresponding type, outer MaybeNew tracks whether the type assignment is new for the node even if both node and type already existed.
    fn resolve_node_and_type<V: AsNodeRef>(
        &self,
        id: V,
        node_type: &str,
    ) -> Result<MaybeNew<(MaybeNew<VID>, MaybeNew<usize>)>, MutationError> {
        let graph = self.core_graph().mutable()?;
        let vid = graph.resolve_node(id)?;
        let mut entry = graph.storage.get_node_mut(vid.inner());
        let mut entry_ref = entry.to_mut();
        let node_store = entry_ref.node_store_mut();
        if node_store.node_type == 0 {
            let node_type_id = graph.node_meta.get_or_create_node_type_id(node_type);
            node_store.update_node_type(node_type_id.inner());
            Ok(MaybeNew::New((vid, node_type_id)))
        } else {
            let node_type_id = graph
                .node_meta
                .get_node_type_id(node_type)
                .filter(|&node_type| node_type == node_store.node_type)
                .ok_or(MutationError::NodeTypeError)?;
            Ok(MaybeNew::Existing((vid, MaybeNew::Existing(node_type_id))))
        }
    }

    /// map property key to internal id, allocating new property if needed
    fn resolve_graph_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, MutationError> {
        Ok(self
            .core_graph()
            .mutable()?
            .graph_meta
            .resolve_property(prop, dtype, is_static)?)
    }

    /// map property key to internal id, allocating new property if needed and checking property type.
    /// returns `None` if the type does not match
    fn resolve_node_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, MutationError> {
        Ok(self
            .core_graph()
            .mutable()?
            .node_meta
            .resolve_prop_id(prop, dtype, is_static)?)
    }

    fn resolve_edge_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, MutationError> {
        Ok(self
            .core_graph()
            .mutable()?
            .edge_meta
            .resolve_prop_id(prop, dtype, is_static)?)
    }

    /// add node update
    fn internal_add_node(
        &self,
        t: TimeIndexEntry,
        v: VID,
        props: &[(usize, Prop)],
    ) -> Result<(), MutationError> {
        let graph = self.core_graph().mutable()?;
        graph.update_time(t);
        let mut entry = graph.storage.get_node_mut(v);
        let mut node = entry.to_mut();
        let prop_i = node
            .t_props_log_mut()
            .push(props.iter().map(|(prop_id, prop)| {
                let prop = graph.process_prop_value(prop);
                (*prop_id, prop)
            }))?;
        node.node_store_mut().update_t_prop_time(t, prop_i);
        Ok(())
    }

    /// add edge update
    fn internal_add_edge(
        &self,
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
        props: &[(usize, Prop)],
        layer: usize,
    ) -> Result<MaybeNew<EID>, MutationError> {
        let graph = self.core_graph().mutable()?;
        let edge = graph.link_nodes(src, dst, t, layer, false);
        edge.try_map(|mut edge| {
            let eid = edge.eid();
            let mut edge = edge.as_mut();
            edge.additions_mut(layer).insert(t);
            if !props.is_empty() {
                let edge_layer = edge.layer_mut(layer);
                for (prop_id, prop) in props {
                    let prop = graph.process_prop_value(prop);
                    edge_layer.add_prop(t, *prop_id, prop)?;
                }
            }
            Ok(eid)
        })
    }

    /// add update for an existing edge
    fn internal_add_edge_update(
        &self,
        t: TimeIndexEntry,
        edge: EID,
        props: &[(usize, Prop)],
        layer: usize,
    ) -> Result<(), MutationError> {
        let graph = self.core_graph().mutable()?;
        let mut edge = graph.link_edge(edge, t, layer, false);
        let mut edge = edge.as_mut();
        edge.additions_mut(layer).insert(t);
        if !props.is_empty() {
            let edge_layer = edge.layer_mut(layer);
            for (prop_id, prop) in props {
                let prop = graph.process_prop_value(prop);
                edge_layer.add_prop(t, *prop_id, prop)?
            }
        }
        Ok(())
    }
}

impl InternalAdditionOps for GraphStorage {}
