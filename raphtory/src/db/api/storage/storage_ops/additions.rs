use either::Either;
use raphtory_api::core::{
    entities::{EID, VID},
    storage::timeindex::TimeIndexEntry,
};
use std::sync::atomic::Ordering;

use super::GraphStorage;
use crate::{
    core::{
        entities::{
            graph::tgraph::TemporalGraph,
            nodes::{node_ref::AsNodeRef, node_store::NodeStore},
        },
        utils::errors::GraphError,
        PropType,
    },
    db::api::mutation::internal::InternalAdditionOps,
    prelude::Prop,
};

impl InternalAdditionOps for TemporalGraph {
    fn next_event_id(&self) -> Result<usize, GraphError> {
        Ok(self.event_counter.fetch_add(1, Ordering::Relaxed))
    }

    fn resolve_layer(&self, layer: Option<&str>) -> Result<usize, GraphError> {
        Ok(layer
            .map(|name| self.edge_meta.get_or_create_layer_id(name))
            .unwrap_or(0))
    }

    fn update_node_type(&self, v_id: VID, node_type: &str) -> Result<(), GraphError> {
        let mut node = self.storage.get_node_mut(v_id);
        if node_type == "_default" {
            return Err(GraphError::NodeTypeError(
                "_default type is not allowed to be used on nodes".to_string(),
            ));
        }
        if node.node_type == 0 {
            let node_type_id = self.node_meta.get_or_create_node_type_id(node_type);
            node.update_node_type(node_type_id);
        } else {
            let new_node_type_id = self.node_meta.get_node_type_id(node_type).unwrap_or(0);
            if node.node_type != new_node_type_id {
                return Err(GraphError::NodeTypeError(
                    "Node already has a non-default type".to_string(),
                ));
            }
        }
        Ok(())
    }

    fn resolve_node<V: AsNodeRef>(&self, n: V) -> Result<VID, GraphError> {
        match n.as_gid_ref() {
            Either::Left(id) => {
                let ref_mut = self.logical_to_physical.get_or_init(id, || {
                    let node_store = NodeStore::empty(id.into());
                    self.storage.push_node(node_store)
                })?;
                Ok(ref_mut)
            }
            Either::Right(vid) => Ok(vid),
        }
    }

    fn resolve_graph_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<usize, GraphError> {
        self.graph_meta.resolve_property(prop, dtype, is_static)
    }

    fn resolve_node_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<usize, GraphError> {
        self.node_meta.resolve_prop_id(prop, dtype, is_static)
    }

    fn resolve_edge_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<usize, GraphError> {
        self.edge_meta.resolve_prop_id(prop, dtype, is_static)
    }

    fn process_prop_value(&self, prop: Prop) -> Prop {
        match prop {
            Prop::Str(value) => Prop::Str(self.resolve_str(value)),
            _ => prop,
        }
    }

    fn internal_add_node(
        &self,
        t: TimeIndexEntry,
        v: VID,
        props: Vec<(usize, Prop)>,
    ) -> Result<(), GraphError> {
        self.update_time(t);
        // get the node and update the time index
        let mut node = self.storage.get_node_mut(v);
        node.update_time(t);
        for (id, prop) in props {
            node.add_prop(t, id, prop)?;
        }
        Ok(())
    }

    fn internal_add_edge(
        &self,
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
        props: Vec<(usize, Prop)>,
        layer: usize,
    ) -> Result<EID, GraphError> {
        self.link_nodes(src, dst, t, layer, move |edge| {
            edge.additions_mut(layer).insert(t);
            if !props.is_empty() {
                let edge_layer = edge.layer_mut(layer);
                for (prop_id, prop_value) in props {
                    edge_layer.add_prop(t, prop_id, prop_value)?;
                }
            }
            Ok(())
        })
    }

    fn internal_add_edge_update(
        &self,
        t: TimeIndexEntry,
        edge: EID,
        props: Vec<(usize, Prop)>,
        layer: usize,
    ) -> Result<(), GraphError> {
        self.link_edge(edge, t, layer, |edge| {
            edge.additions_mut(layer).insert(t);
            if !props.is_empty() {
                let edge_layer = edge.layer_mut(layer);
                for (prop_id, prop_value) in props {
                    edge_layer.add_prop(t, prop_id, prop_value)?;
                }
            }
            Ok(())
        })
    }
}

impl InternalAdditionOps for GraphStorage {
    fn next_event_id(&self) -> Result<usize, GraphError> {
        match self {
            GraphStorage::Unlocked(storage) => storage.next_event_id(),
            _ => Err(GraphError::AttemptToMutateImmutableGraph),
        }
    }

    fn resolve_layer(&self, layer: Option<&str>) -> Result<usize, GraphError> {
        match self {
            GraphStorage::Unlocked(storage) => storage.resolve_layer(layer),
            _ => Err(GraphError::AttemptToMutateImmutableGraph),
        }
    }

    fn update_node_type(&self, v_id: VID, node_type: &str) -> Result<(), GraphError> {
        match self {
            GraphStorage::Unlocked(storage) => storage.update_node_type(v_id, node_type),
            _ => Err(GraphError::AttemptToMutateImmutableGraph),
        }
    }

    fn resolve_node<V: AsNodeRef>(&self, n: V) -> Result<VID, GraphError> {
        match self {
            GraphStorage::Unlocked(storage) => storage.resolve_node(n),
            _ => Err(GraphError::AttemptToMutateImmutableGraph),
        }
    }

    fn resolve_graph_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<usize, GraphError> {
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
    ) -> Result<usize, GraphError> {
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
    ) -> Result<usize, GraphError> {
        match self {
            GraphStorage::Unlocked(storage) => {
                storage.resolve_edge_property(prop, dtype, is_static)
            }
            _ => Err(GraphError::AttemptToMutateImmutableGraph),
        }
    }

    fn process_prop_value(&self, prop: Prop) -> Prop {
        match self {
            GraphStorage::Unlocked(storage) => storage.process_prop_value(prop),
            _ => prop,
        }
    }

    fn internal_add_node(
        &self,
        t: TimeIndexEntry,
        v: VID,
        props: Vec<(usize, Prop)>,
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
        props: Vec<(usize, Prop)>,
        layer: usize,
    ) -> Result<EID, GraphError> {
        match self {
            GraphStorage::Unlocked(storage) => storage.internal_add_edge(t, src, dst, props, layer),
            _ => Err(GraphError::AttemptToMutateImmutableGraph),
        }
    }

    fn internal_add_edge_update(
        &self,
        t: TimeIndexEntry,
        edge: EID,
        props: Vec<(usize, Prop)>,
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
