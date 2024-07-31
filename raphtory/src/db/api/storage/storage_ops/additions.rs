use std::sync::atomic::Ordering;

use raphtory_api::core::{
    entities::{EID, VID},
    storage::timeindex::TimeIndexEntry,
};

use crate::{
    core::{entities::nodes::node_ref::AsNodeRef, utils::errors::GraphError, PropType},
    db::api::mutation::internal::InternalAdditionOps,
    prelude::Prop,
};

use super::GraphStorage;

impl InternalAdditionOps for GraphStorage {
    fn next_event_id(&self) -> Result<usize, GraphError> {
        match self {
            GraphStorage::Unlocked(storage) => {
                Ok(storage.event_counter.fetch_add(1, Ordering::Relaxed))
            }
            _ => Err(GraphError::AttemptToMutateImmutableGraph),
        }
    }

    fn resolve_layer(&self, layer: Option<&str>) -> Result<usize, GraphError> {
        match self {
            GraphStorage::Unlocked(_) => Ok(layer
                .map(|name| self.edge_meta().get_or_create_layer_id(name))
                .unwrap_or(0)),
            _ => Err(GraphError::AttemptToMutateImmutableGraph),
        }
    }

    fn resolve_node_type(&self, v_id: VID, node_type: Option<&str>) -> Result<usize, GraphError> {
        match self {
            GraphStorage::Unlocked(storage) => storage.resolve_node_type(v_id, node_type),
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
            GraphStorage::Unlocked(_) => self.graph_meta().resolve_property(prop, dtype, is_static),
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
            GraphStorage::Unlocked(_) => self.node_meta().resolve_prop_id(prop, dtype, is_static),
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
            GraphStorage::Unlocked(_) => self.edge_meta().resolve_prop_id(prop, dtype, is_static),
            _ => Err(GraphError::AttemptToMutateImmutableGraph),
        }
    }

    fn process_prop_value(&self, prop: Prop) -> Prop {
        match self {
            GraphStorage::Unlocked(storage) => match prop {
                Prop::Str(value) => Prop::Str(storage.resolve_str(value)),
                _ => prop,
            },
            GraphStorage::Mem(storage) => match prop {
                Prop::Str(value) => Prop::Str(storage.graph.resolve_str(value)),
                _ => prop,
            },
            #[cfg(feature = "storage")]
            _ => prop,
        }
    }

    fn internal_add_node(
        &self,
        t: TimeIndexEntry,
        v: VID,
        props: Vec<(usize, Prop)>,
        node_type_id: usize,
    ) -> Result<(), GraphError> {
        match self {
            GraphStorage::Unlocked(storage) => storage.add_node_internal(t, v, props, node_type_id),
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
            GraphStorage::Unlocked(storage) => storage.add_edge_internal(t, src, dst, props, layer),
            _ => Err(GraphError::AttemptToMutateImmutableGraph),
        }
    }
}
