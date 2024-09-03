use raphtory_api::core::{
    entities::{EID, VID},
    storage::timeindex::TimeIndexEntry,
};

use super::GraphStorage;
use crate::{
    core::{entities::graph::tgraph::TemporalGraph, utils::errors::GraphError},
    db::api::mutation::internal::InternalPropertyAdditionOps,
    prelude::Prop,
};

impl InternalPropertyAdditionOps for TemporalGraph {
    fn internal_add_properties(
        &self,
        t: TimeIndexEntry,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        for (prop_id, prop) in props {
            let prop = self.process_prop_value(prop);
            self.graph_meta.add_prop(t, *prop_id, prop)?;
        }
        Ok(())
    }

    fn internal_add_constant_properties(&self, props: &[(usize, Prop)]) -> Result<(), GraphError> {
        for (id, prop) in props {
            let prop = self.process_prop_value(prop);
            self.graph_meta.add_constant_prop(*id, prop)?;
        }
        Ok(())
    }

    fn internal_update_constant_properties(
        &self,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        for (id, prop) in props {
            let prop = self.process_prop_value(prop);
            self.graph_meta.update_constant_prop(*id, prop)?;
        }
        Ok(())
    }

    fn internal_add_constant_node_properties(
        &self,
        vid: VID,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        let mut node = self.storage.get_node_mut(vid);
        for (prop_id, prop) in props {
            let prop = self.process_prop_value(prop);
            node.add_constant_prop(*prop_id, prop).map_err(|err| {
                let name = self.node_meta.get_prop_name(*prop_id, true);
                GraphError::ConstantPropertyMutationError {
                    name,
                    new: err.new_value.expect("new value exists"),
                    old: err
                        .previous_value
                        .expect("previous value exists if set failed"),
                }
            })?;
        }
        Ok(())
    }

    fn internal_update_constant_node_properties(
        &self,
        vid: VID,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        let mut node = self.storage.get_node_mut(vid);
        for (prop_id, prop) in props {
            let prop = self.process_prop_value(prop);
            node.update_constant_prop(*prop_id, prop)?;
        }
        Ok(())
    }

    fn internal_add_constant_edge_properties(
        &self,
        eid: EID,
        layer: usize,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        let mut edge = self.storage.get_edge_mut(eid);
        let mut edge = edge.as_mut();
        let edge_layer = edge.layer_mut(layer);
        for (prop_id, prop) in props {
            let prop = self.process_prop_value(prop);
            edge_layer
                .add_constant_prop(*prop_id, prop)
                .map_err(|err| {
                    let name = self.edge_meta.get_prop_name(*prop_id, true);
                    GraphError::ConstantPropertyMutationError {
                        name,
                        new: err.new_value.expect("new value exists"),
                        old: err
                            .previous_value
                            .expect("previous value exists if set failed"),
                    }
                })?;
        }
        Ok(())
    }

    fn internal_update_constant_edge_properties(
        &self,
        eid: EID,
        layer: usize,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        let mut edge = self.storage.get_edge_mut(eid);
        let mut edge = edge.as_mut();
        let edge_layer = edge.layer_mut(layer);
        for (prop_id, prop) in props {
            let prop = self.process_prop_value(prop);
            edge_layer.update_constant_prop(*prop_id, prop)?;
        }
        Ok(())
    }
}

impl InternalPropertyAdditionOps for GraphStorage {
    fn internal_add_properties(
        &self,
        t: TimeIndexEntry,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        match self {
            GraphStorage::Unlocked(storage) => storage.internal_add_properties(t, props),
            _ => Err(GraphError::AttemptToMutateImmutableGraph),
        }
    }

    fn internal_add_constant_properties(&self, props: &[(usize, Prop)]) -> Result<(), GraphError> {
        match self {
            GraphStorage::Unlocked(storage) => storage.internal_add_constant_properties(props),
            _ => Err(GraphError::AttemptToMutateImmutableGraph),
        }
    }

    fn internal_update_constant_properties(
        &self,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        match self {
            GraphStorage::Unlocked(storage) => storage.internal_update_constant_properties(props),
            _ => Err(GraphError::AttemptToMutateImmutableGraph),
        }
    }

    fn internal_add_constant_node_properties(
        &self,
        vid: VID,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        match self {
            GraphStorage::Unlocked(storage) => {
                storage.internal_add_constant_node_properties(vid, props)
            }
            _ => Err(GraphError::AttemptToMutateImmutableGraph),
        }
    }

    fn internal_update_constant_node_properties(
        &self,
        vid: VID,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        match self {
            GraphStorage::Unlocked(storage) => {
                storage.internal_update_constant_node_properties(vid, props)
            }
            _ => Err(GraphError::AttemptToMutateImmutableGraph),
        }
    }

    fn internal_add_constant_edge_properties(
        &self,
        eid: EID,
        layer: usize,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        match self {
            GraphStorage::Unlocked(storage) => {
                storage.internal_add_constant_edge_properties(eid, layer, props)
            }
            _ => Err(GraphError::AttemptToMutateImmutableGraph),
        }
    }

    fn internal_update_constant_edge_properties(
        &self,
        eid: EID,
        layer: usize,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        match self {
            GraphStorage::Unlocked(storage) => {
                storage.internal_update_constant_edge_properties(eid, layer, props)
            }
            _ => Err(GraphError::AttemptToMutateImmutableGraph),
        }
    }
}
