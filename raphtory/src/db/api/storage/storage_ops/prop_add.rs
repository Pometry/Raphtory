use raphtory_api::core::{
    entities::{EID, VID},
    storage::timeindex::TimeIndexEntry,
};

use crate::{
    core::utils::errors::GraphError, db::api::mutation::internal::InternalPropertyAdditionOps,
    prelude::Prop,
};

use super::GraphStorage;

impl InternalPropertyAdditionOps for GraphStorage {
    fn internal_add_properties(
        &self,
        t: TimeIndexEntry,
        props: Vec<(usize, Prop)>,
    ) -> Result<(), GraphError> {
        match self {
            GraphStorage::Unlocked(storage) => storage.add_properties(t, props),
            _ => Err(GraphError::AttemptToMutateImmutableGraph),
        }
    }

    fn internal_add_static_properties(&self, props: Vec<(usize, Prop)>) -> Result<(), GraphError> {
        match self {
            GraphStorage::Unlocked(storage) => storage.add_constant_properties(props),
            _ => Err(GraphError::AttemptToMutateImmutableGraph),
        }
    }

    fn internal_update_static_properties(
        &self,
        props: Vec<(usize, Prop)>,
    ) -> Result<(), GraphError> {
        match self {
            GraphStorage::Unlocked(storage) => storage.update_constant_properties(props),
            _ => Err(GraphError::AttemptToMutateImmutableGraph),
        }
    }

    fn internal_add_constant_node_properties(
        &self,
        vid: VID,
        props: Vec<(usize, Prop)>,
    ) -> Result<(), GraphError> {
        match self {
            GraphStorage::Unlocked(storage) => {
                let mut node = storage.storage.get_node_mut(vid);
                for (prop_id, value) in props {
                    node.add_constant_prop(prop_id, value).map_err(|err| {
                        let name = self.node_meta().get_prop_name(prop_id, true);
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
            _ => Err(GraphError::AttemptToMutateImmutableGraph),
        }
    }

    fn internal_update_constant_node_properties(
        &self,
        vid: VID,
        props: Vec<(usize, Prop)>,
    ) -> Result<(), GraphError> {
        match self {
            GraphStorage::Unlocked(storage) => {
                let mut node = storage.storage.get_node_mut(vid);
                for (prop_id, value) in props {
                    node.update_constant_prop(prop_id, value)?;
                }
                Ok(())
            }
            _ => Err(GraphError::AttemptToMutateImmutableGraph),
        }
    }

    fn internal_add_constant_edge_properties(
        &self,
        eid: EID,
        layer: usize,
        props: Vec<(usize, Prop)>,
    ) -> Result<(), GraphError> {
        match self {
            GraphStorage::Unlocked(storage) => {
                let mut edge = storage.storage.get_edge_mut(eid);
                let edge_layer = edge.layer_mut(layer);
                for (prop_id, value) in props {
                    edge_layer
                        .add_constant_prop(prop_id, value)
                        .map_err(|err| {
                            let name = self.edge_meta().get_prop_name(prop_id, true);
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
            _ => Err(GraphError::AttemptToMutateImmutableGraph),
        }
    }

    fn internal_update_constant_edge_properties(
        &self,
        eid: EID,
        layer: usize,
        props: Vec<(usize, Prop)>,
    ) -> Result<(), GraphError> {
        match self {
            GraphStorage::Unlocked(storage) => {
                let mut edge = storage.storage.get_edge_mut(eid);
                let edge_layer = edge.layer_mut(layer);
                for (prop_id, value) in props {
                    edge_layer.update_constant_prop(prop_id, value)?;
                }
                Ok(())
            }
            _ => Err(GraphError::AttemptToMutateImmutableGraph),
        }
    }
}
