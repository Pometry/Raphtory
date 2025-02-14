use raphtory_api::core::{
    entities::{EID, VID},
    storage::timeindex::TimeIndexEntry,
};

use super::GraphStorage;
use crate::{
    core::{entities::graph::tgraph::TemporalGraph, utils::errors::GraphError},
    db::api::{
        mutation::internal::InternalPropertyAdditionOps,
        storage::graph::edges::edge_storage_ops::{EdgeStorageOps, MemEdge},
    },
    prelude::Prop,
};

impl TemporalGraph {
    fn missing_layer_error(&self, edge: MemEdge, layer_id: usize) -> GraphError {
        let layer = self.get_layer_name(layer_id).to_string();
        let src = self
            .storage
            .get_node(edge.src())
            .get_entry()
            .node()
            .global_id
            .to_string();
        let dst = self
            .storage
            .get_node(edge.dst())
            .get_entry()
            .node()
            .global_id
            .to_string();
        GraphError::InvalidEdgeLayer { layer, src, dst }
    }
}

impl InternalPropertyAdditionOps for TemporalGraph {
    fn internal_add_properties(
        &self,
        t: TimeIndexEntry,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        if !props.is_empty() {
            for (prop_id, prop) in props {
                let prop = self.process_prop_value(prop);
                self.graph_meta.add_prop(t, *prop_id, prop)?;
            }
            self.update_time(t);
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
            node.as_mut()
                .add_constant_prop(*prop_id, prop)
                .map_err(|err| {
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
            node.as_mut().update_constant_prop(*prop_id, prop)?;
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
        let mut edge_mut = edge.as_mut();
        if let Some(edge_layer) = edge_mut.get_layer_mut(layer) {
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
        } else {
            Err(self.missing_layer_error(edge.as_ref(), layer))
        }
    }

    fn internal_update_constant_edge_properties(
        &self,
        eid: EID,
        layer: usize,
        props: &[(usize, Prop)],
    ) -> Result<(), GraphError> {
        let mut edge = self.storage.get_edge_mut(eid);
        let mut edge_mut = edge.as_mut();
        if let Some(edge_layer) = edge_mut.get_layer_mut(layer) {
            for (prop_id, prop) in props {
                let prop = self.process_prop_value(prop);
                edge_layer.update_constant_prop(*prop_id, prop)?;
            }
            Ok(())
        } else {
            Err(self.missing_layer_error(edge.as_ref(), layer))
        }
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

#[cfg(test)]
mod test {
    use crate::{prelude::*, test_storage};
    use itertools::Itertools;

    #[test]
    fn test_graph_temporal_prop_updates_time() {
        let graph = Graph::new();
        graph.add_properties(1, [("test", "test")]).unwrap();
        graph.add_properties(2, [("test", "test2")]).unwrap();
        test_storage!(&graph, |graph| {
            assert_eq!(graph.earliest_time(), Some(1));
            assert_eq!(graph.latest_time(), Some(2));
            assert_eq!(
                graph
                    .properties()
                    .temporal()
                    .get("test")
                    .unwrap()
                    .iter()
                    .collect_vec(),
                [(1, Prop::str("test")), (2, Prop::str("test2"))]
            );
        });
    }

    #[test]
    fn test_constant_edge_prop_updates_multiple_layers() {
        let graph = Graph::new();
        graph.add_edge(0, 1, 2, NO_PROPS, Some("1")).unwrap();
        let edge = graph.edge(1, 2).unwrap();
        // check that it is not possible to add constant properties for layers that do not have updates
        assert!(edge
            .add_constant_properties([("test", "test")], None)
            .is_err());
        assert!(edge
            .add_constant_properties([("test", "test")], Some("2"))
            .is_err());
        assert!(edge
            .update_constant_properties([("test", "test")], None)
            .is_err());
        assert!(edge
            .update_constant_properties([("test", "test")], Some("2"))
            .is_err());

        // make sure we didn't accidentally create a new layer in the failed updates
        assert!(graph.layers("2").is_err());

        // make sure constant property updates for existing layers work
        edge.add_constant_properties([("test", "test")], Some("1"))
            .unwrap();
        assert_eq!(
            edge.properties().constant().get("test"),
            Some(Prop::map([("1", "test")]))
        );
        edge.update_constant_properties([("test", "test2")], Some("1"))
            .unwrap();
        assert_eq!(
            edge.properties().constant().get("test"),
            Some(Prop::map([("1", "test2")]))
        );
    }
}
