use crate::{
    core::{
        entities::{graph::tgraph::InnerTemporalGraph, EID, VID},
        storage::timeindex::TimeIndexEntry,
        utils::errors::GraphError,
    },
    db::api::{mutation::internal::InternalPropertyAdditionOps, view::internal::CoreGraphOps},
    prelude::Prop,
};

impl<const N: usize> InternalPropertyAdditionOps for InnerTemporalGraph<N> {
    fn internal_add_properties(
        &self,
        t: TimeIndexEntry,
        props: Vec<(usize, Prop)>,
    ) -> Result<(), GraphError> {
        self.inner().add_properties(t, props)
    }

    fn internal_add_static_properties(&self, props: Vec<(usize, Prop)>) -> Result<(), GraphError> {
        self.inner().add_constant_properties(props)
    }

    fn internal_update_static_properties(
        &self,
        props: Vec<(usize, Prop)>,
    ) -> Result<(), GraphError> {
        self.inner().update_constant_properties(props)
    }

    fn internal_add_constant_vertex_properties(
        &self,
        vid: VID,
        props: Vec<(usize, Prop)>,
    ) -> Result<(), GraphError> {
        let mut node = self.inner().storage.get_node_mut(vid);
        for (prop_id, value) in props {
            node.add_constant_prop(prop_id, value).map_err(|err| {
                let name = self.vertex_meta().get_prop_name(prop_id, true);
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

    fn internal_update_constant_vertex_properties(
        &self,
        vid: VID,
        props: Vec<(usize, Prop)>,
    ) -> Result<(), GraphError> {
        let mut node = self.inner().storage.get_node_mut(vid);
        for (prop_id, value) in props {
            node.update_constant_prop(prop_id, value)?;
        }
        Ok(())
    }

    fn internal_add_constant_edge_properties(
        &self,
        eid: EID,
        layer: usize,
        props: Vec<(usize, Prop)>,
    ) -> Result<(), GraphError> {
        let mut edge = self.inner().storage.get_edge_mut(eid);
        let mut edge_layer = edge.layer_mut(layer);
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

    fn internal_update_constant_edge_properties(
        &self,
        eid: EID,
        layer: usize,
        props: Vec<(usize, Prop)>,
    ) -> Result<(), GraphError> {
        let mut edge = self.inner().storage.get_edge_mut(eid);
        let mut edge_layer = edge.layer_mut(layer);
        for (prop_id, value) in props {
            edge_layer.update_constant_prop(prop_id, value)?;
        }
        Ok(())
    }
}
