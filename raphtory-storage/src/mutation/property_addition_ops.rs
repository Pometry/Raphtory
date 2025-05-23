use crate::{
    core_ops::CoreGraphOps,
    graph::{graph::GraphStorage, nodes::node_storage_ops::NodeStorageOps},
    mutation::MutationError,
};
use raphtory_api::{
    core::{
        entities::{
            properties::prop::{validate_prop, Prop},
            EID, VID,
        },
        storage::timeindex::TimeIndexEntry,
    },
    inherit::Base,
};

pub trait InternalPropertyAdditionOps: CoreGraphOps {
    type Error: From<MutationError>;
    fn internal_add_properties(
        &self,
        t: TimeIndexEntry,
        props: &[(usize, Prop)],
    ) -> Result<(), Self::Error> {
        let graph = self.core_graph().mutable()?;
        if !props.is_empty() {
            for (prop_id, prop) in props {
                let prop = graph.process_prop_value(prop);
                let prop = validate_prop(prop).map_err(MutationError::from)?;
                graph
                    .graph_meta
                    .add_prop(t, *prop_id, prop)
                    .map_err(MutationError::from)?;
            }
            graph.update_time(t);
        }
        Ok(())
    }

    fn internal_add_constant_properties(&self, props: &[(usize, Prop)]) -> Result<(), Self::Error> {
        let graph = self.core_graph().mutable()?;
        for (id, prop) in props {
            let prop = graph.process_prop_value(prop);
            let prop = validate_prop(prop).map_err(MutationError::from)?;
            graph
                .graph_meta
                .add_constant_prop(*id, prop)
                .map_err(MutationError::from)?;
        }
        Ok(())
    }

    fn internal_update_constant_properties(
        &self,
        props: &[(usize, Prop)],
    ) -> Result<(), Self::Error> {
        let graph = self.core_graph().mutable()?;
        for (id, prop) in props {
            let prop = graph.process_prop_value(prop);
            let prop = validate_prop(prop).map_err(MutationError::from)?;
            graph.graph_meta.update_constant_prop(*id, prop);
        }
        Ok(())
    }

    fn internal_add_constant_node_properties(
        &self,
        vid: VID,
        props: &[(usize, Prop)],
    ) -> Result<(), Self::Error> {
        let graph = self.core_graph().mutable()?;
        let mut node = graph.storage.get_node_mut(vid);
        for (prop_id, prop) in props {
            let prop = graph.process_prop_value(prop);
            let prop = validate_prop(prop).map_err(MutationError::from)?;
            node.as_mut()
                .add_constant_prop(*prop_id, prop)
                .map_err(MutationError::from)?;
        }
        Ok(())
    }

    fn internal_update_constant_node_properties(
        &self,
        vid: VID,
        props: &[(usize, Prop)],
    ) -> Result<(), Self::Error> {
        let graph = self.core_graph().mutable()?;
        let mut node = graph.storage.get_node_mut(vid);
        for (prop_id, prop) in props {
            let prop = graph.process_prop_value(prop);
            let prop = validate_prop(prop).map_err(MutationError::from)?;
            node.as_mut()
                .update_constant_prop(*prop_id, prop)
                .map_err(MutationError::from)?;
        }
        Ok(())
    }

    fn internal_add_constant_edge_properties(
        &self,
        eid: EID,
        layer: usize,
        props: &[(usize, Prop)],
    ) -> Result<(), Self::Error> {
        let graph = self.core_graph().mutable()?;
        let mut edge = graph.storage.get_edge_mut(eid);
        let mut edge_mut = edge.as_mut();
        if let Some(edge_layer) = edge_mut.get_layer_mut(layer) {
            for (prop_id, prop) in props {
                let prop = graph.process_prop_value(prop);
                let prop = validate_prop(prop).map_err(MutationError::from)?;
                edge_layer
                    .add_constant_prop(*prop_id, prop)
                    .map_err(MutationError::from)?;
            }
            Ok(())
        } else {
            let layer = graph.get_layer_name(layer).to_string();
            let src = self.core_node(edge.as_ref().src()).id().to_string();
            let dst = self.core_node(edge.as_ref().dst()).id().to_string();
            Err(MutationError::InvalidEdgeLayer { layer, src, dst }.into())
        }
    }

    fn internal_update_constant_edge_properties(
        &self,
        eid: EID,
        layer: usize,
        props: &[(usize, Prop)],
    ) -> Result<(), Self::Error> {
        let graph = self.core_graph().mutable()?;
        let mut edge = graph.storage.get_edge_mut(eid);
        let mut edge_mut = edge.as_mut();
        if let Some(edge_layer) = edge_mut.get_layer_mut(layer) {
            for (prop_id, prop) in props {
                let prop = graph.process_prop_value(prop);
                let prop = validate_prop(prop).map_err(MutationError::from)?;
                edge_layer
                    .update_constant_prop(*prop_id, prop)
                    .map_err(MutationError::from)?;
            }
            Ok(())
        } else {
            let layer = graph.get_layer_name(layer).to_string();
            let src = self.core_node(edge.as_ref().src()).id().to_string();
            let dst = self.core_node(edge.as_ref().dst()).id().to_string();
            Err(MutationError::InvalidEdgeLayer { layer, src, dst }.into())
        }
    }
}

impl InternalPropertyAdditionOps for GraphStorage {
    type Error = MutationError;
}

pub trait InheritPropertyAdditionOps: Base + CoreGraphOps {}

impl<G: InheritPropertyAdditionOps> InternalPropertyAdditionOps for G
where
    G::Base: InternalPropertyAdditionOps,
{
    type Error = <G::Base as InternalPropertyAdditionOps>::Error;

    #[inline]
    fn internal_add_properties(
        &self,
        t: TimeIndexEntry,
        props: &[(usize, Prop)],
    ) -> Result<(), Self::Error> {
        self.base().internal_add_properties(t, props)
    }

    #[inline]
    fn internal_add_constant_properties(&self, props: &[(usize, Prop)]) -> Result<(), Self::Error> {
        self.base().internal_add_constant_properties(props)
    }

    #[inline]
    fn internal_update_constant_properties(
        &self,
        props: &[(usize, Prop)],
    ) -> Result<(), Self::Error> {
        self.base().internal_update_constant_properties(props)
    }

    #[inline]
    fn internal_add_constant_node_properties(
        &self,
        vid: VID,
        props: &[(usize, Prop)],
    ) -> Result<(), Self::Error> {
        self.base()
            .internal_add_constant_node_properties(vid, props)
    }

    #[inline]
    fn internal_update_constant_node_properties(
        &self,
        vid: VID,
        props: &[(usize, Prop)],
    ) -> Result<(), Self::Error> {
        self.base()
            .internal_update_constant_node_properties(vid, props)
    }

    #[inline]
    fn internal_add_constant_edge_properties(
        &self,
        eid: EID,
        layer: usize,
        props: &[(usize, Prop)],
    ) -> Result<(), Self::Error> {
        self.base()
            .internal_add_constant_edge_properties(eid, layer, props)
    }

    #[inline]
    fn internal_update_constant_edge_properties(
        &self,
        eid: EID,
        layer: usize,
        props: &[(usize, Prop)],
    ) -> Result<(), Self::Error> {
        self.base()
            .internal_update_constant_edge_properties(eid, layer, props)
    }
}
