use crate::{
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
use raphtory_core::entities::graph::tgraph::TemporalGraph;

pub trait InternalPropertyAdditionOps {
    type Error: From<MutationError>;
    fn internal_add_properties(
        &self,
        t: TimeIndexEntry,
        props: &[(usize, Prop)],
    ) -> Result<(), Self::Error>;
    fn internal_add_constant_properties(&self, props: &[(usize, Prop)]) -> Result<(), Self::Error>;
    fn internal_update_constant_properties(
        &self,
        props: &[(usize, Prop)],
    ) -> Result<(), Self::Error>;
    fn internal_add_constant_node_properties(
        &self,
        vid: VID,
        props: &[(usize, Prop)],
    ) -> Result<(), Self::Error>;
    fn internal_update_constant_node_properties(
        &self,
        vid: VID,
        props: &[(usize, Prop)],
    ) -> Result<(), Self::Error>;
    fn internal_add_constant_edge_properties(
        &self,
        eid: EID,
        layer: usize,
        props: &[(usize, Prop)],
    ) -> Result<(), Self::Error>;
    fn internal_update_constant_edge_properties(
        &self,
        eid: EID,
        layer: usize,
        props: &[(usize, Prop)],
    ) -> Result<(), Self::Error>;
}

impl InternalPropertyAdditionOps for TemporalGraph {
    type Error = MutationError;
    fn internal_add_properties(
        &self,
        t: TimeIndexEntry,
        props: &[(usize, Prop)],
    ) -> Result<(), Self::Error> {
        if !props.is_empty() {
            for (prop_id, prop) in props {
                let prop = self.process_prop_value(prop);
                let prop = validate_prop(prop).map_err(MutationError::from)?;
                self.graph_meta
                    .add_prop(t, *prop_id, prop)
                    .map_err(MutationError::from)?;
            }
            self.update_time(t);
        }
        Ok(())
    }

    fn internal_add_constant_properties(&self, props: &[(usize, Prop)]) -> Result<(), Self::Error> {
        for (id, prop) in props {
            let prop = self.process_prop_value(prop);
            let prop = validate_prop(prop).map_err(MutationError::from)?;
            self.graph_meta
                .add_constant_prop(*id, prop)
                .map_err(MutationError::from)?;
        }
        Ok(())
    }

    fn internal_update_constant_properties(
        &self,
        props: &[(usize, Prop)],
    ) -> Result<(), Self::Error> {
        for (id, prop) in props {
            let prop = self.process_prop_value(prop);
            let prop = validate_prop(prop).map_err(MutationError::from)?;
            self.graph_meta.update_constant_prop(*id, prop);
        }
        Ok(())
    }

    fn internal_add_constant_node_properties(
        &self,
        vid: VID,
        props: &[(usize, Prop)],
    ) -> Result<(), Self::Error> {
        let mut node = self.storage.get_node_mut(vid);
        for (prop_id, prop) in props {
            let prop = self.process_prop_value(prop);
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
        let mut node = self.storage.get_node_mut(vid);
        for (prop_id, prop) in props {
            let prop = self.process_prop_value(prop);
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
        let mut edge = self.storage.get_edge_mut(eid);
        let mut edge_mut = edge.as_mut();
        if let Some(edge_layer) = edge_mut.get_layer_mut(layer) {
            for (prop_id, prop) in props {
                let prop = self.process_prop_value(prop);
                let prop = validate_prop(prop).map_err(MutationError::from)?;
                edge_layer
                    .add_constant_prop(*prop_id, prop)
                    .map_err(MutationError::from)?;
            }
            Ok(())
        } else {
            let layer = self.get_layer_name(layer).to_string();
            let src = self.node(edge.as_ref().src()).as_ref().id().to_string();
            let dst = self.node(edge.as_ref().dst()).as_ref().id().to_string();
            Err(MutationError::InvalidEdgeLayer { layer, src, dst })
        }
    }

    fn internal_update_constant_edge_properties(
        &self,
        eid: EID,
        layer: usize,
        props: &[(usize, Prop)],
    ) -> Result<(), Self::Error> {
        let mut edge = self.storage.get_edge_mut(eid);
        let mut edge_mut = edge.as_mut();
        if let Some(edge_layer) = edge_mut.get_layer_mut(layer) {
            for (prop_id, prop) in props {
                let prop = self.process_prop_value(prop);
                let prop = validate_prop(prop).map_err(MutationError::from)?;
                edge_layer
                    .update_constant_prop(*prop_id, prop)
                    .map_err(MutationError::from)?;
            }
            Ok(())
        } else {
            let layer = self.get_layer_name(layer).to_string();
            let src = self.node(edge.as_ref().src()).as_ref().id().to_string();
            let dst = self.node(edge.as_ref().dst()).as_ref().id().to_string();
            Err(MutationError::InvalidEdgeLayer { layer, src, dst })
        }
    }
}

impl <EXT> InternalPropertyAdditionOps for db4_graph::TemporalGraph<EXT> {
    type Error = MutationError;

    fn internal_add_properties(
        &self,
        t: TimeIndexEntry,
        props: &[(usize, Prop)],
    ) -> Result<(), Self::Error> {
        todo!()
    }

    fn internal_add_constant_properties(&self, props: &[(usize, Prop)]) -> Result<(), Self::Error> {
        todo!()
    }

    fn internal_update_constant_properties(
        &self,
        props: &[(usize, Prop)],
    ) -> Result<(), Self::Error> {
        todo!()
    }

    fn internal_add_constant_node_properties(
        &self,
        vid: VID,
        props: &[(usize, Prop)],
    ) -> Result<(), Self::Error> {
        todo!()
    }

    fn internal_update_constant_node_properties(
        &self,
        vid: VID,
        props: &[(usize, Prop)],
    ) -> Result<(), Self::Error> {
        todo!()
    }

    fn internal_add_constant_edge_properties(
        &self,
        eid: EID,
        layer: usize,
        props: &[(usize, Prop)],
    ) -> Result<(), Self::Error> {
        todo!()
    }

    fn internal_update_constant_edge_properties(
        &self,
        eid: EID,
        layer: usize,
        props: &[(usize, Prop)],
    ) -> Result<(), Self::Error> {
        todo!()
    }
}

impl InternalPropertyAdditionOps for GraphStorage {
    type Error = MutationError;

    fn internal_add_properties(
        &self,
        t: TimeIndexEntry,
        props: &[(usize, Prop)],
    ) -> Result<(), Self::Error> {
        Ok(self.mutable()?.internal_add_properties(t, props)?)
    }

    fn internal_add_constant_properties(&self, props: &[(usize, Prop)]) -> Result<(), Self::Error> {
        Ok(self.mutable()?.internal_add_constant_properties(props)?)
    }

    fn internal_update_constant_properties(
        &self,
        props: &[(usize, Prop)],
    ) -> Result<(), Self::Error> {
        self.mutable()?.internal_update_constant_properties(props)
    }

    fn internal_add_constant_node_properties(
        &self,
        vid: VID,
        props: &[(usize, Prop)],
    ) -> Result<(), Self::Error> {
        self.mutable()?
            .internal_add_constant_node_properties(vid, props)
    }

    fn internal_update_constant_node_properties(
        &self,
        vid: VID,
        props: &[(usize, Prop)],
    ) -> Result<(), Self::Error> {
        self.mutable()?
            .internal_update_constant_node_properties(vid, props)
    }

    fn internal_add_constant_edge_properties(
        &self,
        eid: EID,
        layer: usize,
        props: &[(usize, Prop)],
    ) -> Result<(), Self::Error> {
        self.mutable()?
            .internal_add_constant_edge_properties(eid, layer, props)
    }

    fn internal_update_constant_edge_properties(
        &self,
        eid: EID,
        layer: usize,
        props: &[(usize, Prop)],
    ) -> Result<(), Self::Error> {
        self.mutable()?
            .internal_update_constant_edge_properties(eid, layer, props)
    }
}

pub trait InheritPropertyAdditionOps: Base {}

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
