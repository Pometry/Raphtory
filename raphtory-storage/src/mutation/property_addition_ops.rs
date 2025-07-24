use crate::{
    graph::{graph::GraphStorage, nodes::node_storage_ops::NodeStorageOps},
    mutation::MutationError,
};
use parking_lot::RwLockWriteGuard;
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
use raphtory_core::{
    entities::graph::tgraph::TemporalGraph,
    storage::{raw_edges::EdgeWGuard, EntryMut, NodeSlot},
};

pub trait InternalPropertyAdditionOps {
    type Error: From<MutationError>;
    fn internal_add_properties(
        &self,
        t: TimeIndexEntry,
        props: &[(usize, Prop)],
    ) -> Result<(), Self::Error>;
    fn internal_add_metadata(&self, props: &[(usize, Prop)]) -> Result<(), Self::Error>;
    fn internal_update_metadata(&self, props: &[(usize, Prop)]) -> Result<(), Self::Error>;
    fn internal_add_node_metadata(
        &self,
        vid: VID,
        props: &[(usize, Prop)],
    ) -> Result<EntryMut<RwLockWriteGuard<NodeSlot>>, Self::Error>;
    fn internal_update_node_metadata(
        &self,
        vid: VID,
        props: &[(usize, Prop)],
    ) -> Result<EntryMut<RwLockWriteGuard<NodeSlot>>, Self::Error>;
    fn internal_add_edge_metadata(
        &self,
        eid: EID,
        layer: usize,
        props: &[(usize, Prop)],
    ) -> Result<EdgeWGuard, Self::Error>;
    fn internal_update_edge_metadata(
        &self,
        eid: EID,
        layer: usize,
        props: &[(usize, Prop)],
    ) -> Result<EdgeWGuard, Self::Error>;
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

    fn internal_add_metadata(&self, props: &[(usize, Prop)]) -> Result<(), Self::Error> {
        for (id, prop) in props {
            let prop = self.process_prop_value(prop);
            let prop = validate_prop(prop).map_err(MutationError::from)?;
            self.graph_meta
                .add_metadata(*id, prop)
                .map_err(MutationError::from)?;
        }
        Ok(())
    }

    fn internal_update_metadata(&self, props: &[(usize, Prop)]) -> Result<(), Self::Error> {
        for (id, prop) in props {
            let prop = self.process_prop_value(prop);
            let prop = validate_prop(prop).map_err(MutationError::from)?;
            self.graph_meta.update_metadata(*id, prop);
        }
        Ok(())
    }

    fn internal_add_node_metadata(
        &self,
        vid: VID,
        props: &[(usize, Prop)],
    ) -> Result<EntryMut<RwLockWriteGuard<NodeSlot>>, Self::Error> {
        let mut node = self.storage.get_node_mut(vid);
        for (prop_id, prop) in props {
            let prop = self.process_prop_value(prop);
            let prop = validate_prop(prop).map_err(MutationError::from)?;
            node.as_mut()
                .add_metadata(*prop_id, prop)
                .map_err(MutationError::from)?;
        }
        Ok(node)
    }

    fn internal_update_node_metadata(
        &self,
        vid: VID,
        props: &[(usize, Prop)],
    ) -> Result<EntryMut<RwLockWriteGuard<NodeSlot>>, Self::Error> {
        let mut node = self.storage.get_node_mut(vid);
        for (prop_id, prop) in props {
            let prop = self.process_prop_value(prop);
            let prop = validate_prop(prop).map_err(MutationError::from)?;
            node.as_mut()
                .update_metadata(*prop_id, prop)
                .map_err(MutationError::from)?;
        }
        Ok(node)
    }

    fn internal_add_edge_metadata(
        &self,
        eid: EID,
        layer: usize,
        props: &[(usize, Prop)],
    ) -> Result<EdgeWGuard, Self::Error> {
        let mut edge = self.storage.get_edge_mut(eid);
        let mut edge_mut = edge.as_mut();
        if let Some(edge_layer) = edge_mut.get_layer_mut(layer) {
            for (prop_id, prop) in props {
                let prop = self.process_prop_value(prop);
                let prop = validate_prop(prop).map_err(MutationError::from)?;
                edge_layer
                    .add_metadata(*prop_id, prop)
                    .map_err(MutationError::from)?;
            }
            Ok(edge)
        } else {
            let layer = self.get_layer_name(layer).to_string();
            let src = self.node(edge.as_ref().src()).as_ref().id().to_string();
            let dst = self.node(edge.as_ref().dst()).as_ref().id().to_string();
            Err(MutationError::InvalidEdgeLayer { layer, src, dst })
        }
    }

    fn internal_update_edge_metadata(
        &self,
        eid: EID,
        layer: usize,
        props: &[(usize, Prop)],
    ) -> Result<EdgeWGuard, Self::Error> {
        let mut edge = self.storage.get_edge_mut(eid);
        let mut edge_mut = edge.as_mut();
        if let Some(edge_layer) = edge_mut.get_layer_mut(layer) {
            for (prop_id, prop) in props {
                let prop = self.process_prop_value(prop);
                let prop = validate_prop(prop).map_err(MutationError::from)?;
                edge_layer
                    .update_metadata(*prop_id, prop)
                    .map_err(MutationError::from)?;
            }
            Ok(edge)
        } else {
            let layer = self.get_layer_name(layer).to_string();
            let src = self.node(edge.as_ref().src()).as_ref().id().to_string();
            let dst = self.node(edge.as_ref().dst()).as_ref().id().to_string();
            Err(MutationError::InvalidEdgeLayer { layer, src, dst })
        }
    }
}

impl InternalPropertyAdditionOps for GraphStorage {
    type Error = MutationError;

    fn internal_add_properties(
        &self,
        t: TimeIndexEntry,
        props: &[(usize, Prop)],
    ) -> Result<(), Self::Error> {
        self.mutable()?.internal_add_properties(t, props)
    }

    fn internal_add_metadata(&self, props: &[(usize, Prop)]) -> Result<(), Self::Error> {
        self.mutable()?.internal_add_metadata(props)
    }

    fn internal_update_metadata(&self, props: &[(usize, Prop)]) -> Result<(), Self::Error> {
        self.mutable()?.internal_update_metadata(props)
    }

    fn internal_add_node_metadata(
        &self,
        vid: VID,
        props: &[(usize, Prop)],
    ) -> Result<EntryMut<RwLockWriteGuard<NodeSlot>>, Self::Error> {
        self.mutable()?.internal_add_node_metadata(vid, props)
    }

    fn internal_update_node_metadata(
        &self,
        vid: VID,
        props: &[(usize, Prop)],
    ) -> Result<EntryMut<RwLockWriteGuard<NodeSlot>>, Self::Error> {
        self.mutable()?.internal_update_node_metadata(vid, props)
    }

    fn internal_add_edge_metadata(
        &self,
        eid: EID,
        layer: usize,
        props: &[(usize, Prop)],
    ) -> Result<EdgeWGuard, Self::Error> {
        self.mutable()?
            .internal_add_edge_metadata(eid, layer, props)
    }

    fn internal_update_edge_metadata(
        &self,
        eid: EID,
        layer: usize,
        props: &[(usize, Prop)],
    ) -> Result<EdgeWGuard, Self::Error> {
        self.mutable()?
            .internal_update_edge_metadata(eid, layer, props)
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
    fn internal_add_metadata(&self, props: &[(usize, Prop)]) -> Result<(), Self::Error> {
        self.base().internal_add_metadata(props)
    }

    #[inline]
    fn internal_update_metadata(&self, props: &[(usize, Prop)]) -> Result<(), Self::Error> {
        self.base().internal_update_metadata(props)
    }

    #[inline]
    fn internal_add_node_metadata(
        &self,
        vid: VID,
        props: &[(usize, Prop)],
    ) -> Result<EntryMut<RwLockWriteGuard<NodeSlot>>, Self::Error> {
        self.base().internal_add_node_metadata(vid, props)
    }

    #[inline]
    fn internal_update_node_metadata(
        &self,
        vid: VID,
        props: &[(usize, Prop)],
    ) -> Result<EntryMut<RwLockWriteGuard<NodeSlot>>, Self::Error> {
        self.base().internal_update_node_metadata(vid, props)
    }

    #[inline]
    fn internal_add_edge_metadata(
        &self,
        eid: EID,
        layer: usize,
        props: &[(usize, Prop)],
    ) -> Result<EdgeWGuard, Self::Error> {
        self.base().internal_add_edge_metadata(eid, layer, props)
    }

    #[inline]
    fn internal_update_edge_metadata(
        &self,
        eid: EID,
        layer: usize,
        props: &[(usize, Prop)],
    ) -> Result<EdgeWGuard, Self::Error> {
        self.base().internal_update_edge_metadata(eid, layer, props)
    }
}
