use crate::{
    core_ops::CoreGraphOps,
    graph::{graph::GraphStorage, locked::WriteLockedGraph},
    mutation::MutationError,
};
use raphtory_api::{
    core::{
        entities::{
            properties::prop::{Prop, PropType},
            GidRef, EID, VID,
        },
        storage::{dict_mapper::MaybeNew, timeindex::TimeIndexEntry},
    },
    inherit::Base,
};
use raphtory_core::{
    entities::{graph::tgraph::TemporalGraph, nodes::node_ref::NodeRef},
    storage::{raw_edges::WriteLockedEdges, WriteLockedNodes},
};
use std::sync::atomic::Ordering;

pub trait InternalAdditionOps {
    type Error: From<MutationError>;
    fn write_lock(&self) -> Result<WriteLockedGraph, Self::Error>;
    fn write_lock_nodes(&self) -> Result<WriteLockedNodes, Self::Error>;
    fn write_lock_edges(&self) -> Result<WriteLockedEdges, Self::Error>;
    /// get the sequence id for the next event
    fn next_event_id(&self) -> Result<usize, Self::Error>;
    fn reserve_event_ids(&self, num_ids: usize) -> Result<usize, Self::Error>;
    /// map layer name to id and allocate a new layer if needed
    fn resolve_layer(&self, layer: Option<&str>) -> Result<MaybeNew<usize>, Self::Error>;
    /// map external node id to internal id, allocating a new empty node if needed
    fn resolve_node(&self, id: NodeRef) -> Result<MaybeNew<VID>, Self::Error>;
    fn set_node(&self, gid: GidRef, vid: VID) -> Result<(), Self::Error>;
    /// resolve a node and corresponding type, outer MaybeNew tracks whether the type assignment is new for the node even if both node and type already existed.
    fn resolve_node_and_type(
        &self,
        id: NodeRef,
        node_type: &str,
    ) -> Result<MaybeNew<(MaybeNew<VID>, MaybeNew<usize>)>, Self::Error>;
    /// map property key to internal id, allocating new property if needed
    fn resolve_graph_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, Self::Error>;
    /// map property key to internal id, allocating new property if needed and checking property type.
    /// returns `None` if the type does not match
    fn resolve_node_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, Self::Error>;
    fn resolve_edge_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, Self::Error>;
    /// add node update
    fn internal_add_node(
        &self,
        t: TimeIndexEntry,
        v: VID,
        props: &[(usize, Prop)],
    ) -> Result<(), Self::Error>;
    /// add edge update
    fn internal_add_edge(
        &self,
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
        props: &[(usize, Prop)],
        layer: usize,
    ) -> Result<MaybeNew<EID>, Self::Error>;
    /// add update for an existing edge
    fn internal_add_edge_update(
        &self,
        t: TimeIndexEntry,
        edge: EID,
        props: &[(usize, Prop)],
        layer: usize,
    ) -> Result<(), Self::Error>;
}

impl InternalAdditionOps for TemporalGraph {
    type Error = MutationError;

    fn write_lock(&self) -> Result<WriteLockedGraph, Self::Error> {
        Ok(WriteLockedGraph::new(self))
    }

    fn write_lock_nodes(&self) -> Result<WriteLockedNodes, Self::Error> {
        Ok(self.storage.nodes.write_lock())
    }

    fn write_lock_edges(&self) -> Result<WriteLockedEdges, Self::Error> {
        Ok(self.storage.edges.write_lock())
    }

    /// get the sequence id for the next event
    fn next_event_id(&self) -> Result<usize, Self::Error> {
        Ok(self.event_counter.fetch_add(1, Ordering::Relaxed))
    }

    fn reserve_event_ids(&self, num_ids: usize) -> Result<usize, Self::Error> {
        Ok(self.event_counter.fetch_add(num_ids, Ordering::Relaxed))
    }

    /// map layer name to id and allocate a new layer if needed
    fn resolve_layer(&self, layer: Option<&str>) -> Result<MaybeNew<usize>, Self::Error> {
        let id = self
            .resolve_layer_inner(layer)
            .map_err(MutationError::from)?;
        Ok(id)
    }

    /// map external node id to internal id, allocating a new empty node if needed
    fn resolve_node(&self, id: NodeRef) -> Result<MaybeNew<VID>, Self::Error> {
        Ok(self.resolve_node_inner(id).map_err(MutationError::from)?)
    }

    fn set_node(&self, gid: GidRef, vid: VID) -> Result<(), Self::Error> {
        Ok(self
            .logical_to_physical
            .set(gid, vid)
            .map_err(MutationError::from)?)
    }

    /// resolve a node and corresponding type, outer MaybeNew tracks whether the type assignment is new for the node even if both node and type already existed.
    fn resolve_node_and_type(
        &self,
        id: NodeRef,
        node_type: &str,
    ) -> Result<MaybeNew<(MaybeNew<VID>, MaybeNew<usize>)>, Self::Error> {
        let vid = self.resolve_node(id).map_err(MutationError::from)?;
        let mut entry = self.storage.get_node_mut(vid.inner());
        let mut entry_ref = entry.to_mut();
        let node_store = entry_ref.node_store_mut();
        if node_store.node_type == 0 {
            let node_type_id = self.node_meta.get_or_create_node_type_id(node_type);
            node_store.update_node_type(node_type_id.inner());
            Ok(MaybeNew::New((vid, node_type_id)))
        } else {
            let node_type_id = self
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
    ) -> Result<MaybeNew<usize>, Self::Error> {
        Ok(self
            .graph_meta
            .resolve_property(prop, dtype, is_static)
            .map_err(MutationError::from)?)
    }

    /// map property key to internal id, allocating new property if needed and checking property type.
    /// returns `None` if the type does not match
    fn resolve_node_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, Self::Error> {
        Ok(self
            .node_meta
            .resolve_prop_id(prop, dtype, is_static)
            .map_err(MutationError::from)?)
    }

    fn resolve_edge_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, Self::Error> {
        Ok(self
            .edge_meta
            .resolve_prop_id(prop, dtype, is_static)
            .map_err(MutationError::from)?)
    }

    /// add node update
    fn internal_add_node(
        &self,
        t: TimeIndexEntry,
        v: VID,
        props: &[(usize, Prop)],
    ) -> Result<(), Self::Error> {
        self.update_time(t);
        let mut entry = self.storage.get_node_mut(v);
        let mut node = entry.to_mut();
        let prop_i = node
            .t_props_log_mut()
            .push(props.iter().map(|(prop_id, prop)| {
                let prop = self.process_prop_value(prop);
                (*prop_id, prop)
            }))
            .map_err(MutationError::from)?;
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
    ) -> Result<MaybeNew<EID>, Self::Error> {
        let edge = self.link_nodes(src, dst, t, layer, false);
        edge.try_map(|mut edge| {
            let eid = edge.eid();
            let mut edge = edge.as_mut();
            edge.additions_mut(layer).insert(t);
            if !props.is_empty() {
                let edge_layer = edge.layer_mut(layer);
                for (prop_id, prop) in props {
                    let prop = self.process_prop_value(prop);
                    edge_layer
                        .add_prop(t, *prop_id, prop)
                        .map_err(MutationError::from)?;
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
    ) -> Result<(), Self::Error> {
        let mut edge = self.link_edge(edge, t, layer, false);
        let mut edge = edge.as_mut();
        edge.additions_mut(layer).insert(t);
        if !props.is_empty() {
            let edge_layer = edge.layer_mut(layer);
            for (prop_id, prop) in props {
                let prop = self.process_prop_value(prop);
                edge_layer
                    .add_prop(t, *prop_id, prop)
                    .map_err(MutationError::from)?
            }
        }
        Ok(())
    }
}

impl InternalAdditionOps for GraphStorage {
    type Error = MutationError;

    fn write_lock(&self) -> Result<WriteLockedGraph, Self::Error> {
        self.mutable()?.write_lock()
    }

    fn write_lock_nodes(&self) -> Result<WriteLockedNodes, Self::Error> {
        self.mutable()?.write_lock_nodes()
    }

    fn write_lock_edges(&self) -> Result<WriteLockedEdges, Self::Error> {
        self.mutable()?.write_lock_edges()
    }

    fn next_event_id(&self) -> Result<usize, Self::Error> {
        self.mutable()?.next_event_id()
    }

    fn reserve_event_ids(&self, num_ids: usize) -> Result<usize, Self::Error> {
        self.mutable()?.reserve_event_ids(num_ids)
    }

    fn resolve_layer(&self, layer: Option<&str>) -> Result<MaybeNew<usize>, Self::Error> {
        self.mutable()?.resolve_layer(layer)
    }

    fn resolve_node(&self, id: NodeRef) -> Result<MaybeNew<VID>, Self::Error> {
        self.mutable()?.resolve_node(id)
    }

    fn set_node(&self, gid: GidRef, vid: VID) -> Result<(), Self::Error> {
        self.mutable()?.set_node(gid, vid)
    }

    fn resolve_node_and_type(
        &self,
        id: NodeRef,
        node_type: &str,
    ) -> Result<MaybeNew<(MaybeNew<VID>, MaybeNew<usize>)>, Self::Error> {
        self.mutable()?.resolve_node_and_type(id, node_type)
    }

    fn resolve_graph_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, Self::Error> {
        self.mutable()?
            .resolve_graph_property(prop, dtype, is_static)
    }

    fn resolve_node_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, Self::Error> {
        self.mutable()?
            .resolve_node_property(prop, dtype, is_static)
    }

    fn resolve_edge_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, Self::Error> {
        self.mutable()?
            .resolve_edge_property(prop, dtype, is_static)
    }

    fn internal_add_node(
        &self,
        t: TimeIndexEntry,
        v: VID,
        props: &[(usize, Prop)],
    ) -> Result<(), Self::Error> {
        self.mutable()?.internal_add_node(t, v, props)
    }

    fn internal_add_edge(
        &self,
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
        props: &[(usize, Prop)],
        layer: usize,
    ) -> Result<MaybeNew<EID>, Self::Error> {
        self.mutable()?.internal_add_edge(t, src, dst, props, layer)
    }

    fn internal_add_edge_update(
        &self,
        t: TimeIndexEntry,
        edge: EID,
        props: &[(usize, Prop)],
        layer: usize,
    ) -> Result<(), Self::Error> {
        self.mutable()?
            .internal_add_edge_update(t, edge, props, layer)
    }
}

pub trait InheritAdditionOps: Base + CoreGraphOps {}

impl<G: InheritAdditionOps> InternalAdditionOps for G
where
    G::Base: InternalAdditionOps,
{
    type Error = <G::Base as InternalAdditionOps>::Error;

    #[inline]
    fn write_lock(&self) -> Result<WriteLockedGraph, Self::Error> {
        self.base().write_lock()
    }

    #[inline]
    fn write_lock_nodes(&self) -> Result<WriteLockedNodes, Self::Error> {
        self.base().write_lock_nodes()
    }

    #[inline]
    fn write_lock_edges(&self) -> Result<WriteLockedEdges, Self::Error> {
        self.base().write_lock_edges()
    }

    #[inline]
    fn next_event_id(&self) -> Result<usize, Self::Error> {
        self.base().next_event_id()
    }

    #[inline]
    fn reserve_event_ids(&self, num_ids: usize) -> Result<usize, Self::Error> {
        self.base().reserve_event_ids(num_ids)
    }

    #[inline]
    fn resolve_layer(&self, layer: Option<&str>) -> Result<MaybeNew<usize>, Self::Error> {
        self.base().resolve_layer(layer)
    }

    #[inline]
    fn resolve_node(&self, id: NodeRef) -> Result<MaybeNew<VID>, Self::Error> {
        self.base().resolve_node(id)
    }

    #[inline]
    fn set_node(&self, gid: GidRef, vid: VID) -> Result<(), Self::Error> {
        self.base().set_node(gid, vid)
    }

    #[inline]
    fn resolve_node_and_type(
        &self,
        id: NodeRef,
        node_type: &str,
    ) -> Result<MaybeNew<(MaybeNew<VID>, MaybeNew<usize>)>, Self::Error> {
        self.base().resolve_node_and_type(id, node_type)
    }

    #[inline]
    fn resolve_graph_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, Self::Error> {
        self.base().resolve_graph_property(prop, dtype, is_static)
    }

    #[inline]
    fn resolve_node_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, Self::Error> {
        self.base().resolve_node_property(prop, dtype, is_static)
    }

    #[inline]
    fn resolve_edge_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, Self::Error> {
        self.base().resolve_edge_property(prop, dtype, is_static)
    }

    #[inline]
    fn internal_add_node(
        &self,
        t: TimeIndexEntry,
        v: VID,
        props: &[(usize, Prop)],
    ) -> Result<(), Self::Error> {
        self.base().internal_add_node(t, v, props)
    }

    #[inline]
    fn internal_add_edge(
        &self,
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
        props: &[(usize, Prop)],
        layer: usize,
    ) -> Result<MaybeNew<EID>, Self::Error> {
        self.base().internal_add_edge(t, src, dst, props, layer)
    }

    #[inline]
    fn internal_add_edge_update(
        &self,
        t: TimeIndexEntry,
        edge: EID,
        props: &[(usize, Prop)],
        layer: usize,
    ) -> Result<(), Self::Error> {
        self.base().internal_add_edge_update(t, edge, props, layer)
    }
}
