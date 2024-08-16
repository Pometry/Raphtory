use crate::{
    core::{entities::nodes::node_ref::AsNodeRef, utils::errors::GraphError, Prop, PropType},
    db::api::{
        mutation::internal::InternalAdditionOps,
        storage::nodes::node_storage_ops::NodeStorageOps,
        view::{
            internal::CoreGraphOps, serialise::ProtoGraph, Base, BoxableGraphView, InheritViewOps,
        },
    },
};
use either::Either;
use parking_lot::Mutex;
use raphtory_api::core::{
    entities::{EID, VID},
    storage::{dict_mapper::MaybeNew, timeindex::TimeIndexEntry},
};

pub struct CachedGraph<G, W> {
    graph: G,
    writer: W,
    proto_delta: Mutex<ProtoGraph>,
}

impl<G, W> Base for CachedGraph<G, W> {
    type Base = G;

    #[inline]
    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<W: Send + Sync, G: BoxableGraphView> InheritViewOps for CachedGraph<G, W> {}

impl<G: InternalAdditionOps + CoreGraphOps, W> InternalAdditionOps for CachedGraph<G, W> {
    #[inline]
    fn next_event_id(&self) -> Result<usize, GraphError> {
        self.graph.next_event_id()
    }

    #[inline]
    fn resolve_layer(&self, layer: Option<&str>) -> Result<MaybeNew<usize>, GraphError> {
        let id = self.graph.resolve_layer(layer)?;
        if let MaybeNew::New(id) = id {
            if let Some(layer) = layer {
                self.proto_delta.lock().new_layer(layer, id)
            }
        }
        Ok(id)
    }

    fn resolve_node<V: AsNodeRef>(&self, id: V) -> Result<MaybeNew<VID>, GraphError> {
        match id.as_gid_ref() {
            Either::Left(gid) => {
                let id = self.graph.resolve_node(gid)?;
                if let MaybeNew::New(id) = id {
                    self.proto_delta.lock().new_node(gid, id, 0);
                }
                Ok(id)
            }
            Either::Right(id) => Ok(MaybeNew::Existing(id)),
        }
    }

    fn resolve_node_and_type<V: AsNodeRef>(
        &self,
        id: V,
        node_type: &str,
    ) -> Result<MaybeNew<(MaybeNew<VID>, MaybeNew<usize>)>, GraphError> {
        let ids = self.graph.resolve_node_and_type(id, node_type)?;
        if let MaybeNew::New((MaybeNew::Existing(node_id), type_id)) = ids {
            // type assignment changed but node already exists
            self.proto_delta
                .lock()
                .update_node_type(node_id, type_id.inner());
        }
        if let (MaybeNew::New(node_id), type_id) = ids.inner() {
            // new node created
            let node = self.graph.core_node_entry(node_id);
            let gid = node.id();
            self.proto_delta
                .lock()
                .new_node(gid, node_id, type_id.inner());
        }
        if let (_, MaybeNew::New(type_id)) = ids.inner() {
            self.proto_delta.lock().new_node_type(node_type, type_id);
        }
        Ok(ids)
    }

    fn resolve_graph_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, GraphError> {
        let id = self.graph.resolve_graph_property(prop, dtype, is_static)?;
        if let MaybeNew::New(id) = id {
            if is_static {
                self.proto_delta.lock().new_graph_cprop(prop, id);
            } else {
                self.proto_delta.lock().new_graph_tprop(prop, id, &dtype);
            }
        }
        Ok(id)
    }

    fn resolve_node_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, GraphError> {
        let id = self.graph.resolve_node_property(prop, dtype, is_static)?;
        if let MaybeNew::New(id) = id {
            if is_static {
                self.proto_delta.lock().new_node_cprop(prop, id, &dtype);
            } else {
                self.proto_delta.lock().new_node_tprop(prop, id, &dtype);
            }
        }
        Ok(id)
    }

    fn resolve_edge_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, GraphError> {
        let id = self.graph.resolve_edge_property(prop, dtype, is_static)?;
        if let MaybeNew::New(id) = id {
            if is_static {
                self.proto_delta.lock().new_edge_cprop(prop, id, &dtype);
            } else {
                self.proto_delta.lock().new_edge_tprop(prop, id, &dtype);
            }
        }
        Ok(id)
    }

    fn internal_add_node(
        &self,
        t: TimeIndexEntry,
        v: VID,
        props: Vec<(usize, Prop)>,
    ) -> Result<(), GraphError> {
        self.proto_delta.lock().update_node_tprops(
            v,
            t,
            props.iter().map(|(id, prop)| (*id, prop)),
        );
        self.graph.internal_add_node(t, v, props)
    }

    fn internal_add_edge(
        &self,
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
        props: Vec<(usize, Prop)>,
        layer: usize,
    ) -> Result<MaybeNew<EID>, GraphError> {
        let eid = self
            .graph
            .internal_add_edge(t, src, dst, props.clone(), layer)?;
        if let MaybeNew::New(eid) = eid {
            self.proto_delta.lock().new_edge(src, dst, eid);
        }
        self.proto_delta
            .lock()
            .update_edge_tprops(eid.inner(), t, layer, props.into_iter());
        Ok(eid)
    }

    fn internal_add_edge_update(
        &self,
        t: TimeIndexEntry,
        edge: EID,
        props: Vec<(usize, Prop)>,
        layer: usize,
    ) -> Result<(), GraphError> {
        self.proto_delta.lock().update_edge_tprops(
            edge,
            t,
            layer,
            props.iter().map(|(id, prop)| (*id, prop)),
        );
        self.graph.internal_add_edge_update(t, edge, props, layer)
    }
}
