use crate::{
    core::{entities::nodes::node_ref::AsNodeRef, utils::errors::GraphError, Prop, PropType},
    db::api::{
        mutation::internal::InternalAdditionOps,
        view::{serialise::ProtoGraph, Base, BoxableGraphView, InheritViewOps},
    },
};
use raphtory_api::core::{
    entities::{EID, VID},
    storage::{dict_mapper::MaybeNew, timeindex::TimeIndexEntry},
};

pub struct CachedGraph<G, W> {
    graph: G,
    writer: W,
    proto_delta: ProtoGraph,
}

impl<G, W> Base for CachedGraph<G, W> {
    type Base = G;

    #[inline]
    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<W: Send + Sync, G: BoxableGraphView> InheritViewOps for CachedGraph<G, W> {}

impl<G: InternalAdditionOps, W> InternalAdditionOps for CachedGraph<G, W> {
    fn next_event_id(&self) -> Result<usize, GraphError> {
        todo!()
    }

    fn resolve_layer(&self, layer: Option<&str>) -> Result<MaybeNew<usize>, GraphError> {
        todo!()
    }

    fn resolve_node_type(&self, vid: VID, node_type: &str) -> Result<MaybeNew<usize>, GraphError> {
        todo!()
    }

    fn set_node_type(&self, v_id: VID, node_type: usize) -> Result<(), GraphError> {
        todo!()
    }

    fn resolve_node<V: AsNodeRef>(&self, id: V) -> Result<MaybeNew<VID>, GraphError> {
        todo!()
    }

    fn resolve_graph_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, GraphError> {
        todo!()
    }

    fn resolve_node_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, GraphError> {
        todo!()
    }

    fn resolve_edge_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<MaybeNew<usize>, GraphError> {
        todo!()
    }

    fn internal_add_node(
        &self,
        t: TimeIndexEntry,
        v: VID,
        props: Vec<(usize, Prop)>,
    ) -> Result<(), GraphError> {
        todo!()
    }

    fn internal_add_edge(
        &self,
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
        props: Vec<(usize, Prop)>,
        layer: usize,
    ) -> Result<EID, GraphError> {
        todo!()
    }

    fn internal_add_edge_update(
        &self,
        t: TimeIndexEntry,
        edge: EID,
        props: Vec<(usize, Prop)>,
        layer: usize,
    ) -> Result<(), GraphError> {
        todo!()
    }
}
