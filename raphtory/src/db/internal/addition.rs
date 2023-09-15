use crate::{
    core::{
        entities::{graph::tgraph::InnerTemporalGraph, EID, VID},
        storage::timeindex::TimeIndexEntry,
        utils::errors::GraphError,
        PropType,
    },
    db::api::mutation::internal::InternalAdditionOps,
    prelude::Prop,
};
use std::sync::atomic::Ordering;

impl<const N: usize> InternalAdditionOps for InnerTemporalGraph<N> {
    #[inline]
    fn next_event_id(&self) -> usize {
        self.inner().event_counter.fetch_add(1, Ordering::Relaxed)
    }

    #[inline]
    fn resolve_layer(&self, layer: Option<&str>) -> usize {
        layer
            .map(|name| self.inner().edge_meta.get_or_create_layer_id(name))
            .unwrap_or(0)
    }

    #[inline]
    fn resolve_vertex(&self, id: u64, name: Option<&str>) -> VID {
        self.inner().resolve_vertex(id, name)
    }

    #[inline]
    fn resolve_graph_property(&self, prop: &str, is_static: bool) -> usize {
        self.inner().graph_props.resolve_property(prop, is_static)
    }

    #[inline]
    fn resolve_vertex_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<usize, GraphError> {
        self.inner()
            .vertex_meta
            .resolve_prop_id(prop, dtype, is_static)
    }

    #[inline]
    fn resolve_edge_property(
        &self,
        prop: &str,
        dtype: PropType,
        is_static: bool,
    ) -> Result<usize, GraphError> {
        self.inner()
            .edge_meta
            .resolve_prop_id(prop, dtype, is_static)
    }

    #[inline]
    fn internal_add_vertex(
        &self,
        t: TimeIndexEntry,
        v: VID,
        props: Vec<(usize, Prop)>,
    ) -> Result<(), GraphError> {
        self.inner().add_vertex_internal(t, v, props)
    }

    #[inline]
    fn internal_add_edge(
        &self,
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
        props: Vec<(usize, Prop)>,
        layer: usize,
    ) -> Result<EID, GraphError> {
        self.inner().add_edge_internal(t, src, dst, props, layer)
    }
}
