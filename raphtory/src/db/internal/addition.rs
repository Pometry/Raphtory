use crate::{
    core::{
        entities::{graph::tgraph::InnerTemporalGraph, EID, VID},
        storage::timeindex::TimeIndexEntry,
        utils::errors::GraphError,
    },
    db::api::mutation::internal::InternalAdditionOps,
    prelude::Prop,
};
use std::sync::atomic::Ordering;

impl<const N: usize> InternalAdditionOps for InnerTemporalGraph<N> {
    fn next_event_id(&self) -> usize {
        self.inner().event_counter.fetch_add(1, Ordering::Relaxed)
    }

    fn internal_add_vertex(
        &self,
        t: TimeIndexEntry,
        v: u64,
        name: Option<&str>,
        props: Vec<(String, Prop)>,
    ) -> Result<VID, GraphError> {
        self.inner().add_vertex_internal(t, v, name, props)
    }

    fn internal_add_edge(
        &self,
        t: TimeIndexEntry,
        src: u64,
        dst: u64,
        props: Vec<(String, Prop)>,
        layer: Option<&str>,
    ) -> Result<EID, GraphError> {
        self.inner().add_edge_internal(t, src, dst, props, layer)
    }
}
