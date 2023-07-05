use crate::{
    core::{entities::{graph::tgraph::InnerTemporalGraph, vertices::vertex_ref::VertexRef}, utils::errors::GraphError},
    db::api::mutation::internal::InternalAdditionOps,
    prelude::Prop,
};

impl<const N: usize> InternalAdditionOps for InnerTemporalGraph<N> {
    fn internal_add_vertex(
        &self,
        t: i64,
        v: u64,
        name: Option<&str>,
        props: Vec<(String, Prop)>,
    ) -> Result<VertexRef, GraphError> {
        let v_id = self.add_vertex_internal(t, v, name, props)?;

        Ok(VertexRef::Local(v_id))
    }

    fn internal_add_edge(
        &self,
        t: i64,
        src: u64,
        dst: u64,
        props: Vec<(String, Prop)>,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        self.add_edge_internal(t, src, dst, props, layer)
    }
}
