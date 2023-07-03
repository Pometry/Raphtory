use crate::{
    core::{tgraph::graph::tgraph::InnerTemporalGraph, utils::errors::GraphError},
    db::api::mutation::internal::InternalPropertyAdditionOps,
    prelude::Prop,
};

impl<const N: usize> InternalPropertyAdditionOps for InnerTemporalGraph<N> {
    fn internal_add_vertex_properties(
        &self,
        v: u64,
        data: Vec<(String, Prop)>,
    ) -> Result<(), GraphError> {
        self.add_vertex_properties_internal(v, data)
    }

    fn internal_add_properties(
        &self,
        t: i64,
        props: Vec<(String, Prop)>,
    ) -> Result<(), GraphError> {
        self.add_property(t, props)
    }

    fn internal_add_static_properties(&self, props: Vec<(String, Prop)>) -> Result<(), GraphError> {
        self.add_static_property(props)
    }

    fn internal_add_edge_properties(
        &self,
        src: u64,
        dst: u64,
        props: Vec<(String, Prop)>,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        self.add_edge_properties_internal(src, dst, props, layer)
    }
}
