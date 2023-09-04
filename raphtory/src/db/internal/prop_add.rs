use crate::{
    core::{
        entities::{graph::tgraph::InnerTemporalGraph, vertices::vertex_ref::VertexRef, EID},
        storage::{lazy_vec::IllegalSet, timeindex::TimeIndexEntry},
        utils::errors::{GraphError, IllegalMutate},
    },
    db::api::mutation::internal::InternalPropertyAdditionOps,
    prelude::Prop,
};

impl<const N: usize> InternalPropertyAdditionOps for InnerTemporalGraph<N> {
    fn internal_add_vertex_properties(
        &self,
        v: u64,
        data: Vec<(String, Prop)>,
    ) -> Result<(), GraphError> {
        self.inner().add_vertex_properties_internal(v, data)
    }

    fn internal_add_properties(
        &self,
        t: TimeIndexEntry,
        props: Vec<(String, Prop)>,
    ) -> Result<(), GraphError> {
        self.inner().add_property(t, props)
    }

    fn internal_add_static_properties(&self, props: Vec<(String, Prop)>) -> Result<(), GraphError> {
        self.inner().add_static_property(props)
    }

    fn internal_add_edge_properties(
        &self,
        eid: EID,
        props: Vec<(String, Prop)>,
        layer: usize,
    ) -> Result<(), IllegalMutate> {
        self.inner().add_edge_properties_internal(eid, props, layer)
    }
}
