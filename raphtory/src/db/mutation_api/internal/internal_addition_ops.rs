use crate::core::tgraph_shard::errors::GraphError;
use crate::core::Prop;

pub trait InternalAdditionOps {
    fn internal_add_vertex(
        &self,
        t: i64,
        v: u64,
        name: Option<&str>,
        props: Vec<(String, Prop)>,
    ) -> Result<(), GraphError>;

    fn internal_add_edge(
        &self,
        t: i64,
        src: u64,
        dst: u64,
        props: Vec<(String, Prop)>,
        layer: Option<&str>,
    ) -> Result<(), GraphError>;
}


pub trait InheritAdditionOps {
    type Internal: InternalAdditionOps + ?Sized;
    fn graph(&self) -> &Self::Internal;
}

impl<G: InheritAdditionOps> InternalAdditionOps for G {
    #[inline(always)]
    fn internal_add_vertex(&self, t: i64, v: u64, name: Option<&str>, props: Vec<(String, Prop)>) -> Result<(), GraphError> {
        self.graph().internal_add_vertex(t, v, name, props)
    }

    #[inline(always)]
    fn internal_add_edge(&self, t: i64, src: u64, dst: u64, props: Vec<(String, Prop)>, layer: Option<&str>) -> Result<(), GraphError> {
        self.graph().internal_add_edge(t, src, dst, props, layer)
    }
}