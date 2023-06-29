use crate::core::errors::GraphError;
use crate::core::Prop;
use crate::db::view_api::internal::Base;

/// internal (dyn friendly) methods for adding properties
pub trait InternalPropertyAdditionOps {
    /// internal (dyn friendly)
    fn internal_add_vertex_properties(
        &self,
        v: u64,
        data: Vec<(String, Prop)>,
    ) -> Result<(), GraphError>;

    fn internal_add_properties(&self, t: i64, props: Vec<(String, Prop)>)
        -> Result<(), GraphError>;

    fn internal_add_static_properties(&self, props: Vec<(String, Prop)>) -> Result<(), GraphError>;

    fn internal_add_edge_properties(
        &self,
        src: u64,
        dst: u64,
        props: Vec<(String, Prop)>,
        layer: Option<&str>,
    ) -> Result<(), GraphError>;
}

pub trait InheritPropertyAdditionOps: Base {}

impl<G: InheritPropertyAdditionOps + ?Sized> DelegatePropertyAdditionOps for G
where
    <G as Base>::Base: InternalPropertyAdditionOps,
{
    type Internal = <G as Base>::Base;

    fn graph(&self) -> &Self::Internal {
        self.base()
    }
}

pub trait DelegatePropertyAdditionOps {
    type Internal: InternalPropertyAdditionOps + ?Sized;

    fn graph(&self) -> &Self::Internal;
}

impl<G: DelegatePropertyAdditionOps> InternalPropertyAdditionOps for G {
    #[inline(always)]
    fn internal_add_vertex_properties(
        &self,
        v: u64,
        data: Vec<(String, Prop)>,
    ) -> Result<(), GraphError> {
        self.graph().internal_add_vertex_properties(v, data)
    }

    #[inline(always)]
    fn internal_add_properties(
        &self,
        t: i64,
        props: Vec<(String, Prop)>,
    ) -> Result<(), GraphError> {
        self.graph().internal_add_properties(t, props)
    }

    #[inline(always)]
    fn internal_add_static_properties(&self, props: Vec<(String, Prop)>) -> Result<(), GraphError> {
        self.graph().internal_add_static_properties(props)
    }

    #[inline(always)]
    fn internal_add_edge_properties(
        &self,
        src: u64,
        dst: u64,
        props: Vec<(String, Prop)>,
        layer: Option<&str>,
    ) -> Result<(), GraphError> {
        self.graph()
            .internal_add_edge_properties(src, dst, props, layer)
    }
}
