use crate::core::edge_ref::EdgeRef;
use crate::core::timeindex::TimeIndex;
use crate::core::tprop::TProp;
use crate::core::vertex_ref::{LocalVertexRef, VertexRef};
use crate::core::{Direction, Prop};
use crate::db::view_api::internal::core_ops::InheritCoreOps;
use crate::db::view_api::internal::graph_ops::InheritGraphOps;
use crate::db::view_api::internal::time_semantics::{InheritTimeSemantics, TimeSemantics};
use crate::db::view_api::internal::{CoreGraphOps, GraphViewInternalOps};
use crate::db::view_api::BoxedIter;
use std::ops::Range;

/// Trait for implementing all internal operations by wrapping another graph
pub trait WrappedGraph {
    type Internal: GraphViewInternalOps + Send + Sync + ?Sized;

    fn graph(&self) -> &Self::Internal;
}

impl<G: GraphViewInternalOps + Send + Sync> WrappedGraph for &G {
    type Internal = G;

    fn graph(&self) -> &Self::Internal {
        &self
    }
}

impl<G: WrappedGraph + Send + Sync> InheritGraphOps for G {
    type Internal = G::Internal;

    fn graph(&self) -> &Self::Internal {
        self.graph()
    }
}

impl<G: WrappedGraph<Internal = I>, I: TimeSemantics + ?Sized> InheritTimeSemantics for G {
    type Internal = G::Internal;

    fn graph(&self) -> &Self::Internal {
        self.graph()
    }
}

impl<G: WrappedGraph<Internal = I>, I: CoreGraphOps + ?Sized> InheritCoreOps for G {
    type Internal = G::Internal;

    fn graph(&self) -> &Self::Internal {
        self.graph()
    }
}
