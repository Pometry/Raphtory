use crate::db::view_api::internal::core_ops::InheritCoreOps;
use crate::db::view_api::internal::graph_ops::InheritGraphOps;
use crate::db::view_api::internal::time_semantics::{InheritTimeSemantics, TimeSemantics};
use crate::db::view_api::internal::{BoxableGraphView, CoreGraphOps};
use std::sync::Arc;

/// Trait for implementing all internal operations by wrapping another graph
pub trait WrappedGraph {
    type Internal: BoxableGraphView + ?Sized;

    fn graph(&self) -> &Self::Internal;
}

impl WrappedGraph for Arc<dyn BoxableGraphView> {
    type Internal = dyn BoxableGraphView;

    fn graph(&self) -> &Self::Internal {
        self.as_ref()
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
