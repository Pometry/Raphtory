use crate::db::view_api::internal::core_ops::{DelegateCoreOps, InheritCoreOps};
use crate::db::view_api::internal::graph_ops::{DelegateGraphOps, InheritGraphOps};
use crate::db::view_api::internal::time_semantics::{
    DelegateTimeSemantics, InheritTimeSemantics, TimeSemantics,
};
use crate::db::view_api::internal::{BoxableGraphView, InheritViewOps, Inheritable};
use std::sync::Arc;

impl Inheritable for Arc<dyn BoxableGraphView> {
    type Base = dyn BoxableGraphView;

    fn base(&self) -> &Self::Base {
        self.as_ref()
    }
}

impl InheritViewOps for Arc<dyn BoxableGraphView> {}
