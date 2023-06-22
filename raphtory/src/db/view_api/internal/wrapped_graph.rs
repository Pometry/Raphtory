use crate::db::view_api::internal::{BoxableGraphView, InheritViewOps, Inheritable};
use std::sync::Arc;

impl Inheritable for Arc<dyn BoxableGraphView> {
    type Base = dyn BoxableGraphView;

    fn base(&self) -> &Self::Base {
        self.as_ref()
    }
}

impl InheritViewOps for Arc<dyn BoxableGraphView> {}
