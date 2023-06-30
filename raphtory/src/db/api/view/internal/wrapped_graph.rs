use crate::db::api::view::internal::{Base, BoxableGraphView, InheritViewOps};
use std::sync::Arc;

impl Base for Arc<dyn BoxableGraphView> {
    type Base = dyn BoxableGraphView;

    fn base(&self) -> &Self::Base {
        self.as_ref()
    }
}

impl InheritViewOps for Arc<dyn BoxableGraphView> {}
