use crate::db::api::view::internal::{BoxableGraphView, InheritViewOps};
use std::sync::Arc;

impl<'graph> InheritViewOps for Arc<dyn BoxableGraphView<'graph>> {}
