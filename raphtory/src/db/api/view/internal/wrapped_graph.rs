use crate::db::api::view::internal::{BoxableGraphView, InheritViewOps};
use std::sync::Arc;

impl<'graph> InheritViewOps<'graph> for Arc<dyn BoxableGraphView<'graph>> {}
