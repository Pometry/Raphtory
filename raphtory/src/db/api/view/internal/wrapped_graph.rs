use crate::db::api::view::internal::{BoxableGraphView, InheritViewOps};
use std::sync::Arc;

impl InheritViewOps for Arc<dyn BoxableGraphView> {}
