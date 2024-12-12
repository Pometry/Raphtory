use crate::db::api::view::internal::{BoxableGraphView, InheritIndexSearch, InheritViewOps};
use std::sync::Arc;

impl InheritViewOps for Arc<dyn BoxableGraphView> {}

impl InheritIndexSearch for Arc<dyn BoxableGraphView> {}
