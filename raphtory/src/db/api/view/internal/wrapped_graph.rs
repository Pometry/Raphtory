use crate::db::api::properties::internal::InheritPropertiesOps;
use crate::db::api::view::internal::{Base, BoxableGraphView, InheritViewOps};
use std::sync::Arc;

impl InheritViewOps for Arc<dyn BoxableGraphView> {}
