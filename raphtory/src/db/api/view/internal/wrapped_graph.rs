use crate::db::api::view::internal::{
    BoxableGraphView, InheritEdgeHistoryFilter, InheritNodeHistoryFilter, InheritViewOps,
};
use std::sync::Arc;

use crate::db::api::view::internal::InheritStorageOps;

impl InheritViewOps for Arc<dyn BoxableGraphView> {}

impl InheritStorageOps for Arc<dyn BoxableGraphView> {}

impl InheritNodeHistoryFilter for Arc<dyn BoxableGraphView> {}

impl InheritEdgeHistoryFilter for Arc<dyn BoxableGraphView> {}
