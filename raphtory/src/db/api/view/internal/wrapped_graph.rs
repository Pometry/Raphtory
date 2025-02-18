use crate::db::api::view::internal::{
    BoxableGraphView, InheritEdgeHistoryFilter, InheritIndexSearch, InheritNodeHistoryFilter,
    InheritViewOps,
};
use std::sync::Arc;

impl InheritViewOps for Arc<dyn BoxableGraphView> {}

impl InheritIndexSearch for Arc<dyn BoxableGraphView> {}

impl InheritNodeHistoryFilter for Arc<dyn BoxableGraphView> {}

impl InheritEdgeHistoryFilter for Arc<dyn BoxableGraphView> {}
