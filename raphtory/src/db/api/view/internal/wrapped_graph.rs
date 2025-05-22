use crate::db::api::view::internal::{
    BoxableGraphView, InheritEdgeHistoryFilter, InheritNodeHistoryFilter, InheritViewOps,
};
use std::sync::Arc;

use crate::db::api::view::internal::InheritStorageOps;

impl<'graph> InheritViewOps for Arc<dyn BoxableGraphView + 'graph> {}

impl<'graph> InheritStorageOps for Arc<dyn BoxableGraphView + 'graph> {}

impl<'graph> InheritNodeHistoryFilter for Arc<dyn BoxableGraphView + 'graph> {}

impl<'graph> InheritEdgeHistoryFilter for Arc<dyn BoxableGraphView + 'graph> {}
