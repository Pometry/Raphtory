use crate::db::api::view::internal::{
    BoxableGraphView, InheritEdgeHistoryFilter, InheritNodeHistoryFilter, InheritStorageOps,
    InheritViewOps,
};
use std::sync::Arc;

impl<T: ?Sized + Send + Sync> InheritViewOps for Arc<T> {}

impl<T: BoxableGraphView + ?Sized> InheritStorageOps for Arc<T> {}

impl<T: BoxableGraphView + ?Sized> InheritNodeHistoryFilter for Arc<T> {}

impl<T: BoxableGraphView + ?Sized> InheritEdgeHistoryFilter for Arc<T> {}
