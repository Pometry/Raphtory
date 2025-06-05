use crate::db::api::view::internal::{
    BoxableGraphView, InheritEdgeHistoryFilter, InheritNodeHistoryFilter, InheritStorageOps,
    InheritViewOps,
};
use std::sync::Arc;

impl<T: ?Sized + Send + Sync> InheritViewOps for Arc<T> {}

impl<T: BoxableGraphView + ?Sized> InheritStorageOps for Arc<T> {}

impl<T: BoxableGraphView + ?Sized> InheritNodeHistoryFilter for Arc<T> {}

impl<T: BoxableGraphView + ?Sized> InheritEdgeHistoryFilter for Arc<T> {}

#[cfg(feature = "proto")]
mod serialise {
    use crate::{
        errors::GraphError,
        serialise::{
            incremental::{GraphWriter, InternalCache},
            GraphFolder,
        },
    };
    use std::{ops::Deref, sync::Arc};

    impl<T: InternalCache> InternalCache for Arc<T> {
        fn init_cache(&self, path: &GraphFolder) -> Result<(), GraphError> {
            self.deref().init_cache(path)
        }

        fn get_cache(&self) -> Option<&GraphWriter> {
            self.deref().get_cache()
        }
    }
}
