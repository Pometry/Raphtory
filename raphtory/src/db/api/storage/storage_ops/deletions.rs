use crate::{core::utils::errors::GraphError, db::api::mutation::internal::InternalDeletionOps};

use super::GraphStorage;

impl InternalDeletionOps for GraphStorage {
    fn internal_delete_edge(
        &self,
        t: raphtory_api::core::storage::timeindex::TimeIndexEntry,
        src: raphtory_api::core::entities::VID,
        dst: raphtory_api::core::entities::VID,
        layer: usize,
    ) -> Result<(), GraphError> {
        match self {
            GraphStorage::Unlocked(storage) => storage.delete_edge(t, src, dst, layer),
            _ => Err(GraphError::AttemptToMutateImmutableGraph),
        }
    }
}
