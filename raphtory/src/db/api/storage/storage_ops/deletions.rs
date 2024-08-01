use raphtory_api::core::{entities::VID, storage::timeindex::TimeIndexEntry};

use crate::{core::utils::errors::GraphError, db::api::mutation::internal::InternalDeletionOps};

use super::GraphStorage;

impl InternalDeletionOps for GraphStorage {
    fn internal_delete_edge(
        &self,
        t: TimeIndexEntry,
        src: VID,
        dst: VID,
        layer: usize,
    ) -> Result<(), GraphError> {
        match self {
            GraphStorage::Unlocked(storage) => storage.delete_edge(t, src, dst, layer),
            _ => Err(GraphError::AttemptToMutateImmutableGraph),
        }
    }
}
