use db4_graph::WriteLockedGraph;
use raphtory_api::core::storage::graph_folder::GraphFolderError;
use storage::{Extension, error::StorageError};
use thiserror::Error;

/// Represents a temporary graph with pending writes.
pub struct StagedGraph<'a> {
    graph: WriteLockedGraph<'a, Extension>,
}

pub trait StagingOps {
    fn stage(&self) -> Result<StagedGraph<'_>, StagingError>;
}

impl<'a> StagedGraph<'a> {
    pub fn new(graph: WriteLockedGraph<'a, Extension>) -> Self {
        Self { graph }
    }

    pub fn commit(self) -> Result<(), StagingError> {
        todo!()
    }

    pub fn rollback(self) -> Result<(), StagingError> {
        todo!()
    }
}

#[derive(Debug, Error)]
pub enum StagingError {
    #[error("Graph directory is missing")]
    MissingGraphDir,

    #[error(transparent)]
    GraphFolder(#[from] GraphFolderError),

    #[error(transparent)]
    Storage(#[from] StorageError),
}
