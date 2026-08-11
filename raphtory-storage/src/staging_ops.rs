use db4_graph::WriteLockedGraph;
use raphtory_api::core::storage::graph_folder::{
    GraphFolder, GraphFolderError, WriteableGraphFolder,
};
use storage::{error::StorageError, Extension};
use thiserror::Error;

/// Represents a temporary graph with pending writes.
pub struct StagedGraph<'a> {
    graph: WriteLockedGraph<'a, Extension>,

    /// The original graph folder (points at the live `.raph` data).
    graph_folder: GraphFolder,

    /// The in-progress swap folder (points at the `.dirty` staging data).
    writeable_folder: WriteableGraphFolder,
}

pub trait StagingOps {
    fn stage(&self) -> Result<StagedGraph<'_>, StagingError>;
}

impl<'a> StagedGraph<'a> {
    pub fn new(
        graph: WriteLockedGraph<'a, Extension>,
        graph_folder: GraphFolder,
        writeable_folder: WriteableGraphFolder,
    ) -> Self {
        Self {
            graph,
            graph_folder,
            writeable_folder,
        }
    }

    pub fn commit(self) -> Result<(), StagingError> {
        // FIXME: Update metadata here.

        self.writeable_folder
            .finish()
            .map_err(StagingError::Commit)?;

        Ok(())
    }

    pub fn rollback(self) -> Result<(), StagingError> {
        todo!()
    }
}

#[derive(Debug, Error)]
pub enum StagingError {
    #[error("graph directory is missing")]
    MissingGraphDir,

    #[error("failed to initialise staging directory")]
    InitStagingDir(#[source] GraphFolderError),

    #[error("failed to commit staged graph")]
    Commit(#[source] GraphFolderError),

    #[error(transparent)]
    GraphFolder(#[from] GraphFolderError),

    #[error(transparent)]
    Storage(#[from] StorageError),
}
