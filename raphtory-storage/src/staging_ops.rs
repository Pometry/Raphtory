use db4_graph::WriteLockedGraph;
use raphtory_api::core::storage::graph_folder::{
    GraphFolder, GraphFolderError, WriteableGraphFolder,
};
use storage::{error::StorageError, Extension};
use thiserror::Error;

use crate::graph::graph::GraphStorage;

/// Represents a graph forked from an existing graph.
pub struct StagedGraph<'a> {
    staged_graph: GraphStorage,

    staged_folder: WriteableGraphFolder,

    live_graph: WriteLockedGraph<'a, Extension>,

    live_folder: GraphFolder,
}

pub trait StagingOps {
    fn stage(&self) -> Result<StagedGraph<'_>, StagingError>;
}

impl<'a> StagedGraph<'a> {
    pub fn new(
        staged_graph: GraphStorage,
        staged_folder: WriteableGraphFolder,
        live_graph: WriteLockedGraph<'a, Extension>,
        live_folder: GraphFolder,
    ) -> Self {
        Self {
            staged_graph,
            staged_folder,
            live_graph,
            live_folder,
        }
    }

    pub fn commit(self) -> Result<(), StagingError> {
        // FIXME: Update metadata here.

        self.staged_folder.finish().map_err(StagingError::Commit)?;

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
