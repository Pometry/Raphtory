use db4_graph::WriteLockedGraph;
use raphtory_api::core::storage::graph_folder::{
    GraphFolder, GraphFolderError, WriteableGraphFolder,
};
use storage::{error::StorageError, Extension};
use thiserror::Error;

use crate::graph::graph::GraphStorage;

/// Represents a graph forked from an existing graph.
pub struct StagedGraph<'a> {
    graph: GraphStorage,

    folder: WriteableGraphFolder,

    live_graph: WriteLockedGraph<'a, Extension>,

    live_folder: GraphFolder,
}

pub trait StagingOps {
    fn stage(&self) -> Result<StagedGraph<'_>, StagingError>;
}

impl<'a> StagedGraph<'a> {
    pub fn new(
        graph: GraphStorage,
        folder: WriteableGraphFolder,
        live_graph: WriteLockedGraph<'a, Extension>,
        live_folder: GraphFolder,
    ) -> Self {
        Self {
            graph,
            folder,
            live_graph,
            live_folder,
        }
    }

    pub fn graph(&self) -> &GraphStorage {
        &self.graph
    }

    pub fn commit(self) -> Result<(), StagingError> {
        // FIXME: Update metadata here.

        self.folder.finish().map_err(StagingError::Commit)?;

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
