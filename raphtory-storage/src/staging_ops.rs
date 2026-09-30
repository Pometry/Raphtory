use crate::graph::{
    graph::{GraphStorage, Immutable},
    locked::ReadLockedGraph,
};
use db4_graph::TemporalGraph;
use raphtory_api::core::storage::graph_folder::{
    GraphFolder, GraphFolderError, GraphPaths, WriteableGraphFolder,
};
use storage::{
    error::StorageError,
    persist::{config::ConfigOps, control_file::ControlFileOps, strategy::PersistenceStrategy},
    wal::{GraphWalOps, WalOps},
    Config, Extension,
};
use thiserror::Error;

/// Isolated fork of a graph used for batching writes before an atomic commit.
pub struct StagedGraph {
    graph: GraphStorage,

    folder: WriteableGraphFolder,

    src_graph: ReadLockedGraph,

    src_folder: GraphFolder,
}

impl StagedGraph {
    pub fn new(
        graph: GraphStorage,
        folder: WriteableGraphFolder,
        src_graph: ReadLockedGraph,
        src_folder: GraphFolder,
    ) -> Self {
        Self {
            graph,
            folder,
            src_graph,
            src_folder,
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

impl GraphStorage {
    pub fn stage(&self) -> Result<StagedGraph, StagingError> {
        let src_graph = match self {
            GraphStorage::Unlocked(graph) => {
                // Unlocked graphs may have pending writes that need to be flushed to disk.
                let mut write_locked_graph = graph.write_locked_graph();
                write_locked_graph.flush()?;

                // Since the graph is fully flushed to disk, we can safely log a checkpoint.
                // Nothing to redo prior to this checkpoint since everything is on disk.
                let redo_lsn = None;
                let wal = graph.extension().wal();
                let checkpoint_lsn = wal.log_checkpoint(redo_lsn)?;
                wal.flush(checkpoint_lsn)?;

                let control_file = graph.extension().control_file();
                control_file.set_checkpoint(checkpoint_lsn);
                control_file.save()?;

                // TODO: Between dropping the write locks and acquiring the read locks,
                // another write could mutate the graph. Implement and use atomic lock
                // downgrading to prevent this.
                drop(write_locked_graph);
                ReadLockedGraph::new(graph.clone())
            }
            GraphStorage::Mem(locked_graph) => {
                // Callers need to call flush themselves before staging locked graphs.
                if locked_graph.graph.is_dirty() {
                    return Err(StagingError::DirtyGraph);
                }

                locked_graph.clone()
            }
        };

        let src_path = src_graph
            .graph
            .graph_dir()
            .ok_or(StagingError::MissingGraphDir)?;

        let src_folder = GraphFolder::from_graph_path(src_path)?;

        let staged_folder = src_folder
            .clone()
            .init_swap()
            .map_err(StagingError::InitStagingDir)?;

        let staged_path = staged_folder
            .graph_path()
            .map_err(StagingError::InitStagingDir)?;

        // Copy existing flushed data to the staged graph to create a fork.
        src_graph.graph.copy_to(&staged_path)?;

        // Load a fresh extension so that the staged graph has its own WAL, control file, etc.
        let config = Config::load_from_dir(&staged_path)?;
        let extension = Extension::load(&staged_path, config)?;

        let temporal_graph = TemporalGraph::load(staged_path, extension)?;
        let staged_graph = GraphStorage::from(temporal_graph);

        Ok(StagedGraph::new(
            staged_graph,
            staged_folder,
            src_graph,
            src_folder,
        ))
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

    #[error(transparent)]
    Immutable(#[from] Immutable),

    #[error("graph is dirty, call flush() before staging")]
    DirtyGraph,
}
