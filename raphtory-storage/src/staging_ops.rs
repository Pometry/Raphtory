use crate::graph::{
    graph::{GraphStorage, Immutable},
    locked::ReadLockedGraph,
};
use db4_graph::TemporalGraph;
use raphtory_api::core::storage::graph_folder::{
    GraphFolder, GraphFolderError, GraphMetadata, GraphPaths, Metadata, WriteableGraphFolder,
};
use raphtory_core::entities::LayerIds;
use storage::{
    error::StorageError,
    persist::{config::ConfigOps, control_file::ControlFileOps, strategy::PersistenceStrategy},
    wal::{GraphWalOps, WalOps},
    Config, Extension,
};
use thiserror::Error;

/// Isolated fork of a graph used for staging writes before atomically
/// applying them to the source graph.
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

    pub fn finish(self) -> Result<(), StagingError> {
        self.graph.flush()?;

        let src_meta = self.src_folder.read_metadata()?;

        let new_meta = Metadata {
            path: self.folder.relative_graph_path()?,
            meta: GraphMetadata {
                node_count: self.graph.unfiltered_num_nodes(&LayerIds::All),
                edge_count: self.graph.unfiltered_num_edges(&LayerIds::All),
                graph_type: src_meta.graph_type,
                is_diskgraph: src_meta.is_diskgraph,
            },
        };

        self.folder.write_metadata(new_meta)?;
        self.folder.finish().map_err(StagingError::Finish)?;

        Ok(())
    }

    pub fn discard(self) -> Result<(), StagingError> {
        self.folder.discard().map_err(StagingError::Discard)
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
                // Callers need to call flush themselves before staging a ReadLockedGraph.
                if locked_graph.graph.is_dirty() {
                    return Err(StagingError::DirtyGraph);
                }

                locked_graph.clone()
            }
        };

        src_graph.stage()
    }
}

impl ReadLockedGraph {
    fn stage(&self) -> Result<StagedGraph, StagingError> {
        let src_path = self
            .graph
            .graph_dir()
            .ok_or(StagingError::MissingGraphDir)?;

        let src_folder = GraphFolder::from_graph_path(src_path)?;
        let staged_folder = src_folder.clone().init_swap().map_err(StagingError::Init)?;
        let staged_graph_path = staged_folder.graph_path().map_err(StagingError::Init)?;

        // Copy existing flushed data to the staged graph to create a fork.
        self.graph.copy_to(&staged_graph_path)?;

        // Load a fresh extension so that the staged graph has its own WAL, control file, etc.
        let config = Config::load_from_dir(&staged_graph_path)?;
        let extension = Extension::load(&staged_graph_path, config)?;

        let temporal_graph = TemporalGraph::load(staged_graph_path, extension)?;
        let staged_graph = GraphStorage::from(temporal_graph);

        Ok(StagedGraph::new(
            staged_graph,
            staged_folder,
            self.clone(),
            src_folder,
        ))
    }
}

#[derive(Debug, Error)]
pub enum StagingError {
    #[error(transparent)]
    GraphFolder(#[from] GraphFolderError),

    #[error(transparent)]
    Storage(#[from] StorageError),

    #[error(transparent)]
    Immutable(#[from] Immutable),

    #[error("graph directory is missing")]
    MissingGraphDir,

    #[error("graph is dirty, call flush() before staging")]
    DirtyGraph,

    #[error("failed to initialise staging")]
    Init(#[source] GraphFolderError),

    #[error("failed to finish staging")]
    Finish(#[source] GraphFolderError),

    #[error("failed to discard staging")]
    Discard(#[source] GraphFolderError),
}
