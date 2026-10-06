use crate::{
    core_ops::CoreGraphOps,
    graph::{
        graph::{GraphStorage, Immutable},
        locked::ReadLockedGraph,
    },
};
use db4_graph::TemporalGraph;
use parking_lot::{lock_api::ArcMutexGuard, RawMutex};
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

/// Wrapper around a graph that can buffer writes and apply them as an atomic operation.
pub struct Stage<G> {
    /// The staged graph that can accept writes.
    graph: G,

    /// The directory on disk that holds data for this stage.
    folder: WriteableGraphFolder,

    /// The graph being staged from.
    /// Read locks need to be held to prevent concurrent writes while staging.
    _src_graph: ReadLockedGraph,

    src_folder: GraphFolder,

    /// Allows only one thread to stage `_src_graph` at a time.
    _guard: ArcMutexGuard<RawMutex, ()>,
}

impl<G: CoreGraphOps + From<GraphStorage>> Stage<G> {
    pub fn new(src: &G) -> Result<Self, StageError> {
        src.core_graph().stage()
    }

    pub fn graph(&self) -> &G {
        &self.graph
    }

    /// Finalise all writes and promote the staged graph as the new primary graph.
    pub fn finish(self) -> Result<G, StageError> {
        let storage = self.graph.core_graph();
        storage.flush()?;

        let src_meta = self.src_folder.read_metadata()?;

        let new_meta = Metadata {
            path: self.folder.relative_graph_path()?,
            meta: GraphMetadata {
                node_count: storage.unfiltered_num_nodes(&LayerIds::All),
                edge_count: storage.unfiltered_num_edges(&LayerIds::All),
                graph_type: src_meta.graph_type,
                is_diskgraph: src_meta.is_diskgraph,
            },
        };

        self.folder.write_metadata(new_meta)?;
        let cleanup_old = false; // Keep the previous data folders around as archives.
        self.folder
            .finish(cleanup_old)
            .map_err(StageError::Finish)?;

        Ok(self.graph)
    }

    /// Abandon this stage and cleanup its files on disk.
    pub fn discard(self) -> Result<(), StageError> {
        // Drop graph before removing files on disk to prevent dangling references.
        drop(self.graph);
        self.folder.discard().map_err(StageError::Discard)
    }
}

impl GraphStorage {
    pub(crate) fn stage<G: From<GraphStorage>>(&self) -> Result<Stage<G>, StageError> {
        let (src_graph, guard) = match self {
            GraphStorage::Unlocked(graph) => {
                let guard = graph.try_stage_guard().ok_or(StageError::InProgress)?;

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
                (ReadLockedGraph::new(graph.clone()), guard)
            }
            GraphStorage::Locked(locked_graph) => {
                let guard = locked_graph
                    .graph
                    .try_stage_guard()
                    .ok_or(StageError::InProgress)?;

                // Callers need to call flush themselves before staging a locked graph.
                if locked_graph.graph.is_dirty() {
                    return Err(StageError::DirtyGraph);
                }

                (locked_graph.clone(), guard)
            }
        };

        let src_path = src_graph.graph.graph_dir().ok_or(StageError::MissingGraphDir)?;
        let src_folder = GraphFolder::from_graph_path(src_path)?;
        let staged_folder = src_folder.clone().init_swap().map_err(StageError::Init)?;
        let staged_graph_path = staged_folder.graph_path().map_err(StageError::Init)?;

        // Copy existing flushed data to the staged graph to create a fork.
        src_graph.graph.copy_to(&staged_graph_path)?;

        // Load a fresh extension so that the staged graph has its own WAL, control file, etc.
        let config = Config::load_from_dir(&staged_graph_path)?;
        let extension = Extension::load(&staged_graph_path, config)?;

        let temporal_graph = TemporalGraph::load(staged_graph_path, extension)?;
        let graph = G::from(GraphStorage::from(temporal_graph));

        Ok(Stage {
            graph,
            folder: staged_folder,
            _src_graph: src_graph,
            src_folder,
            _guard: guard,
        })
    }
}

#[derive(Debug, Error)]
pub enum StageError {
    #[error(transparent)]
    GraphFolder(#[from] GraphFolderError),

    #[error(transparent)]
    Storage(#[from] StorageError),

    #[error(transparent)]
    Immutable(#[from] Immutable),

    #[error("graph directory is missing")]
    MissingGraphDir,

    #[error("graph is dirty, call flush() before stage()")]
    DirtyGraph,

    #[error("stage already in progress")]
    InProgress,

    #[error("failed to initialise stage")]
    Init(#[source] GraphFolderError),

    #[error("failed to finish stage")]
    Finish(#[source] GraphFolderError),

    #[error("failed to discard stage")]
    Discard(#[source] GraphFolderError),
}
