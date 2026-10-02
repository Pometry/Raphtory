use crate::graph::{
    graph::{GraphStorage, Immutable},
    locked::ReadLockedGraph,
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

/// Holds state for an ongoing `stage` call on a graph.
pub struct Handle {
    /// The underlying storage of the staged graph.
    storage: GraphStorage,

    /// The directory on disk that holds data for this stage.
    folder: WriteableGraphFolder,

    /// The graph being staged from.
    /// Read locks need to be held to prevent concurrent writes during a stage.
    _src_graph: ReadLockedGraph,

    src_folder: GraphFolder,

    /// Allows only one thread to stage `_src_graph` at a time.
    _guard: ArcMutexGuard<RawMutex, ()>,
}

impl Handle {
    pub fn new(
        storage: GraphStorage,
        folder: WriteableGraphFolder,
        src_graph: ReadLockedGraph,
        src_folder: GraphFolder,
        guard: ArcMutexGuard<RawMutex, ()>,
    ) -> Self {
        Self {
            storage,
            folder,
            _src_graph: src_graph,
            src_folder,
            _guard: guard,
        }
    }

    pub fn storage(&self) -> &GraphStorage {
        &self.storage
    }

    pub fn finish(self) -> Result<GraphStorage, StageError> {
        self.storage.flush()?;

        let src_meta = self.src_folder.read_metadata()?;

        let new_meta = Metadata {
            path: self.folder.relative_graph_path()?,
            meta: GraphMetadata {
                node_count: self.storage.unfiltered_num_nodes(&LayerIds::All),
                edge_count: self.storage.unfiltered_num_edges(&LayerIds::All),
                graph_type: src_meta.graph_type,
                is_diskgraph: src_meta.is_diskgraph,
            },
        };

        self.folder.write_metadata(new_meta)?;
        self.folder.finish().map_err(StageError::Finish)?;

        Ok(self.storage)
    }

    pub fn discard(self) -> Result<(), StageError> {
        self.folder.discard().map_err(StageError::Discard)
    }
}

impl GraphStorage {
    pub fn stage(&self) -> Result<Handle, StageError> {
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

        src_graph.stage(guard)
    }
}

impl ReadLockedGraph {
    fn stage(self, guard: ArcMutexGuard<RawMutex, ()>) -> Result<Handle, StageError> {
        let src_path = self.graph.graph_dir().ok_or(StageError::MissingGraphDir)?;

        let src_folder = GraphFolder::from_graph_path(src_path)?;
        let staged_folder = src_folder.clone().init_swap().map_err(StageError::Init)?;
        let staged_graph_path = staged_folder.graph_path().map_err(StageError::Init)?;

        // Copy existing flushed data to the staged graph to create a fork.
        self.graph.copy_to(&staged_graph_path)?;

        // Load a fresh extension so that the staged graph has its own WAL, control file, etc.
        let config = Config::load_from_dir(&staged_graph_path)?;
        let extension = Extension::load(&staged_graph_path, config)?;

        let temporal_graph = TemporalGraph::load(staged_graph_path, extension)?;
        let staged_graph = GraphStorage::from(temporal_graph);

        Ok(Handle::new(
            staged_graph,
            staged_folder,
            self,
            src_folder,
            guard,
        ))
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
