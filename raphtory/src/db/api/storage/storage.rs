use crate::{
    db::api::view::{
        internal::{
            InheritEdgeHistoryFilter, InheritNodeHistoryFilter, InternalStorageOps, Static,
        },
        Base, InheritViewOps,
    },
    errors::GraphError,
};
use db4_graph::{TemporalGraph, WriteLockedGraph};
use raphtory_storage::{
    core_ops::InheritCoreGraphOps,
    graph::graph::{GraphStorage, Immutable},
    layer_ops::InheritLayerOps,
    mutation::{addition_ops::InternalAdditionOps, MutationError},
};
use std::{
    fmt::{Display, Formatter},
    path::Path,
    sync::Arc,
};
// Re-export for raphtory dependencies to use when creating graphs.
pub use storage::{
    error::StorageError,
    persist::{args::ArgsOps, config::ConfigOps, strategy::PersistenceStrategy},
    read_constant_graph_properties, Args, Config, Extension,
};

#[derive(Debug)]
pub struct Storage {
    graph: GraphStorage,
}

impl From<GraphStorage> for Storage {
    fn from(graph: GraphStorage) -> Self {
        Self { graph }
    }
}

impl InheritLayerOps for Storage {}
impl InheritCoreGraphOps for Storage {}

impl Display for Storage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.graph, f)
    }
}

impl Base for Storage {
    type Base = GraphStorage;

    #[inline]
    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl Storage {
    pub fn new_with_config(args: Args) -> Result<Self, GraphError> {
        let ext = Extension::new(None, args.into())?;
        let temporal_graph = TemporalGraph::new(ext)?;

        Ok(Self {
            graph: GraphStorage::Unlocked(Arc::new(temporal_graph)),
        })
    }

    /// Produce a read-only handle backed by the same `TemporalGraph` as
    /// this storage. Mutations on the returned `Storage` return
    /// `Immutable::ReadLockedImmutable`. The handle holds read locks on
    /// every segment, so writers on the original `Storage` will block
    /// until the read-only handle is dropped — this is not a snapshot.
    pub fn read_only(&self) -> Self {
        Self {
            graph: self.graph.lock(),
        }
    }
}

#[cfg(feature = "io")]
mod io {
    use super::*;
    use raphtory_storage::{graph::locked::ReadLockedGraph, recovery_ops::RecoveryOps};

    impl Storage {
        pub fn new_at_path_with_config(
            path: impl AsRef<Path>,
            args: Args,
        ) -> Result<Self, GraphError> {
            let path = path.as_ref();
            std::fs::create_dir_all(path)?;

            let config: Config = args.into();
            config.save_to_dir(path)?;

            let ext = Extension::new(Some(path), config)?;
            let temporal_graph = TemporalGraph::new_at_path_with_ext(path, ext)?;

            Ok(Self {
                graph: GraphStorage::Unlocked(Arc::new(temporal_graph)),
            })
        }

        fn load_with_extension(path: &Path, ext: Extension) -> Result<Self, GraphError> {
            let temporal_graph = TemporalGraph::load(path, ext)?;

            // Run crash recovery if needed.
            temporal_graph.run_recovery()?;

            Ok(Self {
                graph: GraphStorage::Unlocked(Arc::new(temporal_graph)),
            })
        }

        fn load_read_only_with_extension(path: &Path, ext: Extension) -> Result<Self, GraphError> {
            // Skip crash recovery: recovery mutates the graph and is unsafe
            // when other handles are attached to the same directory.
            let temporal_graph = TemporalGraph::load_read_only(path, ext)?;

            // `ReadLockedGraph` holds read locks on the underlying segments, so
            // every mutation entry point that goes through
            // `GraphStorage::mutable()?` errors with `ReadLockedImmutable`.
            let locked = ReadLockedGraph::new(Arc::new(temporal_graph));

            Ok(Self {
                graph: GraphStorage::Locked(locked),
            })
        }

        pub fn load(path: impl AsRef<Path>) -> Result<Self, GraphError> {
            let path = path.as_ref();
            let config = Config::load_from_dir(path)?;
            let ext = Extension::load(path, config)?;

            Self::load_with_extension(path, ext)
        }

        pub fn load_with_config(path: impl AsRef<Path>, args: Args) -> Result<Self, GraphError> {
            let path = path.as_ref();
            let config = Config::load_from_dir(path)?;
            let config = args.apply_to_config(config)?;
            config.save_to_dir(path)?;
            let ext = Extension::load(path, config)?;

            Self::load_with_extension(path, ext)
        }

        /// Load the graph as a read-only snapshot — multiple processes can open
        /// the same graph directory concurrently. Mutating operations on the
        /// returned graph will return errors from the underlying storage.
        pub fn load_read_only(path: impl AsRef<Path>) -> Result<Self, GraphError> {
            let path = path.as_ref();
            let config = Config::load_from_dir(path)?;
            let ext = Extension::load(path, config)?;

            Self::load_read_only_with_extension(path, ext)
        }

        /// Load the graph as a read-only snapshot — multiple processes can open
        /// the same graph directory concurrently. Mutating operations on the
        /// returned graph will return errors from the underlying storage.
        pub fn load_read_only_with_config(
            path: impl AsRef<Path>,
            args: Args,
        ) -> Result<Self, GraphError> {
            // NOTE: config is explicitly not saved to disk here.
            let path = path.as_ref();
            let config = Config::load_from_dir(path)?;
            let config = args.apply_to_config(config)?;
            let ext = Extension::load(path, config)?;

            Self::load_read_only_with_extension(path, ext)
        }
    }
}

impl InternalStorageOps for Storage {
    fn get_storage(&self) -> Option<&Storage> {
        Some(self)
    }

    fn disk_storage_path(&self) -> Option<&Path> {
        self.graph.disk_storage_path()
    }
}

impl InheritNodeHistoryFilter for Storage {}
impl InheritEdgeHistoryFilter for Storage {}

impl InheritViewOps for Storage {}

impl Static for Storage {}

impl InternalAdditionOps for Storage {
    fn write_lock(&self) -> Result<WriteLockedGraph<'_, Extension>, MutationError> {
        Ok(self.graph.write_lock()?)
    }

    fn mutable(&self) -> Result<&TemporalGraph, Immutable> {
        self.graph.mutable()
    }
}
