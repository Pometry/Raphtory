use crate::{
    api::{
        edges::EdgeSegmentOps,
        graph_props::GraphPropSegmentOps,
        nodes::{
            GlobalPropCandidates, NodeSegmentOps, PropPredicate, PropSemantics, SelectedProps,
        },
    },
    error::StorageError,
    persist::{
        config::{BaseConfig, ConfigOps},
        control_file::{ControlFileOps, NoControlFile},
    },
    segments::{
        edge::segment::{EdgeSegmentView, MemEdgeSegment},
        graph_prop::{GraphPropSegmentView, segment::MemGraphPropSegment},
        node::segment::{MemNodeSegment, NodeSegmentView},
    },
    wal::{GraphWalOps, WalOps, no_wal::NoWal},
};
use std::{
    fmt::Debug,
    ops::DerefMut,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

pub trait PersistenceStrategy: Debug + Clone + Send + Sync + 'static {
    type NS: NodeSegmentOps;
    type ES: EdgeSegmentOps;
    type GS: GraphPropSegmentOps;
    type Wal: WalOps + GraphWalOps;
    type Config: ConfigOps;
    type ControlFile: ControlFileOps;

    fn new(config: Self::Config, graph_dir: Option<&Path>) -> Result<Self, StorageError>;

    fn load(graph_dir: &Path) -> Result<Self, StorageError>;

    fn load_with_config(graph_dir: &Path, config: Self::Config) -> Result<Self, StorageError>;

    fn config(&self) -> &Self::Config;

    fn config_mut(&mut self) -> &mut Self::Config;

    fn wal(&self) -> &Self::Wal;

    fn control_file(&self) -> &Self::ControlFile;

    /// Called after every write and checks memory limits to decide if a flush is needed
    fn persist_node_segment<MP: DerefMut<Target = MemNodeSegment>>(
        &self,
        node_segment: &Self::NS,
        writer: MP,
    ) where
        Self: Sized;

    /// Called after every write and checks memory limits to decide if a flush is needed
    fn persist_edge_segment<MP: DerefMut<Target = MemEdgeSegment>>(
        &self,
        edge_segment: &Self::ES,
        writer: MP,
    ) where
        Self: Sized;

    fn persist_graph_prop_segment<MP: DerefMut<Target = MemGraphPropSegment>>(
        &self,
        graph_prop_segment: &Self::GS,
        writer: MP,
    ) where
        Self: Sized;

    /// Indicates whether the strategy persists to disk or not.
    fn disk_storage_enabled() -> bool;

    /// Estimated global memory used
    fn memory_tracker(&self) -> &Arc<AtomicUsize>;

    fn estimated_size(&self) -> usize {
        self.memory_tracker().load(Ordering::Relaxed)
    }

    /// Called by bulk loaders to decide if a global flush should be triggered
    fn should_flush(&self) -> bool;
    fn should_pause(&self) -> bool;

    /// Resolve a node property predicate to a global candidate superset using
    /// secondary indexes, if this strategy maintains them. `metadata` selects
    /// the metadata prop-id space over the temporal one; `max_segment_len` is
    /// the VID stride. `None` means the predicate cannot be served and the
    /// caller should scan as usual. Candidates may include non-matching nodes
    /// — callers must verify every candidate.
    fn node_prop_candidates<'a>(
        &self,
        _segments: impl Iterator<Item = &'a Self::NS>,
        _max_segment_len: u32,
        _prop_id: usize,
        _metadata: bool,
        _predicate: &PropPredicate,
        _semantics: PropSemantics,
    ) -> Option<GlobalPropCandidates>
    where
        Self: Sized,
        Self::NS: 'a,
    {
        None
    }

    /// Rebuild the secondary property indexes over `selected`. A no-op for
    /// strategies without index support.
    fn build_node_prop_index<'a>(
        &self,
        _segments: impl Iterator<Item = &'a Self::NS> + Send,
        _max_segment_len: u32,
        _selected: &SelectedProps,
    ) -> Result<(), StorageError>
    where
        Self: Sized,
        Self::NS: 'a,
    {
        Ok(())
    }

    /// Replace the persisted set of node property names that index builds
    /// consider; `None` restores "every indexable property". Takes effect at
    /// the next build, and survives reopening the graph.
    fn set_indexed_node_props(&self, _names: Option<Vec<String>>) -> Result<(), StorageError> {
        Ok(())
    }

    /// The persisted set of node property names index builds consider, or
    /// `None` when every indexable property is considered.
    fn indexed_node_props(&self) -> Option<Vec<String>> {
        None
    }
}

#[derive(Debug, Clone)]
pub struct NoOpStrategy {
    config: BaseConfig,
    memory_tracker: Arc<AtomicUsize>,
    wal: NoWal,
    control_file: NoControlFile,
}

impl PersistenceStrategy for NoOpStrategy {
    type NS = NodeSegmentView<Self>;
    type ES = EdgeSegmentView<Self>;
    type GS = GraphPropSegmentView<Self>;
    type Wal = NoWal;
    type Config = BaseConfig;
    type ControlFile = NoControlFile;

    fn new(config: BaseConfig, _graph_dir: Option<&Path>) -> Result<Self, StorageError> {
        Ok(Self {
            config,
            wal: NoWal,
            control_file: NoControlFile,
            memory_tracker: Arc::new(AtomicUsize::new(0)),
        })
    }

    fn load(_graph_dir: &Path) -> Result<Self, StorageError> {
        Err(StorageError::DiskStorageNotSupported)
    }

    fn load_with_config(_graph_dir: &Path, _config: Self::Config) -> Result<Self, StorageError> {
        Err(StorageError::DiskStorageNotSupported)
    }

    fn config(&self) -> &Self::Config {
        &self.config
    }

    fn config_mut(&mut self) -> &mut Self::Config {
        &mut self.config
    }

    fn wal(&self) -> &Self::Wal {
        &self.wal
    }

    fn control_file(&self) -> &Self::ControlFile {
        &self.control_file
    }

    fn persist_node_segment<MP: DerefMut<Target = MemNodeSegment>>(
        &self,
        _node_page: &Self::NS,
        _writer: MP,
    ) {
        // No operation
    }

    fn persist_edge_segment<MP: DerefMut<Target = MemEdgeSegment>>(
        &self,
        _edge_page: &Self::ES,
        _writer: MP,
    ) {
        // No operation
    }

    fn persist_graph_prop_segment<MP: DerefMut<Target = MemGraphPropSegment>>(
        &self,
        _graph_segment: &Self::GS,
        _writer: MP,
    ) {
        // No operation
    }

    fn disk_storage_enabled() -> bool {
        false
    }

    fn memory_tracker(&self) -> &Arc<AtomicUsize> {
        &self.memory_tracker
    }

    fn should_flush(&self) -> bool {
        false
    }

    fn should_pause(&self) -> bool {
        false
    }
}
