use crate::{
    api::{edges::EdgeSegmentOps, graph_props::GraphPropSegmentOps, nodes::NodeSegmentOps},
    error::StorageError,
    persist::config::{BaseConfig, ConfigOps},
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

    fn new(config: Self::Config, graph_dir: Option<&Path>) -> Result<Self, StorageError>;

    fn load(graph_dir: &Path) -> Result<Self, StorageError>;

    fn load_with_config(graph_dir: &Path, config: Self::Config) -> Result<Self, StorageError> {
        let mut extension = Self::load(graph_dir)?;
        extension.config_mut().update(config);
        Ok(extension)
    }

    fn config(&self) -> &Self::Config;

    fn config_mut(&mut self) -> &mut Self::Config;

    fn wal(&self) -> &Self::Wal;

    /// Used by bulk loaders to trigger a flush of all segments
    /// (ignores memory limits and always triggers a flush if it is not already in progress)
    fn flush_node_segment<MP: DerefMut<Target = MemNodeSegment>>(
        &self,
        ns: &Self::NS,
        writer: MP,
        pause: bool,
    ) where
        Self: Sized;

    /// Called after every write and checks memory limits to decide if a flush is needed
    fn persist_node_segment<MP: DerefMut<Target = MemNodeSegment>>(
        &self,
        node_segment: &Self::NS,
        writer: MP,
    ) where
        Self: Sized;

    /// Used by bulk loaders to trigger a flush of all segments
    /// (ignores memory limits and always triggers a flush if it is not already in progress)
    fn flush_edge_segment<MP: DerefMut<Target = MemEdgeSegment>>(
        &self,
        edge_page: &Self::ES,
        writer: MP,
        pause: bool,
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
}

#[derive(Debug, Clone)]
pub struct NoOpStrategy {
    config: BaseConfig,
    memory_tracker: Arc<AtomicUsize>,
    wal: NoWal,
}

impl PersistenceStrategy for NoOpStrategy {
    type NS = NodeSegmentView<Self>;
    type ES = EdgeSegmentView<Self>;
    type GS = GraphPropSegmentView<Self>;
    type Wal = NoWal;
    type Config = BaseConfig;

    fn new(config: BaseConfig, _graph_dir: Option<&Path>) -> Result<Self, StorageError> {
        Ok(Self {
            config,
            memory_tracker: Arc::new(AtomicUsize::new(0)),
            wal: NoWal,
        })
    }

    fn load(_graph_dir: &Path) -> Result<Self, StorageError> {
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

    fn flush_node_segment<MP: DerefMut<Target = MemNodeSegment>>(&self, _ns: &Self::NS, _writer: MP, _pause: bool)
    where
        Self: Sized,
    {
    }

    fn persist_node_segment<MP: DerefMut<Target = MemNodeSegment>>(
        &self,
        _node_page: &Self::NS,
        _writer: MP,
    ) {
        // No operation
    }

    fn flush_edge_segment<MP: DerefMut<Target = MemEdgeSegment>>(
        &self,
        _edge_page: &Self::ES,
        _writer: MP,
         _pause: bool
    ) where
        Self: Sized,
    {
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
