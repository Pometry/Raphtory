use crate::{
    api::{edges::EdgeSegmentOps, graph_props::GraphPropSegmentOps, nodes::NodeSegmentOps},
    segments::{
        edge::segment::{EdgeSegmentView, MemEdgeSegment},
        graph_prop::{GraphPropSegmentView, segment::MemGraphPropSegment},
        node::segment::{MemNodeSegment, NodeSegmentView},
    },
    wal::{WalOps, no_wal::NoWal},
};
use crate::error::StorageError;
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, ops::DerefMut, path::Path, sync::Arc};

pub const DEFAULT_MAX_PAGE_LEN_NODES: u32 = 131_072; // 2^17
pub const DEFAULT_MAX_PAGE_LEN_EDGES: u32 = 1_048_576; // 2^20

pub trait PersistenceStrategy: Debug + Clone + Send + Sync + 'static {
    type NS: NodeSegmentOps;
    type ES: EdgeSegmentOps;
    type GS: GraphPropSegmentOps;
    type Wal: WalOps;
    type Config: ConfigOps;

    fn new(config: Self::Config, wal: Arc<Self::Wal>) -> Self;

    fn config(&self) -> &Self::Config;

    fn wal(&self) -> &Self::Wal;

    fn persist_node_segment<MP: DerefMut<Target = MemNodeSegment>>(
        &self,
        node_segment: &Self::NS,
        writer: MP,
    ) where
        Self: Sized;

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

    /// Indicate whether the strategy persists to disk or not.
    fn disk_storage_enabled() -> bool;
}

pub trait ConfigOps: Serialize + Deserialize<'static> {
    fn load_from_dir(dir: impl AsRef<Path>) -> Result<Self, StorageError>;

    fn save_to_dir(&self, dir: impl AsRef<Path>) -> Result<(), StorageError>;
}

#[derive(Debug, Clone)]
pub struct NoOpStrategy {
    config: NoOpConfig,
    wal: Arc<NoWal>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoOpConfig {
    pub max_node_page_len: u32,
    pub max_edge_page_len: u32,
}

impl NoOpConfig {
    pub fn new(max_node_page_len: u32, max_edge_page_len: u32) -> Self {
        Self { max_node_page_len, max_edge_page_len }
    }
}

impl Default for NoOpConfig {
    fn default() -> Self {
        Self {
            max_node_page_len: DEFAULT_MAX_PAGE_LEN_NODES,
            max_edge_page_len: DEFAULT_MAX_PAGE_LEN_EDGES,
        }
    }
}

impl ConfigOps for NoOpConfig {
    fn load_from_dir(_dir: impl AsRef<Path>) -> Result<Self, StorageError> {
        Ok(Self::default())
    }

    fn save_to_dir(&self, _dir: impl AsRef<Path>) -> Result<(), StorageError> {
        Ok(())
    }
}

impl PersistenceStrategy for NoOpStrategy {
    type ES = EdgeSegmentView<Self>;
    type NS = NodeSegmentView<Self>;
    type GS = GraphPropSegmentView<Self>;
    type Wal = NoWal;
    type Config = NoOpConfig;

    fn new(config: Self::Config, wal: Arc<Self::Wal>) -> Self {
        Self {
            config,
            wal,
        }
    }

    fn config(&self) -> &Self::Config {
        &self.config
    }

    fn wal(&self) -> &Self::Wal {
        &self.wal
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
}
