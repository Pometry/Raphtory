use std::ops::DerefMut;
use std::fmt::Debug;
use std::path::Path;
use std::sync::Arc;
use serde::{Deserialize, Serialize};
use crate::error::StorageError;
use crate::segments::{
    edge::segment::{EdgeSegmentView, MemEdgeSegment},
    graph_prop::{GraphPropSegmentView, segment::MemGraphPropSegment},
    node::segment::{MemNodeSegment, NodeSegmentView},
};
use crate::wal::no_wal::NoWal;
use crate::wal::Wal;

pub const DEFAULT_MAX_PAGE_LEN_NODES: u32 = 131_072; // 2^17
pub const DEFAULT_MAX_PAGE_LEN_EDGES: u32 = 1_048_576; // 2^20
pub const DEFAULT_MAX_MEMORY_BYTES: usize = 32 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistenceConfig {
    pub max_node_page_len: u32,
    pub max_edge_page_len: u32,
    pub max_memory_bytes: usize,
    pub bg_flush_enabled: bool,
    pub node_types: Vec<String>,
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            max_node_page_len: DEFAULT_MAX_PAGE_LEN_NODES,
            max_edge_page_len: DEFAULT_MAX_PAGE_LEN_EDGES,
            max_memory_bytes: DEFAULT_MAX_MEMORY_BYTES,
            bg_flush_enabled: true,
            node_types: Vec::new(),
        }
    }
}

impl PersistenceConfig {
    const CONFIG_FILE: &str = "persistence_config.json";

    pub fn load_from_dir(dir: impl AsRef<Path>) -> Result<Self, StorageError> {
        let config_file = dir.as_ref().join(Self::CONFIG_FILE);
        let config_file = std::fs::File::open(config_file)?;
        let config = serde_json::from_reader(config_file)?;
        Ok(config)
    }

    pub fn save_to_dir(&self, dir: impl AsRef<Path>) -> Result<(), StorageError> {
        let config_file = dir.as_ref().join(Self::CONFIG_FILE);
        let config_file = std::fs::File::create(&config_file)?;
        serde_json::to_writer_pretty(config_file, self)?;
        Ok(())
    }

    pub fn new_with_memory(max_memory_bytes: usize) -> Self {
        Self {
            max_memory_bytes,
            ..Default::default()
        }
    }

    pub fn new_with_page_lens(
        max_memory_bytes: usize,
        max_node_page_len: u32,
        max_edge_page_len: u32,
    ) -> Self {
        Self {
            max_memory_bytes,
            max_node_page_len,
            max_edge_page_len,
            ..Default::default()
        }
    }

    pub fn with_bg_flush(mut self) -> Self {
        self.bg_flush_enabled = true;
        self
    }

    pub fn node_types(&self) -> &[String] {
        &self.node_types
    }

    pub fn set_node_types(&mut self, types: impl IntoIterator<Item = impl AsRef<str>>) {
        self.node_types = types
            .into_iter()
            .map(|s| s.as_ref().to_string())
            .collect();
    }
}

pub trait PersistenceStrategy: Debug + Clone + Send + Sync + 'static {
    type NS;
    type ES;
    type GS;
    type WalType: Wal;

    fn new(config: PersistenceConfig, wal: Arc<Self::WalType>) -> Self;

    fn config(&self) -> &PersistenceConfig;

    // Need this to set node_types.
    // TODO: Remove this once we have a better way to set node_types.
    fn config_mut(&mut self) -> &mut PersistenceConfig;

    fn wal(&self) -> &Self::WalType;

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

#[derive(Debug, Clone)]
pub struct NoOpStrategy {
    config: PersistenceConfig,
    wal: Arc<NoWal>,
}


impl PersistenceStrategy for NoOpStrategy {
    type ES = EdgeSegmentView<Self>;
    type NS = NodeSegmentView<Self>;
    type GS = GraphPropSegmentView<Self>;
    type WalType = NoWal;

    fn new(config: PersistenceConfig, wal: Arc<Self::WalType>) -> Self {
        Self { config, wal }
    }

    fn config(&self) -> &PersistenceConfig {
        &self.config
    }

    fn config_mut(&mut self) -> &mut PersistenceConfig {
        &mut self.config
    }

    fn wal(&self) -> &Self::WalType {
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
