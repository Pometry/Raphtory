use std::ops::DerefMut;
use std::fmt::Debug;

use serde::{Deserialize, Serialize};

use crate::segments::{
    edge::segment::{EdgeSegmentView, MemEdgeSegment},
    graph_prop::{GraphPropSegmentView, segment::MemGraphPropSegment},
    node::segment::{MemNodeSegment, NodeSegmentView},
};

pub const DEFAULT_MAX_PAGE_LEN_NODES: u32 = 131_072; // 2^17
pub const DEFAULT_MAX_PAGE_LEN_EDGES: u32 = 1_048_576; // 2^20
pub const DEFAULT_MAX_MEMORY_BYTES: usize = 32 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistenceConfig {
    pub max_node_page_len: u32,
    pub max_edge_page_len: u32,
    pub max_memory_bytes: usize,
    pub is_parallel: bool,
    pub node_types: Vec<String>,
}

impl PersistenceConfig {
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

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            max_node_page_len: DEFAULT_MAX_PAGE_LEN_NODES,
            max_edge_page_len: DEFAULT_MAX_PAGE_LEN_EDGES,
            max_memory_bytes: DEFAULT_MAX_MEMORY_BYTES,
            is_parallel: false,
            node_types: Vec::new(),
        }
    }
}

pub trait PersistenceStrategy: Debug + Clone + Default + Send + Sync + 'static + for<'de> Deserialize<'de> + Serialize {
    type NS;
    type ES;
    type GS;

    fn config(&self) -> &PersistenceConfig;
    fn config_mut(&mut self) -> &mut PersistenceConfig;

    fn persist_node_segment<MP: DerefMut<Target = MemNodeSegment>>(
        &self,
        node_page: &Self::NS,
        writer: MP,
    ) where
        Self: Sized;

    fn persist_edge_page<MP: DerefMut<Target = MemEdgeSegment>>(
        &self,
        edge_page: &Self::ES,
        writer: MP,
    ) where
        Self: Sized;

    fn persist_graph_props<MP: DerefMut<Target = MemGraphPropSegment>>(
        &self,
        graph_segment: &Self::GS,
        writer: MP,
    ) where
        Self: Sized;

    /// Indicate whether the strategy persists to disk or not.
    fn disk_storage_enabled() -> bool;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoOpStrategy {
    config: PersistenceConfig,
}

impl NoOpStrategy {
    pub fn new(max_node_page_len: u32, max_edge_page_len: u32) -> Self {
        Self {
            config: PersistenceConfig {
                max_node_page_len,
                max_edge_page_len,
                max_memory_bytes: usize::MAX,
                is_parallel: false,
                node_types: Vec::new(),
            },
        }
    }
}

impl Default for NoOpStrategy {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_PAGE_LEN_NODES, DEFAULT_MAX_PAGE_LEN_EDGES)
    }
}

impl PersistenceStrategy for NoOpStrategy {
    type ES = EdgeSegmentView<Self>;
    type NS = NodeSegmentView<Self>;
    type GS = GraphPropSegmentView<Self>;

    fn config(&self) -> &PersistenceConfig {
        &self.config
    }

    // Use builder pattern with_config.
    fn config_mut(&mut self) -> &mut PersistenceConfig {
        &mut self.config
    }

    fn persist_node_segment<MP: DerefMut<Target = MemNodeSegment>>(
        &self,
        _node_page: &Self::NS,
        _writer: MP,
    ) {
        // No operation
    }

    fn persist_edge_page<MP: DerefMut<Target = MemEdgeSegment>>(
        &self,
        _edge_page: &Self::ES,
        _writer: MP,
    ) {
        // No operation
    }

    fn persist_graph_props<MP: DerefMut<Target = MemGraphPropSegment>>(
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
