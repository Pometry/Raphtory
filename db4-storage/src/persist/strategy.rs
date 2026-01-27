use std::ops::DerefMut;

use crate::{
    api::{edges::EdgeSegmentOps, graph_props::GraphPropSegmentOps, nodes::NodeSegmentOps},
    segments::{
        edge::segment::{EdgeSegmentView, MemEdgeSegment},
        graph_prop::{GraphPropSegmentView, segment::MemGraphPropSegment},
        node::segment::{MemNodeSegment, NodeSegmentView},
    },
};
use serde::{Deserialize, Serialize};

pub const DEFAULT_MAX_PAGE_LEN_NODES: u32 = 131_072; // 2^17
pub const DEFAULT_MAX_PAGE_LEN_EDGES: u32 = 1_048_576; // 2^20
pub const DEFAULT_MAX_MEMORY_BYTES: usize = 32 * 1024 * 1024;

pub trait Config:
    Default + std::fmt::Debug + Clone + Send + Sync + 'static + for<'a> Deserialize<'a> + Serialize
{
    fn max_node_page_len(&self) -> u32;
    fn max_edge_page_len(&self) -> u32;

    fn max_memory_bytes(&self) -> usize;
    fn is_parallel(&self) -> bool;
    fn node_types(&self) -> &[String];
    fn with_node_types(&self, types: impl IntoIterator<Item = impl AsRef<str>>) -> Self;
}

pub trait PersistentStrategy: Config {
    type NS: NodeSegmentOps;
    type ES: EdgeSegmentOps;
    type GS: GraphPropSegmentOps;

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

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct NoOpStrategy {
    max_node_page_len: u32,
    max_edge_page_len: u32,
}

impl NoOpStrategy {
    pub fn new(max_node_page_len: u32, max_edge_page_len: u32) -> Self {
        Self {
            max_node_page_len,
            max_edge_page_len,
        }
    }
}

impl Default for NoOpStrategy {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_PAGE_LEN_NODES, DEFAULT_MAX_PAGE_LEN_EDGES)
    }
}

impl Config for NoOpStrategy {
    fn max_node_page_len(&self) -> u32 {
        self.max_node_page_len
    }

    #[inline(always)]
    fn max_edge_page_len(&self) -> u32 {
        self.max_edge_page_len
    }

    fn max_memory_bytes(&self) -> usize {
        usize::MAX
    }

    fn is_parallel(&self) -> bool {
        false
    }

    fn node_types(&self) -> &[String] {
        &[]
    }

    fn with_node_types(&self, _types: impl IntoIterator<Item = impl AsRef<str>>) -> Self {
        *self
    }
}

impl PersistentStrategy for NoOpStrategy {
    type ES = EdgeSegmentView<Self>;
    type NS = NodeSegmentView<Self>;
    type GS = GraphPropSegmentView<Self>;

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
