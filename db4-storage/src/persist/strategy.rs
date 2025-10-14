use std::ops::DerefMut;

use serde::{Deserialize, Serialize};

use crate::segments::{
    edge::{EdgeSegmentView, MemEdgeSegment},
    node::{MemNodeSegment, NodeSegmentView},
};

pub const DEFAULT_MAX_PAGE_LEN_NODES: u32 = 131_072; // 2^17
pub const DEFAULT_MAX_PAGE_LEN_EDGES: u32 = 1_048_576; // 2^20

pub trait Config:
    Default + std::fmt::Debug + Clone + Send + Sync + 'static + for<'a> Deserialize<'a> + Serialize
{
    fn max_node_page_len(&self) -> u32;
    fn max_edge_page_len(&self) -> u32;
    fn is_parallel(&self) -> bool;
}

pub trait PersistentStrategy: Config {
    type NS;
    type ES;
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

    fn max_edge_page_len(&self) -> u32 {
        self.max_edge_page_len
    }

    fn is_parallel(&self) -> bool {
        false
    }
}

impl PersistentStrategy for NoOpStrategy {
    type ES = EdgeSegmentView<Self>;
    type NS = NodeSegmentView<Self>;

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
}
