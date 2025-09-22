use std::ops::DerefMut;

use crate::segments::{
    edge::{EdgeSegmentView, MemEdgeSegment},
    node::{MemNodeSegment, NodeSegmentView},
};

pub trait PersistentStrategy: Default + Clone + std::fmt::Debug + Send + Sync + 'static {
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

    /// Indicate whether the strategy persists to disk or not.
    fn is_persistent() -> bool;
}

impl PersistentStrategy for () {
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

    fn is_persistent() -> bool {
        false
    }
}
