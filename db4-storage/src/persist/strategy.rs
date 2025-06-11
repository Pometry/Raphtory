use std::ops::DerefMut;

use crate::segments::{
    edge::{EdgeSegmentView, MemEdgeSegment},
    node::MemNodeSegment,
};

pub trait PersistentStrategy: Default + Clone + std::fmt::Debug + Send + Sync + 'static {
    type NS;
    type ES;
    fn persist_node_page<MP: DerefMut<Target = MemNodeSegment>>(
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

impl PersistentStrategy for () {
    type ES = EdgeSegmentView;
    type NS = MemNodeSegment;

    fn persist_node_page<MP: DerefMut<Target = MemNodeSegment>>(
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
