use crate::{
    core_ops::CoreGraphOps, graph::graph::Immutable, mutation::addition_ops::InheritAdditionOps,
};
use raphtory_api::{
    core::entities::properties::prop::{InvalidBigDecimal, PropError},
    inherit::Base,
    ArcRwLockReadGuard, ArcRwLockWriteGuard,
};
use raphtory_core::entities::properties::props::{MetadataError, TPropError};
use std::sync::Arc;
use storage::{
    error::StorageError,
    pages::{
        edge_page::writer::EdgeWriter,
        graph_prop_page::writer::GraphPropWriter,
        node_page::writer::{NodeWriter, NodeWriters},
        node_type_index::writer::NodeTypeIndexWriter,
    },
    resolver::mapping_resolver::InvalidNodeId,
    segments::{
        edge::segment::MemEdgeSegment, node::segment::MemNodeSegment,
        node_type_index::MemNodeTypeIndex,
    },
    Extension, ES, GS, NS, NTI,
};
use thiserror::Error;

pub mod addition_ops;
pub mod addition_ops_ext;

pub type NodeWriterT<'a> = NodeWriter<'a, ArcRwLockWriteGuard<MemNodeSegment>, NS<Extension>>;
pub type NodeWritersT<'a> = NodeWriters<'a, ArcRwLockWriteGuard<MemNodeSegment>, NS<Extension>>;
pub type EdgeWriterT<'a> = EdgeWriter<'a, ArcRwLockWriteGuard<MemEdgeSegment>, ES<Extension>>;
pub type GraphPropWriterT<'a> = GraphPropWriter<'a, GS<Extension>>;
pub type NodeTypeIndexWriterT<'a> =
    NodeTypeIndexWriter<'a, ArcRwLockReadGuard<MemNodeTypeIndex>, NTI>;

#[derive(Error, Debug)]
pub enum MutationError {
    #[error(transparent)]
    Immutable(#[from] Immutable),
    #[error("Node type already set")]
    NodeTypeError,
    #[error(transparent)]
    InvalidNodeId(#[from] InvalidNodeId),
    #[error(transparent)]
    PropError(#[from] PropError),
    #[error(transparent)]
    TPropError(#[from] TPropError),
    #[error(transparent)]
    InvalidBigDecimal(#[from] InvalidBigDecimal),
    #[error(transparent)]
    MetadataError(#[from] MetadataError),
    #[error("Layer {layer} does not exist for edge ({src}, {dst})")]
    InvalidEdgeLayer {
        layer: String,
        src: String,
        dst: String,
    },
    #[error("Storage error: {0}")]
    StorageError(#[from] StorageError),
}

pub trait InheritMutationOps: Base {}

impl<G: InheritMutationOps> InheritAdditionOps for G {}
impl<T: CoreGraphOps + ?Sized> InheritMutationOps for Arc<T> {}
