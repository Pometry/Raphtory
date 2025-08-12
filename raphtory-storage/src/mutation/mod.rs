use crate::{
    core_ops::CoreGraphOps,
    graph::graph::Immutable,
    mutation::{
        addition_ops::InheritAdditionOps, deletion_ops::InheritDeletionOps,
        property_addition_ops::InheritPropertyAdditionOps,
    },
};
use raphtory_api::{
    core::entities::properties::prop::{InvalidBigDecimal, PropError},
    inherit::Base,
};
use raphtory_core::entities::{
    graph::{logical_to_physical::InvalidNodeId, tgraph::TooManyLayers},
    properties::{
        props::{MetadataError, TPropError},
        tprop::IllegalPropType,
    },
};
use std::sync::Arc;
use storage::{error::StorageError, resolver::GIDResolverError};
use thiserror::Error;

pub mod addition_ops;
pub mod addition_ops_ext;
pub mod deletion_ops;
pub mod property_addition_ops;

#[derive(Error, Debug)]
pub enum MutationError {
    #[error(transparent)]
    Immutable(#[from] Immutable),
    #[error(transparent)]
    TooManyLayers(#[from] TooManyLayers),
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
    IllegalPropType(#[from] IllegalPropType),
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

impl From<GIDResolverError> for MutationError {
    fn from(error: GIDResolverError) -> Self {
        match error {
            GIDResolverError::StorageError(e) => MutationError::StorageError(e),
            GIDResolverError::InvalidNodeId(e) => MutationError::InvalidNodeId(e),
        }
    }
}

pub trait InheritMutationOps: Base {}

impl<G: InheritMutationOps> InheritAdditionOps for G {}
impl<G: InheritMutationOps> InheritPropertyAdditionOps for G {}
impl<G: InheritMutationOps> InheritDeletionOps for G {}

impl<T: CoreGraphOps + ?Sized> InheritMutationOps for Arc<T> {}
