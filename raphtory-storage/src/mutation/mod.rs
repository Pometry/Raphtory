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
        props::{ConstPropError, TPropError},
        tprop::IllegalPropType,
    },
};
use std::sync::Arc;
use storage::error::DBV4Error;
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
    ConstPropError(#[from] ConstPropError),
    #[error("Layer {layer} does not exist for edge ({src}, {dst})")]
    InvalidEdgeLayer {
        layer: String,
        src: String,
        dst: String,
    },
    #[error("Storage error: {0}")]
    DBV4Error(DBV4Error),
}

pub trait InheritMutationOps: Base {}

impl<G: InheritMutationOps> InheritAdditionOps for G {}
impl<G: InheritMutationOps> InheritPropertyAdditionOps for G {}
impl<G: InheritMutationOps> InheritDeletionOps for G {}

impl<T: CoreGraphOps + ?Sized> InheritMutationOps for Arc<T> {}
