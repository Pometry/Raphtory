use crate::graph::graph::Immutable;
use raphtory_api::core::entities::{
    properties::prop::{InvalidBigDecimal, PropError},
    MAX_LAYER,
};
use raphtory_core::entities::{
    graph::logical_to_physical::InvalidNodeId,
    properties::{
        props::{ConstPropError, TPropError},
        tprop::IllegalPropType,
    },
};
use thiserror::Error;

pub mod addition_ops;
pub mod deletion_ops;
pub mod property_addition_ops;

#[derive(Error, Debug)]
pub enum MutationError {
    #[error(transparent)]
    Immutable(#[from] Immutable),
    #[error("More than {MAX_LAYER} layers are not supported")]
    TooManyLayers,
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
}
