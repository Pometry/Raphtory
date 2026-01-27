use raphtory_api::core::{entities::MAX_LAYER, storage::arc_str::ArcStr};
use std::fmt::Debug;
use thiserror::Error;

#[derive(Error, Debug)]
#[error("Invalid layer: {invalid_layer}. Valid layers: {valid_layers:?}")]
pub struct InvalidLayer {
    invalid_layer: ArcStr,
    valid_layers: Vec<String>,
}

#[derive(Error, Debug)]
#[error("At most {MAX_LAYER} layers are supported.")]
pub struct TooManyLayers;

impl InvalidLayer {
    pub fn new(invalid_layer: ArcStr, valid_layers: Vec<String>) -> Self {
        Self {
            invalid_layer,
            valid_layers,
        }
    }
}
