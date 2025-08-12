use raphtory_api::core::{
    entities::MAX_LAYER
    ,
    storage::arc_str::ArcStr
    ,
};
use std::{fmt::Debug};
use thiserror::Error;


#[derive(Error, Debug)]
#[error("Invalid layer: {invalid_layer}. Valid layers: {valid_layers}")]
pub struct InvalidLayer {
    invalid_layer: ArcStr,
    valid_layers: String,
}

#[derive(Error, Debug)]
#[error("At most {MAX_LAYER} layers are supported.")]
pub struct TooManyLayers;

impl InvalidLayer {
    pub fn new(invalid_layer: ArcStr, valid: Vec<String>) -> Self {
        let valid_layers = valid.join(", ");
        Self {
            invalid_layer,
            valid_layers,
        }
    }
}
