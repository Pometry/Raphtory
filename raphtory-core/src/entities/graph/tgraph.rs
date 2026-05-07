use raphtory_api::core::{entities::MAX_EID, storage::arc_str::ArcStr};
use std::fmt::Debug;
use thiserror::Error;

#[derive(Error, Debug)]
#[error("Invalid layer: {invalid_layer}. Valid layers: {valid_layers:?}")]
pub struct InvalidLayer {
    invalid_layer: ArcStr,
    valid_layers: Vec<String>,
}

impl InvalidLayer {
    pub fn new(invalid_layer: ArcStr, valid_layers: Vec<String>) -> Self {
        Self {
            invalid_layer,
            valid_layers,
        }
    }
}
