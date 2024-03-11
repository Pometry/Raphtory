use crate::{
    core::{entities::LayerIds, utils::errors::GraphError},
    db::api::view::internal::InternalLayerOps,
    prelude::Layer,
};

use super::ArrowGraph;

impl InternalLayerOps for ArrowGraph {
    fn layer_ids(&self) -> LayerIds {
        LayerIds::One(0)
    }

    fn layer_ids_from_names(&self, key: Layer) -> Result<LayerIds, GraphError> {
        match key {
            Layer::All | Layer::Default => Ok(LayerIds::One(0)), // FIXME: need to handle all correctly
            Layer::One(name) => {
                let name = name.as_ref();
                self.layer_names()
                    .iter()
                    .enumerate()
                    .find(move |(_, ref n)| n == &name)
                    .map(|(i, _)| LayerIds::One(i))
                    .ok_or_else(|| GraphError::InvalidLayer(name.to_string()))
            }
            _ => todo!("Layer ids for multiple names not implemented for ArrowGraph"),
        }
    }

    fn valid_layer_ids_from_names(&self, key: Layer) -> LayerIds {
        match key {
            Layer::All | Layer::Default => LayerIds::One(0), // FIXME: need to handle all correctly
            Layer::One(name) => {
                let name = name.as_ref();
                self.layer_names()
                    .iter()
                    .enumerate()
                    .find(move |(_, ref n)| n == &name)
                    .map(|(i, _)| LayerIds::One(i))
                    .unwrap_or(LayerIds::None)
            }
            _ => todo!("Layer ids for multiple names not implemented for ArrowGraph"),
        }
    }
}
