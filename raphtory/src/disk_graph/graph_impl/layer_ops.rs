use crate::{
    core::{entities::LayerIds, utils::errors::GraphError},
    db::api::view::internal::InternalLayerOps,
    prelude::Layer,
};
use itertools::Itertools;

use super::DiskGraph;

impl InternalLayerOps for DiskGraph {
    fn layer_ids(&self) -> &LayerIds {
        match self.inner.layers().len() {
            0 => &LayerIds::None,
            1 => &LayerIds::One(0),
            _ => &LayerIds::All,
        }
    }

    fn layer_ids_from_names(&self, key: Layer) -> Result<LayerIds, GraphError> {
        let valid_layers = self
            .inner
            .layer_names()
            .into_iter()
            .map(|x| x.clone())
            .collect_vec();
        match key {
            Layer::All => Ok(LayerIds::All),
            Layer::Default => Ok(LayerIds::One(0)),
            Layer::One(name) => {
                let id = self
                    .inner
                    .find_layer_id(&name)
                    .ok_or_else(|| GraphError::invalid_layer(name.to_string(), valid_layers))?;
                Ok(LayerIds::One(id))
            }
            Layer::None => Ok(LayerIds::None),
            Layer::Multiple(names) => {
                let ids = names
                    .iter()
                    .map(|name| {
                        self.inner.find_layer_id(name).ok_or_else(|| {
                            GraphError::invalid_layer(name.to_string(), valid_layers.clone())
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(LayerIds::Multiple(ids.into()))
            }
        }
    }

    fn valid_layer_ids_from_names(&self, key: Layer) -> LayerIds {
        match key {
            Layer::All | Layer::Default => LayerIds::One(0), // FIXME: need to handle all correctly
            Layer::One(name) => {
                let name = name.as_ref();
                self.inner
                    .layer_names()
                    .iter()
                    .enumerate()
                    .find(move |(_, ref n)| n == &name)
                    .map(|(i, _)| LayerIds::One(i))
                    .unwrap_or(LayerIds::None)
            }
            _ => todo!("Layer ids for multiple names not implemented for Diskgraph"),
        }
    }
}
