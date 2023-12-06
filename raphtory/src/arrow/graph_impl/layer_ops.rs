use crate::{core::entities::LayerIds, db::api::view::internal::InternalLayerOps, prelude::Layer};

use super::Graph2;

impl InternalLayerOps for Graph2 {
    fn layer_ids(&self) -> LayerIds {
        LayerIds::One(0)
    }

    fn layer_ids_from_names(&self, key: Layer) -> LayerIds {
        match key {
            Layer::All | Layer::Default => LayerIds::One(0),
            Layer::One(name) => {
                if name == "default" {
                    LayerIds::One(0)
                } else {
                    let name = name.as_ref();
                    self.layer_names()
                        .iter()
                        .enumerate()
                        .find(move |(_, ref n)| n == &name)
                        .map(|(i, _)| LayerIds::One(i))
                        .unwrap_or_else(|| {
                            panic!(
                                "Layer name {} not found in graph. Available layers: {:?}",
                                name,
                                self.layer_names()
                            )
                        })
                }
            }
            _ => todo!("Layer ids from names not implemented for Graph2"),
        }
    }
}
