pub mod edges;
pub mod graph;
pub mod locked;
pub mod nodes;
pub mod variants;

use raphtory_api::core::entities::{properties::meta::STATIC_GRAPH_LAYER_ID, LayerId, LayerIds};
use raphtory_core::entities::LayerVariants;
use storage::utils::Iter3;

/// Build an iterator over all layer ids we should visit when reading
/// node-style temporal data: `STATIC_GRAPH_LAYER_ID` is always included
/// (unlayered entities must be visible in every view), plus the layers
/// requested in `layer_ids`.
pub fn layer_ids_with_static(
    num_layers: usize,
    layer_ids: &LayerIds,
) -> impl Iterator<Item = LayerId> + Send + Sync + 'static {
    match layer_ids {
        LayerIds::None => LayerVariants::None(std::iter::once(STATIC_GRAPH_LAYER_ID)),
        LayerIds::All => LayerVariants::All((0..num_layers).map(LayerId)),
        LayerIds::One(id) => {
            if *id == STATIC_GRAPH_LAYER_ID {
                LayerVariants::One(std::iter::once(*id))
            } else {
                LayerVariants::Multiple(Iter3::I([STATIC_GRAPH_LAYER_ID, *id].into_iter()))
            }
        }
        LayerIds::Multiple(ids) => {
            if ids.contains(STATIC_GRAPH_LAYER_ID) {
                LayerVariants::Multiple(Iter3::J(ids.clone().into_iter()))
            } else {
                let v = std::iter::once(STATIC_GRAPH_LAYER_ID).chain(ids.clone().into_iter());
                LayerVariants::Multiple(Iter3::K(v))
            }
        }
    }
}
