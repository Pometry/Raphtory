pub mod edges;
pub mod graph;
pub mod nodes;
pub mod properties;

pub use raphtory_api::core::entities::*;

pub fn diff<'a>(
    left: &LayerIds,
    graph: impl crate::prelude::GraphViewOps<'a>,
    other: &LayerIds,
) -> LayerIds {
    match (left, other) {
        (LayerIds::None, _) => LayerIds::None,
        (this, LayerIds::None) => this.clone(),
        (_, LayerIds::All) => LayerIds::None,
        (LayerIds::One(id), other) => {
            if other.contains(id) {
                LayerIds::None
            } else {
                LayerIds::One(*id)
            }
        }
        (LayerIds::Multiple(ids), other) => {
            let ids: Vec<usize> = ids.iter().filter(|id| !other.contains(id)).collect();
            match ids.len() {
                0 => LayerIds::None,
                1 => LayerIds::One(ids[0]),
                _ => LayerIds::Multiple(ids.into()),
            }
        }
        (LayerIds::All, other) => {
            let all_layer_ids: Vec<usize> = graph
                .unique_layers()
                .map(|name| graph.get_layer_id(name.as_ref()).unwrap())
                .filter(|id| !other.contains(id))
                .collect();
            match all_layer_ids.len() {
                0 => LayerIds::None,
                1 => LayerIds::One(all_layer_ids[0]),
                _ => LayerIds::Multiple(all_layer_ids.into()),
            }
        }
    }
}
