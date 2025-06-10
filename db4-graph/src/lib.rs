use std::sync::Arc;

use raphtory_api::core::entities::properties::meta::Meta;
use storage::Layer;

pub struct TemporalGraph<EXP = ()> {
    layers: boxcar::Vec<Layer<EXP>>,
    edge_meta: Arc<Meta>,
    node_meta: Arc<Meta>,
}
