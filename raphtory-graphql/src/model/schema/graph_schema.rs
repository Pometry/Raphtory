use crate::model::schema::{
    cache::SchemaCache, layer_schema::LayerSchema, node_schema::NodeSchema,
};
use dynamic_graphql::SimpleObject;
use itertools::Itertools;
use raphtory::{db::api::view::DynamicGraph, prelude::*};
use raphtory_api::core::entities::properties::meta::STATIC_GRAPH_LAYER_ID;
use raphtory_storage::{core_ops::CoreGraphOps, layer_ops::InternalLayerOps};
use std::sync::Arc;

#[derive(SimpleObject)]
pub(crate) struct GraphSchema {
    pub(crate) nodes: Vec<NodeSchema>,
    layers: Vec<LayerSchema<DynamicGraph>>,
}

impl GraphSchema {
    /// `cache` is `Some` only for the unfiltered, base graph.
    /// Other views (such as filtered, layered, windowed) pass `None` and recompute every time.
    pub fn new(graph: &DynamicGraph, cache: Option<Arc<SchemaCache>>) -> Self {
        let node_types = graph.node_meta().node_type_meta().ids();
        let nodes = node_types
            .map(|node_type| NodeSchema::new(node_type, graph.clone(), cache.clone()))
            .collect();

        let layers = graph
            .layer_ids() // FIXME: Do we want to use unique_layers() here instead?
            .iter(graph.num_layers() + 1)
            // skip the internal `_static_graph` layer
            .filter(|&layer_id| layer_id != STATIC_GRAPH_LAYER_ID)
            .map(|layer_id| LayerSchema::new(graph.clone(), layer_id, cache.clone()))
            .collect_vec();

        GraphSchema { nodes, layers }
    }
}
