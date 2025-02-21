use crate::model::schema::{layer_schema::LayerSchema, node_schema::NodeSchema, DEFAULT_NODE_TYPE};
use dynamic_graphql::SimpleObject;
use itertools::Itertools;
use raphtory::{
    db::api::view::{internal::CoreGraphOps, DynamicGraph},
    prelude::*,
};

#[derive(SimpleObject)]
pub(crate) struct GraphSchema {
    pub(crate) nodes: Vec<NodeSchema>,
    layers: Vec<LayerSchema<DynamicGraph>>,
}

impl GraphSchema {
    pub fn new(graph: &DynamicGraph) -> Self {
        let node_types = (0..graph.node_meta().node_type_meta().len());
        let nodes = node_types
            .map(|node_type| NodeSchema::new(node_type, graph.clone()))
            .collect();

        let layers = graph
            .unique_layers()
            .map(|layer_name| graph.layers(layer_name).unwrap().into())
            .collect_vec();

        GraphSchema { nodes, layers }
    }
}
