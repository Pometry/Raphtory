use crate::model::schema::{layer_schema::LayerSchema, node_schema::NodeSchema};
use dynamic_graphql::SimpleObject;
use itertools::Itertools;
use raphtory::{db::api::view::DynamicGraph, prelude::*};

#[derive(SimpleObject)]
pub(crate) struct GraphSchema {
    nodes: Vec<NodeSchema>,
    layers: Vec<LayerSchema<DynamicGraph>>,
}

impl GraphSchema {
    pub fn new(graph: &DynamicGraph) -> Self {
        let nodes = graph
            .nodes()
            .iter()
            .filter_map(|node| node.properties().get("type").map(|p| p.to_string()))
            .unique()
            .map(|node_type| NodeSchema::new(node_type, graph.clone()))
            .collect_vec();

        let layers = graph
            .unique_layers()
            .map(|layer_name| graph.layer(layer_name).unwrap().into())
            .collect_vec();

        GraphSchema { nodes, layers }
    }
}
