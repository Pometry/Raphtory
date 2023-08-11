use crate::model::schema::node_schema::NodeSchema;
use dynamic_graphql::SimpleObject;
use itertools::Itertools;
use raphtory::db::api::properties::Properties;
use raphtory::db::api::view::internal::{BoxableGraphView, DynamicGraph};
use raphtory::prelude::{GraphViewOps, VertexViewOps};
use std::collections::HashSet;

#[derive(SimpleObject)]
pub(crate) struct GraphSchema {
    nodes: Vec<NodeSchema>,
}

impl GraphSchema {
    pub fn new(graph: &DynamicGraph) -> Self {
        let nodes = graph
            .vertices()
            .iter()
            .filter_map(|vertex| vertex.properties().get("type").map(|p| p.to_string()))
            .unique()
            .map(|node_type| NodeSchema::new(node_type, graph.clone()))
            .collect_vec();
        GraphSchema { nodes }
    }
}
