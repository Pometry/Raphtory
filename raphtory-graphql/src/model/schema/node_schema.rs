use crate::model::schema::{merge_schemas, property_schema::PropertySchema, SchemaAggregate};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use itertools::Itertools;
use raphtory::{
    db::{api::view::DynamicGraph, graph::node::NodeView},
    prelude::{GraphViewOps, NodeViewOps},
};
use std::collections::{HashMap, HashSet};

#[derive(ResolvedObject)]
pub(crate) struct NodeSchema {
    type_name: String,
    graph: DynamicGraph,
}

impl NodeSchema {
    pub fn new(node_type: String, graph: DynamicGraph) -> Self {
        Self {
            type_name: node_type,
            graph,
        }
    }
}

#[ResolvedObjectFields]
impl NodeSchema {
    async fn type_name(&self) -> String {
        self.type_name.clone()
    }

    /// Returns the list of property schemas for this node
    async fn properties(&self) -> Vec<PropertySchema> {
        let filter_type = |node: &NodeView<DynamicGraph>| match node.node_type() {
            Some(node_type) => node_type.to_string() == self.type_name,
            None => false,
        };

        let filtered_nodes = self.graph.nodes().iter_owned().filter(filter_type);

        let schema: SchemaAggregate = filtered_nodes
            .map(collect_node_schema)
            .reduce(merge_schemas)
            .unwrap_or_else(|| HashMap::new());

        schema.into_iter().map(|prop| prop.into()).collect_vec()
    }
}

fn collect_node_schema(node: NodeView<DynamicGraph>) -> SchemaAggregate {
    node.properties()
        .iter()
        .map(|(key, value)| (key.to_string(), HashSet::from([value.to_string()])))
        .collect()
}
