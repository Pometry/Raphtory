use crate::model::schema::{
    merge_schemas, property_schema::PropertySchema, SchemaAggregate, DEFAULT_NODE_TYPE,
};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use itertools::Itertools;
use raphtory::{
    db::{
        api::view::{internal::CoreGraphOps, DynamicGraph},
        graph::node::NodeView,
    },
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
            None => DEFAULT_NODE_TYPE == self.type_name,
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
        .map(|(key, value)| {
            let temporal_prop = node
                .base_graph
                .node_meta()
                .get_prop_id(&key.to_string(), false);
            let constant_prop = node
                .base_graph
                .node_meta()
                .get_prop_id(&key.to_string(), true);

            let key_with_prop_type = if temporal_prop.is_some() {
                let p_type = node
                    .base_graph
                    .node_meta()
                    .temporal_prop_meta()
                    .get_dtype(temporal_prop.unwrap());
                (key.to_string(), p_type.unwrap().to_string())
            } else if constant_prop.is_some() {
                let p_type = node
                    .base_graph
                    .node_meta()
                    .const_prop_meta()
                    .get_dtype(constant_prop.unwrap());
                (key.to_string(), p_type.unwrap().to_string())
            } else {
                (key.to_string(), "NONE".to_string())
            };

            (key_with_prop_type, HashSet::from([value.to_string()]))
        })
        .collect()
}
