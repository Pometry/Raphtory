use crate::model::schema::{
    get_node_type, merge_schemas, property_schema::PropertySchema, SchemaAggregate,
};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use itertools::Itertools;
use raphtory::{
    db::{api::view::StaticGraphViewOps, graph::edge::EdgeView},
    prelude::{EdgeViewOps, GraphViewOps},
};
use std::collections::HashSet;

#[derive(ResolvedObject)]
pub(crate) struct EdgeSchema<G: StaticGraphViewOps> {
    graph: G,
    src_type: String,
    dst_type: String,
}

impl<G: StaticGraphViewOps> EdgeSchema<G> {
    pub fn new(graph: G, src_type: String, dst_type: String) -> Self {
        Self {
            graph,
            src_type,
            dst_type,
        }
    }
}

#[ResolvedObjectFields]
impl<G: StaticGraphViewOps> EdgeSchema<G> {
    /// Returns the type of source for these edges
    async fn src_type(&self) -> String {
        self.src_type.clone()
    }

    /// Returns the type of destination for these edges
    async fn dst_type(&self) -> String {
        self.dst_type.clone()
    }

    /// Returns the list of property schemas for edges connecting these types of nodes
    async fn properties(&self) -> Vec<PropertySchema> {
        let filter_types = |edge: &EdgeView<&G>| {
            let src_type = get_node_type(edge.src());
            let dst_type = get_node_type(edge.dst());
            src_type == self.src_type && dst_type == self.dst_type
        };
        let edges = self.graph.edges();
        let filtered_edges = edges.iter().filter(filter_types);

        let schema: SchemaAggregate = filtered_edges
            .map(collect_edge_schema)
            .reduce(merge_schemas)
            .unwrap_or_default();

        schema.into_iter().map(|prop| prop.into()).collect_vec()
    }
}

fn collect_edge_schema<'graph, G: GraphViewOps<'graph>>(edge: EdgeView<G>) -> SchemaAggregate {
    edge.properties()
        .iter()
        .map(|(key, value)| {
            let temporal_prop = edge
                .base_graph
                .edge_meta()
                .get_prop_id(&key.to_string(), false);
            let constant_prop = edge
                .base_graph
                .edge_meta()
                .get_prop_id(&key.to_string(), true);

            let key_with_prop_type = if temporal_prop.is_some() {
                let p_type = edge
                    .base_graph
                    .edge_meta()
                    .temporal_prop_meta()
                    .get_dtype(temporal_prop.unwrap());
                (key.to_string(), p_type.unwrap().to_string())
            } else if constant_prop.is_some() {
                let p_type = edge
                    .base_graph
                    .edge_meta()
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
