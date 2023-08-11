use crate::model::schema::property_schema::PropertySchema;
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use itertools::Itertools;
use raphtory::core::Prop;
use raphtory::db::api::properties::Properties;
use raphtory::db::api::view::internal::{BoxableGraphView, DynamicGraph};
use raphtory::db::graph::vertex::VertexView;
use raphtory::prelude::{GraphViewOps, VertexViewOps};
use std::collections::{HashMap, HashSet};
use std::ops::Deref;

type SchemaAggregate = HashMap<String, HashSet<String>>;

const ENUM_BOUNDARY: usize = 20;

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

    async fn properties(&self) -> Vec<PropertySchema> {
        let filter_type = |vertex: &VertexView<DynamicGraph>| match vertex.properties().get("type")
        {
            Some(node_type) => node_type.to_string() == self.type_name,
            None => false,
        };

        let filtered_vertices = self.graph.vertices().iter().filter(filter_type);

        let schema: SchemaAggregate = filtered_vertices
            .map(collect_vertex_schema)
            .reduce(merge_schemas)
            .unwrap_or_else(|| HashMap::new());

        schema.into_iter().map(|prop| prop.into()).collect_vec()
    }
}

fn collect_vertex_schema(vertex: VertexView<DynamicGraph>) -> SchemaAggregate {
    let pairs = vertex
        .properties()
        .iter()
        .map(|(key, value)| (key.to_owned(), HashSet::from([value.to_string()])))
        .collect_vec();
    HashMap::from_iter(pairs)
}

fn merge_schemas(s1: SchemaAggregate, s2: SchemaAggregate) -> SchemaAggregate {
    let mut merged_map = s1.clone();

    for (key, set2) in s2 {
        if let Some(set1) = merged_map.get_mut(&key) {
            // Here, an empty set means: too many values to be interpreted as an enumerated type
            if set1.len() > 0 && set2.len() > 0 {
                set1.extend(set2);
            }
            if set1.len() > ENUM_BOUNDARY {
                set1.clear();
            }
        } else {
            merged_map.insert(key, set2);
        }
    }

    merged_map
}
