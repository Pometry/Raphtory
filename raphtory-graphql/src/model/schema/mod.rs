use raphtory::{
    db::{api::view::StaticGraphViewOps, graph::vertex::VertexView},
    prelude::VertexViewOps,
};
use std::collections::{HashMap, HashSet};

pub(crate) mod edge_schema;
pub(crate) mod graph_schema;
pub(crate) mod layer_schema;
pub(crate) mod node_schema;
pub(crate) mod property_schema;

const ENUM_BOUNDARY: usize = 20;

fn get_vertex_type<G: StaticGraphViewOps>(vertex: VertexView<G>) -> String {
    let prop = vertex.properties().get("type");
    prop.map(|prop| prop.to_string())
        .unwrap_or_else(|| "NONE".to_string())
}

type SchemaAggregate = HashMap<String, HashSet<String>>;

fn merge_schemas(mut s1: SchemaAggregate, s2: SchemaAggregate) -> SchemaAggregate {
    for (key, set2) in s2 {
        if let Some(set1) = s1.get_mut(&key) {
            // Here, an empty set means: too many values to be interpreted as an enumerated type
            if set1.len() > 0 && set2.len() > 0 {
                set1.extend(set2);
            }
            if set1.len() > ENUM_BOUNDARY {
                set1.clear();
            }
        } else {
            s1.insert(key, set2);
        }
    }

    s1
}
