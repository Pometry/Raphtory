use raphtory::{
    db::graph::node::NodeView,
    prelude::{GraphViewOps, NodeViewOps},
};
use rustc_hash::FxHashMap;
use std::collections::HashSet;

pub(crate) mod edge_schema;
pub(crate) mod graph_schema;
pub(crate) mod layer_schema;
pub(crate) mod node_schema;
pub(crate) mod property_schema;

const ENUM_BOUNDARY: usize = 20;

const DEFAULT_NODE_TYPE: &'static str = "None";

fn get_node_type<'graph, G: GraphViewOps<'graph>>(node: NodeView<G>) -> String {
    match node.node_type() {
        None => "None".into(),
        Some(n) => n.to_string(),
    }
}

type SchemaAggregate = FxHashMap<(String, String), HashSet<String>>;

fn merge_schemas(mut s1: SchemaAggregate, s2: SchemaAggregate) -> SchemaAggregate {
    for ((key, prop_type), set2) in s2 {
        if let Some(set1) = s1.get_mut(&(key.clone(), prop_type.clone())) {
            // Here, an empty set means: too many values to be interpreted as an enumerated type
            if set1.len() > 0 && set2.len() > 0 {
                set1.extend(set2);
            }
            if set1.len() > ENUM_BOUNDARY {
                set1.clear();
            }
        } else {
            s1.insert((key, prop_type), set2);
        }
    }
    s1
}
