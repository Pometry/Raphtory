use raphtory::{
    db::graph::node::NodeView,
    prelude::{GraphViewOps, NodeViewOps},
};
use rustc_hash::FxHashMap;
use std::collections::HashSet;

pub(crate) mod cache;
pub(crate) mod edge_schema;
pub(crate) mod graph_schema;
pub(crate) mod layer_schema;
pub(crate) mod node_schema;
pub(crate) mod property_schema;

const ENUM_BOUNDARY: usize = 20;

/// Above this many entities (nodes graph-wide for `NodeSchema`, edges
/// graph-wide for `EdgeSchema`), schema resolvers skip collecting property
/// values (variants) and only return keys and types
const MAX_DETAILED_SCHEMA_ENTITIES: usize = 1000;

const DEFAULT_NODE_TYPE: &'static str = "None";

fn get_node_type<'graph, G: GraphViewOps<'graph>>(node: NodeView<'graph, G>) -> String {
    match node.node_type() {
        None => "None".into(),
        Some(n) => n.to_string(),
    }
}

/// Maps each `(property key, property type)` to its distinct values.
/// An empty HashSet means "too many values" so we're skipping value collection.
type SchemaAggregate = FxHashMap<(String, String), HashSet<String>>;
