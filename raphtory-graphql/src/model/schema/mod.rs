pub(crate) mod cache;
pub(crate) mod graph_schema;
pub(crate) mod layer_schema;
pub(crate) mod node_schema;
pub(crate) mod property_schema;

/// Maximum number of distinct values collected per property key. More than that and we don't report any.
const ENUM_BOUNDARY: usize = 20;

/// Above this many entities (nodes graph-wide for `NodeSchema`, edges in the
/// layer for `LayerSchema`), schema resolvers skip collecting property values
/// (variants) and only return keys and types
const MAX_DETAILED_SCHEMA_ENTITIES: usize = 1000;

const DEFAULT_NODE_TYPE: &'static str = "None";
