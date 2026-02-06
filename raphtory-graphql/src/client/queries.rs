use graphql_client::GraphQLQuery;
use serde_json::Value as JsonValue;

type PropertyOutput = JsonValue;

// Graph creation queries (technically mutations)
#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "schema.graphql",
    query_path = "graphql-queries/graph_creation.graphql",
    response_derives = "Debug"
)]
pub struct DeleteGraph;

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "schema.graphql",
    query_path = "graphql-queries/graph_creation.graphql",
    response_derives = "Debug"
)]
pub struct CreateGraph;

// Graph update queries
#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "schema.graphql",
    query_path = "graphql-queries/graph_updates.graphql",
    response_derives = "Debug"
)]
pub struct AddNode;

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "schema.graphql",
    query_path = "graphql-queries/graph_updates.graphql",
    response_derives = "Debug"
)]
pub struct AddNodeWithMetadata;

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "schema.graphql",
    query_path = "graphql-queries/graph_updates.graphql",
    response_derives = "Debug"
)]
pub struct AddEdge;

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "schema.graphql",
    query_path = "graphql-queries/graph_updates.graphql",
    response_derives = "Debug"
)]
pub struct AddEdgeWithMetadata;

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "schema.graphql",
    query_path = "graphql-queries/graph_updates.graphql",
    response_derives = "Debug"
)]
pub struct DeleteEdge;

// persistence check queries
#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "schema.graphql",
    query_path = "graphql-queries/persistence_checks.graphql",
    response_derives = "Debug"
)]
pub struct HasNode;

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "schema.graphql",
    query_path = "graphql-queries/persistence_checks.graphql",
    response_derives = "Debug"
)]
pub struct HasEdge;

#[derive(GraphQLQuery)]
#[graphql(
    schema_path = "schema.graphql",
    query_path = "graphql-queries/persistence_checks.graphql",
    response_derives = "Debug"
)]
pub struct HasDeletedEdge;

pub trait GqlPropertyTypes {
    type PropertyValue;
    type ObjectEntry;
    type PropertyInput;

    fn v_u64(x: i64) -> Self::PropertyValue;
    fn v_i64(x: i64) -> Self::PropertyValue;
    fn v_f64(x: f64) -> Self::PropertyValue;
    fn v_str(x: String) -> Self::PropertyValue;
    fn v_bool(x: bool) -> Self::PropertyValue;
    fn v_list(x: Vec<Self::PropertyValue>) -> Self::PropertyValue;
    fn v_object(x: Vec<Self::ObjectEntry>) -> Self::PropertyValue;

    fn obj_entry(key: String, value: Self::PropertyValue) -> Self::ObjectEntry;
    fn prop_input(key: String, value: Self::PropertyValue) -> Self::PropertyInput;
}

pub struct NodePropertyTypes;

impl GqlPropertyTypes for NodePropertyTypes {
    type PropertyValue = add_node::Value;
    type ObjectEntry = add_node::ObjectEntry;
    type PropertyInput = add_node::PropertyInput;

    fn v_u64(x: i64) -> Self::PropertyValue {
        add_node::Value::U64(x)
    }

    fn v_i64(x: i64) -> Self::PropertyValue {
        add_node::Value::I64(x)
    }

    fn v_f64(x: f64) -> Self::PropertyValue {
        add_node::Value::F64(x)
    }

    fn v_str(x: String) -> Self::PropertyValue {
        add_node::Value::Str(x)
    }

    fn v_bool(x: bool) -> Self::PropertyValue {
        add_node::Value::Bool(x)
    }

    fn v_list(x: Vec<Self::PropertyValue>) -> Self::PropertyValue {
        add_node::Value::List(x)
    }

    fn v_object(x: Vec<Self::ObjectEntry>) -> Self::PropertyValue {
        add_node::Value::Object(x)
    }

    fn obj_entry(key: String, value: Self::PropertyValue) -> Self::ObjectEntry {
        add_node::ObjectEntry { key, value }
    }

    fn prop_input(key: String, value: Self::PropertyValue) -> Self::PropertyInput {
        add_node::PropertyInput { key, value }
    }
}

pub struct NodeWithMetadataPropertyTypes;

impl GqlPropertyTypes for NodeWithMetadataPropertyTypes {
    type PropertyValue = add_node_with_metadata::Value;
    type ObjectEntry = add_node_with_metadata::ObjectEntry;
    type PropertyInput = add_node_with_metadata::PropertyInput;

    fn v_u64(x: i64) -> Self::PropertyValue {
        add_node_with_metadata::Value::U64(x)
    }

    fn v_i64(x: i64) -> Self::PropertyValue {
        add_node_with_metadata::Value::I64(x)
    }

    fn v_f64(x: f64) -> Self::PropertyValue {
        add_node_with_metadata::Value::F64(x)
    }

    fn v_str(x: String) -> Self::PropertyValue {
        add_node_with_metadata::Value::Str(x)
    }

    fn v_bool(x: bool) -> Self::PropertyValue {
        add_node_with_metadata::Value::Bool(x)
    }

    fn v_list(x: Vec<Self::PropertyValue>) -> Self::PropertyValue {
        add_node_with_metadata::Value::List(x)
    }

    fn v_object(x: Vec<Self::ObjectEntry>) -> Self::PropertyValue {
        add_node_with_metadata::Value::Object(x)
    }

    fn obj_entry(key: String, value: Self::PropertyValue) -> Self::ObjectEntry {
        add_node_with_metadata::ObjectEntry { key, value }
    }

    fn prop_input(key: String, value: Self::PropertyValue) -> Self::PropertyInput {
        add_node_with_metadata::PropertyInput { key, value }
    }
}

pub struct EdgePropertyTypes;

impl GqlPropertyTypes for EdgePropertyTypes {
    type PropertyValue = add_edge::Value;
    type ObjectEntry = add_edge::ObjectEntry;
    type PropertyInput = add_edge::PropertyInput;

    fn v_u64(x: i64) -> Self::PropertyValue {
        add_edge::Value::U64(x)
    }

    fn v_i64(x: i64) -> Self::PropertyValue {
        add_edge::Value::I64(x)
    }

    fn v_f64(x: f64) -> Self::PropertyValue {
        add_edge::Value::F64(x)
    }

    fn v_str(x: String) -> Self::PropertyValue {
        add_edge::Value::Str(x)
    }

    fn v_bool(x: bool) -> Self::PropertyValue {
        add_edge::Value::Bool(x)
    }

    fn v_list(x: Vec<Self::PropertyValue>) -> Self::PropertyValue {
        add_edge::Value::List(x)
    }

    fn v_object(x: Vec<Self::ObjectEntry>) -> Self::PropertyValue {
        add_edge::Value::Object(x)
    }

    fn obj_entry(key: String, value: Self::PropertyValue) -> Self::ObjectEntry {
        add_edge::ObjectEntry { key, value }
    }

    fn prop_input(key: String, value: Self::PropertyValue) -> Self::PropertyInput {
        add_edge::PropertyInput { key, value }
    }
}

pub struct EdgeWithMetadataPropertyTypes;

impl GqlPropertyTypes for EdgeWithMetadataPropertyTypes {
    type PropertyValue = add_edge_with_metadata::Value;
    type ObjectEntry = add_edge_with_metadata::ObjectEntry;
    type PropertyInput = add_edge_with_metadata::PropertyInput;

    fn v_u64(x: i64) -> Self::PropertyValue {
        add_edge_with_metadata::Value::U64(x)
    }

    fn v_i64(x: i64) -> Self::PropertyValue {
        add_edge_with_metadata::Value::I64(x)
    }

    fn v_f64(x: f64) -> Self::PropertyValue {
        add_edge_with_metadata::Value::F64(x)
    }

    fn v_str(x: String) -> Self::PropertyValue {
        add_edge_with_metadata::Value::Str(x)
    }

    fn v_bool(x: bool) -> Self::PropertyValue {
        add_edge_with_metadata::Value::Bool(x)
    }

    fn v_list(x: Vec<Self::PropertyValue>) -> Self::PropertyValue {
        add_edge_with_metadata::Value::List(x)
    }

    fn v_object(x: Vec<Self::ObjectEntry>) -> Self::PropertyValue {
        add_edge_with_metadata::Value::Object(x)
    }

    fn obj_entry(key: String, value: Self::PropertyValue) -> Self::ObjectEntry {
        add_edge_with_metadata::ObjectEntry { key, value }
    }

    fn prop_input(key: String, value: Self::PropertyValue) -> Self::PropertyInput {
        add_edge_with_metadata::PropertyInput { key, value }
    }
}
