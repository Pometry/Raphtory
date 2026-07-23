mod error;
pub mod graphql_transport;
pub mod op;
pub mod remote_client;
pub mod remote_edge;
pub mod remote_edges;
pub mod remote_graph;
pub mod remote_history;
pub mod remote_metadata;
pub mod remote_nested_edges;
pub mod remote_node;
pub mod remote_nodes;
pub mod remote_path_from_graph;
pub mod remote_path_from_node;
pub mod remote_schema;
pub mod transport;

pub use error::ClientError;
pub use graphql_transport::GraphqlTransport;
pub use op::{
    AddEdge, AddEdgeMetadata, AddEdgeUpdates, AddEdges, AddGraphMetadata, AddGraphProperty,
    AddNode, AddNodeMetadata, AddNodeUpdates, AddNodes, CreateNode, DeleteEdge, DeleteEdgeAtTime,
    EdgeAddition, NodeAddition, Op, ReadExpr, SetNodeType, TemporalUpdate, UpdateEdgeMetadata,
    UpdateGraphMetadata, UpdateNodeMetadata, WriteOp,
};
pub use remote_edge::RemoteEdge;
pub use remote_edges::RemoteEdges;
pub use remote_graph::RemoteGraph;
pub use remote_history::{
    RemoteEventTime, RemoteHistory, RemoteHistoryDateTimes, RemoteHistoryEventIds,
    RemoteHistoryTimestamps, RemoteIntervals,
};
pub use remote_metadata::{
    RemoteMetadata, RemoteProperties, RemoteProperty, RemotePropertyTuple,
    RemoteTemporalProperties, RemoteTemporalProperty,
};
pub use remote_nested_edges::RemoteNestedEdges;
pub use remote_node::RemoteNode;
pub use remote_nodes::RemoteNodes;
pub use remote_path_from_graph::RemotePathFromGraph;
pub use remote_path_from_node::RemotePathFromNode;
pub use remote_schema::{
    RemoteEdgeSchema, RemoteGraphSchema, RemoteLayerSchema, RemoteNodeSchema, RemotePropertySchema,
};
pub use transport::Transport;

use raphtory_api::core::entities::properties::prop::Prop;
use std::collections::HashMap;

/// Check if a server at the given URL is online (responds with 200).
pub fn is_online(url: &str) -> bool {
    reqwest::blocking::Client::new()
        .get(url)
        .send()
        .map(|response| response.status().as_u16() == 200)
        .unwrap_or(false)
}

pub(crate) fn inner_collection(value: &Prop) -> String {
    match value {
        Prop::Str(value) => format!("{{ str: {} }}", serde_json::to_string(value).unwrap()),
        Prop::U8(value) => format!("{{ u8: {} }}", value),
        Prop::U16(value) => format!("{{ u16: {} }}", value),
        Prop::I32(value) => format!("{{ i32: {} }}", value),
        Prop::I64(value) => format!("{{ i64: {} }}", value),
        Prop::U32(value) => format!("{{ u32: {} }}", value),
        Prop::U64(value) => format!("{{ u64: {} }}", value),
        Prop::F32(value) => format!("{{ f32: {} }}", value),
        Prop::F64(value) => format!("{{ f64: {} }}", value),
        Prop::Bool(value) => format!("{{ bool: {} }}", value),
        Prop::List(value) => {
            let vec: Vec<String> = value.iter().map(|p| inner_collection(&p)).collect();
            format!("{{ list: [{}] }}", vec.join(", "))
        }
        Prop::Map(value) => {
            let properties_array: Vec<String> = value
                .iter()
                .map(|(k, v)| {
                    format!(
                        "{{ key: {}, value: {} }}",
                        serde_json::to_string(k).unwrap(),
                        inner_collection(v)
                    )
                })
                .collect();
            format!("{{ object: [{}] }}", properties_array.join(", "))
        }
        Prop::DTime(dt) => format!("{{ dtime: \"{}\" }}", dt.to_rfc3339()),
        Prop::NDTime(ndt) => format!(
            "{{ ndtime: \"{}\" }}",
            ndt.format("%Y-%m-%dT%H:%M:%S%.3f").to_string()
        ),
        Prop::Decimal(value) => format!("{{ decimal: \"{}\" }}", value.to_string()),
    }
}

fn to_graphql_valid(key: &String, value: &Prop) -> String {
    match value {
        Prop::Str(value) => format!(
            "{{ key: {}, value: {{ str: {} }} }}",
            serde_json::to_string(key).unwrap(),
            serde_json::to_string(value).unwrap()
        ),
        Prop::U8(value) => format!(
            "{{ key: {}, value: {{ u8: {} }} }}",
            serde_json::to_string(key).unwrap(),
            value
        ),
        Prop::U16(value) => format!(
            "{{ key: {}, value: {{ u16: {} }} }}",
            serde_json::to_string(key).unwrap(),
            value
        ),
        Prop::I32(value) => format!(
            "{{ key: {}, value: {{ i32: {} }} }}",
            serde_json::to_string(key).unwrap(),
            value
        ),
        Prop::I64(value) => format!(
            "{{ key: {}, value: {{ i64: {} }} }}",
            serde_json::to_string(key).unwrap(),
            value
        ),
        Prop::U32(value) => format!(
            "{{ key: {}, value: {{ u32: {} }} }}",
            serde_json::to_string(key).unwrap(),
            value
        ),
        Prop::U64(value) => format!(
            "{{ key: {}, value: {{ u64: {} }} }}",
            serde_json::to_string(key).unwrap(),
            value
        ),
        Prop::F32(value) => format!(
            "{{ key: {}, value: {{ f32: {} }} }}",
            serde_json::to_string(key).unwrap(),
            value
        ),
        Prop::F64(value) => format!(
            "{{ key: {}, value: {{ f64: {} }} }}",
            serde_json::to_string(key).unwrap(),
            value
        ),
        Prop::Bool(value) => format!(
            "{{ key: {}, value: {{ bool: {} }} }}",
            serde_json::to_string(key).unwrap(),
            value
        ),
        Prop::List(value) => {
            let vec: Vec<String> = value.iter().map(|p| inner_collection(&p)).collect();
            format!(
                "{{ key: {}, value: {{ list: [{}] }} }}",
                serde_json::to_string(key).unwrap(),
                vec.join(", ")
            )
        }
        Prop::Map(value) => {
            let properties_array: Vec<String> = value
                .iter()
                .map(|(k, v)| {
                    format!(
                        "{{ key: {}, value: {} }}",
                        serde_json::to_string(k).unwrap(),
                        inner_collection(v)
                    )
                })
                .collect();
            format!(
                "{{ key: {}, value: {{ object: [{}] }} }}",
                serde_json::to_string(key).unwrap(),
                properties_array.join(", ")
            )
        }
        Prop::DTime(dt) => format!(
            "{{ key: {}, value: {{ dtime: \"{}\" }} }}",
            serde_json::to_string(key).unwrap(),
            dt.to_rfc3339()
        ),
        Prop::NDTime(ndt) => format!(
            "{{ key: {}, value: {{ ndtime: \"{}\" }} }}",
            serde_json::to_string(key).unwrap(),
            ndt.format("%Y-%m-%dT%H:%M:%S%.3f").to_string()
        ),
        Prop::Decimal(value) => format!(
            "{{ key: {}, value: {{ decimal: \"{}\" }} }}",
            serde_json::to_string(key).unwrap(),
            value.to_string()
        ),
    }
}

pub(crate) fn build_property_string(properties: HashMap<String, Prop>) -> String {
    let properties_array: Vec<String> = properties
        .iter()
        .map(|(k, v)| to_graphql_valid(k, v))
        .collect();

    format!("[{}]", properties_array.join(", "))
}
