mod error;
pub mod graphql_transport;
pub mod op;
pub mod remote_client;
pub mod remote_collection_metadata;
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
pub use remote_collection_metadata::{ColumnarProps, RemoteMetadataView, RemotePropertiesView};
pub use remote_edge::RemoteEdge;
pub use remote_edges::RemoteEdges;
pub use remote_graph::RemoteGraph;
pub use remote_history::{
    RemoteEventTime, RemoteHistory, RemoteHistoryDateTimes, RemoteHistoryEventIds,
    RemoteHistoryTimestamps, RemoteIntervals,
};
pub use remote_metadata::{
    RemoteMetadata, RemoteProperties, RemotePropertyTuple, RemoteTemporalProperties,
    RemoteTemporalProperty,
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

use crate::model::graph::property::{ObjectEntry, Value};
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

/// Convert a property map into the `[PropertyInput!]` wire shape
/// (`[{key, value}]`, where `value` is the `Value` @oneOf JSON). Serialization
/// of `Value` rejects non-finite floats, so a `NaN`/`Infinity` surfaces as an
/// error rather than a silent `null`. Shared by the write appliers (as a query
/// variable) and the batch `NodeAddition`/`EdgeAddition` serializers.
pub(crate) fn properties_to_input(
    properties: &HashMap<String, Prop>,
) -> Result<Vec<ObjectEntry>, ClientError> {
    properties
        .iter()
        .map(|(k, v)| {
            Ok(ObjectEntry {
                key: k.clone(),
                value: Value::try_from(v)?,
            })
        })
        .collect()
}
