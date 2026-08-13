//! Remote handle types: the client-side mirrors of the server's graph, entity
//! and collection views.
//!
//! Each handle records the view/read operations applied to it and defers to the
//! transport plumbing in [`crate::client`] ([`Transport`](crate::client::Transport),
//! [`Op`](crate::client::op::Op), [`GraphqlTransport`](crate::client::GraphqlTransport))
//! to turn them into a request. The modules here hold no transport logic of their
//! own; they only build and interpret operations.
//!
//! Every type is re-exported from [`crate::client`], so `crate::client::RemoteGraph`
//! and `crate::client::remote::RemoteGraph` name the same type.

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

pub use remote_client::RemoteClient;
pub use remote_collection_metadata::{ColumnarProps, RemoteMetadataView, RemotePropertiesView};
pub use remote_edge::RemoteEdge;
pub use remote_edges::RemoteEdges;
pub use remote_graph::RemoteGraph;
pub use remote_history::{
    RemoteHistory, RemoteHistoryDateTimes, RemoteHistoryEventIds, RemoteHistoryTimestamps,
    RemoteIntervals,
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
