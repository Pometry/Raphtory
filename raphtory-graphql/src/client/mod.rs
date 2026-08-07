//! The client: remote handle types plus the transport plumbing they run on.
//!
//! - [`remote`] holds the handle types ([`RemoteGraph`], [`RemoteNode`], …).
//! - [`op`], [`transport`], [`graphql_transport`] and `error` are the plumbing:
//!   the operation vocabulary, the transport trait, its GraphQL implementation
//!   and the error type.
//!
//! Everything public in [`remote`] — its submodules included — is re-exported
//! here, so `client::RemoteGraph`, `client::remote::RemoteGraph` and
//! `client::remote_graph::RemoteGraph` all name the same type.

mod error;
pub mod graphql_transport;
pub mod op;
pub mod remote;
pub mod transport;

pub use error::ClientError;
pub use graphql_transport::GraphqlTransport;
pub use op::{
    AddEdge, AddEdgeMetadata, AddEdgeUpdates, AddEdges, AddGraphMetadata, AddGraphProperty,
    AddNode, AddNodeMetadata, AddNodeUpdates, AddNodes, CreateNode, DeleteEdge, DeleteEdgeAtTime,
    EdgeAddition, NodeAddition, Op, ReadExpr, SetNodeType, TemporalUpdate, UpdateEdgeMetadata,
    UpdateGraphMetadata, UpdateNodeMetadata, WriteOp,
};
// Glob so the handle types *and* their modules keep their pre-move paths under
// `client::` (e.g. `client::remote_graph::RemoteGraph`).
pub use remote::*;
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
