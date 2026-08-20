//! The client: remote handle types plus the transport plumbing they run on.
//!
//! - [`remote`] holds the handle types ([`RemoteGraph`], [`RemoteNode`], …).
//! - [`remote_client`], [`op`], [`transport`], [`graphql_transport`] and `error`
//!   are the plumbing: the wire client,
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
pub mod remote_client;
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
pub use remote_client::RemoteClient;
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

/// Collect a property argument into the map the wire ops carry.
///
/// The write methods take properties the same way the local `AdditionOps` do —
/// any `IntoIterator` of `(key, value)` where the key is string-like and the
/// value converts to a `Prop` — so `[("score", 1i64)]`, a `Vec<(String, Prop)>`
/// and a `HashMap` are all accepted, and `NO_PROPS` means none. This is where
/// that argument becomes the concrete map an `Op` holds.
pub(crate) fn collect_props<PN: AsRef<str>, P: Into<Prop>>(
    props: impl IntoIterator<Item = (PN, P)>,
) -> HashMap<String, Prop> {
    props
        .into_iter()
        .map(|(k, v)| (k.as_ref().to_string(), v.into()))
        .collect()
}

/// As `collect_props`, for the ops whose property field is optional: an empty
/// argument is `None` so the field is omitted from the request rather than sent
/// as an empty list, matching what the client did before properties became a
/// local-style iterator.
pub(crate) fn collect_opt_props<PN: AsRef<str>, P: Into<Prop>>(
    props: impl IntoIterator<Item = (PN, P)>,
) -> Option<HashMap<String, Prop>> {
    let props = collect_props(props);
    (!props.is_empty()).then_some(props)
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
