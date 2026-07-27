//! Error type for the GraphQL client.

use raphtory_api::core::storage::graph_folder::GraphFolderError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("Network/request error: {0}")]
    Request(#[from] reqwest::Error),

    #[error("{0}")]
    HttpError(String),

    #[error("GraphQL errors: {0}")]
    GraphQLErrors(String),

    /// The server denied the request for lack of permission. Distinguished from
    /// other GraphQL errors by a structured `extensions.code` in the response, so
    /// it never fires for a missing graph (which stays `NotFound`).
    #[error("Permission denied: {0}")]
    PermissionDenied(String),

    #[error("Invalid response: {0}")]
    InvalidResponse(String),

    /// A user-supplied value cannot be rendered into a valid GraphQL query —
    /// e.g. a non-finite float (NaN / infinity), or an unrecognised graph type.
    #[error("Invalid input: {0}")]
    InvalidInput(String),

    /// The operation works locally but cannot yet be expressed over the
    /// remote transport — there is no server field that re-addresses the
    /// entity the way a materialized handle would need. A transport
    /// limitation, not a semantic one; the message names the workaround.
    #[error("Not yet supported over the remote transport: {0}")]
    Unsupported(String),

    /// The read expression referenced a node or edge that isn't visible under
    /// the current view (either absent from the graph entirely, or filtered
    /// out by the accumulated view chain). Fired when a terminal RPC returns
    /// `null` at a selection intermediate — the client can't distinguish "not
    /// in graph" from "not in view" from a single response, so it treats both
    /// as `NotFound`.
    #[error("{0} not found in view")]
    NotFound(String),

    #[error("JSON parse error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Graph encode/decode error: {0}")]
    Graph(#[from] raphtory::errors::GraphError),

    #[error("An error when parsing Jinja query templates: {0}")]
    JinjaError(String),

    #[error("The request did not succeed.")]
    UnsuccessfulResponse,

    #[error(transparent)]
    GraphFolder(#[from] GraphFolderError),
}
