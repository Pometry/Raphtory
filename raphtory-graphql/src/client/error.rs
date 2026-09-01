//! Error type for the GraphQL client, plus the classification of server-side
//! GraphQL errors into it.

use crate::data::{CODE_ACCESS_DENIED, CODE_GRAPH_NOT_FOUND};
use raphtory_api::core::{storage::graph_folder::GraphFolderError, utils::time::ParseTimeError};
use serde_json::Value as JsonValue;
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

    /// The read expression referenced a node or edge that isn't visible under
    /// the current view (either absent from the graph entirely, or filtered
    /// out by the accumulated view chain). Fired when a terminal RPC returns
    /// `null` at a selection intermediate — the client can't distinguish "not
    /// in graph" from "not in view" from a single response, so it treats both
    /// as `NotFound`.
    #[error("{0} not found in view")]
    NotFound(String),

    /// The target graph does not exist — or exists but the caller lacks the
    /// namespace visibility to know it does. The server reports both as the
    /// same `GRAPH_NOT_FOUND` code with an identical message, so the two stay
    /// indistinguishable: telling them apart would let a caller map out what
    /// they cannot read. The message is the server's verbatim (e.g. `Graph does
    /// not exist`); unlike `NotFound` it is not about a view, so it is surfaced
    /// as-is rather than suffixed.
    #[error("{0}")]
    GraphNotFound(String),

    #[error("JSON parse error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Graph encode/decode error: {0}")]
    Graph(#[from] raphtory::errors::GraphError),

    #[error("The request did not succeed.")]
    UnsuccessfulResponse,

    #[error(transparent)]
    GraphFolder(#[from] GraphFolderError),
}

/// A time argument that cannot be interpreted — e.g. an unparseable datetime
/// string. Write methods take `TryIntoInputTime` (the same trait the local
/// mutation API uses), so the parse happens client-side and reports here rather
/// than travelling to the server as a nonsense timestamp.
impl From<ParseTimeError> for ClientError {
    fn from(err: ParseTimeError) -> Self {
        ClientError::InvalidInput(format!("invalid time: {err}"))
    }
}

/// Query rendering writes into a `String`, which cannot actually fail — but the
/// renderer propagates the result rather than discarding it, so that no call
/// site reads as if it were swallowing an error.
impl From<std::fmt::Error> for ClientError {
    fn from(err: std::fmt::Error) -> Self {
        ClientError::InvalidInput(format!("failed to render query: {err}"))
    }
}

/// Extract the machine-readable `extensions.code` from a single GraphQL error
/// object, if present.
fn error_code(error: &JsonValue) -> Option<&str> {
    error.get("extensions")?.get("code")?.as_str()
}

/// Turn a GraphQL `errors` array into the appropriate `ClientError`, keying off
/// the structured `extensions.code` the server attaches rather than message
/// wording.
///
/// A forbidden-but-hidden graph reports `GRAPH_NOT_FOUND` exactly as a genuinely
/// missing one does, so both map to `GraphNotFound` and **never** to a
/// permission error. Classifying one of them as a permission error would restore
/// the distinction the server took care to remove.
pub(crate) fn classify_graphql_errors(errors: &JsonValue, query: &str) -> ClientError {
    let mut access_denied = false;
    let mut graph_not_found = false;
    let mut not_found_message: Option<String> = None;
    if let JsonValue::Array(error_objects) = errors {
        for error in error_objects {
            match error_code(error) {
                Some(CODE_ACCESS_DENIED) => access_denied = true,
                Some(CODE_GRAPH_NOT_FOUND) => {
                    graph_not_found = true;
                    if not_found_message.is_none() {
                        not_found_message = error
                            .get("message")
                            .and_then(|m| m.as_str())
                            .map(str::to_owned);
                    }
                }
                _ => {}
            }
        }
    }

    let message = match errors {
        JsonValue::Array(errors) => errors
            .iter()
            .map(|e| format!("{}", e))
            .collect::<Vec<_>>()
            .join("\n\t"),
        _ => format!("{}", errors),
    };

    if graph_not_found && !access_denied {
        return ClientError::GraphNotFound(
            not_found_message.unwrap_or_else(|| "Graph does not exist".to_owned()),
        );
    }
    if access_denied {
        return ClientError::PermissionDenied(format!(
            "the server denied the request:\n\t{}",
            message
        ));
    }
    ClientError::GraphQLErrors(format!(
        "After sending query to the server:\n\t{}\nGot the following errors:\n\t{}",
        query, message
    ))
}

#[cfg(test)]
mod error_classification_tests {
    use super::*;
    use serde_json::json;

    /// A forbidden graph and a genuinely missing one both surface as
    /// `GraphNotFound` with the same message — never as a `PermissionDenied` —
    /// so an unauthorized caller cannot tell them apart.
    #[test]
    fn forbidden_and_missing_graph_are_indistinguishable() {
        // What the server sends for a graph hidden by policy AND for one that
        // doesn't exist: identical GRAPH_NOT_FOUND with the same message.
        let hidden = json!([{"message": "Graph does not exist",
                             "extensions": {"code": "GRAPH_NOT_FOUND"}}]);
        let missing = json!([{"message": "Graph does not exist",
                              "extensions": {"code": "GRAPH_NOT_FOUND"}}]);

        let hidden_err = classify_graphql_errors(&hidden, "q");
        let missing_err = classify_graphql_errors(&missing, "q");

        assert!(matches!(hidden_err, ClientError::GraphNotFound(_)));
        assert!(matches!(missing_err, ClientError::GraphNotFound(_)));
        // Never a permission error — that would leak existence.
        assert!(!matches!(hidden_err, ClientError::PermissionDenied(_)));
        // Byte-for-byte identical to the caller.
        assert_eq!(format!("{hidden_err}"), format!("{missing_err}"));
    }

    #[test]
    fn access_denied_maps_to_permission_denied() {
        let denied = json!([{"message": "no", "extensions": {"code": "ACCESS_DENIED"}}]);
        assert!(matches!(
            classify_graphql_errors(&denied, "q"),
            ClientError::PermissionDenied(_)
        ));
    }

    #[test]
    fn graph_not_found_display_has_no_view_suffix() {
        let err = ClientError::GraphNotFound("Graph 'g' does not exist".to_owned());
        assert_eq!(format!("{err}"), "Graph 'g' does not exist");
        assert!(!format!("{err}").contains("not found in view"));
    }

    #[test]
    fn uncoded_errors_fall_through_to_graphql_errors() {
        let other = json!([{"message": "boom"}]);
        assert!(matches!(
            classify_graphql_errors(&other, "q"),
            ClientError::GraphQLErrors(_)
        ));
    }
}
