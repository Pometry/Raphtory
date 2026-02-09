//! Error type for the GraphQL client.

use thiserror::Error;

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("Network/request error: {0}")]
    Request(#[from] reqwest::Error),

    #[error("HTTP error: status {0}, body: {1}")]
    HttpStatus(u16, String),

    #[error("GraphQL errors: {0}")]
    GraphQLErrors(String),

    #[error("Invalid response: {0}")]
    InvalidResponse(String),

    #[error("JSON parse error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Graph encode/decode error: {0}")]
    Graph(#[from] raphtory::errors::GraphError),
}
