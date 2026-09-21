use crate::{
    client::{error::classify_graphql_errors, ClientError, RemoteGraph},
    data::GqlGraphType,
    url_encode::url_decode_graph,
};
use raphtory::{db::api::view::MaterializedGraph, prelude::Args};
use raphtory_api::core::storage::graph_folder::GraphFolder;
use reqwest::{multipart, multipart::Part, Client};
use serde_json::{json, Value as JsonValue};
use std::{collections::HashMap, io::Cursor};
use url::Url;

/// Client for interacting with a Raphtory GraphQL server.
#[derive(Clone, Debug)]
pub struct RemoteClient {
    pub(crate) url: Url,
    pub(crate) token: String,
    client: Client,
}

impl RemoteClient {
    /// Create a new client. Does not perform a connectivity check; use [`client::is_online`] first if needed.
    pub fn new(url: Url, token: Option<String>) -> Self {
        Self {
            url,
            token: token.unwrap_or_default(),
            client: Client::new(),
        }
    }

    /// Create a new client and verify the server is reachable (GET url, expect 200).
    /// Returns an error if the server is not reachable.
    pub async fn connect(url: Url, token: Option<String>) -> Result<Self, ClientError> {
        let token = token.unwrap_or_default();
        let client = Client::new();

        let response = client
            .get(url.clone())
            .bearer_auth(&token)
            .send()
            .await
            .map_err(|e| {
                ClientError::HttpError(format!(
                    "Could not connect to the given server - no response --{e}"
                ))
            })?;
        if response.status() != 200 {
            let text = response.text().await.unwrap_or_default();
            return Err(ClientError::HttpError(format!(
                "Could not connect to the given server - response {}",
                text
            )));
        }

        Ok(Self { url, token, client })
    }

    /// Return a copy of this client that authenticates with `token` instead of
    /// the current one. Purely client-side — performs no server round-trip and
    /// reuses the same underlying HTTP connection pool.
    pub fn with_token(&self, token: impl Into<String>) -> Self {
        Self {
            url: self.url.clone(),
            token: token.into(),
            client: self.client.clone(),
        }
    }

    /// Returns true if the server could be reached and returns a healthy response.
    pub async fn is_healthy(&self) -> bool {
        // `join` fails for cannot-be-a-base URLs (`mailto:` and friends); such a
        // server is simply not reachable, so report unhealthy rather than panic.
        let Ok(health_url) = self.url.join("health") else {
            return false;
        };

        let response_res = self
            .client
            .get(health_url)
            .bearer_auth(&self.token)
            .send()
            .await;

        if let Ok(response) = response_res {
            if response.status().is_success() {
                if let Ok(v) = response.json::<JsonValue>().await {
                    if v.get("healthy") == Some(&JsonValue::Bool(true)) {
                        return true;
                    }
                }
            }
        }

        false
    }

    /// Execute a GraphQL query asynchronously.
    /// Returns the `data` object as a map; errors if the response contains GraphQL `errors`.
    pub async fn query(
        &self,
        query: &str,
        variables: JsonValue,
    ) -> Result<HashMap<String, JsonValue>, ClientError> {
        let request_body = json!({
            "query": query,
            "variables": variables
        });

        let response = self
            .client
            .post(self.url.clone())
            .bearer_auth(&self.token)
            .json(&request_body)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status().as_u16();
            let text = response.text().await.unwrap_or_default();
            return Err(ClientError::HttpError(format!(
                "HTTP error: status {status}, body: {text}"
            )));
        }

        let mut graphql_result: HashMap<String, JsonValue> = response.json().await?;

        if let Some(errors) = graphql_result.remove("errors") {
            return Err(classify_graphql_errors(&errors, query));
        }

        match graphql_result.remove("data") {
            Some(JsonValue::Object(data)) => Ok(data.into_iter().collect()),
            _ => Err(ClientError::InvalidResponse(format!(
                "Error while reading server response for query:\n\t{query}"
            ))),
        }
    }

    /// Send a graph (base64-encoded string) to the server.
    pub async fn send_graph(
        &self,
        path: &str,
        encoded_graph: &str,
        overwrite: bool,
    ) -> Result<(), ClientError> {
        let query = r#"
            mutation SendGraph($path: String!, $graph: String!, $overwrite: Boolean!) {
                sendGraph(path: $path, graph: $graph, overwrite: $overwrite)
            }
        "#
        .to_owned();
        let variables = json!({
            "path": json!(path),
            "graph": json!(encoded_graph),
            "overwrite": json!(overwrite),
        });

        let data = self.query(&query, variables).await?;
        match data.get("sendGraph") {
            Some(JsonValue::String(_)) => Ok(()),
            _ => Err(ClientError::InvalidResponse(format!(
                "Error Sending Graph. Got response {:?}",
                data
            ))),
        }
    }

    /// Upload a graph from a local file path (zip) via multipart.
    pub async fn upload_graph(
        &self,
        path: &str,
        file_path: &str,
        overwrite: bool,
    ) -> Result<(), ClientError> {
        let folder = GraphFolder::from(file_path);
        let mut buffer = Vec::new();
        folder.zip_from_folder(Cursor::new(&mut buffer))?;

        // Build the operations object with `json!` so `path` is escaped — a path
        // containing a quote or backslash would otherwise break out of the
        // hand-written JSON string.
        let operations = json!({
            "query": "mutation UploadGraph($path: String!, $graph: Upload!, $overwrite: Boolean!) { uploadGraph(path: $path, graph: $graph, overwrite: $overwrite) }",
            "variables": { "path": path, "overwrite": overwrite, "graph": null },
        })
        .to_string();

        let form = multipart::Form::new()
            .text("operations", operations)
            .text("map", r#"{"0": ["variables.graph"]}"#)
            .part("0", Part::bytes(buffer).file_name(file_path.to_string()));

        let response = self
            .client
            .post(self.url.clone())
            .bearer_auth(&self.token)
            .multipart(form)
            .send()
            .await?;

        let status = response.status();
        let text = response.text().await?;

        if !status.is_success() {
            return Err(ClientError::HttpError(format!(
                "Error Uploading Graph. Status: {}. Response: {}",
                status.as_u16(),
                text
            )));
        }

        let mut data: HashMap<String, JsonValue> = serde_json::from_str(&text)?;
        match data.remove("data") {
            Some(JsonValue::Object(_)) => Ok(()),
            // Route errors through the shared classifier so `ACCESS_DENIED` /
            // `GRAPH_NOT_FOUND` map to `PermissionDenied` / `GraphNotFound` here
            // too — keeping the existence-non-disclosure shape consistent with
            // every other op instead of a bare `GraphQLErrors`.
            _ => match data.remove("errors") {
                Some(errors) => Err(classify_graphql_errors(&errors, "uploadGraph")),
                _ => Err(ClientError::InvalidResponse(format!(
                    "Error Uploading Graph. Unexpected response: {}",
                    text
                ))),
            },
        }
    }

    /// Copy graph on the server.
    pub async fn copy_graph(&self, path: &str, new_path: &str) -> Result<(), ClientError> {
        let query = r#"
            mutation CopyGraph($path: String!, $newPath: String!) {
              copyGraph(path: $path, newPath: $newPath)
            }"#
        .to_owned();
        let variables = json!({
            "path": json!(path),
            "newPath": json!(new_path),
        });

        let data = self.query(&query, variables).await?;
        match data.get("copyGraph") {
            Some(JsonValue::Bool(true)) => Ok(()),
            _ => Err(ClientError::InvalidResponse(format!(
                "Error while reading server response for query:\n\t{query}\nGot data:\n\t'{data:?}'"
            ))),
        }
    }

    /// Move graph on the server.
    pub async fn move_graph(&self, path: &str, new_path: &str) -> Result<(), ClientError> {
        let query = r#"
            mutation MoveGraph($path: String!, $newPath: String!) {
              moveGraph(path: $path, newPath: $newPath)
            }"#
        .to_owned();
        let variables = json!({
            "path": json!(path),
            "newPath": json!(new_path),
        });

        let data = self.query(&query, variables).await?;
        match data.get("moveGraph") {
            Some(JsonValue::Bool(true)) => Ok(()),
            _ => Err(ClientError::InvalidResponse(format!(
                "Error while reading server response for query:\n\t{query}\nGot data:\n\t'{data:?}'"
            ))),
        }
    }

    /// Delete graph on the server.
    pub async fn delete_graph(&self, path: &str) -> Result<(), ClientError> {
        let query = r#"
            mutation DeleteGraph($path: String!) {
              deleteGraph(path: $path)
            }"#
        .to_owned();
        let variables = json!({
            "path": json!(path),
        });

        let data = self.query(&query, variables).await?;
        match data.get("deleteGraph") {
            Some(JsonValue::Bool(true)) => Ok(()),
            _ => Err(ClientError::InvalidResponse(format!(
                "Error while reading server response for query:\n\t{query}\nGot data:\n\t'{data:?}'"
            ))),
        }
    }

    /// Receive graph from the server. Returns the base64-encoded graph string.
    pub async fn receive_graph(&self, path: &str) -> Result<String, ClientError> {
        let query = r#"
            query ReceiveGraph($path: String!) {
                receiveGraph(path: $path)
            }"#
        .to_owned();
        let variables = json!({
            "path": json!(path),
        });

        let data = self.query(&query, variables).await?;
        match data.get("receiveGraph") {
            Some(JsonValue::String(s)) => Ok(s.clone()),
            _ => Err(ClientError::InvalidResponse(format!(
                "Error while reading server response for query:\n\t{query}\nGot data:\n\t'{data:?}'"
            ))),
        }
    }

    /// Receive graph from the server and decode to MaterializedGraph.
    pub async fn receive_graph_decoded(
        &self,
        path: &str,
    ) -> Result<MaterializedGraph, ClientError> {
        let encoded = self.receive_graph(path).await?;
        url_decode_graph(encoded, Args::default()).map_err(ClientError::from)
    }

    /// Create a new empty graph on the server.
    pub async fn new_graph(&self, path: &str, graph_type: GqlGraphType) -> Result<(), ClientError> {
        // `graphType` is a GraphQL enum, so the value is spliced in as a bare
        // token. Taking the enum rather than a string means there is no
        // unvalidated value to splice.
        let query = r#"
            mutation NewGraph($path: String!) {
              newGraph(path: $path, graphType: EVENT)
            }"#
        .to_owned()
        .replace("EVENT", graph_type.as_gql());

        let variables = json!({
            "path": json!(path),
        });

        let data = self.query(&query, variables).await?;
        match data.get("newGraph") {
            Some(JsonValue::Bool(true)) => Ok(()),
            _ => Err(ClientError::InvalidResponse(format!(
                "Error while reading server response for query:\n\t{query}\nGot data:\n\t'{data:?}'"
            ))),
        }
    }

    pub fn remote_graph(&self, path: String) -> RemoteGraph {
        RemoteGraph::new(path, self.clone())
    }
}
