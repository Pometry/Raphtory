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

/// Scheme of server URLs that point at a unix socket, e.g. `unix:///path/to/raphtory.sock`.
const UNIX_SCHEME: &str = "unix";

/// Build the HTTP client for a server URL, together with the URL its requests go to.
///
/// A `unix:///path/to/raphtory.sock` URL routes every request through that socket
/// and sends it to `/`; any other URL is used as-is over TCP.
fn http_client(url: Url) -> Result<(Url, Client), ClientError> {
    if url.scheme() == UNIX_SCHEME {
        unix_socket_client(&url)
    } else {
        Ok((url, Client::new()))
    }
}

#[cfg(unix)]
fn unix_socket_client(url: &Url) -> Result<(Url, Client), ClientError> {
    let socket = url.to_file_path().map_err(|_| {
        ClientError::InvalidInput(format!(
            "Invalid unix socket URL '{url}': expected unix:///absolute/path/to/socket"
        ))
    })?;
    let client = Client::builder().unix_socket(socket).build()?;
    // The socket alone decides where requests go; this URL only supplies the
    // request target and the `Host` header.
    let url = Url::parse("http://localhost/").expect("static URL is valid");
    Ok((url, client))
}

#[cfg(not(unix))]
fn unix_socket_client(url: &Url) -> Result<(Url, Client), ClientError> {
    Err(ClientError::InvalidInput(format!(
        "Unix socket URL '{url}' is not supported on this platform"
    )))
}

/// Client for interacting with a Raphtory GraphQL server.
#[derive(Clone, Debug)]
pub struct RemoteClient {
    /// Where requests are sent. For a `unix://` server URL this is `http://localhost/`
    /// and `client` carries the socket.
    pub(crate) url: Url,
    pub(crate) token: String,
    client: Client,
}

impl RemoteClient {
    /// Create a new client. Does not perform a connectivity check; use [`RemoteClient::is_online`] first if needed.
    ///
    /// `url` is either an `http(s)://` URL or `unix:///path/to/socket`.
    ///
    /// # Panics
    ///
    /// If `url` is a malformed `unix://` URL, or one used on a non-unix platform.
    /// Use [`RemoteClient::connect`] to get these as errors instead.
    pub fn new(url: Url, token: Option<String>) -> Self {
        let (url, client) = http_client(url).unwrap_or_else(|e| panic!("{e}"));
        Self {
            url,
            token: token.unwrap_or_default(),
            client,
        }
    }

    /// Create a new client and verify the server is reachable (GET url, expect 200).
    /// Returns an error if the server is not reachable.
    ///
    /// `url` is either an `http(s)://` URL or `unix:///path/to/socket`.
    pub async fn connect(url: Url, token: Option<String>) -> Result<Self, ClientError> {
        let token = token.unwrap_or_default();
        let (url, client) = http_client(url)?;

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

    /// Returns true if the server responds to a GET on its URL with 200. Unlike
    /// [`client::is_online`](crate::client::is_online), this goes through the
    /// client's own transport, so it also works for `unix://` URLs.
    pub async fn is_online(&self) -> bool {
        self.client
            .get(self.url.clone())
            .send()
            .await
            .map(|response| response.status().as_u16() == 200)
            .unwrap_or(false)
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
        "#;
        let variables = json!({
            "path": json!(path),
            "graph": json!(encoded_graph),
            "overwrite": json!(overwrite),
        });

        let data = self.query(query, variables).await?;
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

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::server::GraphServer;
    use tempfile::tempdir;
    use tokio::net::{TcpStream, UnixListener};

    #[test]
    fn unix_url_sends_requests_to_root() {
        let (url, _) = http_client(Url::parse("unix:///tmp/raphtory.sock").unwrap()).unwrap();
        assert_eq!(url.as_str(), "http://localhost/");
    }

    #[test]
    fn http_url_is_kept() {
        let (url, _) = http_client(Url::parse("http://localhost:1736/").unwrap()).unwrap();
        assert_eq!(url.as_str(), "http://localhost:1736/");
    }

    #[test]
    fn unix_url_with_host_is_rejected() {
        let err = http_client(Url::parse("unix://somehost/tmp/raphtory.sock").unwrap());
        assert!(matches!(err, Err(ClientError::InvalidInput(_))));
    }

    /// Forward every connection on a unix socket to the server's TCP port, as the
    /// sandbox does, and drive the client end to end through it.
    #[tokio::test]
    async fn client_and_remote_graph_work_over_unix_socket() {
        let tmp_dir = tempdir().unwrap();
        let server = GraphServer::new(tmp_dir.path().join("graphs"), None, Args::default())
            .await
            .unwrap();
        let running = server.start_with_port(0).await.unwrap();
        let port = running.port();

        let socket = tmp_dir.path().join("raphtory.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        tokio::spawn(async move {
            while let Ok((mut unix, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut tcp = TcpStream::connect(("localhost", port)).await.unwrap();
                    let _ = tokio::io::copy_bidirectional(&mut unix, &mut tcp).await;
                });
            }
        });

        let url = Url::parse(&format!("unix://{}", socket.display())).unwrap();
        let client = RemoteClient::connect(url, None).await.unwrap();
        assert!(client.is_online().await);
        assert!(client.is_healthy().await);

        client
            .new_graph("unix-graph", GqlGraphType::Event)
            .await
            .unwrap();
        let rg = client.remote_graph("unix-graph".into());
        rg.add_node(1i64, "ben", raphtory::prelude::NO_PROPS, None, None)
            .await
            .unwrap();
        let data = client
            .query(
                r#"{ graph(path: "unix-graph") { countNodes } }"#,
                serde_json::json!({}),
            )
            .await
            .unwrap();
        assert_eq!(data["graph"]["countNodes"], 1);
    }
}
