//! Pure Rust GraphQL client for Raphtory GraphQL server.
//!
//! No Python dependency; usable from Rust and from PyO3 bindings.

mod error;
pub mod remote_graph;

pub use error::ClientError;
pub use remote_graph::GraphQLRemoteGraph;

use crate::url_encode::url_decode_graph;
use raphtory::{
    db::api::{storage::storage::Config, view::MaterializedGraph},
    serialise::GraphFolder,
};
use raphtory_api::core::entities::properties::prop::Prop;
use reqwest::{multipart, multipart::Part, Client};
use serde_json::{json, Value as JsonValue};
use std::{collections::HashMap, io::Cursor};

/// Check if a server at the given URL is online (responds with 200).
pub fn is_online(url: &str) -> bool {
    reqwest::blocking::Client::new()
        .get(url)
        .send()
        .map(|response| response.status().as_u16() == 200)
        .unwrap_or(false)
}

/// Pure Rust client for Raphtory GraphQL operations.
#[derive(Clone)]
pub struct RaphtoryGraphQLClient {
    pub(crate) url: String,
    pub(crate) token: String,
    client: Client,
}

impl RaphtoryGraphQLClient {
    /// Create a new client. Does not perform a connectivity check; use [`is_online`] first if needed.
    pub fn new(url: String, token: String) -> Self {
        Self {
            url: url.clone(),
            token,
            client: Client::new(),
        }
    }

    /// Create a new client and verify the server is reachable (GET url, expect 200).
    /// Returns an error if the server is not reachable.
    pub fn connect(url: String, token: Option<String>) -> Result<Self, ClientError> {
        let token = token.unwrap_or_default();
        let client = reqwest::blocking::Client::new();
        let response = client
            .get(&url)
            .bearer_auth(&token)
            .send()
            .map_err(ClientError::from)?;
        if response.status() != 200 {
            let status = response.status().as_u16();
            let text = response.text().unwrap_or_default();
            return Err(ClientError::HttpStatus(
                status,
                format!(
                    "Could not connect to the given server - response {}",
                    status
                ),
            ));
        }
        Ok(Self {
            url,
            token,
            client: Client::new(),
        })
    }

    /// Execute a GraphQL query asynchronously.
    /// Returns the `data` object as a map; errors if the response contains GraphQL `errors`.
    pub async fn query_async(
        &self,
        query: &str,
        variables: HashMap<String, JsonValue>,
    ) -> Result<HashMap<String, JsonValue>, ClientError> {
        let request_body = json!({
            "query": query,
            "variables": variables
        });

        let response = self
            .client
            .post(&self.url)
            .bearer_auth(&self.token)
            .json(&request_body)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status().as_u16();
            let text = response.text().await.unwrap_or_default();
            return Err(ClientError::HttpStatus(status, text));
        }

        let mut graphql_result: HashMap<String, JsonValue> = response.json().await?;

        if let Some(errors) = graphql_result.remove("errors") {
            let message = match errors {
                JsonValue::Array(errors) => errors
                    .iter()
                    .map(|e| format!("{}", e))
                    .collect::<Vec<_>>()
                    .join("\n\t"),
                _ => format!("{}", errors),
            };
            return Err(ClientError::GraphQLErrors(format!(
                "After sending query to the server:\n\t{}\nGot the following errors:\n\t{}",
                query, message
            )));
        }

        match graphql_result.remove("data") {
            Some(JsonValue::Object(data)) => Ok(data.into_iter().collect()),
            _ => Err(ClientError::InvalidResponse(format!(
                "Error while reading server response for query:\n\t{query}"
            ))),
        }
    }

    /// Send a graph (base64-encoded string) to the server.
    pub async fn send_graph_async(
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
        let variables: HashMap<String, JsonValue> = [
            ("path".to_owned(), json!(path)),
            ("graph".to_owned(), json!(encoded_graph)),
            ("overwrite".to_owned(), json!(overwrite)),
        ]
        .into_iter()
        .collect();

        let data = self.query_async(&query, variables).await?;
        match data.get("sendGraph") {
            Some(JsonValue::String(_)) => Ok(()),
            _ => Err(ClientError::InvalidResponse(format!(
                "Error Sending Graph. Got response {:?}",
                data
            ))),
        }
    }

    /// Upload a graph from a local file path (zip) via multipart.
    pub async fn upload_graph_async(
        &self,
        path: &str,
        file_path: &str,
        overwrite: bool,
    ) -> Result<(), ClientError> {
        let folder = GraphFolder::from(file_path);
        let mut buffer = Vec::new();
        folder.zip_from_folder(Cursor::new(&mut buffer))?;

        let variables = format!(
            r#""path": "{}", "overwrite": {}, "graph": null"#,
            path, overwrite
        );
        let operations = format!(
            r#"{{
            "query": "mutation UploadGraph($path: String!, $graph: Upload!, $overwrite: Boolean!) {{ uploadGraph(path: $path, graph: $graph, overwrite: $overwrite) }}",
            "variables": {{ {} }}
        }}"#,
            variables
        );

        let form = multipart::Form::new()
            .text("operations", operations)
            .text("map", r#"{"0": ["variables.graph"]}"#)
            .part("0", Part::bytes(buffer).file_name(file_path.to_string()));

        let response = self
            .client
            .post(&self.url)
            .bearer_auth(&self.token)
            .multipart(form)
            .send()
            .await?;

        let status = response.status();
        let text = response.text().await?;

        if !status.is_success() {
            return Err(ClientError::HttpStatus(
                status.as_u16(),
                format!(
                    "Error Uploading Graph. Status: {}. Response: {}",
                    status, text
                ),
            ));
        }

        let mut data: HashMap<String, JsonValue> = serde_json::from_str(&text)?;
        match data.remove("data") {
            Some(JsonValue::Object(_)) => Ok(()),
            _ => match data.remove("errors") {
                Some(JsonValue::Array(errors)) => Err(ClientError::GraphQLErrors(format!(
                    "Error Uploading Graph. Got errors:\n\t{:#?}",
                    errors
                ))),
                _ => Err(ClientError::InvalidResponse(format!(
                    "Error Uploading Graph. Unexpected response: {}",
                    text
                ))),
            },
        }
    }

    /// Copy graph on the server.
    pub async fn copy_graph_async(&self, path: &str, new_path: &str) -> Result<(), ClientError> {
        let query = r#"
            mutation CopyGraph($path: String!, $newPath: String!) {
              copyGraph(path: $path, newPath: $newPath)
            }"#
        .to_owned();
        let variables: HashMap<String, JsonValue> = [
            ("path".to_owned(), json!(path)),
            ("newPath".to_owned(), json!(new_path)),
        ]
        .into_iter()
        .collect();

        let data = self.query_async(&query, variables).await?;
        match data.get("copyGraph") {
            Some(JsonValue::Bool(true)) => Ok(()),
            _ => Err(ClientError::InvalidResponse(format!(
                "Error while reading server response for query:\n\t{query}\nGot data:\n\t'{data:?}'"
            ))),
        }
    }

    /// Move graph on the server.
    pub async fn move_graph_async(&self, path: &str, new_path: &str) -> Result<(), ClientError> {
        let query = r#"
            mutation MoveGraph($path: String!, $newPath: String!) {
              moveGraph(path: $path, newPath: $newPath)
            }"#
        .to_owned();
        let variables: HashMap<String, JsonValue> = [
            ("path".to_owned(), json!(path)),
            ("newPath".to_owned(), json!(new_path)),
        ]
        .into_iter()
        .collect();

        let data = self.query_async(&query, variables).await?;
        match data.get("moveGraph") {
            Some(JsonValue::Bool(true)) => Ok(()),
            _ => Err(ClientError::InvalidResponse(format!(
                "Error while reading server response for query:\n\t{query}\nGot data:\n\t'{data:?}'"
            ))),
        }
    }

    /// Delete graph on the server.
    pub async fn delete_graph_async(&self, path: &str) -> Result<(), ClientError> {
        let query = r#"
            mutation DeleteGraph($path: String!) {
              deleteGraph(path: $path)
            }"#
        .to_owned();
        let variables: HashMap<String, JsonValue> =
            [("path".to_owned(), json!(path))].into_iter().collect();

        let data = self.query_async(&query, variables).await?;
        match data.get("deleteGraph") {
            Some(JsonValue::Bool(true)) => Ok(()),
            _ => Err(ClientError::InvalidResponse(format!(
                "Error while reading server response for query:\n\t{query}\nGot data:\n\t'{data:?}'"
            ))),
        }
    }

    /// Receive graph from the server. Returns the base64-encoded graph string.
    pub async fn receive_graph_async(&self, path: &str) -> Result<String, ClientError> {
        let query = r#"
            query ReceiveGraph($path: String!) {
                receiveGraph(path: $path)
            }"#
        .to_owned();
        let variables: HashMap<String, JsonValue> =
            [("path".to_owned(), json!(path))].into_iter().collect();

        let data = self.query_async(&query, variables).await?;
        match data.get("receiveGraph") {
            Some(JsonValue::String(s)) => Ok(s.clone()),
            _ => Err(ClientError::InvalidResponse(format!(
                "Error while reading server response for query:\n\t{query}\nGot data:\n\t'{data:?}'"
            ))),
        }
    }

    /// Receive graph from the server and decode to MaterializedGraph.
    pub async fn receive_graph_decoded_async(
        &self,
        path: &str,
    ) -> Result<MaterializedGraph, ClientError> {
        let encoded = self.receive_graph_async(path).await?;
        url_decode_graph(encoded, Config::default()).map_err(ClientError::from)
    }

    /// Create a new empty graph on the server.
    pub async fn new_graph_async(&self, path: &str, graph_type: &str) -> Result<(), ClientError> {
        let query = r#"
            mutation NewGraph($path: String!) {
              newGraph(path: $path, graphType: EVENT)
            }"#
        .to_owned()
        .replace("EVENT", graph_type);

        let variables: HashMap<String, JsonValue> =
            [("path".to_owned(), json!(path))].into_iter().collect();

        let data = self.query_async(&query, variables).await?;
        match data.get("newGraph") {
            Some(JsonValue::Bool(true)) => Ok(()),
            _ => Err(ClientError::InvalidResponse(format!(
                "Error while reading server response for query:\n\t{query}\nGot data:\n\t'{data:?}'"
            ))),
        }
    }

    /// Create index on the server. `index_spec` must serialize to the GraphQL IndexSpecInput shape.
    pub async fn create_index_async(
        &self,
        path: &str,
        index_spec: JsonValue,
        in_ram: bool,
    ) -> Result<(), ClientError> {
        let query = r#"
            mutation CreateIndex($path: String!, $indexSpec: IndexSpecInput!, $inRam: Boolean!) {
                createIndex(path: $path, indexSpec: $indexSpec, inRam: $inRam)
            }
        "#
        .to_owned();

        let variables: HashMap<String, JsonValue> = [
            ("path".to_string(), json!(path)),
            ("indexSpec".to_string(), index_spec),
            ("inRam".to_string(), json!(in_ram)),
        ]
        .into_iter()
        .collect();

        let data = self.query_async(&query, variables).await?;
        match data.get("createIndex") {
            Some(JsonValue::Bool(true)) => Ok(()),
            _ => Err(ClientError::InvalidResponse(format!(
                "Failed to create index, server returned: {:?}",
                data
            ))),
        }
    }
}

pub(crate) fn inner_collection(value: &Prop) -> String {
    match value {
        Prop::Str(value) => format!("{{ str: {} }}", serde_json::to_string(value).unwrap()),
        Prop::U8(value) => format!("{{ u64: {} }}", value),
        Prop::U16(value) => format!("{{ u64: {} }}", value),
        Prop::I32(value) => format!("{{ i64: {} }}", value),
        Prop::I64(value) => format!("{{ i64: {} }}", value),
        Prop::U32(value) => format!("{{ u64: {} }}", value),
        Prop::U64(value) => format!("{{ u64: {} }}", value),
        Prop::F32(value) => format!("{{ f64: {} }}", value),
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
        Prop::DTime(value) => format!("{{ str: {} }}", serde_json::to_string(value).unwrap()),
        Prop::NDTime(value) => format!("{{ str: {} }}", serde_json::to_string(value).unwrap()),
        Prop::Decimal(value) => format!("{{ decimal: {} }}", value),
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
            "{{ key: {}, value: {{ u64: {} }} }}",
            serde_json::to_string(key).unwrap(),
            value
        ),
        Prop::U16(value) => format!(
            "{{ key: {}, value: {{ u64: {} }} }}",
            serde_json::to_string(key).unwrap(),
            value
        ),
        Prop::I32(value) => format!(
            "{{ key: {}, value: {{ i64: {} }} }}",
            serde_json::to_string(key).unwrap(),
            value
        ),
        Prop::I64(value) => format!(
            "{{ key: {}, value: {{ i64: {} }} }}",
            serde_json::to_string(key).unwrap(),
            value
        ),
        Prop::U32(value) => format!(
            "{{ key: {}, value: {{ u64: {} }} }}",
            serde_json::to_string(key).unwrap(),
            value
        ),
        Prop::U64(value) => format!(
            "{{ key: {}, value: {{ u64: {} }} }}",
            serde_json::to_string(key).unwrap(),
            value
        ),
        Prop::F32(value) => format!(
            "{{ key: {}, value: {{ f64: {} }} }}",
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
        Prop::DTime(value) => format!(
            "{{ key: {}, value: {{ str: {} }} }}",
            serde_json::to_string(key).unwrap(),
            serde_json::to_string(value).unwrap()
        ),
        Prop::NDTime(value) => format!(
            "{{ key: {}, value: {{ str: {} }} }}",
            serde_json::to_string(key).unwrap(),
            serde_json::to_string(value).unwrap()
        ),
        Prop::Decimal(value) => format!(
            "{{ key: {}, value: {{ decimal: \"{}\" }} }}",
            serde_json::to_string(key).unwrap(),
            value
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
