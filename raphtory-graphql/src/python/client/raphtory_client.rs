use crate::{
    python::{
        client::remote_graph::PyRemoteGraph, encode_graph, server::is_online,
        translate_from_python, translate_map_to_python,
    },
    url_encode::url_decode_graph,
};
use pyo3::{
    exceptions::{PyException, PyValueError},
    prelude::*,
    types::PyDict,
};
use raphtory::{
    db::api::view::MaterializedGraph,
    python::utils::{errors::adapt_err_value, execute_async_task},
};
use reqwest::{header, multipart, multipart::Part, Client};
use serde_json::{json, Value as JsonValue};
use std::{collections::HashMap, fs::File, io::Read, path::Path};
use tracing::debug;

/// A client for handling GraphQL operations in the context of Raphtory.
///
/// Arguments:
///     url (str): the URL of the Raphtory GraphQL server
#[derive(Clone)]
#[pyclass(name = "RaphtoryClient", module = "raphtory.graphql")]
pub struct PyRaphtoryClient {
    pub(crate) url: String,
    pub(crate) token: String,
}

impl PyRaphtoryClient {
    pub(crate) fn query_with_json_variables(
        &self,
        query: String,
        variables: HashMap<String, JsonValue>,
    ) -> PyResult<HashMap<String, JsonValue>> {
        let client = self.clone();
        let (graphql_query, graphql_result) = execute_async_task(move || async move {
            client.send_graphql_query(query, variables).await
        })?;
        let mut graphql_result = graphql_result;
        match graphql_result.remove("data") {
            Some(JsonValue::Object(data)) => Ok(data.into_iter().collect()),
            _ => match graphql_result.remove("errors") {
                Some(JsonValue::Array(errors)) => {
                    let formatted_errors = errors
                        .iter()
                        .map(|err| format!("{}", err))
                        .collect::<Vec<_>>()
                        .join("\n\t");

                    Err(PyException::new_err(format!(
                        "After sending query to the server:\n\t{}\nGot the following errors:\n\t{}",
                        graphql_query.to_string(),
                        formatted_errors
                    )))
                }
                _ => Err(PyException::new_err(format!(
                    "Error while reading server response for query:\n\t{graphql_query}"
                ))),
            },
        }
    }

    /// Returns the query sent and the response
    async fn send_graphql_query(
        &self,
        query: String,
        variables: HashMap<String, JsonValue>,
    ) -> PyResult<(JsonValue, HashMap<String, JsonValue>)> {
        let request_body = json!({
            "query": query,
            "variables": variables
        });

        let response = Client::new()
            .post(&self.url)
            .bearer_auth(&self.token)
            .json(&request_body)
            .send()
            .await
            .map_err(|err| adapt_err_value(&err))?;

        response
            .json()
            .await
            .map_err(|err| adapt_err_value(&err))
            .map(|json| (request_body, json))
    }
}

#[pymethods]
impl PyRaphtoryClient {
    #[new]
    pub(crate) fn new(url: String, token: Option<String>) -> PyResult<Self> {
        let token = token.unwrap_or("".to_owned());
        match reqwest::blocking::Client::new()
            .get(&url)
            .bearer_auth(&token)
            .send()
        {
            Ok(response) => {
                if response.status() == 200 {
                    Ok(Self { url, token })
                } else {
                    Err(PyValueError::new_err(format!(
                        "Could not connect to the given server - response {}",
                        response.status()
                    )))
                }
            }
            Err(e) => Err(PyValueError::new_err(format!(
                "Could not connect to the given server - no response --{}",
                e.to_string()
            ))),
        }
    }

    /// Check if the server is online.
    ///
    /// Returns:
    ///    bool: Returns true if server is online otherwise false.
    fn is_server_online(&self) -> bool {
        is_online(&self.url)
    }

    /// Make a GraphQL query against the server.
    ///
    /// Arguments:
    ///   query (str): the query to make.
    ///   variables (dict[str, Any], optional): a dict of variables present on the query and their values.
    ///
    /// Returns:
    ///    dict[str, Any]: The `data` field from the graphQL response.
    #[pyo3(signature = (query, variables = None))]
    pub(crate) fn query<'py>(
        &self,
        py: Python<'py>,
        query: String,
        variables: Option<HashMap<String, Bound<'py, PyAny>>>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let variables = variables.unwrap_or_else(|| HashMap::new());
        let mut json_variables = HashMap::new();
        for (key, value) in variables {
            let json_value = translate_from_python(value)?;
            json_variables.insert(key, json_value);
        }

        let data = self.query_with_json_variables(query, json_variables)?;
        translate_map_to_python(py, data)
    }

    /// Send a graph to the server
    ///
    /// Arguments:
    ///   path (str): the path of the graph
    ///   graph (Graph | PersistentGraph): the graph to send
    ///   overwrite (bool): overwrite existing graph. Defaults to False.
    ///
    /// Returns:
    ///    dict[str, Any]: The `data` field from the graphQL response after executing the mutation.
    #[pyo3(signature = (path, graph, overwrite = false))]
    fn send_graph(&self, path: String, graph: MaterializedGraph, overwrite: bool) -> PyResult<()> {
        let encoded_graph = encode_graph(graph)?;

        let query = r#"
            mutation SendGraph($path: String!, $graph: String!, $overwrite: Boolean!) {
                sendGraph(path: $path, graph: $graph, overwrite: $overwrite)
            }
        "#
        .to_owned();
        let variables = [
            ("path".to_owned(), json!(path)),
            ("graph".to_owned(), json!(encoded_graph)),
            ("overwrite".to_owned(), json!(overwrite)),
        ];

        let data = self.query_with_json_variables(query, variables.into())?;

        match data.get("sendGraph") {
            Some(JsonValue::String(name)) => {
                debug!("Sent graph '{name}' to the server");
                Ok(())
            }
            _ => Err(PyException::new_err(format!(
                "Error Sending Graph. Got response {:?}",
                data
            ))),
        }
    }

    /// Upload graph file from a path `file_path` on the client
    ///
    /// Arguments:
    ///   path (str): the name of the graph
    ///   file_path (str): the path of the graph on the client
    ///   overwrite (bool): overwrite existing graph. Defaults to False.
    ///
    /// Returns:
    ///    dict[str, Any]: The `data` field from the graphQL response after executing the mutation.
    #[pyo3(signature = (path, file_path, overwrite = false))]
    fn upload_graph(&self, path: String, file_path: String, overwrite: bool) -> PyResult<()> {
        let remote_client = self.clone();
        execute_async_task(move || async move {
            let client = Client::new();

            let mut file =
                File::open(Path::new(&file_path)).map_err(|err| adapt_err_value(&err))?;

            let mut buffer = Vec::new();
            file.read_to_end(&mut buffer)
                .map_err(|err| adapt_err_value(&err))?;

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
                .part("0", Part::bytes(buffer).file_name(file_path.clone()));

            let response = client
                .post(&remote_client.url)
                .bearer_auth(&remote_client.token)
                .multipart(form)
                .send()
                .await
                .map_err(|err| adapt_err_value(&err))?;

            let status = response.status();
            let text = response.text().await.map_err(|err| adapt_err_value(&err))?;

            if !status.is_success() {
                return Err(PyException::new_err(format!(
                    "Error Uploading Graph. Status: {}. Response: {}",
                    status, text
                )));
            }

            let mut data: HashMap<String, JsonValue> =
                serde_json::from_str(&text).map_err(|err| {
                    PyException::new_err(format!(
                        "Failed to parse JSON response: {}. Response text: {}",
                        err, text
                    ))
                })?;

            match data.remove("data") {
                Some(JsonValue::Object(_)) => Ok(()),
                _ => match data.remove("errors") {
                    Some(JsonValue::Array(errors)) => Err(PyException::new_err(format!(
                        "Error Uploading Graph. Got errors:\n\t{:#?}",
                        errors
                    ))),
                    _ => Err(PyException::new_err(format!(
                        "Error Uploading Graph. Unexpected response: {}",
                        text
                    ))),
                },
            }
        })
    }

    /// Copy graph from a path `path` on the server to a `new_path` on the server
    ///
    /// Arguments:
    ///   path (str): the path of the graph to be copied
    ///   new_path (str): the new path of the copied graph
    ///
    /// Returns:
    ///    None:
    #[pyo3(signature = (path, new_path))]
    fn copy_graph(&self, path: String, new_path: String) -> PyResult<()> {
        let query = r#"
            mutation CopyGraph($path: String!, $newPath: String!) {
              copyGraph(
                path: $path,
                newPath: $newPath,
              )
            }"#
        .to_owned();

        let variables = [
            ("path".to_owned(), json!(path)),
            ("newPath".to_owned(), json!(new_path)),
        ];

        let data = self.query_with_json_variables(query.clone(), variables.into())?;
        match data.get("copyGraph") {
            Some(JsonValue::Bool(res)) => Ok((*res).clone()),
            _ => Err(PyException::new_err(format!(
                "Error while reading server response for query:\n\t{query}\nGot data:\n\t'{data:?}'"
            ))),
        }?;
        Ok(())
    }

    /// Move graph from a path `path` on the server to a `new_path` on the server
    ///
    /// Arguments:
    ///   path (str): the path of the graph to be moved
    ///   new_path (str): the new path of the moved graph
    ///
    /// Returns:
    ///    None:
    #[pyo3(signature = (path, new_path))]
    fn move_graph(&self, path: String, new_path: String) -> PyResult<()> {
        let query = r#"
            mutation MoveGraph($path: String!, $newPath: String!) {
              moveGraph(
                path: $path,
                newPath: $newPath,
              )
            }"#
        .to_owned();

        let variables = [
            ("path".to_owned(), json!(path)),
            ("newPath".to_owned(), json!(new_path)),
        ];

        let data = self.query_with_json_variables(query.clone(), variables.into())?;
        match data.get("moveGraph") {
            Some(JsonValue::Bool(res)) => Ok((*res).clone()),
            _ => Err(PyException::new_err(format!(
                "Error while reading server response for query:\n\t{query}\nGot data:\n\t'{data:?}'"
            ))),
        }?;
        Ok(())
    }

    /// Delete graph from a path `path` on the server
    ///
    /// Arguments:
    ///   path (str): the path of the graph to be deleted
    ///
    /// Returns:
    ///     None:
    #[pyo3(signature = (path))]
    fn delete_graph(&self, path: String) -> PyResult<()> {
        let query = r#"
            mutation DeleteGraph($path: String!) {
              deleteGraph(
                path: $path,
              )
            }"#
        .to_owned();

        let variables = [("path".to_owned(), json!(path))];

        let data = self.query_with_json_variables(query.clone(), variables.into())?;
        match data.get("deleteGraph") {
            Some(JsonValue::Bool(res)) => Ok((*res).clone()),
            _ => Err(PyException::new_err(format!(
                "Error while reading server response for query:\n\t{query}\nGot data:\n\t'{data:?}'"
            ))),
        }?;
        Ok(())
    }

    /// Receive graph from a path `path` on the server
    ///
    /// Note:
    ///     This downloads a copy of the graph. Modifications are not persistet to the server.
    ///
    /// Arguments:
    ///   path (str): the path of the graph to be received
    ///
    /// Returns:
    ///    Union[Graph, PersistentGraph]: A copy of the graph
    fn receive_graph(&self, path: String) -> PyResult<MaterializedGraph> {
        let query = r#"
            query ReceiveGraph($path: String!) {
                receiveGraph(path: $path)
            }"#
        .to_owned();
        let variables = [("path".to_owned(), json!(path))];
        let data = self.query_with_json_variables(query.clone(), variables.into())?;
        match data.get("receiveGraph") {
            Some(JsonValue::String(graph)) => {
                let mat_graph = url_decode_graph(graph)?;
                Ok(mat_graph)
            }
            _ => Err(PyException::new_err(format!(
                "Error while reading server response for query:\n\t{query}\nGot data:\n\t'{data:?}'"
            ))),
        }
    }

    /// Create a new empty Graph on the server at `path`
    ///
    /// Arguments:
    ///   path (str): the path of the graph to be created
    ///   graph_type (Literal["EVENT", "PERSISTENT"]): the type of graph that should be created - this can be EVENT or PERSISTENT
    ///
    /// Returns:
    ///    None:
    ///
    fn new_graph(&self, path: String, graph_type: String) -> PyResult<()> {
        let query = r#"
            mutation NewGraph($path: String!) {
              newGraph(
                path: $path,
                graphType: EVENT
              )
            }"#
        .to_owned();
        let query = query.replace("EVENT", &*graph_type);

        let variables = [("path".to_owned(), json!(path))];

        let data = self.query_with_json_variables(query.clone(), variables.into())?;
        match data.get("newGraph") {
            Some(JsonValue::Bool(res)) => Ok((*res).clone()),
            _ => Err(PyException::new_err(format!(
                "Error while reading server response for query:\n\t{query}\nGot data:\n\t'{data:?}'"
            ))),
        }?;
        Ok(())
    }

    /// Get a RemoteGraph reference to a graph on the server at `path`
    ///
    /// Arguments:
    ///   path (str): the path of the graph to be created
    ///
    /// Returns:
    ///    RemoteGraph: the remote graph reference
    ///
    fn remote_graph(&self, path: String) -> PyRemoteGraph {
        PyRemoteGraph {
            path,
            client: self.clone(),
        }
    }
}
