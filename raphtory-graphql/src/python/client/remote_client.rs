use crate::{
    client::{is_online, remote_client::RemoteClient, ClientError},
    data::GqlGraphType,
    model::graph::filtering::{GqlEdgeFilter, GqlNodeFilter},
    python::{
        client::remote_graph::PyRemoteGraph, encode_graph, translate_from_python,
        translate_map_to_python, translate_to_python,
    },
};
use pyo3::{
    exceptions::{PyException, PyValueError},
    prelude::*,
    types::PyDict,
};
use raphtory::{
    db::api::view::MaterializedGraph,
    errors::GraphError,
    python::{filter::filter_expr::PyFilterExpr, utils::execute_async_task},
};
use serde_json::{json, Value as JsonValue};
use std::{collections::HashMap, future::Future, str::FromStr, sync::Arc};
use tracing::debug;
use url::Url;

/// A client for handling GraphQL operations in the context of Raphtory.
///
/// Arguments:
///     url (str): the URL of the Raphtory GraphQL server
///     token (str, optional): a bearer token sent with every request; omit for
///         an unauthenticated server.
#[derive(Clone)]
#[pyclass(name = "RaphtoryClient", module = "raphtory.graphql", from_py_object)]
pub struct PyRaphtoryClient {
    pub(crate) client: RemoteClient,
}

impl PyRaphtoryClient {
    /// The underlying client, for callers building on top of it.
    pub fn remote_client(&self) -> &RemoteClient {
        &self.client
    }
}

impl PyRaphtoryClient {
    /// Run an async operation that returns Result<O, ClientError> and map errors to PyErr.
    pub(crate) fn run_async<F, Fut, O>(&self, f: F) -> PyResult<O>
    where
        F: FnOnce(RemoteClient) -> Fut + Send + 'static,
        Fut: Future<Output = Result<O, ClientError>> + Send + 'static,
        O: Send + 'static,
    {
        let client = self.client.clone();
        let fut = f(client);
        let result = execute_async_task(|| fut);
        result.map_err(PyErr::from)
    }

    pub(crate) fn query_with_json_variables(
        &self,
        query: String,
        variables: serde_json::Map<String, JsonValue>,
    ) -> PyResult<HashMap<String, JsonValue>> {
        self.run_async(move |client| async move {
            client.query(&query, JsonValue::Object(variables)).await
        })
    }
}

#[pymethods]
impl PyRaphtoryClient {
    #[new]
    #[pyo3(signature = (url, token=None))]
    pub(crate) fn new(url: String, token: Option<String>) -> PyResult<Self> {
        let url = Url::parse(url.as_str()).map_err(|e| PyException::new_err(e.to_string()))?;
        let client =
            execute_async_task(|| RemoteClient::connect(url, token)).map_err(PyErr::from)?;
        Ok(Self { client })
    }

    /// Return a new client identical to this one but authenticating with a
    /// different bearer token.
    ///
    /// Purely client-side: no server round-trip is made. Useful for acting as a
    /// different principal (for example, an admin dropping to a reader token)
    /// without reconstructing the client.
    ///
    /// Arguments:
    ///     token (str): the bearer token the returned client should send.
    ///
    /// Returns:
    ///     RaphtoryClient: a new client using the given token.
    fn with_token(&self, token: String) -> Self {
        Self {
            client: self.client.with_token(token),
        }
    }

    /// Check if the server is online.
    ///
    /// Returns:
    ///     bool: Returns true if server is online otherwise false.
    fn is_server_online(&self) -> bool {
        is_online(self.client.url.as_ref())
    }

    /// Make a GraphQL query against the server.
    ///
    /// Arguments:
    ///     query (str): the query to make.
    ///     variables (dict[str, Any], optional): a dict of variables present on the query and their values.
    ///
    /// Returns:
    ///     dict[str, Any]: The data field from the graphQL response.
    #[pyo3(signature = (query, variables = None))]
    pub(crate) fn query<'py>(
        &self,
        py: Python<'py>,
        query: String,
        variables: Option<HashMap<String, Bound<'py, PyAny>>>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let variables = variables.unwrap_or_default();
        let mut json_variables = serde_json::Map::new();
        for (key, value) in variables {
            let json_value = translate_from_python(value)?;
            json_variables.insert(key, json_value);
        }
        let data = py.detach(|| self.query_with_json_variables(query, json_variables))?;
        translate_map_to_python(py, data)
    }

    /// Send a graph to the server
    ///
    /// Arguments:
    ///     path (str): the path of the graph
    ///     graph (Graph | PersistentGraph): the graph to send
    ///     overwrite (bool): overwrite existing graph. Defaults to False.
    ///
    /// Returns:
    ///     None:
    #[pyo3(signature = (path, graph, overwrite = false))]
    fn send_graph(&self, path: String, graph: MaterializedGraph, overwrite: bool) -> PyResult<()> {
        let encoded_graph = encode_graph(graph)?;
        let path_clone = path.clone();
        self.run_async(move |client| async move {
            client
                .send_graph(&path_clone, &encoded_graph, overwrite)
                .await
        })?;
        debug!("Sent graph '{path}' to the server");
        Ok(())
    }

    /// Upload graph file from a path file_path on the client
    ///
    /// Arguments:
    ///     path (str): the name of the graph
    ///     file_path (str): the path of the graph on the client
    ///     overwrite (bool): overwrite existing graph. Defaults to False.
    ///
    /// Returns:
    ///     None:
    #[pyo3(signature = (path, file_path, overwrite = false))]
    fn upload_graph(&self, path: String, file_path: String, overwrite: bool) -> PyResult<()> {
        self.run_async(move |client| async move {
            client.upload_graph(&path, &file_path, overwrite).await
        })
    }

    /// Copy graph from a path path on the server to a new_path on the server
    ///
    /// Arguments:
    ///     path (str): the path of the graph to be copied
    ///     new_path (str): the new path of the copied graph
    ///
    /// Returns:
    ///     None:
    #[pyo3(signature = (path, new_path))]
    fn copy_graph(&self, path: String, new_path: String) -> PyResult<()> {
        self.run_async(move |client| async move { client.copy_graph(&path, &new_path).await })
    }

    /// Move graph from a path path on the server to a new_path on the server
    ///
    /// Arguments:
    ///     path (str): the path of the graph to be moved
    ///     new_path (str): the new path of the moved graph
    ///
    /// Returns:
    ///     None:
    #[pyo3(signature = (path, new_path))]
    fn move_graph(&self, path: String, new_path: String) -> PyResult<()> {
        self.run_async(move |client| async move { client.move_graph(&path, &new_path).await })
    }

    /// Delete graph from a path path on the server
    ///
    /// Arguments:
    ///     path (str): the path of the graph to be deleted
    ///
    /// Returns:
    ///     None:
    #[pyo3(signature = (path))]
    fn delete_graph(&self, path: String) -> PyResult<()> {
        self.run_async(move |client| async move { client.delete_graph(&path).await })
    }

    /// Receive graph from a path path on the server
    ///
    /// Note:
    /// This downloads a copy of the graph. Modifications are not persisted to the server.
    ///
    /// Arguments:
    ///     path (str): the path of the graph to be received
    ///
    /// Returns:
    ///     Union[Graph, PersistentGraph]: A copy of the graph
    fn receive_graph(&self, path: String) -> PyResult<MaterializedGraph> {
        self.run_async(move |client| async move { client.receive_graph_decoded(&path).await })
    }

    /// Create a new empty Graph on the server at path
    ///
    /// Arguments:
    ///     path (str): the path of the graph to be created
    ///     graph_type (Literal["EVENT", "PERSISTENT"]): the type of graph that should be created - this can be EVENT or PERSISTENT
    ///
    /// Returns:
    ///     RemoteGraph: a reference to the newly created graph.
    ///
    fn new_graph(&self, path: String, graph_type: String) -> PyResult<PyRemoteGraph> {
        // The Python surface keeps the string spelling; parsing it here is the
        // boundary where it becomes a typed graph model, so an invalid value
        // fails with a clear message before any request is sent.
        let graph_type = GqlGraphType::from_str(&graph_type).map_err(PyValueError::new_err)?;
        let create_path = path.clone();
        self.run_async(
            move |client| async move { client.new_graph(&create_path, graph_type).await },
        )?;
        Ok(self.remote_graph(path))
    }

    /// Get a RemoteGraph reference to a graph on the server at path
    ///
    /// Arguments:
    ///     path (str): the path of the graph to be created
    ///
    /// Returns:
    ///     RemoteGraph: the remote graph reference
    ///
    fn remote_graph(&self, path: String) -> PyRemoteGraph {
        PyRemoteGraph {
            graph: Arc::new(self.client.remote_graph(path)),
        }
    }
}
