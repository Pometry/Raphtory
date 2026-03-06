use crate::{
    client::{
        is_online, raphtory_client::RaphtoryGraphQLClient, remote_graph::GraphQLRemoteGraph,
        ClientError,
    },
    python::{
        client::{remote_graph::PyRemoteGraph, PyRemoteIndexSpec},
        encode_graph, translate_from_python, translate_map_to_python,
    },
};
use pyo3::{exceptions::PyException, prelude::*, types::PyDict};
use raphtory::{db::api::view::MaterializedGraph, python::utils::execute_async_task};
use serde_json::Value as JsonValue;
use std::{collections::HashMap, future::Future, sync::Arc};
use tracing::debug;

/// A client for handling GraphQL operations in the context of Raphtory.
///
/// Arguments:
///     url (str): the URL of the Raphtory GraphQL server
///     token:
#[derive(Clone)]
#[pyclass(name = "RaphtoryClient", module = "raphtory.graphql")]
pub struct PyRaphtoryClient {
    pub(crate) client: RaphtoryGraphQLClient,
}

impl PyRaphtoryClient {
    /// Run an async operation that returns Result<O, ClientError> and map errors to PyErr.
    pub(crate) fn run_async<F, Fut, O>(&self, f: F) -> PyResult<O>
    where
        F: FnOnce(RaphtoryGraphQLClient) -> Fut + Send + 'static,
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
        variables: HashMap<String, JsonValue>,
    ) -> PyResult<HashMap<String, JsonValue>> {
        self.run_async(move |client| async move { client.query(&query, variables).await })
    }
}

#[pymethods]
impl PyRaphtoryClient {
    #[new]
    #[pyo3(signature = (url, token=None))]
    pub(crate) fn new(url: String, token: Option<String>) -> PyResult<Self> {
        let client = RaphtoryGraphQLClient::connect(url, token).map_err(PyErr::from)?;
        Ok(Self { client })
    }

    /// Check if the server is online.
    ///
    /// Returns:
    ///     bool: Returns true if server is online otherwise false.
    fn is_server_online(&self) -> bool {
        is_online(&self.client.url)
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
        let variables = variables.unwrap_or_else(|| HashMap::new());
        let mut json_variables = HashMap::new();
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
    ///     dict[str, Any]: The data field from the graphQL response after executing the mutation.
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
    ///     dict[str, Any]: The data field from the graphQL response after executing the mutation.
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
    ///     None:
    ///
    fn new_graph(&self, path: String, graph_type: String) -> PyResult<()> {
        self.run_async(move |client| async move { client.new_graph(&path, &graph_type).await })
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
            graph: Arc::new(GraphQLRemoteGraph::new(path, self.client.clone())),
        }
    }

    /// Create Index for graph on the server at 'path'
    ///
    /// Arguments:
    ///     path: the path of the graph to be created
    ///     RemoteIndexSpec (RemoteIndexSpec): spec specifying the properties that need to be indexed
    ///     in_ram (bool): create index in ram
    ///
    /// Returns:
    ///     None:
    ///
    #[pyo3(signature = (path, index_spec, in_ram = true))]
    fn create_index(
        &self,
        path: String,
        index_spec: PyRemoteIndexSpec,
        in_ram: bool,
    ) -> PyResult<()> {
        let spec_value =
            serde_json::to_value(&index_spec).map_err(|e| PyException::new_err(e.to_string()))?;
        self.run_async(
            move |client| async move { client.create_index(&path, spec_value, in_ram).await },
        )
    }
}
