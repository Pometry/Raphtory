use crate::{
    client::{is_online, remote_client::RemoteClient, ClientError},
    data::GqlGraphType,
    model::graph::filtering::{GqlEdgeFilter, GqlFilter, GqlNodeFilter},
    python::{
        client::{remote_graph::PyRemoteGraph, PyRemoteIndexSpec},
        encode_graph, translate_from_python, translate_map_to_python, translate_to_python,
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

/// Map a case-insensitive permission string to the `GraphPermission` GraphQL
/// enum literal understood by the server.
fn permission_literal(permission: &str) -> PyResult<&'static str> {
    match permission.to_ascii_lowercase().as_str() {
        "read" => Ok("READ"),
        "write" => Ok("WRITE"),
        "introspect" => Ok("INTROSPECT"),
        other => Err(PyValueError::new_err(format!(
            "invalid permission '{other}': expected one of 'read', 'write', 'introspect'"
        ))),
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
    ///     path (str): the path of the graph
    ///
    /// Returns:
    ///     RemoteGraph: the remote graph reference
    ///
    fn remote_graph(&self, path: String) -> PyRemoteGraph {
        PyRemoteGraph {
            graph: Arc::new(self.client.remote_graph(path)),
        }
    }

    /// Create Index for graph on the server at 'path'
    ///
    /// Arguments:
    ///     path (str): the path of the graph to index
    ///     index_spec (RemoteIndexSpec): spec specifying the properties that need to be indexed
    ///     in_ram (bool): create index in ram. Defaults to True.
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

    /// Create a role in the server's permissions store.
    ///
    /// Requires an admin (write-access) token. Only available when the server
    /// was started with a permissions store.
    ///
    /// Arguments:
    ///     name (str): the name of the role to create
    ///
    /// Returns:
    ///     bool: True if the role was created.
    fn create_role(&self, name: String) -> PyResult<bool> {
        self.run_async(move |client| async move { client.create_role(&name).await })
    }

    /// Delete a role from the server's permissions store.
    ///
    /// Requires an admin (write-access) token.
    ///
    /// Arguments:
    ///     name (str): the name of the role to delete
    ///
    /// Returns:
    ///     bool: True if the role was deleted.
    fn delete_role(&self, name: String) -> PyResult<bool> {
        self.run_async(move |client| async move { client.delete_role(&name).await })
    }

    /// Grant a role access to a single graph.
    ///
    /// Requires an admin (write-access) token.
    ///
    /// Arguments:
    ///     role (str): the role to grant access to
    ///     path (str): the path of the graph
    ///     permission (str): one of "read", "write", "introspect" (case-insensitive)
    ///
    /// Returns:
    ///     bool: True if the grant was applied.
    ///
    /// Raises:
    ///     ValueError: if permission is not one of "read", "write", "introspect".
    fn grant_graph(&self, role: String, path: String, permission: String) -> PyResult<bool> {
        let perm = permission_literal(&permission)?;
        self.run_async(move |client| async move { client.grant_graph(&role, &path, perm).await })
    }

    /// Revoke a role's access to a single graph.
    ///
    /// Requires an admin (write-access) token.
    ///
    /// Arguments:
    ///     role (str): the role to revoke access from
    ///     path (str): the path of the graph
    ///
    /// Returns:
    ///     bool: True if the access was revoked.
    fn revoke_graph(&self, role: String, path: String) -> PyResult<bool> {
        self.run_async(move |client| async move { client.revoke_graph(&role, &path).await })
    }

    /// Grant a role access to a namespace.
    ///
    /// Requires an admin (write-access) token.
    ///
    /// Arguments:
    ///     role (str): the role to grant access to
    ///     path (str): the namespace path
    ///     permission (str): one of "read", "write", "introspect" (case-insensitive)
    ///     recursive (bool): also grant existing descendants. Defaults to False.
    ///         Every currently existing descendant of the namespace is granted
    ///         individually.
    ///
    /// Returns:
    ///     bool: True if the grant was applied.
    ///
    /// Raises:
    ///     ValueError: if permission is not one of "read", "write", "introspect".
    #[pyo3(signature = (role, path, permission, recursive = false))]
    fn grant_namespace(
        &self,
        role: String,
        path: String,
        permission: String,
        recursive: bool,
    ) -> PyResult<bool> {
        let perm = permission_literal(&permission)?;
        self.run_async(move |client| async move {
            client.grant_namespace(&role, &path, perm, recursive).await
        })
    }

    /// Revoke a role's access to a namespace.
    ///
    /// Requires an admin (write-access) token.
    ///
    /// Arguments:
    ///     role (str): the role to revoke access from
    ///     path (str): the namespace path
    ///     recursive (bool): also revoke existing descendants. Defaults to False.
    ///         Every currently existing descendant of the namespace is revoked
    ///         individually.
    ///
    /// Returns:
    ///     bool: True if the access was revoked.
    #[pyo3(signature = (role, path, recursive = false))]
    fn revoke_namespace(&self, role: String, path: String, recursive: bool) -> PyResult<bool> {
        self.run_async(move |client| async move {
            client.revoke_namespace(&role, &path, recursive).await
        })
    }

    /// Grant a role read-only access to a graph, restricted by a filter.
    ///
    /// The reader (a `{"access": "ro"}` token bearing this role) sees only the
    /// nodes/edges matching `filter`, with the given property/metadata keys hidden.
    /// Requires an admin (write-access) token.
    ///
    /// Arguments:
    ///     role (str): the role to grant filtered access to
    ///     path (str): the path of the graph
    ///     filter (FilterExpr): a filter expression from `raphtory.filter`; a node
    ///         filter restricts visible nodes, an edge filter restricts visible edges.
    ///     hidden_properties (dict[str, list[str]], optional): temporal property keys
    ///         to hide, keyed by "node", "edge", and/or "graph".
    ///     hidden_metadata (dict[str, list[str]], optional): metadata keys to hide,
    ///         keyed by "node", "edge", and/or "graph".
    ///
    /// Returns:
    ///     bool: True if the grant was applied.
    ///
    /// Raises:
    ///     ValueError: if the filter cannot be represented as a GraphQL node or
    ///         edge filter.
    #[pyo3(signature = (role, path, filter, hidden_properties = None, hidden_metadata = None))]
    fn grant_graph_filtered_read_only(
        &self,
        role: String,
        path: String,
        filter: PyFilterExpr,
        hidden_properties: Option<HashMap<String, Vec<String>>>,
        hidden_metadata: Option<HashMap<String, Vec<String>>>,
    ) -> PyResult<bool> {
        // Reuse the RemoteGraph `.filter()` conversion path: try node first,
        // fall back to edge, then serialize the resulting GraphQL filter type.
        //
        // Build a `GqlFilter` and let its own `Serialize` produce the wire
        // shape rather than hand-writing the key. The variant names are the
        // schema's field names, so a rename on the input type carries through
        // here instead of silently sending a field the server will reject.
        let gql_filter = if let Ok(node) = filter.try_as_node_filter() {
            let gql: GqlNodeFilter = node
                .try_into()
                .map_err(|e: GraphError| PyValueError::new_err(e.to_string()))?;
            GqlFilter::Nodes(gql)
        } else {
            let edge = filter
                .try_as_edge_filter()
                .map_err(|e| PyValueError::new_err(e.to_string()))?;
            let gql: GqlEdgeFilter = edge
                .try_into()
                .map_err(|e: GraphError| PyValueError::new_err(e.to_string()))?;
            GqlFilter::Edges(gql)
        };
        let row_filter =
            serde_json::to_value(&gql_filter).map_err(|e| PyValueError::new_err(e.to_string()))?;

        let mut access_filter = serde_json::Map::new();
        access_filter.insert("filter".to_owned(), row_filter);
        if let Some(hp) = hidden_properties {
            access_filter.insert("hiddenProperties".to_owned(), json!(hp));
        }
        if let Some(hm) = hidden_metadata {
            access_filter.insert("hiddenMetadata".to_owned(), json!(hm));
        }
        let filter_value = JsonValue::Object(access_filter);

        self.run_async(move |client| async move {
            client
                .grant_graph_filtered_read_only(&role, &path, filter_value)
                .await
        })
    }

    /// Return this token's own permission grants.
    ///
    /// Reads only what the calling role has been granted, so it never discloses
    /// other roles or graphs. Available to any authenticated caller (does not
    /// require an admin token). Only available when the server was started with
    /// a permissions store.
    ///
    /// Returns:
    ///     dict[str, Any]: a mapping with keys ``roles`` (list of str),
    ///     ``graphs`` (list of ``{"path", "permission", "filtered"}``) and
    ///     ``namespaces`` (list of ``{"path", "permission"}``). ``roles`` is empty
    ///     when the token carries no role claim, in which case both lists are empty.
    ///     A token naming several roles gets their grants merged
    ///     most-permissive-wins, so an unfiltered grant from one role supersedes a
    ///     filtered grant from another on the same graph.
    fn viewer_permissions<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let value = py
            .detach(|| self.run_async(|client| async move { client.viewer_permissions().await }))?;
        translate_to_python(py, value)
    }

    /// List every role name in the server's permissions store.
    ///
    /// Requires an admin (write-access) token.
    ///
    /// Returns:
    ///     list[str]: the role names.
    fn list_roles(&self, py: Python<'_>) -> PyResult<Vec<String>> {
        py.detach(|| self.run_async(|client| async move { client.list_roles().await }))
    }

    /// Fetch a single role's grants by name.
    ///
    /// Requires an admin (write-access) token.
    ///
    /// Arguments:
    ///     name (str): the role to look up
    ///
    /// Returns:
    ///     Optional[dict[str, Any]]: a mapping with keys ``name``, ``graphs``
    ///     (list of ``{"path", "permission", "filtered"}``) and ``namespaces``
    ///     (list of ``{"path", "permission"}``), or None if the role does not exist.
    ///     ``filtered`` is True when the grant carries an access filter, without
    ///     exposing the filter's contents.
    fn get_role<'py>(&self, py: Python<'py>, name: String) -> PyResult<Bound<'py, PyAny>> {
        let value = py
            .detach(|| self.run_async(move |client| async move { client.get_role(&name).await }))?;
        translate_to_python(py, value)
    }
}
