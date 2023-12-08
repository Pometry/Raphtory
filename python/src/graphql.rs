use crossbeam_channel::Sender;
use pyo3::{
    exceptions,
    exceptions::{PyException, PyTypeError, PyValueError},
    prelude::*,
    types::{PyDict, PyFunction},
};
use raphtory_core::{
    db::api::view::MaterializedGraph,
    python::{
        packages::vectors::PyDocumentTemplate,
        utils::{errors::adapt_err_value, execute_async_task},
    },
    vectors::{embeddings::openai_embedding, EmbeddingFunction},
};
use raphtory_graphql::{url_decode_graph, url_encode_graph, RaphtoryServer};
use reqwest::Client;
use serde_json::{json, Map, Number, Value};
use std::{
    collections::HashMap,
    future::Future,
    path::PathBuf,
    thread,
    thread::{sleep, JoinHandle},
    time::Duration,
};
use tokio::{self, io::Result as IoResult};

/// A class for defining and running a Raphtory GraphQL server
#[pyclass(name = "RaphtoryServer")]
pub(crate) struct PyRaphtoryServer(Option<RaphtoryServer>);

impl PyRaphtoryServer {
    fn new(server: RaphtoryServer) -> Self {
        Self(Some(server))
    }

    fn with_vectorised_generic_embedding<F: EmbeddingFunction + Clone + 'static>(
        slf: PyRefMut<Self>,
        graph_names: Vec<String>,
        embedding: F,
        cache: String,
        node_document: Option<String>,
        edge_document: Option<String>,
    ) -> PyResult<Self> {
        let template = PyDocumentTemplate::new(node_document, edge_document);
        let server = take_server_ownership(slf)?;
        execute_async_task(move || async move {
            let new_server = server
                .with_vectorised(
                    graph_names,
                    embedding,
                    &PathBuf::from(cache),
                    Some(template),
                )
                .await;
            Ok(Self::new(new_server))
        })
    }
}

#[pymethods]
impl PyRaphtoryServer {
    #[new]
    fn py_new(
        graphs: Option<HashMap<String, MaterializedGraph>>,
        graph_dir: Option<&str>,
    ) -> PyResult<Self> {
        let server = match (graphs, graph_dir) {
            (Some(graphs), Some(dir)) => Ok(RaphtoryServer::from_map_and_directory(graphs, dir)),
            (Some(graphs), None) => Ok(RaphtoryServer::from_map(graphs)),
            (None, Some(dir)) => Ok(RaphtoryServer::from_directory(dir)),
            (None, None) => Err(PyValueError::new_err(
                "You need to specify at least `graphs` or `graph_dir`",
            )),
        }?;

        Ok(PyRaphtoryServer::new(server))
    }

    /// Vectorise a subset of the graphs of the server.
    ///
    /// Note:
    ///   If no embedding function is provided, the server will attempt to use the OpenAI API
    ///   embedding model, which will only work if the env variable OPENAI_API_KEY is set
    ///   appropriately
    ///
    /// Arguments:
    ///   * `graph_names`: the names of the graphs to vectorise.
    ///   * `cache`: the directory to use as cache for the embeddings.
    ///   * `embedding`: the embedding function to translate documents to embeddings.
    ///   * `node_document`: the property name to use as the source for the documents on nodes.
    ///   * `edge_document`: the property name to use as the source for the documents on edges.
    ///
    /// Returns:
    ///    A new server object containing the vectorised graphs.
    fn with_vectorised(
        slf: PyRefMut<Self>,
        graph_names: Vec<String>,
        cache: String,
        // TODO: support more models by just providing a string, e.g. "openai", here and in the VectorisedGraph API
        embedding: Option<&PyFunction>,
        node_document: Option<String>,
        edge_document: Option<String>,
    ) -> PyResult<Self> {
        match embedding {
            Some(embedding) => {
                let embedding: Py<PyFunction> = embedding.into();
                Self::with_vectorised_generic_embedding(
                    slf,
                    graph_names,
                    embedding,
                    cache,
                    node_document,
                    edge_document,
                )
            }
            None => Self::with_vectorised_generic_embedding(
                slf,
                graph_names,
                openai_embedding,
                cache,
                node_document,
                edge_document,
            ),
        }
    }

    // // TODO: this is doable!!!
    // pub fn register_algorithm(self, name: String, algorithm: &PyFunction) -> RaphtoryServer {
    //     self.0.register_algorithm(???)
    // }

    /// Start the server and return a handle to it.
    ///
    /// Arguments:
    ///   * `port`: the port to use (defaults to 1736).
    #[pyo3(signature = (port = 1736))]
    pub fn start(slf: PyRefMut<Self>, port: u16) -> PyResult<PyRunningRaphtoryServer> {
        let (sender, receiver) = crossbeam_channel::bounded::<BridgeCommand>(1);
        let server = take_server_ownership(slf)?;

        let cloned_sender = sender.clone();

        let join_handle = thread::spawn(move || {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async move {
                    let handler = server.start_with_port(port);
                    let tokio_sender = handler._get_sender().clone();
                    tokio::task::spawn_blocking(move || {
                        match receiver.recv().expect("Failed to wait for cancellation") {
                            BridgeCommand::StopServer => tokio_sender
                                .blocking_send(())
                                .expect("Failed to send cancellation signal"),
                            BridgeCommand::StopListening => (),
                        }
                    });
                    let result = handler.wait().await;
                    _ = cloned_sender.send(BridgeCommand::StopListening);
                    result
                })
        });

        Ok(PyRunningRaphtoryServer::new(join_handle, sender, port))
    }

    /// Run the server until completion.
    ///
    /// Arguments:
    ///   * `port`: the port to use (defaults to 1736).
    #[pyo3(signature = (port = 1736))]
    pub fn run(slf: PyRefMut<Self>, port: u16) -> PyResult<()> {
        wait_server(&mut Self::start(slf, port)?.0)
    }
}

fn take_server_ownership(mut server: PyRefMut<PyRaphtoryServer>) -> PyResult<RaphtoryServer> {
    let new_server = server.0.take().ok_or_else(|| {
        PyException::new_err(
            "Server object has already been used, please create another one from scratch",
        )
    })?;
    Ok(new_server)
}

fn wait_server(running_server: &mut Option<ServerHandler>) -> PyResult<()> {
    let owned_running_server = running_server
        .take()
        .ok_or_else(|| PyException::new_err(RUNNING_SERVER_CONSUMED_MSG))?;
    owned_running_server
        .join_handle
        .join()
        .expect("error when waiting for the server thread to complete")
        .map_err(|e| adapt_err_value(&e))
}

const RUNNING_SERVER_CONSUMED_MSG: &str =
    "Running server object has already been used, please create another one from scratch";

/// A Raphtory server handler that also enables querying the server
#[pyclass(name = "RunningRaphtoryServer")]
pub(crate) struct PyRunningRaphtoryServer(Option<ServerHandler>);

enum BridgeCommand {
    StopServer,
    StopListening,
}

struct ServerHandler {
    join_handle: JoinHandle<IoResult<()>>,
    sender: Sender<BridgeCommand>,
    client: PyRaphtoryClient,
}

impl PyRunningRaphtoryServer {
    fn new(
        join_handle: JoinHandle<IoResult<()>>,
        sender: Sender<BridgeCommand>,
        port: u16,
    ) -> Self {
        let url = format!("http://localhost:{port}");
        Self(Some(ServerHandler {
            join_handle,
            sender,
            client: PyRaphtoryClient::new(url),
        }))
    }

    fn apply_if_alive<O, F>(&self, function: F) -> PyResult<O>
    where
        F: FnOnce(&ServerHandler) -> PyResult<O>,
    {
        match &self.0 {
            Some(handler) => function(handler),
            None => Err(PyException::new_err(RUNNING_SERVER_CONSUMED_MSG)),
        }
    }
}

#[pymethods]
impl PyRunningRaphtoryServer {
    /// Stop the server.
    pub(crate) fn stop(&self) -> PyResult<()> {
        self.apply_if_alive(|handler| {
            handler
                .sender
                .send(BridgeCommand::StopServer)
                .expect("Failed when sending cancellation signal");
            Ok(())
        })
    }

    /// Wait until server completion.
    pub(crate) fn wait(mut slf: PyRefMut<Self>) -> PyResult<()> {
        wait_server(&mut slf.0)
    }

    /// Wait for the server to be online.
    ///
    /// Arguments:
    ///   * `timeout_millis`: the timeout in milliseconds (default 5000).
    fn wait_for_online(&self, timeout_millis: Option<u64>) -> PyResult<()> {
        self.apply_if_alive(|handler| handler.client.wait_for_online(timeout_millis))
    }

    /// Make a graphQL query against the server.
    ///
    /// Arguments:
    ///   * `query`: the query to make.
    ///   * `variables`: a dict of variables present on the query and their values.
    ///
    /// Returns:
    ///    The `data` field from the graphQL response.
    fn query(
        &self,
        py: Python,
        query: String,
        variables: Option<HashMap<String, PyObject>>,
    ) -> PyResult<HashMap<String, PyObject>> {
        self.apply_if_alive(|handler| handler.client.query(py, query, variables))
    }

    /// Send a graph to the server.
    ///
    /// Arguments:
    ///   * `name`: the name of the graph sent.
    ///   * `graph`: the graph to send.
    ///
    /// Returns:
    ///    The `data` field from the graphQL response after executing the mutation.
    fn send_graph(
        &self,
        py: Python,
        name: String,
        graph: MaterializedGraph,
    ) -> PyResult<HashMap<String, PyObject>> {
        self.apply_if_alive(|handler| handler.client.send_graph(py, name, graph))
    }

    /// Set the server to load all the graphs from its path `path`.
    ///
    /// Arguments:
    ///   * `path`: the path to load the graphs from.
    ///   * `overwrite`: whether or not to overwrite existing graphs (defaults to False)
    ///
    /// Returns:
    ///    The `data` field from the graphQL response after executing the mutation.
    #[pyo3(signature=(path, overwrite = false))]
    fn load_graphs_from_path(
        &self,
        py: Python,
        path: String,
        overwrite: bool,
    ) -> PyResult<HashMap<String, PyObject>> {
        self.apply_if_alive(|handler| handler.client.load_graphs_from_path(py, path, overwrite))
    }
}

/// A client for handling GraphQL operations in the context of Raphtory.
#[derive(Clone)]
#[pyclass(name = "RaphtoryClient")]
pub(crate) struct PyRaphtoryClient {
    url: String,
}

impl PyRaphtoryClient {
    fn query_with_json_variables(
        &self,
        query: String,
        variables: HashMap<String, Value>,
    ) -> PyResult<HashMap<String, Value>> {
        let client = self.clone();
        let (graphql_query, graphql_result) = execute_async_task(move || async move {
            client.send_graphql_query(query, variables).await
        })?;
        let mut graphql_result = graphql_result;

        match graphql_result.remove("data") {
            Some(Value::Object(data)) => Ok(data.into_iter().collect()),
            _ => match graphql_result.remove("errors") {
                Some(Value::Array(errors)) => Err(PyException::new_err(format!(
                    "After sending query to the server:\n\t{graphql_query}\nGot the following errors:\n\t{errors:?}"
                ))),
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
        variables: HashMap<String, Value>,
    ) -> PyResult<(Value, HashMap<String, Value>)> {
        let client = Client::new();

        let request_body = json!({
            "query": query,
            "variables": variables
        });

        let response = client
            .post(&self.url)
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

    fn generic_load_graphs(
        &self,
        py: Python,
        load_function: &str,
        path: String,
    ) -> PyResult<HashMap<String, PyObject>> {
        let query =
            format!("mutation LoadGraphs($path: String!) {{ {load_function}(path: $path) }}");
        let variables = [("path".to_owned(), json!(path))];

        let data = self.query_with_json_variables(query.clone(), variables.into())?;

        match data.get(load_function) {
            Some(Value::Array(loads)) => {
                let num_graphs = loads.len();
                println!("Loaded {num_graphs} graph(s)");
                translate_map_to_python(py, data)
            }
            _ => Err(PyException::new_err(format!(
                "Error while reading server response for query:\n\t{query}\nGot data:\n\t'{data:?}'"
            ))),
        }
    }

    fn is_online(&self) -> bool {
        match reqwest::blocking::get(&self.url) {
            Ok(response) => response.status().as_u16() == 200,
            _ => false,
        }
    }
}

const WAIT_CHECK_INTERVAL_MILLIS: u64 = 200;

#[pymethods]
impl PyRaphtoryClient {
    #[new]
    fn new(url: String) -> Self {
        Self { url }
    }

    /// Wait for the server to be online.
    ///
    /// Arguments:
    ///   * `millis`: the minimum number of milliseconds to wait (default 5000).
    fn wait_for_online(&self, millis: Option<u64>) -> PyResult<()> {
        let millis = millis.unwrap_or(5000);
        let num_intervals = millis / WAIT_CHECK_INTERVAL_MILLIS;

        let mut online = false;
        for _ in 0..num_intervals {
            if self.is_online() {
                online = true;
                break;
            } else {
                sleep(Duration::from_millis(WAIT_CHECK_INTERVAL_MILLIS))
            }
        }

        if online {
            Ok(())
        } else {
            Err(PyException::new_err(
                "Failed to connect to the server after {millis} milliseconds",
            ))
        }
    }

    /// Make a graphQL query against the server.
    ///
    /// Arguments:
    ///   * `query`: the query to make.
    ///   * `variables`: a dict of variables present on the query and their values.
    ///
    /// Returns:
    ///    The `data` field from the graphQL response.
    fn query(
        &self,
        py: Python,
        query: String,
        variables: Option<HashMap<String, PyObject>>,
    ) -> PyResult<HashMap<String, PyObject>> {
        let variables = variables.unwrap_or_else(|| HashMap::new());
        let mut json_variables = HashMap::new();
        for (key, value) in variables {
            let json_value = translate_from_python(py, value)?;
            json_variables.insert(key, json_value);
        }

        let data = self.query_with_json_variables(query, json_variables)?;
        translate_map_to_python(py, data)
    }

    /// Send a graph to the server.
    ///
    /// Arguments:
    ///   * `name`: the name of the graph sent.
    ///   * `graph`: the graph to send.
    ///
    /// Returns:
    ///    The `data` field from the graphQL response after executing the mutation.
    fn send_graph(
        &self,
        py: Python,
        name: String,
        graph: MaterializedGraph,
    ) -> PyResult<HashMap<String, PyObject>> {
        let encoded_graph = encode_graph(graph)?;

        let query = r#"
            mutation SendGraph($name: String!, $graph: String!) {
                sendGraph(name: $name, graph: $graph)
            }
        "#
        .to_owned();
        let variables = [
            ("name".to_owned(), json!(name)),
            ("graph".to_owned(), json!(encoded_graph)),
        ];

        let data = self.query_with_json_variables(query, variables.into())?;

        match data.get("sendGraph") {
            Some(Value::String(name)) => {
                println!("Sent graph '{name}' to the server");
                translate_map_to_python(py, data)
            }
            _ => Err(PyException::new_err(format!(
                "Error Sending Graph. Got response {:?}",
                data
            ))),
        }
    }

    /// Set the server to load all the graphs from its path `path`.
    ///
    /// Arguments:
    ///   * `path`: the path to load the graphs from.
    ///   * `overwrite`: whether or not to overwrite existing graphs (defaults to False)
    ///
    /// Returns:
    ///    The `data` field from the graphQL response after executing the mutation.
    #[pyo3(signature=(path, overwrite = false))]
    fn load_graphs_from_path(
        &self,
        py: Python,
        path: String,
        overwrite: bool,
    ) -> PyResult<HashMap<String, PyObject>> {
        if overwrite {
            self.generic_load_graphs(py, "loadGraphsFromPath", path)
        } else {
            self.generic_load_graphs(py, "loadNewGraphsFromPath", path)
        }
    }
}

fn translate_from_python(py: Python, value: PyObject) -> PyResult<Value> {
    if let Ok(value) = value.extract::<i64>(py) {
        Ok(Value::Number(value.into()))
    } else if let Ok(value) = value.extract::<f64>(py) {
        Ok(Value::Number(Number::from_f64(value).unwrap()))
    } else if let Ok(value) = value.extract::<bool>(py) {
        Ok(Value::Bool(value))
    } else if let Ok(value) = value.extract::<String>(py) {
        Ok(Value::String(value))
    } else if let Ok(value) = value.extract::<Vec<PyObject>>(py) {
        let mut vec = Vec::new();
        for item in value {
            vec.push(translate_from_python(py, item)?);
        }
        Ok(Value::Array(vec))
    } else if let Ok(value) = value.extract::<&PyDict>(py) {
        let mut map = Map::new();
        for (key, value) in value.iter() {
            let key = key.extract::<String>()?;
            let value = translate_from_python(py, value.into_py(py))?;
            map.insert(key, value);
        }
        Ok(Value::Object(map))
    } else {
        Err(PyErr::new::<PyTypeError, _>("Unsupported type"))
    }
}

fn translate_map_to_python(
    py: Python,
    input: HashMap<String, Value>,
) -> PyResult<HashMap<String, PyObject>> {
    let mut output_dict = HashMap::new();
    for (key, value) in input {
        let py_value = translate_to_python(py, value)?;
        output_dict.insert(key, py_value);
    }

    Ok(output_dict)
}

fn translate_to_python(py: Python, value: serde_json::Value) -> PyResult<PyObject> {
    match value {
        Value::Number(num) => {
            if num.is_i64() {
                Ok(num.as_i64().unwrap().into_py(py))
            } else if num.is_f64() {
                Ok(num.as_f64().unwrap().into_py(py))
            } else {
                Err(PyErr::new::<PyTypeError, _>("Unsupported number type"))
            }
        }
        Value::String(s) => Ok(s.into_py(py)),
        Value::Array(vec) => {
            let mut list = Vec::new();
            for item in vec {
                list.push(translate_to_python(py, item)?);
            }
            Ok(list.into_py(py))
        }
        Value::Object(map) => {
            let dict = PyDict::new(py);
            for (key, value) in map {
                dict.set_item(key, translate_to_python(py, value)?)?;
            }
            Ok(dict.into())
        }
        Value::Bool(b) => Ok(b.into_py(py)),
        Value::Null => Ok(py.None()),
    }
}

fn encode_graph(graph: MaterializedGraph) -> PyResult<String> {
    let result = url_encode_graph(graph);
    match result {
        Ok(s) => Ok(s),
        Err(e) => Err(exceptions::PyValueError::new_err(format!(
            "Error encoding: {:?}",
            e
        ))),
    }
}
