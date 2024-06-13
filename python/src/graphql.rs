use async_graphql::{
    dynamic::{Field, FieldFuture, FieldValue, InputValue, Object, TypeRef, ValueAccessor},
    Value as GraphqlValue,
};

use crossbeam_channel::Sender as CrossbeamSender;
use dynamic_graphql::internal::{Registry, TypeName};
use itertools::intersperse;
use pyo3::{
    exceptions::{PyAttributeError, PyException, PyTypeError, PyValueError},
    prelude::*,
    types::{IntoPyDict, PyDict, PyFunction, PyList},
};
use raphtory_core::{
    db::api::view::MaterializedGraph,
    python::{
        packages::vectors::{
            compute_embedding, into_py_document, translate_py_window, PyDocumentTemplate, PyQuery,
            PyVectorisedGraph, PyWindow,
        },
        types::wrappers::document::PyDocument,
        utils::{errors::adapt_err_value, execute_async_task},
    },
    vectors::{
        embeddings::openai_embedding, vectorised_cluster::VectorisedCluster, Document,
        EmbeddingFunction,
    },
};
use raphtory_graphql::{
    model::algorithms::{
        algorithm_entry_point::AlgorithmEntryPoint, document::GqlDocument,
        global_plugins::GlobalPlugins, vector_algorithms::VectorAlgorithms,
    },
    server_config::CacheConfig,
    url_encode_graph, RaphtoryServer,
};
use reqwest::Client;
use serde_json::{json, Map, Number, Value as JsonValue};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    thread,
    thread::{sleep, JoinHandle},
    time::Duration,
};
use tokio::{self, io::Result as IoResult};

/// A class for accessing graphs hosted in a Raphtory GraphQL server and running global search for
/// graph documents
#[pyclass(name = "GraphqlGraphs")]
pub(crate) struct PyGlobalPlugins(GlobalPlugins);

#[pymethods]
impl PyGlobalPlugins {
    /// Return the top documents with the smallest cosine distance to `query`
    ///
    /// # Arguments
    ///   * query - the text or the embedding to score against
    ///   * limit - the maximum number of documents to return
    ///   * window - the window where documents need to belong to in order to be considered
    ///
    /// # Returns
    ///   A list of documents
    fn search_graph_documents(
        &self,
        py: Python,
        query: PyQuery,
        limit: usize,
        window: PyWindow,
    ) -> Vec<PyDocument> {
        self.search_graph_documents_with_scores(py, query, limit, window)
            .into_iter()
            .map(|(doc, _score)| doc)
            .collect()
    }

    /// Same as `search_graph_documents` but it also returns the scores alongside the documents
    fn search_graph_documents_with_scores(
        &self,
        py: Python,
        query: PyQuery,
        limit: usize,
        window: PyWindow,
    ) -> Vec<(PyDocument, f32)> {
        let window = translate_py_window(window);
        let graphs = self.0.vectorised_graphs.read();
        let cluster = VectorisedCluster::new(&graphs);
        let vectorised_graphs = self.0.vectorised_graphs.read();
        let graph_entry = vectorised_graphs.iter().next();
        let (_, first_graph) = graph_entry
            .expect("trying to search documents with no vectorised graphs on the server");
        let embedding = compute_embedding(first_graph, query);
        let documents = cluster.search_graph_documents_with_scores(&embedding, limit, window);
        documents.into_iter().map(|(doc, score)| {
            let graph = match &doc {
                Document::Graph { name, .. } => {
                    vectorised_graphs.get(name).unwrap()
                }
                _ => panic!("search_graph_documents_with_scores returned a document that is not from a graph"),
            };
            (into_py_document(doc, graph, py), score)
        }).collect()
    }

    /// Return the `VectorisedGraph` with name `name` or `None` if it doesn't exist
    fn get(&self, name: &str) -> Option<PyVectorisedGraph> {
        self.0
            .vectorised_graphs
            .read()
            .get(name)
            .map(|graph| graph.clone().into())
    }
}

/// A class for defining and running a Raphtory GraphQL server
#[pyclass(name = "RaphtoryServer")]
pub(crate) struct PyRaphtoryServer(Option<RaphtoryServer>);

impl PyRaphtoryServer {
    fn new(server: RaphtoryServer) -> Self {
        Self(Some(server))
    }

    fn with_vectorised_generic_embedding<F: EmbeddingFunction + Clone + 'static>(
        slf: PyRefMut<Self>,
        graph_names: Option<Vec<String>>,
        embedding: F,
        cache: String,
        graph_document: Option<String>,
        node_document: Option<String>,
        edge_document: Option<String>,
    ) -> PyResult<Self> {
        let template = PyDocumentTemplate::new(graph_document, node_document, edge_document);
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

    fn with_generic_document_search_function<
        'a,
        E: AlgorithmEntryPoint<'a> + 'static,
        F: Fn(&E, Python) -> PyObject + Send + Sync + 'static,
    >(
        slf: PyRefMut<Self>,
        name: String,
        input: HashMap<String, String>,
        function: &PyFunction,
        adapter: F,
    ) -> PyResult<Self> {
        let function: Py<PyFunction> = function.into();

        let input_mapper = HashMap::from([
            ("str", TypeRef::named_nn(TypeRef::STRING)),
            ("int", TypeRef::named_nn(TypeRef::INT)),
            ("float", TypeRef::named_nn(TypeRef::FLOAT)),
        ]);

        let input_values = input
            .into_iter()
            .map(|(name, type_name)| {
                let type_ref = input_mapper.get(&type_name.as_str()).cloned();
                type_ref
                    .map(|type_ref| InputValue::new(name, type_ref))
                    .ok_or_else(|| {
                        let valid_types = input_mapper.keys().map(|key| key.to_owned());
                        let valid_types_string: String = intersperse(valid_types, ", ").collect();
                        let msg = format!("types in input have to be one of: {valid_types_string}");
                        PyAttributeError::new_err(msg)
                    })
            })
            .collect::<PyResult<Vec<InputValue>>>()?;

        let register_function = |name: &str, registry: Registry, parent: Object| {
            let registry = registry.register::<GqlDocument>();
            let output_type = TypeRef::named_nn_list_nn(GqlDocument::get_type_name());
            let mut field = Field::new(name, output_type, move |ctx| {
                let documents = Python::with_gil(|py| {
                    let entry_point = adapter(ctx.parent_value.downcast_ref().unwrap(), py);
                    let kw_args: HashMap<&str, PyObject> = ctx
                        .args
                        .iter()
                        .map(|(name, value)| (name.as_str(), adapt_graphql_value(&value, py)))
                        .collect();
                    let py_kw_args = kw_args.into_py_dict(py);
                    let result = function.call(py, (entry_point,), Some(py_kw_args)).unwrap();
                    let list = result.downcast::<PyList>(py).unwrap();
                    let py_documents = list.iter().map(|doc| doc.extract::<PyDocument>().unwrap());
                    py_documents
                        .map(|doc| doc.extract_rust_document(py).unwrap())
                        .collect::<Vec<_>>()
                });

                let gql_documents = documents
                    .into_iter()
                    .map(|doc| FieldValue::owned_any(GqlDocument::from(doc)));

                FieldFuture::Value(Some(FieldValue::list(gql_documents)))
            });
            for input_value in input_values {
                field = field.argument(input_value);
            }
            let parent = parent.field(field);
            (registry, parent)
        };
        E::lock_plugins().insert(name, Box::new(register_function));

        let new_server = take_server_ownership(slf)?;
        Ok(Self::new(new_server))
    }
}

#[pymethods]
impl PyRaphtoryServer {
    #[new]
    #[pyo3(
        signature = (work_dir, graphs = None, graph_paths = None, cache_capacity = 30, cache_tti_seconds = 900)
    )]
    fn py_new(
        work_dir: String,
        graphs: Option<HashMap<String, MaterializedGraph>>,
        graph_paths: Option<Vec<String>>,
        cache_capacity: u64,
        cache_tti_seconds: u64,
    ) -> PyResult<Self> {
        let graph_paths = graph_paths.map(|paths| paths.into_iter().map(PathBuf::from).collect());
        let server = RaphtoryServer::new(
            Path::new(&work_dir),
            graphs,
            graph_paths,
            Some(CacheConfig {
                capacity: cache_capacity,
                tti_seconds: cache_tti_seconds,
            }),
        );
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
    ///   * `graph_names`: the names of the graphs to vectorise. All by default.
    ///   * `cache`: the directory to use as cache for the embeddings.
    ///   * `embedding`: the embedding function to translate documents to embeddings.
    ///   * `node_document`: the property name to use as the source for the documents on nodes.
    ///   * `edge_document`: the property name to use as the source for the documents on edges.
    ///
    /// Returns:
    ///    A new server object containing the vectorised graphs.
    fn with_vectorised(
        slf: PyRefMut<Self>,
        cache: String,
        graph_names: Option<Vec<String>>,
        // TODO: support more models by just providing a string, e.g. "openai", here and in the VectorisedGraph API
        embedding: Option<&PyFunction>,
        graph_document: Option<String>,
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
                    graph_document,
                    node_document,
                    edge_document,
                )
            }
            None => Self::with_vectorised_generic_embedding(
                slf,
                graph_names,
                openai_embedding,
                cache,
                graph_document,
                node_document,
                edge_document,
            ),
        }
    }

    /// Register a function in the GraphQL schema for document search over a graph.
    ///
    /// The function needs to take a `VectorisedGraph` as the first argument followed by a
    /// pre-defined set of keyword arguments. Supported types are `str`, `int`, and `float`.
    /// They have to be specified using the `input` parameter as a dict where the keys are the
    /// names of the parameters and the values are the types, expressed as strings.
    ///
    /// Arguments:
    ///   * `name` (`str`): the name of the function in the GraphQL schema.
    ///   * `input` (`dict`): the keyword arguments expected by the function.
    ///   * `function` (`function`): the function to run.
    ///
    /// Returns:
    ///    A new server object containing the vectorised graphs.
    pub fn with_document_search_function(
        slf: PyRefMut<Self>,
        name: String,
        input: HashMap<String, String>,
        function: &PyFunction,
    ) -> PyResult<Self> {
        let adapter =
            |entry_point: &VectorAlgorithms, py: Python| entry_point.graph.clone().into_py(py);
        PyRaphtoryServer::with_generic_document_search_function(slf, name, input, function, adapter)
    }

    /// Register a function in the GraphQL schema for document search among all the graphs.
    ///
    /// The function needs to take a `GraphqlGraphs` object as the first argument followed by a
    /// pre-defined set of keyword arguments. Supported types are `str`, `int`, and `float`.
    /// They have to be specified using the `input` parameter as a dict where the keys are the
    /// names of the parameters and the values are the types, expressed as strings.
    ///
    /// Arguments:
    ///   * `name` (`str`): the name of the function in the GraphQL schema.
    ///   * `input` (`dict`): the keyword arguments expected by the function.
    ///   * `function` (`function`): the function to run.
    ///
    /// Returns:
    ///    A new server object containing the vectorised graphs.
    pub fn with_global_search_function(
        slf: PyRefMut<Self>,
        name: String,
        input: HashMap<String, String>,
        function: &PyFunction,
    ) -> PyResult<Self> {
        let adapter = |entry_point: &GlobalPlugins, py: Python| {
            PyGlobalPlugins(entry_point.clone()).into_py(py)
        };
        PyRaphtoryServer::with_generic_document_search_function(slf, name, input, function, adapter)
    }

    /// Start the server and return a handle to it.
    ///
    /// Arguments:
    ///   * `port`: the port to use (defaults to 1736).
    #[pyo3(
        signature = (port = 1736, log_level = "INFO".to_string(), enable_tracing = false, enable_auth = false)
    )]
    pub fn start(
        slf: PyRefMut<Self>,
        port: u16,
        log_level: String,
        enable_tracing: bool,
        enable_auth: bool,
    ) -> PyResult<PyRunningRaphtoryServer> {
        let (sender, receiver) = crossbeam_channel::bounded::<BridgeCommand>(1);
        let server = take_server_ownership(slf)?;

        let cloned_sender = sender.clone();

        let join_handle = thread::spawn(move || {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async move {
                    let handler =
                        server.start_with_port(port, Some(&log_level), enable_tracing, enable_auth);
                    let running_server = handler.await;
                    let tokio_sender = running_server._get_sender().clone();
                    tokio::task::spawn_blocking(move || {
                        match receiver.recv().expect("Failed to wait for cancellation") {
                            BridgeCommand::StopServer => tokio_sender
                                .blocking_send(())
                                .expect("Failed to send cancellation signal"),
                            BridgeCommand::StopListening => (),
                        }
                    });
                    let result = running_server.wait().await;
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
    #[pyo3(
        signature = (port = 1736, log_level = "INFO".to_string(), enable_tracing = false, enable_auth = false)
    )]
    pub fn run(
        slf: PyRefMut<Self>,
        py: Python,
        port: u16,
        log_level: String,
        enable_tracing: bool,
        enable_auth: bool,
    ) -> PyResult<()> {
        let mut server =
            Self::start(slf, port, log_level, enable_tracing, enable_auth)?.server_handler;
        py.allow_threads(|| wait_server(&mut server))
    }
}

fn adapt_graphql_value(value: &ValueAccessor, py: Python) -> PyObject {
    match value.as_value() {
        GraphqlValue::Number(number) => {
            if number.is_f64() {
                number.as_f64().unwrap().to_object(py)
            } else if number.is_u64() {
                number.as_u64().unwrap().to_object(py)
            } else {
                number.as_i64().unwrap().to_object(py)
            }
        }
        GraphqlValue::String(value) => value.to_object(py),
        GraphqlValue::Boolean(value) => value.to_object(py),
        value => panic!("graphql input value {value} has an unsuported type"),
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
pub(crate) struct PyRunningRaphtoryServer {
    server_handler: Option<ServerHandler>,
}

enum BridgeCommand {
    StopServer,
    StopListening,
}

struct ServerHandler {
    join_handle: JoinHandle<IoResult<()>>,
    sender: CrossbeamSender<BridgeCommand>,
    client: PyRaphtoryClient,
}

impl PyRunningRaphtoryServer {
    fn new(
        join_handle: JoinHandle<IoResult<()>>,
        sender: CrossbeamSender<BridgeCommand>,
        port: u16,
    ) -> Self {
        let url = format!("http://localhost:{port}");
        let server_handler = Some(ServerHandler {
            join_handle,
            sender,
            client: PyRaphtoryClient::new(url),
        });

        PyRunningRaphtoryServer { server_handler }
    }

    fn apply_if_alive<O, F>(&self, function: F) -> PyResult<O>
    where
        F: FnOnce(&ServerHandler) -> PyResult<O>,
    {
        match &self.server_handler {
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
    pub(crate) fn wait(mut slf: PyRefMut<Self>, py: Python) -> PyResult<()> {
        let server = &mut slf.server_handler;
        py.allow_threads(|| wait_server(server))
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
    #[pyo3(signature = (path, overwrite = false))]
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
                Some(JsonValue::Array(errors)) => Err(PyException::new_err(format!(
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
        variables: HashMap<String, JsonValue>,
    ) -> PyResult<(JsonValue, HashMap<String, JsonValue>)> {
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
            Some(JsonValue::Array(loads)) => {
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
            Err(PyException::new_err(format!(
                "Failed to connect to the server after {} milliseconds",
                millis
            )))
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
            Some(JsonValue::String(name)) => {
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
    #[pyo3(signature = (path, overwrite = false))]
    fn load_graphs_from_path(
        &self,
        py: Python,
        path: String,
        overwrite: bool,
    ) -> PyResult<HashMap<String, PyObject>> {
        self.generic_load_graphs(py, "loadGraphsFromPath", path)
    }
}

fn translate_from_python(py: Python, value: PyObject) -> PyResult<JsonValue> {
    if let Ok(value) = value.extract::<i64>(py) {
        Ok(JsonValue::Number(value.into()))
    } else if let Ok(value) = value.extract::<f64>(py) {
        Ok(JsonValue::Number(Number::from_f64(value).unwrap()))
    } else if let Ok(value) = value.extract::<bool>(py) {
        Ok(JsonValue::Bool(value))
    } else if let Ok(value) = value.extract::<String>(py) {
        Ok(JsonValue::String(value))
    } else if let Ok(value) = value.extract::<Vec<PyObject>>(py) {
        let mut vec = Vec::new();
        for item in value {
            vec.push(translate_from_python(py, item)?);
        }
        Ok(JsonValue::Array(vec))
    } else if let Ok(value) = value.extract::<&PyDict>(py) {
        let mut map = Map::new();
        for (key, value) in value.iter() {
            let key = key.extract::<String>()?;
            let value = translate_from_python(py, value.into_py(py))?;
            map.insert(key, value);
        }
        Ok(JsonValue::Object(map))
    } else {
        Err(PyErr::new::<PyTypeError, _>("Unsupported type"))
    }
}

fn translate_map_to_python(
    py: Python,
    input: HashMap<String, JsonValue>,
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
        JsonValue::Number(num) => {
            if num.is_i64() {
                Ok(num.as_i64().unwrap().into_py(py))
            } else if num.is_f64() {
                Ok(num.as_f64().unwrap().into_py(py))
            } else {
                Err(PyErr::new::<PyTypeError, _>("Unsupported number type"))
            }
        }
        JsonValue::String(s) => Ok(s.into_py(py)),
        JsonValue::Array(vec) => {
            let mut list = Vec::new();
            for item in vec {
                list.push(translate_to_python(py, item)?);
            }
            Ok(list.into_py(py))
        }
        JsonValue::Object(map) => {
            let dict = PyDict::new(py);
            for (key, value) in map {
                dict.set_item(key, translate_to_python(py, value)?)?;
            }
            Ok(dict.into())
        }
        JsonValue::Bool(b) => Ok(b.into_py(py)),
        JsonValue::Null => Ok(py.None()),
    }
}

fn encode_graph(graph: MaterializedGraph) -> PyResult<String> {
    let result = url_encode_graph(graph);
    match result {
        Ok(s) => Ok(s),
        Err(e) => Err(PyValueError::new_err(format!("Error encoding: {:?}", e))),
    }
}
