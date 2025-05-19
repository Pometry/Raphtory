use crate::{
    config::{app_config::AppConfigBuilder, auth_config::PUBLIC_KEY_DECODING_ERR_MSG},
    model::{
        algorithms::document::GqlDocument,
        plugins::{entry_point::EntryPoint, query_plugin::QueryPlugin},
    },
    python::{
        adapt_graphql_value,
        global_plugins::PyGlobalPlugins,
        server::{
            running_server::PyRunningGraphServer, take_server_ownership, wait_server, BridgeCommand,
        },
    },
    GraphServer,
};
use async_graphql::dynamic::{Field, FieldFuture, FieldValue, InputValue, Object, TypeRef};
use dynamic_graphql::internal::{Registry, TypeName};
use itertools::intersperse;
use pyo3::{
    exceptions::{PyAttributeError, PyException, PyValueError},
    prelude::*,
    types::{IntoPyDict, PyFunction, PyList},
    IntoPyObjectExt,
};
use raphtory::{
    db::api::view::DynamicGraph,
    python::{packages::vectors::TemplateConfig, types::wrappers::document::PyDocument},
    vectors::{
        embeddings::openai_embedding,
        template::{DocumentTemplate, DEFAULT_EDGE_TEMPLATE, DEFAULT_NODE_TEMPLATE},
        Document, EmbeddingFunction,
    },
};
use std::{collections::HashMap, path::PathBuf, sync::Arc, thread};

/// A class for defining and running a Raphtory GraphQL server
///
/// Arguments:
///     work_dir (str | PathLike): the working directory for the server
///     cache_capacity (int, optional): the maximum number of graphs to keep in memory at once
///     cache_tti_seconds (int, optional): the inactive time in seconds after which a graph is evicted from the cache
///     log_level (str, optional): the log level for the server
///     tracing (bool, optional): whether tracing should be enabled
///     otlp_agent_host (str, optional): OTLP agent host for tracing
///     otlp_agent_port(str, optional): OTLP agent port for tracing
///     otlp_tracing_service_name (str, optional): The OTLP tracing service name
///     config_path (str | PathLike, optional): Path to the config file
#[pyclass(name = "GraphServer", module = "raphtory.graphql")]
pub struct PyGraphServer(pub Option<GraphServer>);

impl<'py> IntoPyObject<'py> for GraphServer {
    type Target = PyGraphServer;
    type Output = Bound<'py, Self::Target>;
    type Error = <Self::Target as IntoPyObject<'py>>::Error;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyGraphServer::new(self).into_pyobject(py)
    }
}

fn template_from_python(nodes: TemplateConfig, edges: TemplateConfig) -> Option<DocumentTemplate> {
    if nodes.is_disabled() && edges.is_disabled() {
        None
    } else {
        Some(DocumentTemplate {
            node_template: nodes.get_template_or(DEFAULT_NODE_TEMPLATE),
            edge_template: edges.get_template_or(DEFAULT_EDGE_TEMPLATE),
        })
    }
}

impl PyGraphServer {
    pub fn new(server: GraphServer) -> Self {
        Self(Some(server))
    }

    fn set_generic_embeddings<F: EmbeddingFunction + Clone + 'static>(
        slf: PyRefMut<Self>,
        cache: String,
        embedding: F,
        nodes: TemplateConfig,
        edges: TemplateConfig,
    ) -> PyResult<GraphServer> {
        let global_template = template_from_python(nodes, edges);
        let server = take_server_ownership(slf)?;
        let cache = PathBuf::from(cache);
        Ok(server.set_embeddings(embedding, &cache, global_template))
    }

    fn with_generic_document_search_function<
        'a,
        E: EntryPoint<'a> + 'static,
        F: Fn(&E, Python) -> PyObject + Send + Sync + 'static,
    >(
        slf: PyRefMut<Self>,
        name: String,
        input: HashMap<String, String>,
        function: Py<PyFunction>,
        adapter: F,
    ) -> PyResult<GraphServer> {
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

        // FIXME: this should return a result!
        let register_function = |name: &str, registry: Registry, parent: Object| {
            let registry = registry.register::<GqlDocument>();
            let output_type = TypeRef::named_nn_list_nn(GqlDocument::get_type_name());
            let mut field = Field::new(name, output_type, move |ctx| {
                let documents: Vec<Document<DynamicGraph>> = Python::with_gil(|py| {
                    let entry_point = adapter(ctx.parent_value.downcast_ref().unwrap(), py);
                    let kw_args: HashMap<&str, PyObject> = ctx
                        .args
                        .iter()
                        .map(|(name, value)| (name.as_str(), adapt_graphql_value(&value, py)))
                        .collect();
                    let py_kw_args = kw_args.into_py_dict(py).unwrap();
                    let result = function
                        .call(py, (entry_point,), Some(&py_kw_args))
                        .unwrap();
                    let list = result.downcast_bound::<PyList>(py).unwrap();
                    let py_documents = list.iter().map(|doc| doc.extract::<PyDocument>().unwrap());
                    py_documents.map(|doc| doc.into()).collect()
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
        Ok(new_server)
    }
}

#[pymethods]
impl PyGraphServer {
    #[new]
    #[pyo3(
        signature = (work_dir, cache_capacity = None, cache_tti_seconds = None, log_level = None, tracing=None, otlp_agent_host=None, otlp_agent_port=None, otlp_tracing_service_name=None, auth_public_key=None, auth_enabled_for_reads=None, config_path = None)
    )]
    fn py_new(
        work_dir: PathBuf,
        cache_capacity: Option<u64>,
        cache_tti_seconds: Option<u64>,
        log_level: Option<String>,
        tracing: Option<bool>,
        otlp_agent_host: Option<String>,
        otlp_agent_port: Option<String>,
        otlp_tracing_service_name: Option<String>,
        auth_public_key: Option<String>,
        auth_enabled_for_reads: Option<bool>,
        config_path: Option<PathBuf>,
    ) -> PyResult<Self> {
        let mut app_config_builder = AppConfigBuilder::new();
        if let Some(log_level) = log_level {
            app_config_builder = app_config_builder.with_log_level(log_level);
        }
        if let Some(tracing) = tracing {
            app_config_builder = app_config_builder.with_tracing(tracing);
        }
        if let Some(otlp_agent_host) = otlp_agent_host {
            app_config_builder = app_config_builder.with_otlp_agent_host(otlp_agent_host);
        }
        if let Some(otlp_agent_port) = otlp_agent_port {
            app_config_builder = app_config_builder.with_otlp_agent_port(otlp_agent_port);
        }
        if let Some(otlp_tracing_service_name) = otlp_tracing_service_name {
            app_config_builder =
                app_config_builder.with_otlp_tracing_service_name(otlp_tracing_service_name);
        }
        if let Some(cache_capacity) = cache_capacity {
            app_config_builder = app_config_builder.with_cache_capacity(cache_capacity);
        }
        if let Some(cache_tti_seconds) = cache_tti_seconds {
            app_config_builder = app_config_builder.with_cache_tti_seconds(cache_tti_seconds);
        }
        app_config_builder = app_config_builder
            .with_auth_public_key(auth_public_key)
            .map_err(|_| PyValueError::new_err(PUBLIC_KEY_DECODING_ERR_MSG))?;
        if let Some(auth_enabled_for_reads) = auth_enabled_for_reads {
            app_config_builder =
                app_config_builder.with_auth_enabled_for_reads(auth_enabled_for_reads);
        }
        let app_config = Some(app_config_builder.build());

        let server = GraphServer::new(work_dir, app_config, config_path)?;
        Ok(PyGraphServer::new(server))
    }

    /// Turn off index for all graphs
    ///
    /// Returns:
    ///     GraphServer: The server with indexing disabled
    fn turn_off_index(slf: PyRefMut<Self>) -> PyResult<GraphServer> {
        let server = take_server_ownership(slf)?;
        Ok(server.turn_off_index())
    }

    /// Setup the server to vectorise graphs with a default template.
    ///
    /// Arguments:
    ///   cache (str):  the directory to use as cache for the embeddings.
    ///   embedding (Callable, optional):  the embedding function to translate documents to embeddings.
    ///   nodes (bool | str): if nodes have to be embedded or not or the custom template to use if a str is provided. Defaults to True.
    ///   edges (bool | str): if edges have to be embedded or not or the custom template to use if a str is provided. Defaults to True.
    ///
    /// Returns:
    ///    GraphServer: A new server object with embeddings setup.
    #[pyo3(
        signature = (cache, embedding = None, nodes = TemplateConfig::Bool(true), edges = TemplateConfig::Bool(true))
    )]
    fn set_embeddings(
        slf: PyRefMut<Self>,
        cache: String,
        embedding: Option<Py<PyFunction>>,
        nodes: TemplateConfig,
        edges: TemplateConfig,
    ) -> PyResult<GraphServer> {
        match embedding {
            Some(embedding) => {
                let embedding: Arc<dyn EmbeddingFunction> = Arc::new(embedding);
                Self::set_generic_embeddings(slf, cache, embedding, nodes, edges)
            }
            None => Self::set_generic_embeddings(slf, cache, openai_embedding, nodes, edges),
        }
    }

    /// Vectorise a subset of the graphs of the server.
    ///
    /// Arguments:
    ///   graph_names (list[str]): the names of the graphs to vectorise. All by default.
    ///   nodes (bool | str): if nodes have to be embedded or not or the custom template to use if a str is provided. Defaults to True.
    ///   edges (bool | str): if edges have to be embedded or not or the custom template to use if a str is provided. Defaults to True.
    ///
    /// Returns:
    ///    GraphServer: A new server object containing the vectorised graphs.
    #[pyo3(
        signature = (graph_names, nodes = TemplateConfig::Bool(true), edges = TemplateConfig::Bool(true))
    )]
    fn with_vectorised_graphs(
        slf: PyRefMut<Self>,
        graph_names: Vec<String>,
        // TODO: support more models by just providing a string, e.g. "openai", here and in the VectorisedGraph API
        nodes: TemplateConfig,
        edges: TemplateConfig,
    ) -> PyResult<GraphServer> {
        let template = template_from_python(nodes, edges).ok_or(PyAttributeError::new_err(
            "node_template and/or edge_template has to be set",
        ))?;
        let server = take_server_ownership(slf)?;
        Ok(server.with_vectorised_graphs(graph_names, template))
    }

    /// Register a function in the GraphQL schema for document search among all the graphs.
    ///
    /// The function needs to take a `GraphqlGraphs` object as the first argument followed by a
    /// pre-defined set of keyword arguments. Supported types are `str`, `int`, and `float`.
    /// They have to be specified using the `input` parameter as a dict where the keys are the
    /// names of the parameters and the values are the types, expressed as strings.
    ///
    /// Arguments:
    ///   name (str): the name of the function in the GraphQL schema.
    ///   input (dict[str, str]):  the keyword arguments expected by the function.
    ///   function (Callable): the function to run.
    ///
    /// Returns:
    ///    GraphServer: A new server object with the function registered
    pub fn with_global_search_function(
        slf: PyRefMut<Self>,
        name: String,
        input: HashMap<String, String>,
        function: Py<PyFunction>,
    ) -> PyResult<GraphServer> {
        let adapter = |entry_point: &QueryPlugin, py: Python| {
            PyGlobalPlugins(entry_point.clone())
                .into_py_any(py)
                .unwrap()
        };
        PyGraphServer::with_generic_document_search_function(slf, name, input, function, adapter)
    }

    /// Start the server and return a handle to it.
    ///
    /// Arguments:
    ///   port (int):  the port to use. Defaults to 1736.
    ///   timeout_ms (int): wait for server to be online. Defaults to 5000.
    ///     The server is stopped if not online within timeout_ms but manages to come online as soon as timeout_ms finishes!
    ///
    /// Returns:
    ///   RunningGraphServer: The running server
    #[pyo3(
        signature = (port = 1736, timeout_ms = 5000)
    )]
    pub fn start(
        slf: PyRefMut<Self>,
        py: Python,
        port: u16,
        timeout_ms: u64,
    ) -> PyResult<PyRunningGraphServer> {
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
                    let running_server = handler.await?;
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

        let mut server = PyRunningGraphServer::new(join_handle, sender, port)?;
        if let Some(_server_handler) = &server.server_handler {
            let url = format!("http://localhost:{port}");
            // we need to release the GIL, otherwise the server will deadlock when trying to use python function as the embedding function
            // and wait_for_server_online will never return
            let result =
                py.allow_threads(|| PyRunningGraphServer::wait_for_server_online(&url, timeout_ms));
            match result {
                Ok(_) => return Ok(server),
                Err(e) => {
                    PyRunningGraphServer::stop_server(&mut server, py)?;
                    Err(e)
                }
            }
        } else {
            Err(PyException::new_err("Failed to start server"))
        }
    }

    /// Run the server until completion.
    ///
    /// Arguments:
    ///   port (int): The port to use. Defaults to 1736.
    ///   timeout_ms (int): Timeout for waiting for the server to start. Defaults to 180000.
    ///
    /// Returns:
    ///     None:
    #[pyo3(
        signature = (port = 1736, timeout_ms = 180000)
    )]
    pub fn run(slf: PyRefMut<Self>, py: Python, port: u16, timeout_ms: u64) -> PyResult<()> {
        let mut server = Self::start(slf, py, port, timeout_ms)?.server_handler;
        py.allow_threads(|| wait_server(&mut server))
    }
}
