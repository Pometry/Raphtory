use crate::{
    config::app_config::AppConfigBuilder,
    model::algorithms::{
        algorithm_entry_point::AlgorithmEntryPoint, document::GqlDocument,
        global_plugins::GlobalPlugins, vector_algorithms::VectorAlgorithms,
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
    exceptions::{PyAttributeError, PyException},
    pyclass, pymethods,
    types::{IntoPyDict, PyFunction, PyList},
    IntoPy, Py, PyObject, PyRefMut, PyResult, Python, ToPyObject,
};
use raphtory::{
    python::{types::wrappers::document::PyDocument, utils::execute_async_task},
    vectors::{
        embedding_cache::EmbeddingCache, embeddings::openai_embedding, template::DocumentTemplate,
        EmbeddingFunction,
    },
};
use std::{collections::HashMap, path::PathBuf, thread};

/// A class for defining and running a Raphtory GraphQL server
#[pyclass(name = "GraphServer")]
pub struct PyGraphServer(pub(crate) Option<GraphServer>);

impl IntoPy<PyObject> for GraphServer {
    fn into_py(self, py: Python) -> PyObject {
        Py::new(py, PyGraphServer(Some(self))).unwrap().into_py(py)
    }
}

fn template_from_python(
    graph_template: Option<String>,
    node_template: Option<String>,
    edge_template: Option<String>,
) -> Option<DocumentTemplate> {
    if graph_template.is_none() && node_template.is_none() && edge_template.is_none() {
        None
    } else {
        Some(DocumentTemplate {
            graph_template,
            node_template,
            edge_template,
        })
    }
}

impl PyGraphServer {
    fn new(server: GraphServer) -> Self {
        Self(Some(server))
    }

    fn set_generic_embeddings<F: EmbeddingFunction + Clone + 'static>(
        slf: PyRefMut<Self>,
        cache: String,
        embedding: F,
        graph_template: Option<String>,
        node_template: Option<String>,
        edge_template: Option<String>,
    ) -> PyResult<GraphServer> {
        let global_template = template_from_python(graph_template, node_template, edge_template);
        let server = take_server_ownership(slf)?;
        let cache = PathBuf::from(cache);
        Ok(server.set_embeddings(embedding, &cache, global_template))
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
    ) -> PyResult<GraphServer> {
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
        Ok(new_server)
    }
}

#[pymethods]
impl PyGraphServer {
    #[new]
    #[pyo3(
        signature = (work_dir, cache_capacity = None, cache_tti_seconds = None, log_level = None, tracing=None, otlp_agent_host=None, otlp_agent_port=None, otlp_tracing_service_name=None, config_path = None)
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
        let app_config = Some(app_config_builder.build());

        let server = GraphServer::new(work_dir, app_config, config_path)?;
        Ok(PyGraphServer::new(server))
    }

    fn set_embeddings(
        slf: PyRefMut<Self>,
        cache: String,
        embedding: Option<&PyFunction>,
        graph_template: Option<String>,
        node_template: Option<String>,
        edge_template: Option<String>,
    ) -> PyResult<GraphServer> {
        match embedding {
            Some(embedding) => {
                let embedding: Py<PyFunction> = embedding.into();
                Self::set_generic_embeddings(
                    slf,
                    cache,
                    embedding,
                    graph_template,
                    node_template,
                    edge_template,
                )
            }
            None => Self::set_generic_embeddings(
                slf,
                cache,
                openai_embedding,
                graph_template,
                node_template,
                edge_template,
            ),
        }
    }

    /// Vectorise a subset of the graphs of the server.
    ///
    /// Note:
    ///   If no embedding function is provided, the server will attempt to use the OpenAI API
    ///   embedding model, which will only work if the env variable OPENAI_API_KEY is set
    ///   appropriately
    ///
    /// Arguments:
    ///   graph_names (List[str]): the names of the graphs to vectorise. All by default.
    ///   cache (str):  the directory to use as cache for the embeddings.
    ///   embedding (Function):  the embedding function to translate documents to embeddings.
    ///   graph_template (String):  the template to use for graphs.
    ///   node_template (String):  the template to use for nodes.
    ///   edge_template (String):  the template to use for edges.
    ///
    /// Returns:
    ///    GraphServer: A new server object containing the vectorised graphs.
    fn with_vectorised_graphs(
        slf: PyRefMut<Self>,
        graph_names: Vec<String>,
        // TODO: support more models by just providing a string, e.g. "openai", here and in the VectorisedGraph API
        graph_template: Option<String>,
        node_template: Option<String>,
        edge_template: Option<String>,
    ) -> PyResult<GraphServer> {
        let template = template_from_python(graph_template, node_template, edge_template).ok_or(
            PyAttributeError::new_err(
                "some of graph_template, node_template, edge_template has to be set",
            ),
        )?;
        let server = take_server_ownership(slf)?;
        Ok(server.with_vectorised_graphs(graph_names, template))
    }

    /// Start the server and return a handle to it.
    ///
    /// Arguments:
    ///   port (int):  the port to use (defaults to 1736).
    ///   timeout_ms (int): wait for server to be online (defaults to 5000). The server is stopped if not online within timeout_ms but manages to come online as soon as timeout_ms finishes!
    #[pyo3(
        signature = (port = 1736, timeout_ms = None)
    )]
    pub fn start(
        slf: PyRefMut<Self>,
        py: Python,
        port: u16,
        timeout_ms: Option<u64>,
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
            match PyRunningGraphServer::wait_for_server_online(&url, timeout_ms) {
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
    ///   port (int): The port to use (defaults to 1736).
    #[pyo3(
        signature = (port = 1736, timeout_ms = Some(180000))
    )]
    pub fn run(
        slf: PyRefMut<Self>,
        py: Python,
        port: u16,
        timeout_ms: Option<u64>,
    ) -> PyResult<()> {
        let mut server = Self::start(slf, py, port, timeout_ms)?.server_handler;
        py.allow_threads(|| wait_server(&mut server))
    }
}
