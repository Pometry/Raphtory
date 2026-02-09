use crate::{
    config::{
        app_config::AppConfigBuilder, auth_config::PUBLIC_KEY_DECODING_ERR_MSG,
        otlp_config::TracingLevel,
    },
    python::server::{running_server::PyRunningGraphServer, wait_server, BridgeCommand},
    GraphServer,
};
use pyo3::{
    exceptions::{PyAttributeError, PyException, PyValueError},
    prelude::*,
};
use raphtory::{
    python::{
        packages::vectors::{PyOpenAIEmbeddings, TemplateConfig},
        utils::block_on,
    },
    vectors::template::{DocumentTemplate, DEFAULT_EDGE_TEMPLATE, DEFAULT_NODE_TEMPLATE},
};
use std::{path::PathBuf, thread};

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
///     auth_public_key:
///     auth_enabled_for_reads:
///     create_index:
#[pyclass(name = "GraphServer", module = "raphtory.graphql")]
pub struct PyGraphServer(GraphServer);

impl<'py> IntoPyObject<'py> for GraphServer {
    type Target = PyGraphServer;
    type Output = Bound<'py, Self::Target>;
    type Error = <Self::Target as IntoPyObject<'py>>::Error;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyGraphServer(self).into_pyobject(py)
    }
}

fn template_from_python(
    nodes: TemplateConfig,
    edges: TemplateConfig,
) -> PyResult<DocumentTemplate> {
    if nodes.is_disabled() && edges.is_disabled() {
        Err(PyAttributeError::new_err(
            "at least one of nodes and edges has to be set to True or some string",
        ))
    } else {
        Ok(DocumentTemplate {
            node_template: nodes.get_template_or(DEFAULT_NODE_TEMPLATE),
            edge_template: edges.get_template_or(DEFAULT_EDGE_TEMPLATE),
        })
    }
}

#[pymethods]
impl PyGraphServer {
    #[new]
    #[pyo3(
        signature = (work_dir, cache_capacity = None, cache_tti_seconds = None, log_level = None, tracing=None, tracing_level=None, otlp_agent_host=None, otlp_agent_port=None, otlp_tracing_service_name=None, auth_public_key=None, auth_enabled_for_reads=None, config_path = None, create_index = None)
    )]
    fn py_new(
        work_dir: PathBuf,
        cache_capacity: Option<u64>,
        cache_tti_seconds: Option<u64>,
        log_level: Option<String>,
        tracing: Option<bool>,
        tracing_level: Option<String>,
        otlp_agent_host: Option<String>,
        otlp_agent_port: Option<String>,
        otlp_tracing_service_name: Option<String>,
        auth_public_key: Option<String>,
        auth_enabled_for_reads: Option<bool>,
        config_path: Option<PathBuf>,
        create_index: Option<bool>,
    ) -> PyResult<Self> {
        let mut app_config_builder = AppConfigBuilder::new();
        if let Some(log_level) = log_level {
            app_config_builder = app_config_builder.with_log_level(log_level);
        }
        if let Some(tracing) = tracing {
            app_config_builder = app_config_builder.with_tracing(tracing);
        }
        if let Some(tracing_level) = tracing_level {
            let json = format!("\"{}\"", tracing_level).to_uppercase();
            let tl: TracingLevel = serde_json::from_str(json.as_str()).map_err(|_| {
                PyValueError::new_err(format!(
                    "Invalid tracing level. Allowed levels {} ",
                    TracingLevel::all_levels_string()
                ))
            })?;
            app_config_builder = app_config_builder.with_tracing_level(tl);
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
        #[cfg(feature = "search")]
        if let Some(create_index) = create_index {
            app_config_builder = app_config_builder.with_create_index(create_index);
        }
        let app_config = Some(app_config_builder.build());

        let server = block_on(GraphServer::new(work_dir, app_config, config_path))?;
        Ok(PyGraphServer(server))
    }

    // TODO: remove this, should be config
    /// Turn off index for all graphs
    fn turn_off_index(mut slf: PyRefMut<Self>) {
        slf.0.turn_off_index()
    }

    /// Vectorise the graph name in the server working directory.
    ///
    /// Arguments:
    ///     name (list[str]): the name of the graph to vectorise.
    ///     embeddings (OpenAIEmbeddings): the embeddings to use
    ///     nodes (bool | str): if nodes have to be embedded or not or the custom template to use if a str is provided. Defaults to True.
    ///     edges (bool | str): if edges have to be embedded or not or the custom template to use if a str is provided. Defaults to True.
    #[pyo3(
        signature = (name, embeddings, nodes = TemplateConfig::Bool(true), edges = TemplateConfig::Bool(true))
    )]
    fn vectorise_graph(
        &self,
        py: Python,
        name: &str,
        embeddings: PyOpenAIEmbeddings,
        nodes: TemplateConfig,
        edges: TemplateConfig,
    ) -> PyResult<()> {
        let template = template_from_python(nodes, edges)?;
        // allow threads just in case the embedding server is using the same python runtime
        py.allow_threads(|| {
            block_on(async move {
                self.0
                    .vectorise_graph(name, &template, embeddings.into())
                    .await?;
                Ok(())
            })
        })
    }

    /// Vectorise all graphs in the server working directory.
    ///
    /// Arguments:
    ///     embeddings (OpenAIEmbeddings): the embeddings to use
    ///     nodes (bool | str): if nodes have to be embedded or not or the custom template to use if a str is provided. Defaults to True.
    ///     edges (bool | str): if edges have to be embedded or not or the custom template to use if a str is provided. Defaults to True.
    #[pyo3(
        signature = (embeddings, nodes = TemplateConfig::Bool(true), edges = TemplateConfig::Bool(true))
    )]
    fn vectorise_all_graphs(
        &self,
        py: Python,
        embeddings: PyOpenAIEmbeddings,
        nodes: TemplateConfig,
        edges: TemplateConfig,
    ) -> PyResult<()> {
        let template = template_from_python(nodes, edges)?;
        // allow threads just in case the embedding server is using the same python runtime
        py.allow_threads(|| {
            block_on(async move {
                self.0
                    .vectorise_all_graphs(&template, embeddings.into())
                    .await?;
                Ok(())
            })
        })
    }

    /// Start the server and return a handle to it.
    ///
    /// Arguments:
    ///     port (int): the port to use. Defaults to 1736.
    ///     timeout_ms (int): wait for server to be online. Defaults to 5000.
    ///
    /// The server is stopped if not online within timeout_ms but manages to come online as soon as timeout_ms finishes!
    ///
    /// Returns:
    ///     RunningGraphServer: The running server
    #[pyo3(
        signature = (port = 1736, timeout_ms = 5000)
    )]
    pub fn start(&self, py: Python, port: u16, timeout_ms: u64) -> PyResult<PyRunningGraphServer> {
        let (sender, receiver) = crossbeam_channel::bounded::<BridgeCommand>(1);
        let cloned_sender = sender.clone();
        let server = self.0.clone();

        let join_handle = thread::spawn(move || {
            block_on(async move {
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
            let result = server.wait_for_server_online(&url, timeout_ms);
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
    ///     port (int): The port to use. Defaults to 1736.
    ///     timeout_ms (int): Timeout for waiting for the server to start. Defaults to 180000.
    ///
    /// Returns:
    ///     None:
    #[pyo3(
        signature = (port = 1736, timeout_ms = 180000)
    )]
    pub fn run(&self, py: Python, port: u16, timeout_ms: u64) -> PyResult<()> {
        let mut server = self.start(py, port, timeout_ms)?.server_handler;
        py.allow_threads(|| wait_server(&mut server))
    }
}
