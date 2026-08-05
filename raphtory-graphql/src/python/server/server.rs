use crate::{
    config::app_config::AppConfigBuilder,
    python::server::{
        running_server::PyRunningGraphServer, wait_server, BridgeCommand, ServerStarted,
    },
    server::{apply_server_extension, ServerError},
    GraphServer,
};
use crossbeam_channel::RecvTimeoutError;
use pyo3::{exceptions::PyRuntimeError, prelude::*, types::PyDict};
use pythonize::depythonize;
use raphtory::{db::api::storage::storage::Args, python::utils::block_on};
#[cfg(feature = "vectors")]
use raphtory::{
    python::packages::vectors::{PyOpenAIEmbeddings, TemplateConfig},
    vectors::template::{DocumentTemplate, DEFAULT_EDGE_TEMPLATE, DEFAULT_NODE_TEMPLATE},
};
use raphtory_api::python::error::adapt_err_value;
use std::{path::PathBuf, thread, time::Duration};

/// A class for defining and running a Raphtory GraphQL server
///
/// Arguments:
///     work_dir (str | PathLike): the working directory for the server
///     cache_capacity (int, optional): the maximum number of graphs to keep in memory at once
///     cache_tti_seconds (int, optional): the inactive time in seconds after which a graph is evicted from the cache
///     log_level (str, optional): the log level for the server
///     tracing (bool, optional): whether tracing should be enabled
///     tracing_level (str, optional): tracing verbosity (e.g. "ERROR", "WARN", "INFO", "DEBUG", "TRACE").
///     otlp_agent_host (str, optional): OTLP agent host for tracing
///     otlp_agent_port(str, optional): OTLP agent port for tracing
///     otlp_tracing_service_name (str, optional): The OTLP tracing service name
///     config_path (str | PathLike, optional): Path to the config file
///     auth_public_key (str, optional): Base64-encoded public key used to verify bearer tokens
///     require_auth_for_reads (bool, optional): Require auth tokens for read queries
///     create_index (bool, optional): Build a search index on startup
///     heavy_query_limit (int, optional): Maximum number of expensive traversal queries (outComponent, inComponent, edges, outEdges, inEdges, neighbours, outNeighbours, inNeighbours) allowed to run simultaneously. Extra queries are parked on a semaphore.
///     exclusive_writes (bool, optional): If True, ingestion/write operations run one at a time and block reads until complete.
///     disable_batching (bool, optional): If True, batched GraphQL requests are rejected. Prevents bypassing per-request depth/complexity limits.
///     max_batch_size (int, optional): Caps the number of queries accepted in a single batched request. Defaults to 10; set to null for unlimited (subject to disable_batching).
///     disable_lists (bool, optional): If True, bulk `list` endpoints on collections are disabled. Clients must use `page` instead.
///     max_page_size (int, optional): Maximum page size allowed on paged collection queries.
///     max_query_depth (int, optional): Maximum nesting depth of a query.
///     max_query_complexity (int, optional): Maximum estimated cost of a query, based on the number of fields selected.
///     max_recursive_depth (int, optional): Internal safety limit to prevent stack overflows from pathologically structured queries (async-graphql default is 32).
///     max_directives_per_field (int, optional): Maximum number of directives on any single field.
///     disable_introspection (bool, optional): If True, schema introspection is disabled entirely.
///     permissions_store_path (str | PathLike, optional): Path to the permissions store (used by the optional auth extension).
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

impl From<ServerError> for PyErr {
    fn from(value: ServerError) -> Self {
        adapt_err_value(&value)
    }
}

#[cfg(feature = "vectors")]
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
        signature = (
            work_dir, config_path=None,permissions_store_path=None, config=None
        )
    )]
    fn py_new(
        work_dir: PathBuf,
        config_path: Option<PathBuf>,
        permissions_store_path: Option<PathBuf>,
        config: Option<&Bound<'_, PyDict>>,
    ) -> PyResult<Self> {
        let mut app_config_builder = AppConfigBuilder::new();
        if let Some(config_path) = config_path {
            app_config_builder
                .load_from_path(config_path)
                .map_err(|err| PyRuntimeError::new_err(format!("Invalid config file: {err}")))?;
        }
        if let Some(config) = config {
            app_config_builder.update_from_json(depythonize(config.as_any())?)?;
        }
        let app_config = Some(app_config_builder.build());
        let server = block_on(GraphServer::new(work_dir, app_config, Args::default()))?;
        let server = apply_server_extension(server, permissions_store_path.as_deref());
        Ok(PyGraphServer(server))
    }

    // TODO: remove this, should be config
    /// Turn off index for all graphs.
    ///
    /// Returns:
    ///     None:
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
    ///
    /// Returns:
    ///     None:
    #[cfg(feature = "vectors")]
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
        py.detach(|| {
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
    ///
    /// Returns:
    ///     None:
    #[cfg(feature = "vectors")]
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
        py.detach(|| {
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
    ///     port (int, optional): the port to use. If not specified, tries 1736 by default and if that is not available starts on an arbitrary port.
    ///                           If specified and the port is in use, the server will fail to start.
    ///     timeout_ms (int): wait for server to be online. Defaults to 5000.
    ///
    /// The server is stopped if not online within timeout_ms but manages to come online as soon as timeout_ms finishes!
    ///
    /// Returns:
    ///     RunningGraphServer: The running server
    #[pyo3(
        signature = (port = None, timeout_ms = 5000)
    )]
    pub fn start(&self, port: Option<u16>, timeout_ms: u64) -> PyResult<PyRunningGraphServer> {
        let (sender, receiver) = crossbeam_channel::bounded::<BridgeCommand>(1);
        let (start_sender, start_receiver) = crossbeam_channel::bounded::<ServerStarted>(1);
        let cloned_sender = sender.clone();
        let server = self.0.clone();

        let join_handle = thread::spawn(move || {
            block_on(async move {
                let running_server = match port {
                    None => server.start().await?,
                    Some(port) => server.start_with_port(port).await?,
                };
                if let Err(_) = start_sender.send(ServerStarted {
                    port: running_server.port(),
                }) {
                    // This happens if the other end of the channel doesn't exist
                    running_server.stop().await;
                    return Ok(());
                };

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

        let port = match start_receiver.recv_timeout(Duration::from_millis(timeout_ms)) {
            Ok(msg) => msg.port,
            Err(err) => {
                match err {
                    RecvTimeoutError::Timeout => {
                        return Err(PyRuntimeError::new_err(format!(
                            "Failed to start server in {timeout_ms} milliseconds"
                        )))
                    }
                    RecvTimeoutError::Disconnected => {
                        // failure in server start, extract the error
                        let result = join_handle.join().unwrap(); // propagate any panic
                        let err = match result {
                            Ok(_) => PyRuntimeError::new_err("Failed to start server"),
                            Err(err) => adapt_err_value(&err),
                        };
                        return Err(err);
                    }
                }
            }
        };

        let server = PyRunningGraphServer::new(join_handle, sender, port)?;
        Ok(server)
    }

    /// Run the server until completion.
    ///
    /// Arguments:
    ///     port (int, optional): The port to use. If not specified, tries 1736 by default and if that is not available starts on an arbitrary port.
    ///                           If specified and the port is in use, the server will fail to start.
    ///     timeout_ms (int): Timeout for waiting for the server to start. Defaults to 180000.
    ///
    /// Returns:
    ///     None:
    #[pyo3(
        signature = (port = None, timeout_ms = 180000)
    )]
    pub fn run(&self, py: Python, port: Option<u16>, timeout_ms: u64) -> PyResult<()> {
        let mut server = self.start(port, timeout_ms)?.server_handler;
        py.detach(|| wait_server(&mut server))
    }
}
