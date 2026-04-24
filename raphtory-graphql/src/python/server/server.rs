use crate::{
    config::{
        app_config::AppConfigBuilder, auth_config::PUBLIC_KEY_DECODING_ERR_MSG,
        otlp_config::TracingLevel,
    },
    python::server::{running_server::PyRunningGraphServer, wait_server, BridgeCommand},
    server::apply_server_extension,
    GraphServer,
};
use pyo3::{
    exceptions::{PyAttributeError, PyException, PyValueError},
    prelude::*,
};
use raphtory::{
    db::api::storage::storage::Config,
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
///     auth_public_key (str, optional): Base64-encoded public key used to verify bearer tokens
///     require_auth_for_reads (bool, optional): Require auth tokens for read queries
///     create_index (bool, optional): Build a search index on startup
///     heavy_query_limit (int, optional): Maximum number of expensive traversal queries (outComponent, inComponent, edges, outEdges, inEdges, neighbours, outNeighbours, inNeighbours) allowed to run simultaneously. Extra queries are parked on a semaphore.
///     exclusive_writes (bool, optional): If True, ingestion/write operations run one at a time and block reads until complete.
///     disable_batching (bool, optional): If True, batched GraphQL requests are rejected. Prevents bypassing per-request depth/complexity limits.
///     max_batch_size (int, optional): Caps the number of queries accepted in a single batched request.
///     disable_lists (bool, optional): If True, bulk `list` endpoints on collections are disabled. Clients must use `page` instead.
///     max_page_size (int, optional): Maximum page size allowed on paged collection queries.
///     max_query_depth (int, optional): Maximum nesting depth of a query.
///     max_query_complexity (int, optional): Maximum estimated cost of a query, based on the number of fields selected.
///     max_recursive_depth (int, optional): Internal safety limit to prevent stack overflows from pathologically structured queries (async-graphql default is 32).
///     max_directives_per_field (int, optional): Maximum number of directives on any single field.
///     disable_introspection (bool, optional): If True, schema introspection is disabled entirely.
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
        signature = (
            work_dir,
            cache_capacity = None,
            cache_tti_seconds = None,
            log_level = None,
            tracing=None,
            tracing_level=None,
            otlp_agent_host=None,
            otlp_agent_port=None,
            otlp_tracing_service_name=None,
            auth_public_key=None,
            require_auth_for_reads=None,
            config_path = None,
            create_index = None,
            heavy_query_limit = None,
            exclusive_writes = None,
            disable_batching = None,
            max_batch_size = None,
            disable_lists = None,
            max_page_size = None,
            max_query_depth = None,
            max_query_complexity = None,
            max_recursive_depth = None,
            max_directives_per_field = None,
            disable_introspection = None,
            permissions_store_path = None
        )
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
        require_auth_for_reads: Option<bool>,
        config_path: Option<PathBuf>,
        create_index: Option<bool>,
        heavy_query_limit: Option<usize>,
        exclusive_writes: Option<bool>,
        disable_batching: Option<bool>,
        max_batch_size: Option<usize>,
        disable_lists: Option<bool>,
        max_page_size: Option<usize>,
        max_query_depth: Option<usize>,
        max_query_complexity: Option<usize>,
        max_recursive_depth: Option<usize>,
        max_directives_per_field: Option<usize>,
        disable_introspection: Option<bool>,
        permissions_store_path: Option<PathBuf>,
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
        if let Some(require_auth_for_reads) = require_auth_for_reads {
            app_config_builder =
                app_config_builder.with_require_auth_for_reads(require_auth_for_reads);
        }
        #[cfg(feature = "search")]
        if let Some(create_index) = create_index {
            app_config_builder = app_config_builder.with_create_index(create_index);
        }
        if heavy_query_limit.is_some() {
            app_config_builder = app_config_builder.with_heavy_query_limit(heavy_query_limit);
        }
        if let Some(exclusive_writes) = exclusive_writes {
            app_config_builder = app_config_builder.with_exclusive_writes(exclusive_writes);
        }
        if let Some(disable_batching) = disable_batching {
            app_config_builder = app_config_builder.with_disable_batching(disable_batching);
        }
        if max_batch_size.is_some() {
            app_config_builder = app_config_builder.with_max_batch_size(max_batch_size);
        }
        if let Some(disable_lists) = disable_lists {
            app_config_builder = app_config_builder.with_disable_lists(disable_lists);
        }
        if max_page_size.is_some() {
            app_config_builder = app_config_builder.with_max_page_size(max_page_size);
        }
        if max_query_depth.is_some() {
            app_config_builder = app_config_builder.with_max_query_depth(max_query_depth);
        }
        if max_query_complexity.is_some() {
            app_config_builder = app_config_builder.with_max_query_complexity(max_query_complexity);
        }
        if max_recursive_depth.is_some() {
            app_config_builder = app_config_builder.with_max_recursive_depth(max_recursive_depth);
        }
        if max_directives_per_field.is_some() {
            app_config_builder =
                app_config_builder.with_max_directives_per_field(max_directives_per_field);
        }
        if let Some(disable_introspection) = disable_introspection {
            app_config_builder =
                app_config_builder.with_disable_introspection(disable_introspection);
        }
        let app_config = Some(app_config_builder.build());

        let server = block_on(GraphServer::new(
            work_dir,
            app_config,
            config_path,
            Config::default(),
        ))?;
        let server = apply_server_extension(server, permissions_store_path.as_deref());
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
        py.detach(|| wait_server(&mut server))
    }
}
