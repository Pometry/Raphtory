use crate::{
    config::{
        app_config::{AppConfig, AppConfigBuilder},
        auth_config::DEFAULT_REQUIRE_AUTH_FOR_READS,
        cache_config::DEFAULT_CACHE_CAPACITY,
        concurrency_config::{
            DEFAULT_DISABLE_BATCHING, DEFAULT_EXCLUSIVE_WRITES, DEFAULT_MAX_BATCH_SIZE,
        },
        log_config::DEFAULT_LOG_LEVEL,
        otlp_config::{
            TracingLevel, TracingProtocol, DEFAULT_OTLP_TRACING_SERVICE_NAME,
            DEFAULT_OTLP_TRANSPORT_PROTOCOL, DEFAULT_TRACING_ENABLED, DEFAULT_TRACING_LEVEL,
        },
        schema_config::DEFAULT_DISABLE_INTROSPECTION,
    },
    model::App,
    server::{apply_server_extension, DEFAULT_PORT},
    GraphServer,
};
use clap::{Parser, Subcommand};
use raphtory::db::api::storage::storage::Config;
use serde::Serialize;
use std::{collections::HashMap, io, path::PathBuf};
use tokio::io::Result as IoResult;

fn parse_json_map(input: &str) -> Result<HashMap<String, String>, serde_json::Error> {
    serde_json::from_str(input)
}

macro_rules! help_with_default {
    ($help:expr, $default:expr) => {
        format!("{} Default: '{}'", $help, $default)
    };
}

#[derive(Parser, Debug)]
#[command(name = "raphtory", about = "Raphtory CLI", version = raphtory::version())]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    #[command(about = "Run the GraphQL server")]
    Server(ServerArgs),
    #[command(about = "Print the GraphQL schema")]
    Schema,
}
#[derive(clap::Args, Debug, Serialize)]
struct ServerArgs {
    #[arg(long, help = "Path to stored config.")]
    config_file: Option<PathBuf>,
    #[arg(
        long,
        env = "RAPHTORY_WORK_DIR",
        default_value = ".",
        help = help_with_default!("Working directory.", "."),
        hide_default_value = true
    )]
    work_dir: PathBuf,

    #[arg(
        long,
        env = "RAPHTORY_PORT",
        help = help_with_default!("Port for Raphtory to run on.", DEFAULT_PORT)
    )]
    port: Option<u16>,

    #[arg(long, env = "RAPHTORY_CACHE_CAPACITY", help = help_with_default!("Cache capacity.", DEFAULT_CACHE_CAPACITY))]
    cache_capacity: Option<u64>,

    #[arg(long, env = "RAPHTORY_LOG_LEVEL", help = help_with_default!("Log level.", DEFAULT_LOG_LEVEL))]
    log_level: Option<String>,

    #[arg(long, env = "RAPHTORY_TRACING", help = help_with_default!("Enable tracing.", DEFAULT_TRACING_ENABLED))]
    tracing: Option<bool>,

    #[arg(
        long,
        env = "RAPHTORY_TRACING_LEVEL",
        help = help_with_default!("Set tracing level.", DEFAULT_TRACING_LEVEL),
        long_help = "Options are:\n'COMPLETE': for full traces through each query.\n'ESSENTIAL': which tracks these key functions addEdge, addEdges, deleteEdge, graph, updateGraph, addNode, node, nodes, edge, edges.\n'MINIMAL': which provides only summary execution times."
    )]
    tracing_level: Option<TracingLevel>,

    #[arg(long, env = "RAPHTORY_OTLP_AGENT_HOST", help = "OTLP agent host.")]
    otlp_agent_host: Option<String>,

    #[arg(
        long,
        env = "RAPHTORY_OTLP_TRACING_SERVICE_NAME",
        help = help_with_default!("OTLP tracing service name.", DEFAULT_OTLP_TRACING_SERVICE_NAME)
    )]
    otlp_tracing_service_name: Option<String>,

    #[arg(
        long,
        env = "RAPHTORY_OTLP_TRANSPORT_PROTOCOL",
        help = help_with_default!("OTLP transport protocol.", DEFAULT_OTLP_TRANSPORT_PROTOCOL)
    )]
    otlp_transport_protocol: Option<TracingProtocol>,

    #[arg(long, env="RAPHTORY_OTLP_TRANSPORT_HEADERS", value_parser = parse_json_map)]
    /// Headers for use with OTLP HTTP protocol (expects a json-encoded map from keys to string values)
    otlp_transport_headers: Option<HashMap<String, String>>,

    #[arg(
        long,
        env = "RAPHTORY_OTLP_TRANSPORT_CERTIFICATE",
        help = "Path to certificate to use for OTLP transport in `.pem` format"
    )]
    otlp_transport_certificate: Option<PathBuf>,

    #[arg(long, env = "RAPHTORY_AUTH_PUBLIC_KEY", help = "Public key for auth")]
    auth_public_key: Option<String>,

    #[arg(
        long,
        env = "RAPHTORY_REQUIRE_AUTH_FOR_READS",
        help = help_with_default!("Require JWT authentication for read requests.", DEFAULT_REQUIRE_AUTH_FOR_READS)
    )]
    require_auth_for_reads: Option<bool>,

    #[arg(
        long,
        env = "RAPHTORY_HEAVY_QUERY_LIMIT",
        help = "Restricts how many expensive graph traversal queries can execute simultaneously.",
        long_help = "Covers operations like connected components, edge traversals, and neighbour lookups (outComponent, inComponent, edges, outEdges, inEdges, neighbours, outNeighbours, inNeighbours). Once the limit is exceeded, queries are parked on a semaphore and wait until a slot becomes available before executing."
    )]
    heavy_query_limit: Option<usize>,

    #[arg(
        long,
        env = "RAPHTORY_EXCLUSIVE_WRITES",
        help = help_with_default!("Ensures only one ingestion/write operation runs at a time and blocks reads until it completes.", DEFAULT_EXCLUSIVE_WRITES)
    )]
    exclusive_writes: Option<bool>,

    #[arg(
        long,
        env = "RAPHTORY_DISABLE_BATCHING",
        help = help_with_default!("Rejects batched GraphQL requests outright. Batching can otherwise be used to circumvent per-request depth and complexity limits.", DEFAULT_DISABLE_BATCHING)
    )]
    disable_batching: Option<bool>,

    #[arg(
        long,
        env = "RAPHTORY_MAX_BATCH_SIZE",
        help = help_with_default!("Caps the number of queries accepted in a single batched HTTP request. Requests whose batch exceeds this size are rejected.", DEFAULT_MAX_BATCH_SIZE)
    )]
    max_batch_size: Option<usize>,

    #[arg(
        long,
        env = "RAPHTORY_DISABLE_LISTS",
        help = "Completely disables bulk list endpoints (e.g. listing all nodes/edges). Essential for large graphs where unbounded list queries could return billions of results and exhaust server resources."
    )]
    disable_lists: Option<bool>,

    #[arg(
        long,
        env = "RAPHTORY_MAX_PAGE_SIZE",
        help = "Maximum page size enforced on paged collection queries. Caps the `limit` argument of `page` so clients can't circumvent `disable_lists` by requesting huge pages."
    )]
    max_page_size: Option<usize>,

    #[arg(
        long,
        env = "RAPHTORY_MAX_QUERY_DEPTH",
        help = "Limits how deeply nested a query can be."
    )]
    max_query_depth: Option<usize>,

    #[arg(
        long,
        env = "RAPHTORY_MAX_QUERY_COMPLEXITY",
        help = "Limits the total estimated cost of a query based on the number of fields selected. Blocks queries that try to fetch too much data in one request."
    )]
    max_query_complexity: Option<usize>,

    #[arg(
        long,
        env = "RAPHTORY_MAX_RECURSIVE_DEPTH",
        help = "Internal safety limit to prevent stack overflows from pathologically structured queries. Falls back to the async-graphql default of 32 if unset."
    )]
    max_recursive_depth: Option<usize>,

    #[arg(
        long,
        env = "RAPHTORY_MAX_DIRECTIVES_PER_FIELD",
        help = "Limits the number of GraphQL directives on any single field. Directives are annotations prefixed with @ that modify how a field is executed (e.g. @skip, @include, @deprecated)."
    )]
    max_directives_per_field: Option<usize>,

    #[arg(
        long,
        env = "RAPHTORY_DISABLE_INTROSPECTION",
        help = help_with_default!("Fully disable schema introspection, preventing clients from discovering the API's structure and available fields. Recommended for production.", DEFAULT_DISABLE_INTROSPECTION)
    )]
    disable_introspection: Option<bool>,

    #[arg(long, env = "RAPHTORY_PUBLIC_DIR", help = "Public directory path")]
    public_dir: Option<PathBuf>,

    #[arg(long, env = "RAPHTORY_PERMISSIONS_STORE_PATH", default_value = None, help = "Path to the JSON permissions store file.")]
    permissions_store_path: Option<PathBuf>,

    #[command(flatten)]
    graph_config: Config,
}

fn generate_config<I, T>(args_iter: I) -> IoResult<Option<(ServerArgs, AppConfig)>>
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString> + Clone,
{
    let args = Args::parse_from(args_iter);
    match args.command {
        Commands::Schema => {
            let schema = App::create_schema().finish().unwrap();
            println!("{}", schema.sdl());
            Ok(None)
        }
        Commands::Server(server_args) => {
            let mut builder = AppConfigBuilder::new();
            if let Some(config_file) = server_args.config_file.clone() {
                builder.load_from_path(config_file)?;
            };
            if let Some(cache_capacity) = server_args.cache_capacity {
                builder.with_cache_capacity(cache_capacity);
            }
            if let Some(log_level) = server_args.log_level.clone() {
                builder.with_log_level(log_level);
            }
            if let Some(tracing) = server_args.tracing {
                builder.with_tracing(tracing);
            }
            if let Some(tracing_level) = server_args.tracing_level.clone() {
                builder.with_tracing_level(tracing_level);
            }
            if let Some(otlp_agent_host) = server_args.otlp_agent_host.clone() {
                builder.with_otlp_agent_host(Some(otlp_agent_host));
            }
            if let Some(otlp_tracing_service_name) = server_args.otlp_tracing_service_name.clone() {
                builder.with_otlp_tracing_service_name(otlp_tracing_service_name);
            }
            if let Some(otlp_transport_protocol) = server_args.otlp_transport_protocol.clone() {
                builder.with_otlp_transport_protocol(otlp_transport_protocol);
            }
            if let Some(otlp_transport_headers) = server_args.otlp_transport_headers.clone() {
                builder.with_otlp_transport_headers(otlp_transport_headers);
            }
            if let Some(otlp_transport_certificate) = server_args.otlp_transport_certificate.clone()
            {
                builder.with_otlp_transport_certificate(Some(otlp_transport_certificate));
            }
            if let Some(auth_public_key) = server_args.auth_public_key.clone() {
                builder
                    .with_auth_public_key(Some(auth_public_key))
                    .map_err(|err| io::Error::new(io::ErrorKind::Other, err))?;
            }
            if let Some(public_dir) = server_args.public_dir.clone() {
                builder.with_public_dir(Some(public_dir));
            }
            if let Some(require_auth_for_reads) = server_args.require_auth_for_reads {
                builder.with_require_auth_for_reads(require_auth_for_reads);
            }
            if let Some(heavy_query_limit) = server_args.heavy_query_limit {
                builder.with_heavy_query_limit(Some(heavy_query_limit));
            }
            if let Some(exclusive_writes) = server_args.exclusive_writes {
                builder.with_exclusive_writes(exclusive_writes);
            }
            if let Some(disable_batching) = server_args.disable_batching {
                builder.with_disable_batching(disable_batching);
            }
            if let Some(max_batch_size) = server_args.max_batch_size {
                builder.with_max_batch_size(Some(max_batch_size));
            }
            if let Some(disable_lists) = server_args.disable_lists {
                builder.with_disable_lists(disable_lists);
            }
            if let Some(max_page_size) = server_args.max_page_size {
                builder.with_max_page_size(Some(max_page_size));
            }
            if let Some(max_query_depth) = server_args.max_query_depth {
                builder.with_max_query_depth(Some(max_query_depth));
            }
            if let Some(max_query_complexity) = server_args.max_query_complexity {
                builder.with_max_query_complexity(Some(max_query_complexity));
            }
            if let Some(max_recursive_depth) = server_args.max_recursive_depth {
                builder.with_max_recursive_depth(Some(max_recursive_depth));
            }
            if let Some(max_directives_per_field) = server_args.max_directives_per_field {
                builder.with_max_directives_per_field(Some(max_directives_per_field));
            }
            if let Some(disable_introspection) = server_args.disable_introspection {
                builder.with_disable_introspection(disable_introspection);
            }

            let app_config = builder.build();
            Ok(Some((server_args, app_config)))
        }
    }
}

pub(crate) async fn cli_with_args<I, T>(args_iter: I) -> IoResult<()>
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString> + Clone,
{
    if let Some((server_args, app_config)) = generate_config(args_iter)? {
        let server = GraphServer::new(
            server_args.work_dir,
            Some(app_config),
            server_args.graph_config,
        )
        .await?;
        let server = apply_server_extension(server, server_args.permissions_store_path.as_deref());
        match server_args.port {
            None => {
                server.run().await?;
            }
            Some(port) => {
                server.run_with_port(port).await?;
            }
        }
    }
    Ok(())
}

pub async fn cli() -> IoResult<()> {
    cli_with_args(std::env::args_os()).await
}

/// Run the Raphtory GraphQL CLI from Python. Uses `sys.argv` for arguments.
///
/// Returns:
///     None:
#[cfg(feature = "python")]
#[pyo3::pyfunction(name = "cli")]
pub fn python_cli() -> pyo3::PyResult<()> {
    // Replace argv[0] with "raphtory" so clap doesn't interpret the script path as a subcommand
    let args = std::iter::once("raphtory".to_string()).chain(std::env::args().skip(2));

    let runtime = tokio::runtime::Runtime::new()?;
    runtime
        .block_on(cli_with_args(args))
        .map_err(|err| pyo3::exceptions::PyIOError::new_err(err.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::Builder;

    fn config_file() -> tempfile::NamedTempFile {
        let mut config_file = Builder::new()
            .suffix(".toml")
            .tempfile()
            .expect("failed to create temporary config file for CLI test");
        write!(config_file, "[cache]\ncapacity = 123\n")
            .expect("failed to write temporary cache config");
        config_file
            .flush()
            .expect("failed to flush temporary cache config");
        config_file
    }

    async fn test_cli_parsing_no_arguments() {
        let args: Vec<&str> = vec![r"target\\debug\\raphtory-server", "server"];
        std::env::remove_var("RAPHTORY_CACHE_CAPACITY");
        let (_, app_config) = generate_config(args).unwrap().unwrap();
        assert_eq!(app_config.cache.capacity, DEFAULT_CACHE_CAPACITY);
    }

    async fn test_cli_parsing_with_config_file() {
        let config_file = config_file();
        let args: Vec<&str> = vec![
            r"target\\debug\\raphtory-server",
            "server",
            "--config-file",
            config_file.path().to_str().unwrap(),
        ];
        std::env::remove_var("RAPHTORY_CACHE_CAPACITY");
        let (_, app_config) = generate_config(args).unwrap().unwrap();
        assert_eq!(app_config.cache.capacity, 123);
    }

    async fn test_cli_parsing_with_env_var() {
        let config_file = config_file();
        let args: Vec<&str> = vec![
            r"target\\debug\\raphtory-server",
            "server",
            "--config-file",
            config_file.path().to_str().unwrap(),
        ];
        std::env::set_var("RAPHTORY_CACHE_CAPACITY", "456");
        let (_, app_config) = generate_config(args).unwrap().unwrap();
        assert_eq!(app_config.cache.capacity, 456);
    }

    async fn test_cli_parsing_with_command_line_arg() {
        let config_file = config_file();
        let args: Vec<&str> = vec![
            r"target\\debug\\raphtory-server",
            "server",
            "--config-file",
            config_file.path().to_str().unwrap(),
            "--cache-capacity",
            "789",
        ];
        std::env::set_var("RAPHTORY_CACHE_CAPACITY", "456");
        let (_, app_config) = generate_config(args).unwrap().unwrap();
        assert_eq!(app_config.cache.capacity, 789);
    }

    #[tokio::test]
    async fn test_cli_parsing() {
        // tests must be synchronized so that env variables are not modified in parallel
        test_cli_parsing_no_arguments().await;
        test_cli_parsing_with_config_file().await;
        test_cli_parsing_with_env_var().await;
        test_cli_parsing_with_command_line_arg().await;
    }
}
