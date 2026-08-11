use crate::{
    config::{
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
    plugin::server::extension::ArgExtensions,
    server::DEFAULT_PORT,
    GraphServer,
};
use clap::{Parser, Subcommand};
use raphtory::db::api::storage::storage::Config;
use serde::Serialize;
use serde_json::json;
use std::{collections::HashMap, fmt::Debug, path::PathBuf};
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
pub struct Args {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    #[command(about = "Run the GraphQL server")]
    Server(ServerArgs),
    #[command(about = "Print the GraphQL schema")]
    Schema,
}
#[derive(clap::Args, Debug, Serialize)]
pub struct ServerArgs {
    #[arg(long, help = "Path to stored config.")]
    pub(crate) config_file: Option<PathBuf>,
    #[arg(
        long,
        env = "RAPHTORY_WORK_DIR",
        default_value = ".",
        help = help_with_default!("Working directory.", "."),
        hide_default_value = true
    )]
    pub(crate) work_dir: PathBuf,

    #[arg(
        long,
        env = "RAPHTORY_PORT",
        help = help_with_default!("Port for Raphtory to run on.", DEFAULT_PORT)
    )]
    port: Option<u16>,

    #[arg(long, env = "RAPHTORY_CACHE_CAPACITY", help = help_with_default!("Cache capacity.", DEFAULT_CACHE_CAPACITY))]
    pub(crate) cache_capacity: Option<u64>,

    #[arg(long, env = "RAPHTORY_LOG_LEVEL", help = help_with_default!("Log level.", DEFAULT_LOG_LEVEL))]
    pub(crate) log_level: Option<String>,

    #[arg(long, env = "RAPHTORY_TRACING", help = help_with_default!("Enable tracing.", DEFAULT_TRACING_ENABLED))]
    pub(crate) tracing: Option<bool>,

    #[arg(
        long,
        env = "RAPHTORY_TRACING_LEVEL",
        help = help_with_default!("Set tracing level.", DEFAULT_TRACING_LEVEL),
        long_help = "Options are:\n'COMPLETE': for full traces through each query.\n'ESSENTIAL': which tracks these key functions addEdge, addEdges, deleteEdge, graph, updateGraph, addNode, node, nodes, edge, edges.\n'MINIMAL': which provides only summary execution times."
    )]
    pub(crate) tracing_level: Option<TracingLevel>,

    #[arg(long, env = "RAPHTORY_OTLP_AGENT_HOST", help = "OTLP agent host.")]
    pub(crate) otlp_agent_host: Option<String>,

    #[arg(
        long,
        env = "RAPHTORY_OTLP_TRACING_SERVICE_NAME",
        help = help_with_default!("OTLP tracing service name.", DEFAULT_OTLP_TRACING_SERVICE_NAME)
    )]
    pub(crate) otlp_tracing_service_name: Option<String>,

    #[arg(
        long,
        env = "RAPHTORY_OTLP_TRANSPORT_PROTOCOL",
        help = help_with_default!("OTLP transport protocol.", DEFAULT_OTLP_TRANSPORT_PROTOCOL)
    )]
    pub(crate) otlp_transport_protocol: Option<TracingProtocol>,

    #[arg(long, env="RAPHTORY_OTLP_TRANSPORT_HEADERS", value_parser = parse_json_map)]
    /// Headers for use with OTLP HTTP protocol (expects a json-encoded map from keys to string values)
    pub(crate) otlp_transport_headers: Option<HashMap<String, String>>,

    #[arg(
        long,
        env = "RAPHTORY_OTLP_TRANSPORT_CERTIFICATE",
        help = "Path to certificate to use for OTLP transport in `.pem` format"
    )]
    pub(crate) otlp_transport_certificate: Option<PathBuf>,

    #[arg(long, env = "RAPHTORY_AUTH_PUBLIC_KEY", help = "Public key for auth")]
    pub(crate) auth_public_key: Option<String>,

    #[arg(
        long,
        env = "RAPHTORY_REQUIRE_AUTH_FOR_READS",
        help = help_with_default!("Require JWT authentication for read requests.", DEFAULT_REQUIRE_AUTH_FOR_READS)
    )]
    pub(crate) require_auth_for_reads: Option<bool>,

    #[arg(
        long,
        env = "RAPHTORY_HEAVY_QUERY_LIMIT",
        help = "Restricts how many expensive graph traversal queries can execute simultaneously.",
        long_help = "Covers operations like connected components, edge traversals, and neighbour lookups (outComponent, inComponent, edges, outEdges, inEdges, neighbours, outNeighbours, inNeighbours). Once the limit is exceeded, queries are parked on a semaphore and wait until a slot becomes available before executing."
    )]
    pub(crate) heavy_query_limit: Option<usize>,

    #[arg(
        long,
        env = "RAPHTORY_EXCLUSIVE_WRITES",
        help = help_with_default!("Ensures only one ingestion/write operation runs at a time and blocks reads until it completes.", DEFAULT_EXCLUSIVE_WRITES)
    )]
    pub(crate) exclusive_writes: Option<bool>,

    #[arg(
        long,
        env = "RAPHTORY_DISABLE_BATCHING",
        help = help_with_default!("Rejects batched GraphQL requests outright. Batching can otherwise be used to circumvent per-request depth and complexity limits.", DEFAULT_DISABLE_BATCHING)
    )]
    pub(crate) disable_batching: Option<bool>,

    #[arg(
        long,
        env = "RAPHTORY_MAX_BATCH_SIZE",
        help = help_with_default!("Caps the number of queries accepted in a single batched HTTP request. Requests whose batch exceeds this size are rejected.", DEFAULT_MAX_BATCH_SIZE)
    )]
    pub(crate) max_batch_size: Option<usize>,

    #[arg(
        long,
        env = "RAPHTORY_DISABLE_LISTS",
        help = "Completely disables bulk list endpoints (e.g. listing all nodes/edges). Essential for large graphs where unbounded list queries could return billions of results and exhaust server resources."
    )]
    pub(crate) disable_lists: Option<bool>,

    #[arg(
        long,
        env = "RAPHTORY_MAX_PAGE_SIZE",
        help = "Maximum page size enforced on paged collection queries. Caps the `limit` argument of `page` so clients can't circumvent `disable_lists` by requesting huge pages."
    )]
    pub(crate) max_page_size: Option<usize>,

    #[arg(
        long,
        env = "RAPHTORY_MAX_QUERY_DEPTH",
        help = "Limits how deeply nested a query can be."
    )]
    pub(crate) max_query_depth: Option<usize>,

    #[arg(
        long,
        env = "RAPHTORY_MAX_QUERY_COMPLEXITY",
        help = "Limits the total estimated cost of a query based on the number of fields selected. Blocks queries that try to fetch too much data in one request."
    )]
    pub(crate) max_query_complexity: Option<usize>,

    #[arg(
        long,
        env = "RAPHTORY_MAX_RECURSIVE_DEPTH",
        help = "Internal safety limit to prevent stack overflows from pathologically structured queries. Falls back to the async-graphql default of 32 if unset."
    )]
    pub(crate) max_recursive_depth: Option<usize>,

    #[arg(
        long,
        env = "RAPHTORY_MAX_DIRECTIVES_PER_FIELD",
        help = "Limits the number of GraphQL directives on any single field. Directives are annotations prefixed with @ that modify how a field is executed (e.g. @skip, @include, @deprecated)."
    )]
    pub(crate) max_directives_per_field: Option<usize>,

    #[arg(
        long,
        env = "RAPHTORY_DISABLE_INTROSPECTION",
        help = help_with_default!("Fully disable schema introspection, preventing clients from discovering the API's structure and available fields. Recommended for production.", DEFAULT_DISABLE_INTROSPECTION)
    )]
    pub(crate) disable_introspection: Option<bool>,

    #[arg(long, env = "RAPHTORY_PUBLIC_DIR", help = "Public directory path")]
    pub(crate) public_dir: Option<PathBuf>,

    #[arg(long, env = "RAPHTORY_PERMISSIONS_STORE_PATH", default_value = None, help = "Path to the JSON permissions store file.")]
    permissions_store_path: Option<PathBuf>,

    #[arg(long, help = "Print the configuration and exit.")]
    print_config: bool,

    #[command(flatten)]
    pub(crate) graph_config: Config,

    #[command(flatten)]
    pub(crate) extensions: ArgExtensions,
}

pub async fn cli_with_args<I, T>(args_iter: I) -> IoResult<()>
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString> + Clone,
{
    let args = Args::parse_from(args_iter);
    match args.command {
        Commands::Server(server_args) => {
            let port = server_args.port;
            let print_config = server_args.print_config;
            let server = GraphServer::new_from_args(server_args).await?;
            if print_config {
                let config = json!(server.config());
                println!("{}", config);
            } else {
                match port {
                    None => {
                        server.run().await?;
                    }
                    Some(port) => {
                        server.run_with_port(port).await?;
                    }
                }
            }
        }
        Commands::Schema => {
            let schema = App::create_schema().finish().unwrap();
            println!("{}", schema.sdl());
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
    use crate::config::app_config::{AppConfig, AppConfigBuilder};
    use std::io::Write;
    use tempfile::Builder;

    fn generate_config(args: Vec<&str>) -> AppConfig {
        let args = Args::try_parse_from(args).unwrap();
        match args.command {
            Commands::Server(server_args) => AppConfigBuilder::new()
                .update_from_args(&server_args)
                .unwrap()
                .build(),
            Commands::Schema => {
                panic!("expected server command")
            }
        }
    }

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
        let args: Vec<&str> = vec![r"raphtory-server", "server"];
        std::env::remove_var("RAPHTORY_CACHE_CAPACITY");
        let app_config = generate_config(args);
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
        let app_config = generate_config(args);
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
        let app_config = generate_config(args);
        assert_eq!(app_config.cache.capacity, 456);
    }

    async fn test_cli_parsing_with_command_line_arg() {
        let config_file = config_file();
        let args: Vec<&str> = vec![
            r"raphtory-server",
            "server",
            "--config-file",
            config_file.path().to_str().unwrap(),
            "--cache-capacity",
            "789",
        ];
        std::env::set_var("RAPHTORY_CACHE_CAPACITY", "456");
        let app_config = generate_config(args);
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

    #[tokio::test]
    async fn test_cli_parsing_extension() {
        // tests must be synchronized so that env variables are not modified in parallel
        test_cli_parsing_no_arguments().await;
        test_cli_parsing_with_config_file().await;
        test_cli_parsing_with_env_var().await;
        test_cli_parsing_with_command_line_arg().await;
    }
}
