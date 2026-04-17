#[cfg(feature = "search")]
use crate::config::index_config::DEFAULT_CREATE_INDEX;
use crate::{
    config::{
        app_config::AppConfigBuilder,
        auth_config::{DEFAULT_AUTH_ENABLED_FOR_READS, PUBLIC_KEY_DECODING_ERR_MSG},
        cache_config::{DEFAULT_CAPACITY, DEFAULT_TTI_SECONDS},
        concurrency_config::{DEFAULT_DISABLE_BATCHING, DEFAULT_EXCLUSIVE_WRITES},
        log_config::DEFAULT_LOG_LEVEL,
        otlp_config::{
            TracingLevel, DEFAULT_OTLP_AGENT_HOST, DEFAULT_OTLP_AGENT_PORT,
            DEFAULT_OTLP_TRACING_SERVICE_NAME, DEFAULT_TRACING_ENABLED, DEFAULT_TRACING_LEVEL,
        },
        schema_config::DEFAULT_DISABLE_INTROSPECTION,
    },
    model::App,
    server::DEFAULT_PORT,
    GraphServer,
};
use clap::{Parser, Subcommand};
use raphtory::db::api::storage::storage::Config;
use std::path::PathBuf;
use tokio::io::Result as IoResult;

#[derive(Parser)]
#[command(name = "raphtory", about = "Raphtory CLI", version = raphtory::version())]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    #[command(about = "Run the GraphQL server")]
    Server(ServerArgs),
    #[command(about = "Print the GraphQL schema")]
    Schema,
}

#[derive(clap::Args)]
struct ServerArgs {
    #[arg(
        long,
        env = "RAPHTORY_WORK_DIR",
        default_value = ".",
        help = "Working directory"
    )]
    work_dir: PathBuf,

    #[arg(long, env = "RAPHTORY_PORT", default_value_t = DEFAULT_PORT, help = "Port for Raphtory to run on")]
    port: u16,

    #[arg(long, env = "RAPHTORY_CACHE_CAPACITY", default_value_t = DEFAULT_CAPACITY, help = "Cache capacity")]
    cache_capacity: u64,

    #[arg(long, env = "RAPHTORY_CACHE_TTI_SECONDS", default_value_t = DEFAULT_TTI_SECONDS, help = "Cache time-to-idle in seconds")]
    cache_tti_seconds: u64,

    #[arg(long, env = "RAPHTORY_LOG_LEVEL", default_value = DEFAULT_LOG_LEVEL, help = "Log level")]
    log_level: String,

    #[arg(long, env = "RAPHTORY_TRACING", default_value_t = DEFAULT_TRACING_ENABLED, help = "Enable tracing")]
    tracing: bool,

    #[arg(long, env = "RAPHTORY_TRACING_LEVEL", default_value_t = DEFAULT_TRACING_LEVEL, help = "Set tracing level. Options are:\n'COMPLETE': for full traces through each query.\n'ESSENTIAL': which tracks these key functions addEdge, addEdges, deleteEdge, graph, updateGraph, addNode, node, nodes, edge, edges.\n'MINIMAL': which provides only summary execution times.")]
    tracing_level: TracingLevel,

    #[arg(long, env = "RAPHTORY_OTLP_AGENT_HOST", default_value = DEFAULT_OTLP_AGENT_HOST, help = "OTLP agent host")]
    otlp_agent_host: String,

    #[arg(long, env = "RAPHTORY_OTLP_AGENT_PORT", default_value = DEFAULT_OTLP_AGENT_PORT, help = "OTLP agent port")]
    otlp_agent_port: String,

    #[arg(long, env = "RAPHTORY_OTLP_TRACING_SERVICE_NAME", default_value = DEFAULT_OTLP_TRACING_SERVICE_NAME, help = "OTLP tracing service name")]
    otlp_tracing_service_name: String,

    #[arg(long, env = "RAPHTORY_AUTH_PUBLIC_KEY", default_value = None, help = "Public key for auth")]
    auth_public_key: Option<String>,

    #[arg(long, env = "RAPHTORY_AUTH_ENABLED_FOR_READS", default_value_t = DEFAULT_AUTH_ENABLED_FOR_READS, help = "Enable auth for reads")]
    auth_enabled_for_reads: bool,

    #[arg(
        long,
        env = "RAPHTORY_HEAVY_QUERY_LIMIT",
        default_value = None,
        help = "Restricts how many expensive graph traversal queries can execute simultaneously. Covers operations like connected components, edge traversals, and neighbour lookups (outComponent, inComponent, edges, outEdges, inEdges, neighbours, outNeighbours, inNeighbours). Once the limit is exceeded, queries are parked on a semaphore and wait until a slot becomes available before executing."
    )]
    heavy_query_limit: Option<usize>,

    #[arg(
        long,
        env = "RAPHTORY_EXCLUSIVE_WRITES",
        default_value_t = DEFAULT_EXCLUSIVE_WRITES,
        help = "Ensures only one ingestion/write operation runs at a time and blocks reads until it completes."
    )]
    exclusive_writes: bool,

    #[arg(
        long,
        env = "RAPHTORY_DISABLE_BATCHING",
        default_value_t = DEFAULT_DISABLE_BATCHING,
        help = "Rejects batched GraphQL requests outright. Batching can otherwise be used to circumvent per-request depth and complexity limits."
    )]
    disable_batching: bool,

    #[arg(
        long,
        env = "RAPHTORY_MAX_BATCH_SIZE",
        default_value = None,
        help = "Caps the number of queries accepted in a single batched HTTP request. Requests whose batch exceeds this size are rejected."
    )]
    max_batch_size: Option<usize>,

    #[arg(
        long,
        env = "RAPHTORY_MAX_QUERY_DEPTH",
        default_value = None,
        help = "Limits how deeply nested a query can be."
    )]
    max_query_depth: Option<usize>,

    #[arg(
        long,
        env = "RAPHTORY_MAX_QUERY_COMPLEXITY",
        default_value = None,
        help = "Limits the total estimated cost of a query based on the number of fields selected. Blocks queries that try to fetch too much data in one request."
    )]
    max_query_complexity: Option<usize>,

    #[arg(
        long,
        env = "RAPHTORY_MAX_RECURSIVE_DEPTH",
        default_value = None,
        help = "Internal safety limit to prevent stack overflows from pathologically structured queries. Falls back to the async-graphql default of 32 if unset."
    )]
    max_recursive_depth: Option<usize>,

    #[arg(
        long,
        env = "RAPHTORY_MAX_DIRECTIVES_PER_FIELD",
        default_value = None,
        help = "Limits the number of GraphQL directives on any single field. Directives are annotations prefixed with @ that modify how a field is executed (e.g. @skip, @include, @deprecated)."
    )]
    max_directives_per_field: Option<usize>,

    #[arg(
        long,
        env = "RAPHTORY_DISABLE_INTROSPECTION",
        default_value_t = DEFAULT_DISABLE_INTROSPECTION,
        help = "Fully disable schema introspection, preventing clients from discovering the API's structure and available fields. Recommended for production."
    )]
    disable_introspection: bool,

    #[arg(long, env = "RAPHTORY_PUBLIC_DIR", default_value = None, help = "Public directory path")]
    public_dir: Option<PathBuf>,

    #[cfg(feature = "search")]
    #[arg(long, env = "RAPHTORY_CREATE_INDEX", default_value_t = DEFAULT_CREATE_INDEX, help = "Enable index creation")]
    create_index: bool,

    #[command(flatten)]
    graph_config: Config,
}

pub(crate) async fn cli_with_args<I, T>(args_iter: I) -> IoResult<()>
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString> + Clone,
{
    let args = Args::parse_from(args_iter);

    match args.command {
        Commands::Schema => {
            let schema = App::create_schema().finish().unwrap();
            println!("{}", schema.sdl());
        }
        Commands::Server(server_args) => {
            let mut builder = AppConfigBuilder::new()
                .with_cache_capacity(server_args.cache_capacity)
                .with_cache_tti_seconds(server_args.cache_tti_seconds)
                .with_log_level(server_args.log_level)
                .with_tracing(server_args.tracing)
                .with_tracing_level(server_args.tracing_level)
                .with_otlp_agent_host(server_args.otlp_agent_host)
                .with_otlp_agent_port(server_args.otlp_agent_port)
                .with_otlp_tracing_service_name(server_args.otlp_tracing_service_name)
                .with_auth_public_key(server_args.auth_public_key)
                .expect(PUBLIC_KEY_DECODING_ERR_MSG)
                .with_public_dir(server_args.public_dir)
                .with_auth_enabled_for_reads(server_args.auth_enabled_for_reads)
                .with_heavy_query_limit(server_args.heavy_query_limit)
                .with_exclusive_writes(server_args.exclusive_writes)
                .with_disable_batching(server_args.disable_batching)
                .with_max_batch_size(server_args.max_batch_size)
                .with_max_query_depth(server_args.max_query_depth)
                .with_max_query_complexity(server_args.max_query_complexity)
                .with_max_recursive_depth(server_args.max_recursive_depth)
                .with_max_directives_per_field(server_args.max_directives_per_field)
                .with_disable_introspection(server_args.disable_introspection);

            #[cfg(feature = "search")]
            {
                builder = builder.with_create_index(server_args.create_index);
            }

            let app_config = Some(builder.build());

            GraphServer::new(
                server_args.work_dir,
                app_config,
                None,
                server_args.graph_config,
            )
            .await?
            .run_with_port(server_args.port)
            .await?;
        }
    }
    Ok(())
}

pub async fn cli() -> IoResult<()> {
    cli_with_args(std::env::args_os()).await
}

#[cfg(feature = "python")]
#[pyo3::pyfunction(name = "cli")]
pub fn python_cli() -> pyo3::PyResult<()> {
    // Replace argv[0] with "raphtory" so clap doesn't interpret the script path as a subcommand
    let args = std::iter::once("raphtory".to_string()).chain(std::env::args().skip(2));

    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime
        .block_on(cli_with_args(args))
        .map_err(|err| pyo3::exceptions::PyIOError::new_err(err.to_string()))
}
