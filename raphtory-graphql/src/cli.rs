#[cfg(feature = "search")]
use crate::config::index_config::DEFAULT_CREATE_INDEX;
use crate::{
    config::{
        app_config::AppConfigBuilder,
        auth_config::{DEFAULT_AUTH_ENABLED_FOR_READS, PUBLIC_KEY_DECODING_ERR_MSG},
        cache_config::{DEFAULT_CAPACITY, DEFAULT_TTI_SECONDS},
        log_config::DEFAULT_LOG_LEVEL,
        otlp_config::{
            TracingLevel, DEFAULT_OTLP_AGENT_HOST, DEFAULT_OTLP_AGENT_PORT,
            DEFAULT_OTLP_TRACING_SERVICE_NAME, DEFAULT_TRACING_ENABLED, DEFAULT_TRACING_LEVEL,
        },
    },
    model::App,
    server::DEFAULT_PORT,
    GraphServer,
};
use clap::{command, Parser, Subcommand};
use std::path::PathBuf;
use tokio::io::Result as IoResult;

#[derive(Parser)]
#[command(
    name = "raphtory",
    about = "Run the GraphServer with specified configurations"
)]
struct Args {
    #[arg(long, env = "RAPHTORY_WORKING_DIR", default_value = ".")]
    working_dir: PathBuf,

    #[arg(long, env = "RAPHTORY_PORT", default_value_t = DEFAULT_PORT)]
    port: u16,

    #[arg(long, env = "RAPHTORY_CACHE_CAPACITY", default_value_t = DEFAULT_CAPACITY)]
    cache_capacity: u64,

    #[arg(long, env = "RAPHTORY_CACHE_TTI_SECONDS", default_value_t = DEFAULT_TTI_SECONDS)]
    cache_tti_seconds: u64,

    #[arg(long, env = "RAPHTORY_LOG_LEVEL", default_value = DEFAULT_LOG_LEVEL)]
    log_level: String,

    #[arg(long, env = "RAPHTORY_TRACING", default_value_t = DEFAULT_TRACING_ENABLED)]
    tracing: bool,

    #[arg(long, env = "RAPHTORY_TRACING_LEVEL", default_value_t = DEFAULT_TRACING_LEVEL)]
    tracing_level: TracingLevel,

    #[arg(long, env = "RAPHTORY_OTLP_AGENT_HOST", default_value = DEFAULT_OTLP_AGENT_HOST)]
    otlp_agent_host: String,

    #[arg(long, env = "RAPHTORY_OTLP_AGENT_PORT", default_value = DEFAULT_OTLP_AGENT_PORT)]
    otlp_agent_port: String,

    #[arg(long, env = "RAPHTORY_OTLP_TRACING_SERVICE_NAME", default_value = DEFAULT_OTLP_TRACING_SERVICE_NAME)]
    otlp_tracing_service_name: String,

    #[arg(long, env = "RAPHTORY_AUTH_PUBLIC_KEY", default_value = None)]
    auth_public_key: Option<String>,

    #[arg(long, env = "RAPHTORY_AUTH_ENABLED_FOR_READS", default_value_t = DEFAULT_AUTH_ENABLED_FOR_READS)]
    auth_enabled_for_reads: bool,

    #[arg(long, env = "RAPHTORY_PUBLIC_DIR", default_value = None)]
    public_dir: Option<PathBuf>,

    #[cfg(feature = "search")]
    #[arg(long, env = "RAPHTORY_CREATE_INDEX", default_value_t = DEFAULT_CREATE_INDEX)]
    create_index: bool,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    #[command(about = "Print the GraphQL schema to the standard output")]
    Schema,
}

pub(crate) async fn cli_with_args<I, T>(args_iter: I) -> IoResult<()>
where
    I: IntoIterator<Item = T>,
    T: Into<std::ffi::OsString> + Clone,
{
    let args = Args::parse_from(args_iter);

    if let Some(Commands::Schema) = args.command {
        let schema = App::create_schema().finish().unwrap();
        println!("{}", schema.sdl());
    } else {
        let mut builder = AppConfigBuilder::new()
            .with_cache_capacity(args.cache_capacity)
            .with_cache_tti_seconds(args.cache_tti_seconds)
            .with_log_level(args.log_level)
            .with_tracing(args.tracing)
            .with_tracing_level(args.tracing_level)
            .with_otlp_agent_host(args.otlp_agent_host)
            .with_otlp_agent_port(args.otlp_agent_port)
            .with_otlp_tracing_service_name(args.otlp_tracing_service_name)
            .with_auth_public_key(args.auth_public_key)
            .expect(PUBLIC_KEY_DECODING_ERR_MSG)
            .with_public_dir(args.public_dir)
            .with_auth_enabled_for_reads(args.auth_enabled_for_reads);

        #[cfg(feature = "search")]
        {
            builder = builder.with_create_index(args.create_index);
        }

        let app_config = Some(builder.build());

        GraphServer::new(args.working_dir, app_config, None)?
            .run_with_port(args.port)
            .await?;
    }
    Ok(())
}

pub(crate) async fn cli() -> IoResult<()> {
    cli_with_args(std::env::args_os()).await
}

#[cfg(feature = "python")]
#[pyo3::pyfunction(name = "cli")]
pub fn python_cli() -> pyo3::PyResult<()> {
    dbg!(std::env::args().collect::<Vec<_>>());
    // Replace argv[0] with "raphtory" so clap doesn't interpret the script path as a subcommand
    let args = std::iter::once("raphtory".to_string()).chain(std::env::args().skip(2));

    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime
        .block_on(cli_with_args(args))
        .map_err(|err| pyo3::exceptions::PyIOError::new_err(err.to_string()))
}
