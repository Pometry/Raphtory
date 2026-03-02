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
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tokio::io::Result as IoResult;

const SKILL_CONTENT: &str = include_str!("../../llms/SKILL.md");

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
    #[command(about = "Install Claude Code skill for the raphtory Python API")]
    InstallSkill,
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

    #[arg(long, env = "RAPHTORY_PUBLIC_DIR", default_value = None, help = "Public directory path")]
    public_dir: Option<PathBuf>,

    #[cfg(feature = "search")]
    #[arg(long, env = "RAPHTORY_CREATE_INDEX", default_value_t = DEFAULT_CREATE_INDEX, help = "Enable index creation")]
    create_index: bool,
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
        Commands::InstallSkill => {
            let home = std::env::var("HOME")
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::NotFound, e))?;
            let skill_dir = PathBuf::from(home)
                .join(".claude")
                .join("skills")
                .join("raphtory-python-api");
            std::fs::create_dir_all(&skill_dir)?;
            let skill_path = skill_dir.join("SKILL.md");
            std::fs::write(&skill_path, SKILL_CONTENT)?;
            println!(
                "Installed raphtory Claude Code skill to {}",
                skill_path.display()
            );
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
                .with_auth_enabled_for_reads(server_args.auth_enabled_for_reads);

            #[cfg(feature = "search")]
            {
                builder = builder.with_create_index(server_args.create_index);
            }

            let app_config = Some(builder.build());

            GraphServer::new(server_args.work_dir, app_config, None)
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
