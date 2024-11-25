use clap::{command, Parser};
use raphtory_graphql::{
    config::{
        app_config::AppConfigBuilder,
        cache_config::{DEFAULT_CAPACITY, DEFAULT_TTI_SECONDS},
        log_config::DEFAULT_LOG_LEVEL,
        otlp_config::{
            DEFAULT_OTLP_AGENT_HOST, DEFAULT_OTLP_AGENT_PORT, DEFAULT_OTLP_TRACING_SERVICE_NAME,
            DEFAULT_TRACING_ENABLED,
        },
    },
    server::DEFAULT_PORT,
    GraphServer,
};
use std::path::PathBuf;
use tokio::io::Result as IoResult;

#[derive(Parser)]
#[command(about = "Run the GraphServer with specified configurations")]
struct Args {
    #[arg(long, default_value = "graphs")]
    working_dir: PathBuf,

    // #[arg(long, env, default_value_t = DEFAULT_PORT)]
    #[clap(long, env = "RAPHTORY_PORT", default_value_t = DEFAULT_PORT)]
    port: u16,

    #[arg(long, default_value_t = DEFAULT_CAPACITY)]
    cache_capacity: u64,

    #[arg(long, default_value_t = DEFAULT_TTI_SECONDS)]
    cache_tti_seconds: u64,

    #[arg(long, default_value = DEFAULT_LOG_LEVEL)]
    log_level: String,

    #[arg(long, default_value_t = DEFAULT_TRACING_ENABLED)]
    tracing: bool,

    #[arg(long, default_value = DEFAULT_OTLP_AGENT_HOST)]
    otlp_agent_host: String,

    #[arg(long, default_value = DEFAULT_OTLP_AGENT_PORT)]
    otlp_agent_port: String,

    #[arg(long, default_value = DEFAULT_OTLP_TRACING_SERVICE_NAME)]
    otlp_tracing_service_name: String,
}

#[tokio::main]
async fn main() -> IoResult<()> {
    let args = Args::parse();

    let app_config = Some(
        AppConfigBuilder::new()
            .with_cache_capacity(args.cache_capacity)
            .with_cache_tti_seconds(args.cache_tti_seconds)
            .with_log_level(args.log_level)
            .with_tracing(args.tracing)
            .with_otlp_agent_host(args.otlp_agent_host)
            .with_otlp_agent_port(args.otlp_agent_port)
            .with_otlp_tracing_service_name(args.otlp_tracing_service_name)
            .build(),
    );

    GraphServer::new(args.working_dir, app_config, None)?
        .run_with_port(args.port)
        .await?;
    Ok(())
}
