use raphtory_graphql::{config::app_config::AppConfigBuilder, GraphServer};
use std::{
    env,
    path::{Path, PathBuf},
};
use tokio::io::Result as IoResult;

#[tokio::main]
async fn main() -> IoResult<()> {
    let default_path = Path::new("/tmp/graphs");

    let work_dir = env::var("GRAPH_DIRECTORY").unwrap_or(default_path.display().to_string());
    let work_dir = PathBuf::from(&work_dir);

    let app_config = Some(
        AppConfigBuilder::new()
            .with_cache_capacity(30)
            .with_cache_tti_seconds(900)
            .with_log_level("DEBUG".to_string())
            .with_tracing(true)
            .with_otlp_agent_host("http://localhost".to_string())
            .with_otlp_agent_port("4317".to_string())
            .with_otlp_tracing_service_name("Raphtory".to_string())
            .build(),
    );

    GraphServer::new(work_dir, app_config, None)?.run().await?;
    Ok(())
}
