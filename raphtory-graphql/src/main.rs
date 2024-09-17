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
    let app_config = Some(AppConfigBuilder::new().with_tracing(true).build());
    GraphServer::new(work_dir, app_config, None)?.run().await?;

    Ok(())
}
