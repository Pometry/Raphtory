use raphtory_graphql::GraphServer;
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

    GraphServer::new(work_dir, None, None)?.run().await?;

    Ok(())
}
