use raphtory_graphql::RaphtoryServer;
use std::{env, path::PathBuf};
use tokio::io::Result as IoResult;

#[tokio::main]
async fn main() -> IoResult<()> {
    let work_dir = env::var("GRAPH_DIRECTORY").unwrap_or("/tmp/graphs".to_string());
    let work_dir = PathBuf::from(&work_dir);

    let args: Vec<String> = env::args().collect();
    let use_auth = args.contains(&"--server".to_string());

    if use_auth {
        RaphtoryServer::new(work_dir, None, None)?
            .run_with_auth(false)
            .await?;
    } else {
        RaphtoryServer::new(work_dir, None, None)?
            .run(false)
            .await?;
    }

    Ok(())
}
