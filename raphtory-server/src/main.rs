use std::io::Result as IoResult;

#[tokio::main]
async fn main() -> IoResult<()> {
    auth::init();
    raphtory_graphql::cli::cli().await
}
