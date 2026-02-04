use std::io::Result as IoResult;

#[tokio::main]
async fn main() -> IoResult<()> {
    raphtory_graphql::cli::cli().await
}
