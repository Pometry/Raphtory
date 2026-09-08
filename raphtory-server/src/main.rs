use std::io::Result as IoResult;

// link in the auth plugin
extern crate auth;

#[tokio::main]
async fn main() -> IoResult<()> {
    raphtory_graphql::cli::cli().await
}
