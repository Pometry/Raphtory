use raphtory_graphql::RaphtoryServer;

#[tokio::main]
async fn main() {
    let graph_directory =
        "/Users/haaroony/Documents/dev/Raphtory/comparison-benchmark/graphql-benchmark/graphs";
    RaphtoryServer::from_directory(&graph_directory)
        .run("INFO")
        .await
        .unwrap()
}
