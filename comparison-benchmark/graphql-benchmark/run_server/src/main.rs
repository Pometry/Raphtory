use raphtory_graphql::RaphtoryServer;

#[tokio::main(flavor = "multi_thread", worker_threads = 63)]
async fn main() {
    let graph_directory =
        "/Users/haaroony/Documents/dev/Raphtory/comparison-benchmark/graphql-benchmark/graphs";
    RaphtoryServer::from_directory(&graph_directory)
        .run("INFO")
        .await
        .unwrap()
}
