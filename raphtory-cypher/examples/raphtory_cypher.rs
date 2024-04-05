use arrow::util::pretty::print_batches;
use raphtory::arrow::graph_impl::ArrowGraph;
use raphtory_cypher::run_cypher;

#[tokio::main]
async fn main() {
    let graph_dir = std::env::args().nth(1).expect("No graph file provided");
    let cypher_query = std::env::args().nth(2).expect("No cypher query provided");

    let graph = ArrowGraph::load_from_dir(graph_dir).expect("Failed to load graph");

    let now = std::time::Instant::now();
    let df = run_cypher(&cypher_query, &graph).await.unwrap();
    let batches = df.collect().await.unwrap();
    println!("Query execution: {:?}", now.elapsed());

    print_batches(&batches).expect("Failed to print batches");
}
