use arrow::util::pretty::print_batches;
use clap::Parser;
use futures::{stream, StreamExt};
use raphtory::arrow::graph_impl::ArrowGraph;
use raphtory_cypher::run_cypher_to_streams;

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Cypher query to run
    #[arg(short, long)]
    query: String,

    /// Graph path on disk
    #[arg(short, long)]
    graph_dir: String,

    /// Print the first batch of results
    #[arg(short, long, default_value_t = false)]
    print: bool,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    // let graph_dir = std::env::args().nth(1).expect("No graph file provided");
    // let cypher_query = std::env::args().nth(2).expect("No cypher query provided");

    let graph = ArrowGraph::load_from_dir(&args.graph_dir).expect("Failed to load graph");

    let now = std::time::Instant::now();
    let streams = run_cypher_to_streams(&args.query, &graph).await.unwrap();

    let num_rows = stream::iter(streams)
        .flatten()
        .enumerate()
        .filter_map(|(i, batch)| async move { batch.ok().map(|b| (i, b)) })
        .map(|(i, batch)| {
            let len = batch.num_rows();
            if i == 0 && args.print {
                print_batches(&[batch]).expect("Failed to print batches");
            }
            len
        })
        .fold(0, |acc, x| async move { acc + x })
        .await;

    println!("Query execution: {:?}, num_rows: {num_rows}", now.elapsed());

}
