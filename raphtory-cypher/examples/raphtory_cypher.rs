use arrow::util::pretty::print_batches;
use clap::Parser;
use futures::{stream, StreamExt};
use raphtory::arrow::graph_impl::ArrowGraph;
use raphtory_cypher::{run_cypher, run_cypher_to_streams};

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

    let graph = ArrowGraph::load_from_dir(&args.graph_dir).expect("Failed to load graph");

    let now = std::time::Instant::now();

    if args.print {
        let df = run_cypher(&args.query, &graph).await.unwrap();
        let batches = df.collect().await.unwrap();
        print_batches(&batches).expect("Failed to print batches");
    } else {
        let streams = run_cypher_to_streams(&args.query, &graph).await.unwrap();
        let num_rows = stream::iter(streams)
            .flatten()
            .filter_map(|batch| async move { batch.ok() })
            .map(|batch| batch.num_rows())
            .fold(0, |acc, x| async move { acc + x })
            .await;
        println!("Query execution: {:?}, num_rows: {num_rows}", now.elapsed());
    }
}
