use arrow::util::pretty::print_batches;
use clap::Parser;
use futures::{stream, StreamExt};
use raphtory::arrow::graph_impl::{ArrowGraph, ParquetLayerCols};
use raphtory_cypher::{run_cypher, run_cypher_to_streams};

/// Query graph with cypher
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct CypherQuery {
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

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct LoadGraph {
    /// Graph path on disk
    #[arg(short, long)]
    graph_dir: String,
    // /// parquet files to load as layers
    // #[arg(last = true)]
    // layers: Vec<String>,
}

/// Query graph with cypher
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
enum Args {
    Query(CypherQuery),
    Load(LoadGraph),
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    match args {
        Args::Query(args) => {
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
        Args::Load(args) => {
            let layer_parquet_cols = vec![
                ParquetLayerCols {
                    parquet_dir: "/mnt/work/pometry/gov/case6/data/poststo.parquet",
                    layer: "POSTSTO",
                    src_col: "src_id",
                    dst_col: "dst_id",
                    time_col: "time",
                },
                ParquetLayerCols {
                    parquet_dir: "/mnt/work/pometry/gov/case6/data/livesin.parquet",
                    layer: "LIVESIN",
                    src_col: "src_id",
                    dst_col: "dst_id",
                    time_col: "time",
                },
                ParquetLayerCols {
                    parquet_dir: "/mnt/work/pometry/gov/case6/data/worksfor.parquet",
                    layer: "WORKSFOR",
                    src_col: "src_id",
                    dst_col: "dst_id",
                    time_col: "time",
                },
            ];
            let node_properties = "/mnt/work/pometry/gov/case6/data/sorted_nodes.parquet";
            ArrowGraph::load_from_parquets(
                args.graph_dir.as_str(),
                layer_parquet_cols,
                Some(node_properties),
                1_000_000,
                5_000_000,
                Some(1_000_000),
                Some(8),
                8,
            )
            .expect("Failed to load graph");
        }
    }
}
