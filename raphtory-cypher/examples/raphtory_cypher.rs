use std::{error::Error, str::FromStr};

use arrow::util::pretty::print_batches;
use clap::Parser;
use futures::{stream, StreamExt};
use raphtory::arrow::graph_impl::{ArrowGraph, ParquetLayerCols};
use raphtory_cypher::{run_cypher, run_cypher_to_streams, run_sql};
use serde::{de::DeserializeOwned, Deserialize};

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

    /// use sql instead of cypher
    #[arg(short, long, default_value_t = false)]
    sql: bool,
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct LoadGraph {
    /// Graph path on disk
    #[arg(short, long)]
    graph_dir: String,
    // /// parquet files to load as layers
    #[arg(short='l', last = true, value_parser = parse_key_val::<String, ArgLayer>)]
    layers: Vec<(String, ArgLayer)>,
}

#[derive(thiserror::Error, Debug, PartialEq)]
enum Err {
    #[error("{0}")]
    Message(String),
}

fn parse_key_val<T, U: DeserializeOwned>(s: &str) -> Result<(T, U), Err>
where
    T: std::str::FromStr,
    U: std::str::FromStr,
    <T as FromStr>::Err: std::fmt::Display,
{
    let pos = s
        .find('=')
        .ok_or_else(|| Err::Message(format!("invalid KEY=value: no `=` found in `{s}`")))?;
    let json = &s[pos + 1..];
    let arg_layer: U = serde_json::from_str(json)
        .map_err(|e| Err::Message(format!("Failed to parse json: {}", e)))?;
    let layer = s[..pos]
        .parse()
        .map_err(|e| Err::Message(format!("Failed to parse key: {}", e)))?;
    Ok((layer, arg_layer))
}

#[derive(Clone, Debug, Deserialize)]
struct ArgLayer {
    path: String,
    src_col: String,
    dst_col: String,
    time_col: String,
}

impl std::str::FromStr for ArgLayer {
    type Err = Box<dyn Error + Send + Sync + 'static>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let layer: ArgLayer = serde_json::from_str(s)?;
        Ok(layer)
    }
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
                let df = if args.sql {
                    run_sql(&args.query, &graph).await.unwrap()
                } else {
                    run_cypher(&args.query, &graph).await.unwrap()
                };

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

            let layers = args.layers;
            let layer_parquet_cols = (0..layers.len())
                .map(|layer_id| {
                    let (
                        layer,
                        ArgLayer {
                            path,
                            src_col,
                            dst_col,
                            time_col,
                        },
                    ) = &layers[layer_id];
                    ParquetLayerCols {
                        parquet_dir: &path,
                        layer: &layer,
                        src_col: &src_col,
                        dst_col: &dst_col,
                        time_col: &time_col,
                    }
                })
                .collect();
            let node_properties = Some("/mnt/work/pometry/gov/case6/data/sorted_nodes.parquet");
            ArrowGraph::load_from_parquets(
                args.graph_dir.as_str(),
                layer_parquet_cols,
                None,
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
