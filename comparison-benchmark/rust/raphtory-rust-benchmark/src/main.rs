use chrono::NaiveDateTime;
use std::{env, time::Instant};
use std::collections::HashMap;
use raphtory::graph_loader::source::csv_loader::CsvLoader;
use raphtory::prelude::{AdditionOps, Graph, GraphViewOps, VertexViewOps};
use serde::Deserialize;
use raphtory::algorithms::pagerank::unweighted_page_rank;
use raphtory::algorithms::connected_components::weakly_connected_components;
use clap::Parser;
use clap::ArgAction;
use std::error::Error;
use std::fs::File;
use std::path::Path;
use csv::StringRecord;
use raphtory::core::utils::hashing::calculate_hash;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Set if the file has a header, default is False
    #[arg(long, action=ArgAction::SetTrue)]
    header: bool,

    /// Delimiter of the csv file
    #[arg(long, default_value = "\t")]
    delimiter: String,

    /// Path to the csv file
    #[arg(long, default_value = "../../python/data/simple-relationships.csv")]
    file_path: String,

    /// Position of the from column in the csv
    #[arg(long, default_value = "0")]
    from_column: usize,

    /// Position of the to column in the csv
    #[arg(long, default_value = "1")]
    to_column: usize,

    /// Position of the time column in the csv, default will ignore time
    #[arg(long, default_value = "-1")]
    time_column: i32,
}

fn main() {
    println!("Raphtory Quick Benchmark");
    let args = Args::parse();
    // Set default values
    println!("Arguments: {:?}", args);
    let header = args.header;
    let delimiter = args.delimiter;
    let file_path = args.file_path;
    let from_column = args.from_column;
    let to_column = args.to_column;
    let time_column = args.time_column;


    println!("Running setup...");
    let mut now = Instant::now();
    // Iterate over the CSV records
    let g = {
        let g = Graph::new();
        CsvLoader::new(file_path)
            .set_header(header)
            .set_delimiter(&delimiter)
            .load_rec_into_graph(&g, |generic_loader: StringRecord, g: &Graph| {
                let src_id = generic_loader.get(from_column).map(|s| s.to_owned()).unwrap();
                let dst_id = generic_loader.get(to_column).map(|s| s.to_owned()).unwrap();
                let mut edge_time = NaiveDateTime::from_timestamp_opt(1, 0).unwrap();
                if time_column != -1 {
                    edge_time = NaiveDateTime::from_timestamp_millis(generic_loader.get(time_column as usize).unwrap().parse().unwrap()).unwrap();
                }
                g.add_edge(
                    edge_time,
                    src_id,
                    dst_id,
                    [],
                    None
                )
                    .expect("Failed to add edge");
            }).expect("Failed to load graph from CSV data files");
        g
    };
    println!(
        "Setup took {} seconds",
        now.elapsed().as_secs_f64()
    );

    println!("Graph has {} vertices and {} edges",
             g.num_vertices(),
             g.num_edges()
    );

    // Degree of all nodes
    now = Instant::now();
    let degree = g.vertices().iter().map(|v| v.degree()).collect::<Vec<_>>();
    println!(
        "Degree: {} seconds",
        now.elapsed().as_secs_f64()
    );

    // Out neighbours of all nodes with time
    now = Instant::now();
    let out_neighbours = g.vertices().iter().map(|v| v.out_neighbours()).collect::<Vec<_>>();
    println!(
        "Out neighbours: {} seconds",
        now.elapsed().as_secs_f64()
    );

    // page rank with time
    now = Instant::now();
    let page_rank: HashMap<String, f64>= unweighted_page_rank(&g, 1000, None, None, true)
        .into_iter()
        .collect();
    println!(
        "Page rank: {} seconds",
        now.elapsed().as_secs_f64()
    );


    // connected components with time
    now = Instant::now();
    let cc: HashMap<String, u64> = weakly_connected_components(&g, usize::MAX, None);
    println!(
        "Connected components: {} seconds",
        now.elapsed().as_secs_f64()
    );


}
