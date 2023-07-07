use chrono::NaiveDateTime;
use std::{env, time::Instant};
use std::collections::HashMap;
use raphtory::graph_loader::source::csv_loader::CsvLoader;
use raphtory::prelude::{AdditionOps, Graph, GraphViewOps, VertexViewOps};
use serde::Deserialize;
use raphtory::algorithms::pagerank::unweighted_page_rank;
use raphtory::algorithms::connected_components::weakly_connected_components;
use clap::{Arg};
use std::error::Error;
use std::fs::File;
use std::path::Path;
use csv::StringRecord;
use raphtory::core::utils::hashing::calculate_hash;

#[derive(Deserialize, std::fmt::Debug)]
pub struct GenericLoader {
    from: String,
    to: String,
}

fn main() {
    println!("Raphtory Quick Benchmark");

    // Set default values
    let mut header = false;
    let mut delimiter = "\t";
    let ts = 1;
    let mut data_dir = "../../python/data/simple-relationships.csv".to_string();

    println!("Running setup...");
    let mut now = Instant::now();
    // Iterate over the CSV records
    let g = {
        let g = Graph::new();
        CsvLoader::new(data_dir)
            .set_header(header)
            .set_delimiter(delimiter)
            .load_rec_into_graph(&g, |generic_loader: StringRecord, g: &Graph| {
                let src_id = generic_loader.get(0).map(|s| s.to_owned()).unwrap();
                let dst_id = generic_loader.get(1).map(|s| s.to_owned()).unwrap();

                g.add_edge(
                    NaiveDateTime::from_timestamp_opt(ts, 0).unwrap(),
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
