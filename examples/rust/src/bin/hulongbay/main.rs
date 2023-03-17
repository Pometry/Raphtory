#![allow(unused_imports)]
use std::collections::HashMap;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::thread::JoinHandle;
use std::{env, thread};

use chrono::{DateTime, Utc};
use docbrown_core::tgraph::TemporalGraph;
use docbrown_core::utils;
use docbrown_core::{Direction, Prop};
use docbrown_db::csv_loader::csv::CsvLoader;
use regex::Regex;
use serde::Deserialize;
use std::fs::File;
use std::io::{prelude::*, BufReader, LineWriter};
use std::time::Instant;

use docbrown_db::graph::Graph;
use docbrown_db::view_api::*;

#[derive(Deserialize, std::fmt::Debug)]
pub struct Edge {
    _unknown0: i64,
    _unknown1: i64,
    _unknown2: i64,
    src: u64,
    dst: u64,
    time: i64,
    _unknown3: u64,
    amount_usd: u64,
}

fn main() {
    let args: Vec<String> = env::args().collect();

    let data_dir = if args.len() < 2 {
        panic!("Failed to provide the path to the hulongbay data directory")
    } else {
        Path::new(args.get(1).unwrap())
    };

    if !data_dir.exists() {
        panic!("Missing data dir = {}", data_dir.to_str().unwrap())
    }

    // If data_dir/graphdb.bincode exists, use bincode to load the graph from binary encoded data files
    // otherwise load the graph from csv data files
    let encoded_data_dir = data_dir.join("graphdb.bincode");

    let graph = if encoded_data_dir.exists() {
        let now = Instant::now();
        let g = Graph::load_from_file(encoded_data_dir.as_path())
            .expect("Failed to load graph from encoded data files");

        println!(
            "Loaded graph from path {} with {} vertices, {} edges, took {} seconds",
            encoded_data_dir.to_str().unwrap(),
            g.num_vertices(),
            g.num_edges(),
            now.elapsed().as_secs()
        );

        g
    } else {
        let g = Graph::new(16);

        let now = Instant::now();

        let _ = CsvLoader::new(data_dir)
            .load_into_graph(&g, |sent: Edge, g: &Graph| {
                let src = sent.src;
                let dst = sent.dst;
                let time = sent.time;

                g.add_edge(
                    time.try_into().unwrap(),
                    src,
                    dst,
                    &vec![("amount".to_string(), Prop::U64(sent.amount_usd))],
                )
            })
            .expect("Failed to load graph from CSV data files");

        println!(
            "Loaded graph from CSV data files {} with {} vertices, {} edges which took {} seconds",
            encoded_data_dir.to_str().unwrap(),
            g.num_vertices(),
            g.num_edges(),
            now.elapsed().as_secs()
        );

        g.save_to_file(encoded_data_dir)
            .expect("Failed to save graph");

        g
    };
}
