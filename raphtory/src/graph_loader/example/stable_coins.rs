use crate::core::{utils, Prop};
use crate::db::graph::Graph;
use crate::db::view_api::internal::GraphViewInternalOps;
use crate::db::view_api::GraphViewOps;
use crate::graph_loader::source::csv_loader::CsvLoader;
use serde::Deserialize;
use std::fs::File;
use std::io::{copy, Cursor};
use std::path::PathBuf;
use std::time::Duration;
use std::{env, fs, io, path::Path, time::Instant};

#[derive(Deserialize, std::fmt::Debug)]
pub struct StableCoin {
    block_number: String,
    transaction_index: u32,
    from_address: String,
    to_address: String,
    time_stamp: i64,
    contract_address: String,
    value: f64,
}

fn fetch_file(file_path: PathBuf, timeout: u64) -> Result<(), Box<dyn std::error::Error>> {
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(timeout))
        .build()?;
    let response = client
        .get("https://raw.githubusercontent.com/Raphtory/Data/main/token_transfers.csv")
        .send()?;
    let mut content = Cursor::new(response.bytes()?);
    let mut file = File::create(&file_path)?;
    copy(&mut content, &mut file)?;

    Ok(())
}

pub fn stable_coin_graph(path: Option<String>, num_shards: usize) -> Graph {
    let default_data_dir: PathBuf = PathBuf::from("/tmp/stablecoin");

    let data_dir = match path {
        Some(path) => PathBuf::from(path),
        None => default_data_dir,
    };

    let dir_str = data_dir.to_str().unwrap();
    fs::create_dir_all(dir_str).expect(&format!("Failed to create directory {}", dir_str));

    if !data_dir.join("token_transfers.csv").exists() {
        fetch_file(data_dir.join("token_transfers.csv"), 10).expect("Failed to fetch stable coin data: https://raw.githubusercontent.com/Raphtory/Data/main/token_transfers.csv");
    }

    let encoded_data_dir = data_dir.join("graphdb.bincode");

    let g = if encoded_data_dir.exists() {
        let now = Instant::now();
        let g = Graph::load_from_file(encoded_data_dir.as_path())
            .expect("Failed to load graph from encoded data files");

        println!(
            "Loaded graph with {} shards from encoded data files {} with {} vertices, {} edges which took {} seconds",
            g.num_shards(),
            encoded_data_dir.to_str().unwrap(),
            g.num_vertices(),
            g.num_edges(),
            now.elapsed().as_secs()
        );

        g
    } else {
        let g = Graph::new(num_shards);
        let now = Instant::now();

        CsvLoader::new(data_dir)
            .set_header(true)
            .set_delimiter(",")
            .load_into_graph(&g, |stablecoin: StableCoin, g: &Graph| {
                g.add_edge(
                    stablecoin.time_stamp,
                    stablecoin.from_address,
                    stablecoin.to_address,
                    &vec![("value".into(), Prop::F64(stablecoin.value.into()))],
                    Some(&stablecoin.contract_address),
                )
                .expect("Failed to add edge");
            })
            .expect("Failed to load graph from CSV data files");

        println!(
            "Loaded graph with {} shards from CSV data files {} with {} vertices, {} edges which took {} seconds",
            g.num_shards(),
            encoded_data_dir.to_str().unwrap(),
            g.num_vertices(),
            g.num_edges(),
            now.elapsed().as_secs()
        );

        g.save_to_file(encoded_data_dir)
            .expect("Failed to save graph");

        g
    };

    g
}
