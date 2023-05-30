use crate::graph_loader::source::csv_loader::CsvLoader;
use chrono::NaiveDateTime;
use raphtory::core::Prop;
use raphtory::db::graph::Graph;
use raphtory::db::view_api::internal::GraphViewInternalOps;
use raphtory::db::view_api::GraphViewOps;
use serde::Deserialize;
use std::collections::HashMap;
use std::fs::File;
use std::io::{copy, Cursor};
use std::path::PathBuf;
use std::time::Duration;
use std::{fs, time::Instant};

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

    fn restore_from_bincode(encoded_data_dir: &PathBuf) -> Option<Graph> {
        if encoded_data_dir.exists() {
            let now = Instant::now();
            let g = Graph::load_from_file(encoded_data_dir.as_path())
                .map_err(|err| {
                    println!(
                        "Restoring from bincode failed with error: {}! Reloading file!",
                        err
                    )
                })
                .ok()?;

            println!(
                "Loaded graph with {} shards from encoded data files {} with {} vertices, {} edges which took {} seconds",
                g.num_shards(),
                encoded_data_dir.to_str().unwrap(),
                g.num_vertices(),
                g.num_edges(),
                now.elapsed().as_secs()
            );

            Some(g)
        } else {
            None
        }
    }

    let g = restore_from_bincode(&encoded_data_dir).unwrap_or_else(|| {
        let g = Graph::new(num_shards);
        let now = Instant::now();

        let contract_addr_labels = HashMap::from([
            ("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", "USD"),
            ("0xd2877702675e6ceb975b4a1dff9fb7baf4c91ea9", "LUNC"),
            ("0xa47c8bf37f92abed4a126bda807a7b7498661acd", "USTC"),
            ("0x6b175474e89094c44da98b954eedeac495271d0f", "Dai"),
            ("0xdac17f958d2ee523a2206206994597c13d831ec7", "USDT"),
            ("0x8e870d67f660d95d5be530380d0ec0bd388289e1", "USDP"),
        ]);

        CsvLoader::new(data_dir)
            .set_header(true)
            .set_delimiter(",")
            .load_into_graph(&g, |stablecoin: StableCoin, g: &Graph| {
                let s: &str = &stablecoin.contract_address;
                let label = *contract_addr_labels.get(s).unwrap();
                g.add_edge(
                    NaiveDateTime::from_timestamp_opt(stablecoin.time_stamp, 0).unwrap(),
                    stablecoin.from_address,
                    stablecoin.to_address,
                    &vec![("value".into(), Prop::F64(stablecoin.value.into()))],
                    Some(label),
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
    });

    g
}
