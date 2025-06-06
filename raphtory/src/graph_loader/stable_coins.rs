use crate::{
    graph_loader::{fetch_file, unzip_file},
    io::csv_loader::CsvLoader,
    prelude::*,
};
use chrono::DateTime;
use regex::Regex;
use serde::Deserialize;
use std::{collections::HashMap, fs, path::PathBuf, time::Instant};
use tracing::{error, info};

#[allow(dead_code)]
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

pub fn stable_coin_graph(path: Option<String>, subset: bool) -> Graph {
    let data_dir = match path {
        Some(path) => PathBuf::from(path),
        None => PathBuf::from("/tmp/stablecoin"),
    };

    if !data_dir.join("token_transfers.csv").exists() {
        let dir_str = data_dir.to_str().unwrap();
        let zip_path = data_dir.join("ERC20-stablecoins.zip");
        let zip_str = zip_path.to_str().unwrap();
        fs::create_dir_all(dir_str)
            .unwrap_or_else(|_| panic!("Failed to create directory {}", dir_str));
        fetch_file(zip_str,false,"https://snap.stanford.edu/data/ERC20-stablecoins.zip",600000).expect("Failed to fetch stable coin data: https://snap.stanford.edu/data/ERC20-stablecoins.zip");
        unzip_file(zip_str,dir_str).expect("Failed to unzip stable coin data from https://snap.stanford.edu/data/ERC20-stablecoins.zip");
    }

    fn restore_from_file(encoded_data_file: &PathBuf) -> Option<Graph> {
        if encoded_data_file.exists() {
            let now = Instant::now();
            let g = Graph::decode(encoded_data_file.as_path())
                .map_err(|err| {
                    error!(
                        "Restoring from bincode failed with error: {}! Reloading file!",
                        err
                    )
                })
                .ok()?;

            info!(
                "Loaded graph from encoded data files {} with {} nodes, {} edges which took {} seconds",
                encoded_data_file.to_str().unwrap(),
                g.count_nodes(),
                g.count_edges(),
                now.elapsed().as_secs()
            );

            Some(g)
        } else {
            None
        }
    }

    let encoded_data_dir = data_dir.join("graphdb_bincode");
    let g = restore_from_file(&encoded_data_dir).unwrap_or_else(|| {
        let g = Graph::new();
        let now = Instant::now();

        let contract_addr_labels = HashMap::from([
            ("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", "USD"),
            ("0xd2877702675e6ceb975b4a1dff9fb7baf4c91ea9", "LUNC"),
            ("0xa47c8bf37f92abed4a126bda807a7b7498661acd", "USTC"),
            ("0x6b175474e89094c44da98b954eedeac495271d0f", "Dai"),
            ("0xdac17f958d2ee523a2206206994597c13d831ec7", "USDT"),
            ("0x8e870d67f660d95d5be530380d0ec0bd388289e1", "USDP"),
        ]);

        let re = if subset {
            Regex::new(r"token_transfers.csv").unwrap()
        } else {
            Regex::new(r"token_transfers(_V\d+\.\d+\.\d+)?\.csv").unwrap()
        };
        CsvLoader::new(data_dir)
            .with_filter(re)
            .set_header(true)
            .set_delimiter(",")
            .load_into_graph(&g, |stablecoin: StableCoin, g: &Graph| {
                let s: &str = &stablecoin.contract_address;
                let label = *contract_addr_labels.get(s).unwrap();
                g.add_edge(
                    DateTime::from_timestamp(stablecoin.time_stamp, 0).unwrap(),
                    stablecoin.from_address,
                    stablecoin.to_address,
                    [("value", stablecoin.value)],
                    Some(label),
                )
                .expect("Failed to add edge");
            })
            .expect("Failed to load graph from CSV data files");

        info!(
            "Loaded graph from CSV data files {} with {} nodes, {} edges which took {} seconds",
            encoded_data_dir.to_str().unwrap(),
            g.count_nodes(),
            g.count_edges(),
            now.elapsed().as_secs()
        );

        g.encode(encoded_data_dir).expect("Failed to save graph");

        g
    });

    g
}
