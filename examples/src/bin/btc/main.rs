#![allow(unused_imports)]
use std::collections::HashMap;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::thread::JoinHandle;
use std::{env, thread};

use chrono::{DateTime, Utc};
use docbrown_core::graph::TemporalGraph;
use docbrown_core::{Direction, Prop};
use docbrown_db::loaders::csv::CsvLoader;
use serde::Deserialize;
use std::time::Instant;
use regex::Regex;
use std::fs::File;
use std::io::{prelude::*, BufReader, LineWriter};

use docbrown_db::graphdb::GraphDB;
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

#[derive(Deserialize, std::fmt::Debug)]
pub struct Sent {
    addr: String,
    txn: String,
    amount_btc: u64,
    amount_usd: f64,
    #[serde(with = "custom_date_format")]
    time: DateTime<Utc>,
}

#[derive(Deserialize, std::fmt::Debug)]
pub struct Received {
    txn: String,
    addr: String,
    amount_btc: u64,
    amount_usd: f64,
    #[serde(with = "custom_date_format")]
    time: DateTime<Utc>,
}

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

fn main() {
    let args: Vec<String> = env::args().collect();

    if let Some(input_folder) = args.get(1) {
        // if input_folder/graphdb.bincode exists, use bincode to load the graph
        // otherwise, load the graph from the csv files

        let test_v = calculate_hash(&"139eeGkMGR6F9EuJQ3qYoXebfkBbNAsLtV:btc");

        let path: PathBuf = [input_folder, "graphdb.bincode"].iter().collect();
        let graph = if path.exists() {
            let now = Instant::now();
            let g = GraphDB::load_from_file(path.as_path()).expect("Failed to load graph");

            println!(
                "Loaded graph from path {} with {} vertices, {} edges, took {} seconds",
                path.to_str().unwrap(),
                g.len(),
                g.edges_len(),
                now.elapsed().as_secs()
            );
            g
        } else {
            let g = GraphDB::new(16);

            let now = Instant::now();

            let _ = CsvLoader::new(input_folder)
                .with_filter(Regex::new(r".+(sent|received)").unwrap())
                .load_into_graph(&g, |sent: Sent, g: &GraphDB| {
                    let src = calculate_hash(&sent.addr);
                    let dst = calculate_hash(&sent.txn);
                    let t = sent.time.timestamp();

                    if src == test_v || dst == test_v {
                        println!("{} sent {} to {}", sent.addr, sent.amount_btc, sent.txn);
                    }

                    g.add_edge(
                        src,
                        dst,
                        t.try_into().unwrap(),
                        &vec![("amount".to_string(), Prop::U64(sent.amount_btc))],
                    )
                })
                .expect("Failed to load graph");

            println!(
                "Loaded {} vertices, {} edges, took {} seconds",
                g.len(),
                g.edges_len(),
                now.elapsed().as_secs()
            );

            g.save_to_file(path).expect("Failed to save graph");

            g
        };

        assert!(graph.contains(test_v));
        let deg_out = graph
            .neighbours_window(0, i64::MAX, test_v, Direction::OUT)
            .count();
        let deg_in = graph
            .neighbours_window(0, i64::MAX, test_v, Direction::IN)
            .count();

        println!(
            "{} has {} out degree and {} in degree",
            test_v, deg_out, deg_in
        );
    }
}

mod custom_date_format {
    use chrono::{DateTime, TimeZone, Utc};
    use serde::{self, Deserialize, Deserializer, Serializer};

    const FORMAT: &'static str = "%Y-%m-%d %H:%M:%S";

    // The signature of a serialize_with function must follow the pattern:
    //
    //    fn serialize<S>(&T, S) -> Result<S::Ok, S::Error>
    //    where
    //        S: Serializer
    //
    // although it may also be generic over the input types T.
    pub fn serialize<S>(date: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
    {
        let s = format!("{}", date.format(FORMAT));
        serializer.serialize_str(&s)
    }

    // The signature of a deserialize_with function must follow the pattern:
    //
    //    fn deserialize<'de, D>(D) -> Result<T, D::Error>
    //    where
    //        D: Deserializer<'de>
    //
    // although it may also be generic over the output types T.
    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
        where
            D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Utc.datetime_from_str(&s, FORMAT)
            .map_err(serde::de::Error::custom)
    }
}
