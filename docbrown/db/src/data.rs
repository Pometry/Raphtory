use crate::data;
use crate::{graphdb::GraphDB, loaders::csv::CsvLoader};
use csv::Reader;
use csv::{ReaderBuilder, StringRecord};
use docbrown_core::utils;
use docbrown_core::{Direction, Prop};
use fetch_data::{fetch, FetchDataError};
use regex::Regex;
use serde::Deserialize;
use std::fs::File;
use std::path::PathBuf;
use std::{env, path::Path, time::Instant};
// In order to add new files to this module, obtain the hash using bin/hash_for_url.rs

pub fn lotr_file() -> Result<PathBuf, FetchDataError> {
    fetch_file(
        "lotr.csv",
        "https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv",
        "c37083a5018827d06de3b884bea8275301d5ef6137dfae6256b793bffb05d033",
    )
}

pub fn twitter_file() -> Result<PathBuf, FetchDataError> {
    fetch_file(
        "twitter.csv",
        "https://raw.githubusercontent.com/Raphtory/Data/main/snap-twitter.csv",
        "b9cbdf68086c0c6b1501efa2e5ac6b1b0d9e069ad9215cebeba244e6e623c1bb",
    )
}

fn fetch_file(name: &str, url: &str, hash: &str) -> Result<PathBuf, FetchDataError> {
    let tmp_dir = env::temp_dir();
    let file = tmp_dir.join(name);
    fetch(url, hash, &file)?;
    Ok(file)
}

pub fn lotr_graph(shards: usize) -> GraphDB {
    let g = GraphDB::new(shards);

    let data_dir = lotr_file().expect("Failed to get lotr.csv file");

    fn parse_record(rec: &StringRecord) -> Option<(String, String, i64)> {
        let src = rec.get(0).and_then(|s| s.parse::<String>().ok())?;
        let dst = rec.get(1).and_then(|s| s.parse::<String>().ok())?;
        let t = rec.get(2).and_then(|s| s.parse::<i64>().ok())?;
        Some((src, dst, t))
    }

    if let Ok(mut reader) = csv::Reader::from_path(data_dir) {
        for rec_res in reader.records() {
            if let Ok(rec) = rec_res {
                if let Some((src, dst, t)) = parse_record(&rec) {
                    let src_id = utils::calculate_hash(&src);
                    let dst_id = utils::calculate_hash(&dst);

                    g.add_vertex(
                        src_id,
                        t,
                        &vec![("name".to_string(), Prop::Str("Character".to_string()))],
                    );
                    g.add_vertex(
                        dst_id,
                        t,
                        &vec![("name".to_string(), Prop::Str("Character".to_string()))],
                    );
                    g.add_edge(
                        src_id,
                        dst_id,
                        t,
                        &vec![(
                            "name".to_string(),
                            Prop::Str("Character Co-occurrence".to_string()),
                        )],
                    );
                }
            }
        }
    }
    g
}

fn build_static_graph_from_path<P: AsRef<Path>>(
    g: GraphDB,
    path: P,
    delimiter: u8,
    header: bool,
) -> GraphDB {
    fn parse_record(rec: &StringRecord) -> Option<(String, String, i64)> {
        let src = rec.get(0).and_then(|s| s.parse::<String>().ok())?;
        let dst = rec.get(1).and_then(|s| s.parse::<String>().ok())?;
        let t = 1;
        Some((src, dst, t))
    }

    if let Ok(mut reader) = ReaderBuilder::new()
        .delimiter(delimiter)
        .has_headers(header)
        .from_path(path)
    {
        for rec_res in reader.records() {
            if let Ok(rec) = rec_res {
                if let Some((src, dst, t)) = parse_record(&rec) {
                    let src_id = utils::calculate_hash(&src);
                    let dst_id = utils::calculate_hash(&dst);

                    g.add_vertex(
                        src_id,
                        t,
                        &vec![("name".to_string(), Prop::Str("User".to_string()))],
                    );
                    g.add_vertex(
                        dst_id,
                        t,
                        &vec![("name".to_string(), Prop::Str("User".to_string()))],
                    );
                    g.add_edge(
                        src_id,
                        dst_id,
                        t,
                        &vec![("name".to_string(), Prop::Str("Tweet".to_string()))],
                    );
                }
            }
        }
    }
    g
}

fn convert_to_bytes(delimiter: &str) -> u8 {
    delimiter.as_bytes()[0]
}

pub fn twitter_graph(shards: usize) -> GraphDB {
    let g = GraphDB::new(shards);
    let data_dir = twitter_file().expect("Failed to get twitter.csv file");
    let delimiter = convert_to_bytes(" ");
    let has_headers = false;
    build_static_graph_from_path(g, data_dir, delimiter, has_headers)
}
