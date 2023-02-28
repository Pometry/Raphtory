use crate::csv_loader::csv::CsvLoader;
use fetch_data::{fetch, FetchDataError};
use std::env;
use std::path::PathBuf;

pub mod lotr_graph;
pub mod twitter_graph;

pub fn fetch_file(name: &str, url: &str, hash: &str) -> Result<PathBuf, FetchDataError> {
    let tmp_dir = env::temp_dir();
    let file = tmp_dir.join(name);
    fetch(url, hash, &file)?;
    Ok(file)
}
