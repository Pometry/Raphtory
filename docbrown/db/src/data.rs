use fetch_data::{fetch, FetchDataError};
use std::path::PathBuf;
use std::env;

// In order to add new files to this module, obtain the hash using bin/hash_for_url.rs

pub fn lotr() -> Result<PathBuf, FetchDataError> {
    fetch_file(
        "lotr.csv",
        "https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv",
        "c37083a5018827d06de3b884bea8275301d5ef6137dfae6256b793bffb05d033",
    )
}

pub fn twitter() -> Result<PathBuf, FetchDataError> {
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
