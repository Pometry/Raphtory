//! `GraphLoader` trait and provides some default implementations for loading a graph.
//! This base class is used to load in-built graphs such as the LOTR, reddit and StackOverflow.
//! It also provides a method to download a CSV file.
//!
//! # Example
//!
//! ```rust
//! use docbrown_db::graph_loader::fetch_file;
//!
//! let path = fetch_file(
//!     "lotr.csv",
//!     "https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv",
//!     600
//! );
//!
//! // check if a file exists at the path
//! assert!(path.is_ok());
//! ```
//!

use crate::csv_loader::csv::CsvLoader;
use std::env;
use std::fs::File;
use std::io::{copy, Cursor};
use std::path::PathBuf;
use std::time::Duration;

pub mod lotr_graph;
pub mod reddit_hyperlinks;
pub mod sx_superuser_graph;

pub fn fetch_file(
    name: &str,
    url: &str,
    timeout: u64,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let tmp_dir = env::temp_dir();
    let filepath = tmp_dir.join(name);
    if !filepath.exists() {
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(timeout))
            .build()?;
        let response = client.get(url).send()?;
        let mut content = Cursor::new(response.bytes()?);
        if !filepath.exists() {
            let mut file = File::create(&filepath)?;
            copy(&mut content, &mut file)?;
        }
    }
    Ok(filepath)
}
