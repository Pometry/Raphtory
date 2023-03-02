use crate::csv_loader::csv::CsvLoader;
use std::path::PathBuf;
use std::{env, iter};
use std::error::Error;
use std::fs::File;
use std::io::{copy, Cursor};

pub mod lotr_graph;
pub mod twitter_graph;

pub fn fetch_file(name: &str, url: &str) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let tmp_dir = env::temp_dir();
    let filepath = tmp_dir.join(name);
    if !filepath.exists() {
        let response = reqwest::blocking::get(url)?;
        let mut content = Cursor::new(response.bytes()?);
        if !filepath.exists() {
            let mut file = File::create(&filepath)?;
            copy(&mut content, &mut file)?;
        }
    }
    Ok(filepath)
}
