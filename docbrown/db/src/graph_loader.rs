use crate::csv_loader::csv::CsvLoader;
use std::env;
use std::fs::File;
use std::io::{copy, Cursor};
use std::path::PathBuf;
use std::time::Duration;

pub mod lotr_graph;
pub mod reddit_hyperlinks;
pub mod sx_superuser_graph;
pub mod twitter_graph;

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
