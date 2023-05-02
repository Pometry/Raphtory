use crate::core::utils;
use crate::db::graph::Graph;
use crate::db::view_api::internal::GraphViewInternalOps;
use crate::db::view_api::GraphViewOps;
use crate::graph_loader::source::csv_loader::CsvLoader;
use serde::Deserialize;
use std::path::PathBuf;
use std::{env, fs, io, path::Path, time::Instant};
use std::time::Duration;
use itertools::Itertools;
use std::io::{copy, Cursor};
use std::fs::File;
use tokio::time::timeout;

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

// fn fetch_file(file_path: PathBuf, timeout: u64,) -> Result<(), Box<dyn std::error::Error>> {
//     let client = reqwest::blocking::Client::builder()
//         .timeout(Duration::from_secs(timeout))
//         .build()?;
//     let response = client.get("https://github.com/Raphtory/Data/raw/main/apache-ivy-2.5.0-bin.zip").send()?;
//     let mut content = Cursor::new(response.bytes()?);
//     let mut file = File::create(&file_path)?;
//     copy(&mut content, &mut file)?;
//     let fname = Path::new(&file_path);
//     let file = File::open(fname).unwrap();
//     let mut archive = zip::ZipArchive::new(file).unwrap();
//
//     for i in 0..archive.len() {
//         let mut file = archive.by_index(i).unwrap();
//         let outpath = match file.enclosed_name() {
//             Some(path) => path.to_owned(),
//             None => continue,
//         };
//
//         {
//             let comment = file.comment();
//             if !comment.is_empty() {
//                 println!("File {i} comment: {comment}");
//             }
//         }
//
//         if (*file.name()).ends_with('/') {
//             println!("File {} extracted to \"{}\"", i, outpath.display());
//             fs::create_dir_all(&outpath).unwrap();
//         } else {
//             println!(
//                 "File {} extracted to \"{}\" ({} bytes)",
//                 i,
//                 outpath.display(),
//                 file.size()
//             );
//             if let Some(p) = outpath.parent() {
//                 if !p.exists() {
//                     fs::create_dir_all(p).unwrap();
//                 }
//             }
//             let mut outfile = fs::File::create(&outpath).unwrap();
//             io::copy(&mut file, &mut outfile).unwrap();
//         }
//
//         // Get and Set permissions
//         #[cfg(unix)]
//         {
//             use std::os::unix::fs::PermissionsExt;
//
//             if let Some(mode) = file.unix_mode() {
//                 fs::set_permissions(&outpath, fs::Permissions::from_mode(mode)).unwrap();
//             }
//         }
//     }
//
//     Ok(())
// }

pub fn stable_coin_graph(path: Option<String>, num_shards: usize, timeout: u64,) -> Graph {
    let default_data_dir: PathBuf = PathBuf::from("/tmp/stablecoin");

    let data_dir = match path {
        Some(path) => PathBuf::from(path),
        None => default_data_dir,
    };

    if !data_dir.exists() {
        panic!("Missing data dir = {}", data_dir.to_str().unwrap())
    }

    if !data_dir.join("token_transfers.csv").exists() {
        println!("Please download the stablecoin data from https://github.com/Raphtory/Data/raw/main/apache-ivy-2.5.0-bin.zip to a desired location and provide that location on next run!");
        // fetch_file(data_dir.join("ERC20-stablecoins.zip"), timeout).expect("Failed to fetch file from SNAP");
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
                    &vec![],
                    None,
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
