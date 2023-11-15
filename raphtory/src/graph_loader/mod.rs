//! Module for loading graphs into raphtory from various sources, like csv, neo4j, etc.
//!
//! Provides the `GraphLoader` trait and some default implementations for loading a graph.
//! This base class is used to load in-built graphs such as the LOTR, reddit and StackOverflow.
//! It also provides a method to download a CSV file.
//!
//! # Examples
//!
//! Load a pre-built graph
//! ```rust
//! use raphtory::algorithms::metrics::degree::average_degree;
//! use raphtory::prelude::*;
//! use raphtory::graph_loader::example::lotr_graph::lotr_graph;
//!
//! let graph = lotr_graph();
//!
//! // Get the in-degree, out-degree of Gandalf
//! // The graph.vertex option returns a result of an option,
//! // so we need to unwrap the result and the option or
//! // we can use this if let instead
//! if let Some(gandalf) = graph.vertex("Gandalf") {
//!    println!("Gandalf in degree: {:?}", gandalf.in_degree());
//!   println!("Gandalf out degree: {:?}", gandalf.out_degree());
//! }
//!
//! // Run an average degree algorithm on the graph
//! println!("Average degree: {:?}", average_degree(&graph));
//! ```
//!
//! Load a graph from csv
//!
//! ```no_run
//! use std::time::Instant;
//! use serde::Deserialize;
//! use raphtory::graph_loader::source::csv_loader::CsvLoader;
//! use raphtory::prelude::*;
//!
//! let data_dir = "/tmp/lotr.csv";
//!
//! #[derive(Deserialize, std::fmt::Debug)]
//! pub struct Lotr {
//!    src_id: String,
//!    dst_id: String,
//!    time: i64,
//! }
//!
//! let g = Graph::new();
//! let now = Instant::now();
//!
//! CsvLoader::new(data_dir)
//! .load_into_graph(&g, |lotr: Lotr, g: &Graph| {
//!     g.add_vertex(
//!         lotr.time,
//!         lotr.src_id.clone(),
//!         [("type", Prop::str("Character"))],
//!     )
//!     .expect("Failed to add vertex");
//!
//!     g.add_vertex(
//!         lotr.time,
//!         lotr.dst_id.clone(),
//!         [("type", Prop::str("Character"))],
//!     )
//!     .expect("Failed to add vertex");
//!
//!     g.add_edge(
//!         lotr.time,
//!         lotr.src_id.clone(),
//!         lotr.dst_id.clone(),
//!         [(
//!             "type",
//!             Prop::str("Character Co-occurrence"),
//!         )],
//!         None,
//!     )
//!     .expect("Failed to add edge");
//! })
//! .expect("Failed to load graph from CSV data files");
//! ```
//!
//! download a file without creating the graph
//!
//! ```rust
//! use raphtory::graph_loader::fetch_file;
//!
//! let path = fetch_file(
//!     "lotr.csv",
//!     true,
//!     "https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv",
//!     600
//! );
//!
//! // check if a file exists at the path
//! assert!(path.is_ok());
//! ```

use std::{
    env,
    fs::{File, *},
    io::{copy, Cursor},
    path::{Path, PathBuf},
    time::Duration,
};
use zip::read::ZipArchive;

pub mod example;
pub mod source;

pub fn fetch_file(
    name: &str,
    tmp_save: bool,
    url: &str,
    timeout: u64,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let filepath = if tmp_save {
        let tmp_dir = env::temp_dir();
        tmp_dir.join(name)
    } else {
        PathBuf::from(name)
    };
    if !filepath.exists() {
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(timeout))
            .build()?;
        let response = client.get(url).send()?.error_for_status()?;
        let mut content = Cursor::new(response.bytes()?);
        if !filepath.exists() {
            let mut file = File::create(&filepath)?;
            copy(&mut content, &mut file)?;
        }
    }
    Ok(filepath)
}

fn unzip_file(zip_file_path: &str, destination_path: &str) -> std::io::Result<()> {
    let file = File::open(zip_file_path)?;
    let mut archive = ZipArchive::new(file)?;

    for i in 0..archive.len() {
        let mut file = archive.by_index(i)?;
        let file_path = file.name();
        let dest_path = format!("{}/{}", destination_path, file_path);

        if file.is_dir() {
            create_dir_all(&dest_path)?;
        } else {
            if let Some(parent) = Path::new(&dest_path).parent() {
                if !parent.exists() {
                    create_dir_all(&parent)?;
                }
            }
            let mut output_file = File::create(&dest_path)?;
            std::io::copy(&mut file, &mut output_file)?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod graph_loader_test {
    use crate::{core::utils::hashing, graph_loader::fetch_file, prelude::*};
    use csv::StringRecord;

    #[test]
    fn test_fetch_file() {
        let path = fetch_file(
            "lotr2.csv",
            true,
            "https://raw.githubusercontent.com/Raphtory/Data/main/lotr_test.csv",
            600,
        );
        assert!(path.is_ok());
    }

    #[test]
    fn test_lotr_load_graph() {
        let g = crate::graph_loader::example::lotr_graph::lotr_graph();
        assert_eq!(g.count_edges(), 701);
    }

    #[test]
    fn test_graph_at() {
        let g = crate::graph_loader::example::lotr_graph::lotr_graph();

        let g_at_empty = g.at(1);
        let g_astart = g.at(7059);
        let g_at_another = g.at(28373);

        assert_eq!(g_at_empty.count_vertices(), 0);
        assert_eq!(g_astart.count_vertices(), 3);
        assert_eq!(g_at_another.count_vertices(), 4);
    }

    #[test]
    fn test_karate_graph() {
        let g = crate::graph_loader::example::karate_club::karate_club_graph();
        assert_eq!(g.count_vertices(), 34);
        assert_eq!(g.count_edges(), 155);
    }

    #[test]
    fn db_lotr() {
        let g = Graph::new();

        let data_dir = crate::graph_loader::example::lotr_graph::lotr_file()
            .expect("Failed to get lotr.csv file");

        fn parse_record(rec: &StringRecord) -> Option<(String, String, i64)> {
            let src = rec.get(0).and_then(|s| s.parse::<String>().ok())?;
            let dst = rec.get(1).and_then(|s| s.parse::<String>().ok())?;
            let t = rec.get(2).and_then(|s| s.parse::<i64>().ok())?;
            Some((src, dst, t))
        }

        if let Ok(mut reader) = csv::Reader::from_path(data_dir) {
            for rec in reader.records().flatten() {
                if let Some((src, dst, t)) = parse_record(&rec) {
                    let src_id = hashing::calculate_hash(&src);
                    let dst_id = hashing::calculate_hash(&dst);

                    g.add_vertex(t, src_id, [("name", Prop::str("Character"))])
                        .unwrap();
                    g.add_vertex(t, dst_id, [("name", Prop::str("Character"))])
                        .unwrap();
                    g.add_edge(
                        t,
                        src_id,
                        dst_id,
                        [("name", Prop::str("Character Co-occurrence"))],
                        None,
                    )
                    .unwrap();
                }
            }
        }

        let gandalf = hashing::calculate_hash(&"Gandalf");
        assert!(g.has_vertex(gandalf));
        assert!(g.has_vertex("Gandalf"))
    }

    #[test]
    fn test_all_degrees_window() {
        let g = crate::graph_loader::example::lotr_graph::lotr_graph();

        assert_eq!(g.count_edges(), 701);
        assert_eq!(g.vertex("Gandalf").unwrap().degree(), 49);
        assert_eq!(
            g.vertex("Gandalf").unwrap().window(1356, 24792).degree(),
            34
        );
        assert_eq!(g.vertex("Gandalf").unwrap().in_degree(), 24);
        assert_eq!(
            g.vertex("Gandalf").unwrap().window(1356, 24792).in_degree(),
            16
        );
        assert_eq!(g.vertex("Gandalf").unwrap().out_degree(), 35);
        assert_eq!(
            g.vertex("Gandalf")
                .unwrap()
                .window(1356, 24792)
                .out_degree(),
            20
        );
    }

    #[test]
    fn test_all_neighbours_window() {
        let g = crate::graph_loader::example::lotr_graph::lotr_graph();

        assert_eq!(g.count_edges(), 701);
        assert_eq!(g.vertex("Gandalf").unwrap().neighbours().iter().count(), 49);

        for v in g
            .vertex("Gandalf")
            .unwrap()
            .window(1356, 24792)
            .neighbours()
            .iter()
        {
            println!("{:?}", v.id())
        }
        assert_eq!(
            g.vertex("Gandalf")
                .unwrap()
                .window(1356, 24792)
                .neighbours()
                .iter()
                .count(),
            34
        );
        assert_eq!(
            g.vertex("Gandalf").unwrap().in_neighbours().iter().count(),
            24
        );
        assert_eq!(
            g.vertex("Gandalf")
                .unwrap()
                .window(1356, 24792)
                .in_neighbours()
                .iter()
                .count(),
            16
        );
        assert_eq!(
            g.vertex("Gandalf").unwrap().out_neighbours().iter().count(),
            35
        );
        assert_eq!(
            g.vertex("Gandalf")
                .unwrap()
                .window(1356, 24792)
                .out_neighbours()
                .iter()
                .count(),
            20
        );
    }

    #[test]
    fn test_all_edges_window() {
        let g = crate::graph_loader::example::lotr_graph::lotr_graph();

        assert_eq!(g.count_edges(), 701);
        assert_eq!(g.vertex("Gandalf").unwrap().edges().count(), 59);
        assert_eq!(
            g.vertex("Gandalf")
                .unwrap()
                .window(1356, 24792)
                .edges()
                .count(),
            36
        );
        assert_eq!(g.vertex("Gandalf").unwrap().in_edges().count(), 24);
        assert_eq!(
            g.vertex("Gandalf")
                .unwrap()
                .window(1356, 24792)
                .in_edges()
                .count(),
            16
        );
        assert_eq!(g.vertex("Gandalf").unwrap().out_edges().count(), 35);
        assert_eq!(
            g.vertex("Gandalf")
                .unwrap()
                .window(1356, 24792)
                .out_edges()
                .count(),
            20
        );
    }
}
