//! `GraphLoader` trait and provides some default implementations for loading a graph.
//! This base class is used to load in-built graphs such as the LOTR, reddit and StackOverflow.
//! It also provides a method to download a CSV file.
//!
//! # Example
//!
//! ```rust
//! use raphtory_io::graph_loader::fetch_file;
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
//!

use std::env;
use std::fs::File;
use std::io::{copy, Cursor};
use std::path::{Path, PathBuf};
use std::time::Duration;
use std::io::prelude::*;
use zip::read::{ZipArchive, ZipFile};
use std::fs::*;

pub mod example;
pub mod source;

pub fn fetch_file(
    name: &str,
    tmp_save:bool,
    url: &str,
    timeout: u64,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let filepath = if tmp_save {
        let tmp_dir = env::temp_dir();
        tmp_dir.join(name)
    }
    else {
        PathBuf::from(name)
    };
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
    use csv::StringRecord;
    use raphtory::{
        core::{utils, Prop},
        db::{
            graph::Graph,
            view_api::{GraphViewOps, TimeOps, VertexViewOps},
        },
    };

    use crate::graph_loader::{fetch_file, unzip_file};
    use crate::graph_loader::example::stable_coins::stable_coin_graph;


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
        let g = crate::graph_loader::example::lotr_graph::lotr_graph(4);
        assert_eq!(g.num_edges(), 701);
    }

    #[test]
    fn test_graph_at() {
        let g = crate::graph_loader::example::lotr_graph::lotr_graph(1);

        let g_at_empty = g.at(1);
        let g_at_start = g.at(7059);
        let g_at_another = g.at(28373);
        let g_at_max = g.at(i64::MAX);
        let g_at_min = g.at(i64::MIN);

        assert_eq!(g_at_empty.num_vertices(), 0);
        assert_eq!(g_at_start.num_vertices(), 70);
        assert_eq!(g_at_another.num_vertices(), 123);
        assert_eq!(g_at_max.num_vertices(), 139);
        assert_eq!(g_at_min.num_vertices(), 0);
    }

    #[test]
    fn db_lotr() {
        let g = Graph::new(4);

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
                    let src_id = utils::calculate_hash(&src);
                    let dst_id = utils::calculate_hash(&dst);

                    g.add_vertex(
                        t,
                        src_id,
                        &vec![("name".to_string(), Prop::Str("Character".to_string()))],
                    )
                    .unwrap();
                    g.add_vertex(
                        t,
                        dst_id,
                        &vec![("name".to_string(), Prop::Str("Character".to_string()))],
                    )
                    .unwrap();
                    g.add_edge(
                        t,
                        src_id,
                        dst_id,
                        &vec![(
                            "name".to_string(),
                            Prop::Str("Character Co-occurrence".to_string()),
                        )],
                        None,
                    )
                    .unwrap();
                }
            }
        }

        let gandalf = utils::calculate_hash(&"Gandalf");
        assert!(g.has_vertex(gandalf));
        assert!(g.has_vertex("Gandalf"))
    }

    #[test]
    fn test_all_degrees_window() {
        let g = crate::graph_loader::example::lotr_graph::lotr_graph(4);

        assert_eq!(g.num_edges(), 701);
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
        let g = crate::graph_loader::example::lotr_graph::lotr_graph(4);

        assert_eq!(g.num_edges(), 701);
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
        let g = crate::graph_loader::example::lotr_graph::lotr_graph(4);

        assert_eq!(g.num_edges(), 701);
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
