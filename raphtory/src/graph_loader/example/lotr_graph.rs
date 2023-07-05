//! Load the Lord of the Rings dataset into a graph.
//! The dataset is available at https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv
//! and is a list of interactions between characters in the Lord of the Rings books
//! and movies. The dataset is a CSV file with the following columns:
//!
//! - src_id: The ID of the source character
//! - dst_id: The ID of the destination character
//! - time: The time of the interaction (in page)
//!
//! ## Dataset statistics
//! * Number of nodes (subreddits) 139
//! * Number of edges (hyperlink between subreddits) 701
//!
//! Example:
//! ```rust
//! use raphtory::graph_loader::example::lotr_graph::lotr_graph;
//! use raphtory::prelude::*;
//!
//! let graph = lotr_graph();
//!
//! println!("The graph has {:?} vertices", graph.num_vertices());
//! println!("The graph has {:?} edges", graph.num_edges());
//! ```
use crate::{
    graph_loader::{fetch_file, source::csv_loader::CsvLoader},
    prelude::*,
};
use serde::Deserialize;
use std::path::PathBuf;

#[derive(Deserialize, std::fmt::Debug)]
pub struct Lotr {
    pub src_id: String,
    pub dst_id: String,
    pub time: i64,
}

/// Downloads the LOTR.csv file from Github
/// and returns the path to the file
///
/// # Returns
/// - A PathBuf to the LOTR.csv file
pub fn lotr_file() -> Result<PathBuf, Box<dyn std::error::Error>> {
    fetch_file(
        "lotr.csv",
        true,
        "https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv",
        600,
    )
}

/// Constructs a graph from the LOTR dataset
/// Including all edges, nodes and timestamps
///
/// # Arguments
///
/// - shards: The number of shards to use for the graph
///
/// # Returns
/// - A Graph containing the LOTR dataset
pub fn lotr_graph() -> Graph {
    let graph = {
        let g = Graph::new();

        CsvLoader::new(lotr_file().unwrap())
            .load_into_graph(&g, |lotr: Lotr, g: &Graph| {
                let src_id = lotr.src_id;
                let dst_id = lotr.dst_id;
                let time = lotr.time;

                g.add_vertex(time, src_id.clone(), [])
                    .map_err(|err| println!("{:?}", err))
                    .ok();
                g.add_vertex(time, dst_id.clone(), [])
                    .map_err(|err| println!("{:?}", err))
                    .ok();
                g.add_edge(time, src_id.clone(), dst_id.clone(), [], None)
                    .expect("Error: Unable to add edge");
            })
            .expect("Failed to load graph from CSV data files");
        g
    };
    graph
}
