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
//! * Number of nodes (characters) 139
//! * Number of edges (interactions) 701
//!
//! Example:
//! ```rust,no_run
//! use raphtory::graph_loader::lotr_graph::lotr_graph;
//! use raphtory::prelude::*;
//!
//! let graph = lotr_graph();
//!
//! println!("The graph has {:?} nodes", graph.count_nodes());
//! println!("The graph has {:?} edges", graph.count_edges());
//! ```
use crate::{graph_loader::fetch_file, io::csv_loader::CsvLoader, prelude::*};
use serde::Deserialize;
use std::path::PathBuf;
use tracing::error;

#[derive(Deserialize, std::fmt::Debug)]
pub struct Lotr {
    pub src_id: String,
    pub dst_id: String,
    pub time: i64,
}

/// Downloads the LOTR.csv file from Github
/// and returns the path to the file
///
/// Returns:
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
/// Returns:
/// - A Graph containing the LOTR dataset
pub fn lotr_graph() -> Graph {
    let graph = {
        let g = Graph::new();

        CsvLoader::new(lotr_file().unwrap())
            .load_into_graph(&g, |lotr: Lotr, g: &Graph| {
                let src_id = lotr.src_id;
                let dst_id = lotr.dst_id;
                let time = lotr.time;

                g.add_node(time, src_id.clone(), NO_PROPS, None)
                    .map_err(|err| error!("{:?}", err))
                    .ok();
                g.add_node(time, dst_id.clone(), NO_PROPS, None)
                    .map_err(|err| error!("{:?}", err))
                    .ok();
                g.add_edge(time, src_id.clone(), dst_id.clone(), NO_PROPS, None)
                    .expect("Error: Unable to add edge");
            })
            .expect("Failed to load graph from CSV data files");
        g
    };
    graph
}
