//! Loads the Stackoverflow super user dataset into a graph.
//! Source: https://snap.stanford.edu/data/sx-superuser.html
//!
//! This is a temporal network of interactions on the stack exchange web site Super User.
//! There are three different types of interactions represented by a directed edge (u, v, t):
//!
//! * user u answered user v's question at time t (in the graph sx-superuser-a2q)
//! * user u commented on user v's question at time t (in the graph sx-superuser-c2q)
//! * user u commented on user v's answer at time t (in the graph sx-superuser-c2a)
//!
//! The graph sx-superuser contains the union of these graphs. These graphs were constructed
//! from the Stack Exchange Data Dump. Node ID numbers correspond to the 'OwnerUserId' tag
//! in that data dump.
//! *NOTE: It may take a while to download the dataset
//!
//! ## Dataset statistics
//! * Dataset statistics (sx-superuser)
//! * Nodes 	194085
//! * Temporal Edges 	1443339
//! * Edges in static graph 	924886
//! * Time span 	2773 days
//!
//! ## Source
//! Ashwin Paranjape, Austin R. Benson, and Jure Leskovec. "Motifs in Temporal Networks."
//! In Proceedings of the Tenth ACM International Conference on Web Search and Data Mining, 2017.
//!
//! ## Properties
//!
//! Header:  SRC DST UNIXTS
//!
//! where edges are separated by a new line and
//!
//! * SRC: id of the source node (a user)
//! * TGT: id of the target node (a user)
//! * UNIXTS: Unix timestamp (seconds since the epoch)
//!
//! Example:
//! ```no_run
//! use raphtory_io::graph_loader::example::sx_superuser_graph::sx_superuser_graph;
//! use raphtory::db::graph::Graph;
//! use raphtory::db::view_api::*;
//!
//! let graph = sx_superuser_graph(1).unwrap();
//!
//! println!("The graph has {:?} vertices", graph.num_vertices());
//! println!("The graph has {:?} edges", graph.num_edges());
//! ```

use raphtory::db::graph::Graph;

use crate::graph_loader::{fetch_file, source::csv_loader::CsvLoader};
use serde::Deserialize;
use std::path::PathBuf;

#[derive(Deserialize, std::fmt::Debug)]
pub struct TEdge {
    src_id: u64,
    dst_id: u64,
    time: i64,
}

/// Download the SX SuperUser dataset
/// and return the path to the file
///
/// # Returns
/// - A PathBuf to the SX SuperUser dataset
pub fn sx_superuser_file() -> Result<PathBuf, Box<dyn std::error::Error>> {
    fetch_file(
        "sx-superuser.txt.gz",
        "https://snap.stanford.edu/data/sx-superuser.txt.gz",
        600,
    )
}

/// Load the SX SuperUser dataset into a graph and return it
///
/// # Arguments
///
/// * `shards` - The number of shards to use for the graph
///
/// # Returns
///
/// - A Result containing the graph or an error
pub fn sx_superuser_graph(shards: usize) -> Result<Graph, Box<dyn std::error::Error>> {
    let graph = Graph::new(shards);
    CsvLoader::new(sx_superuser_file()?)
        .set_delimiter(" ")
        .load_into_graph(&graph, |edge: TEdge, g: &Graph| {
            g.add_edge(edge.time, edge.src_id, edge.dst_id, &vec![], None)
                .expect("Error: Unable to add edge");
        })?;

    Ok(graph)
}

#[cfg(test)]
mod sx_superuser_test {
    use crate::graph_loader::example::sx_superuser_graph::{sx_superuser_file, sx_superuser_graph};

    #[test]
    fn test_download_works() {
        let file = sx_superuser_file().unwrap();
        assert!(file.is_file())
    }

    #[test]
    fn test_graph_loading_works() {
        sx_superuser_graph(2).unwrap();
    }
}
