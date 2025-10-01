use std::path::PathBuf;

use chrono::DateTime;
use raphtory::{io::parquet_loaders::load_node_props_from_parquet, prelude::*};
use raphtory_storage::core_ops::CoreGraphOps;

fn main() {
    // let graph_dir = std::env::args()
    //     .nth(1)
    //     .expect("Please provide the graph directory as the first argument");
    let graph_dir = "/Volumes/Work/graphs/kaia_21";

    let g = Graph::load_from_path(&graph_dir);

    // dbg!(g.unfiltered_num_nodes());
    // dbg!(g.node_meta().temporal_prop_mapper());
    // dbg!(g.node_meta().metadata_mapper());

    // DAY BOUNDED GRAPH from 2022-05-13 00:00:00 UTC to 2025-09-18 00:00:00 UTC
    let start = DateTime::parse_from_rfc3339("2022-05-13T00:00:00Z").unwrap();
    let end = DateTime::parse_from_rfc3339("2025-09-18T00:00:00Z").unwrap();
    println!("{start:?} - {end:?}");
    let wg = g.window(start, end);

    let how_many = wg.nodes().iter().count();
    println!("{how_many}");

    let how_many = wg.edges().iter().count();
    println!("{how_many}");
    // dbg!(g.node_meta().metadata_mapper());
    // let path = PathBuf::from("/Volumes/Work/assets/NODES/19/kaia/NODE_CONST_PROPS.parquet");

    // load_node_props_from_parquet(
    //     &g,
    //     &path,
    //     "cluster_id",
    //     None,
    //     None,
    //     &["name", "category"],
    //     None,
    //     Some(1_000_000),
    // )
    // .unwrap();

    // dbg!(g.node_meta());
    // dbg!(g.node_meta().metadata_mapper());
}
