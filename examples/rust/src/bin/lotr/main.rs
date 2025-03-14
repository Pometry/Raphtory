use raphtory::{
    algorithms::pathing::temporal_reachability::temporally_reachable_nodes,
    io::csv_loader::CsvLoader, logging::global_info_logger, prelude::*,
};
use serde::Deserialize;
use std::{
    env,
    path::{Path, PathBuf},
    time::Instant,
};
use tracing::info;

#[derive(Deserialize, std::fmt::Debug)]
pub struct Lotr {
    src_id: String,
    dst_id: String,
    time: i64,
}

fn main() {
    let args: Vec<String> = env::args().collect();
    global_info_logger();
    let default_data_dir: PathBuf = [env!("CARGO_MANIFEST_DIR"), "src/bin/lotr/data"]
        .iter()
        .collect();

    let data_dir = if args.len() < 2 {
        &default_data_dir
    } else {
        Path::new(args.get(1).unwrap())
    };

    if !data_dir.exists() {
        panic!("Missing data dir = {}", data_dir.to_str().unwrap())
    }

    let encoded_data_dir = data_dir.join("graphdb.bincode");

    let graph = if encoded_data_dir.exists() {
        let now = Instant::now();
        let g = Graph::decode(encoded_data_dir.as_path())
            .expect("Failed to load graph from encoded data files");

        info!(
            "Loaded graph from encoded data files {} with {} nodes, {} edges which took {} seconds",
            encoded_data_dir.to_str().unwrap(),
            g.count_nodes(),
            g.count_edges(),
            now.elapsed().as_secs()
        );

        g
    } else {
        let g = Graph::new();
        let now = Instant::now();

        CsvLoader::new(data_dir)
            .load_into_graph(&g, |lotr: Lotr, g: &Graph| {
                g.add_node(
                    lotr.time,
                    lotr.src_id.clone(),
                    [("type", Prop::str("Character"))],
                    None,
                )
                .expect("Failed to add node");

                g.add_node(
                    lotr.time,
                    lotr.dst_id.clone(),
                    [("type", Prop::str("Character"))],
                    None,
                )
                .expect("Failed to add node");

                g.add_edge(
                    lotr.time,
                    lotr.src_id.clone(),
                    lotr.dst_id.clone(),
                    [("type", Prop::str("Character Co-occurrence"))],
                    None,
                )
                .expect("Failed to add edge");
            })
            .expect("Failed to load graph from CSV data files");

        info!(
            "Loaded graph from CSV data files {} with {} nodes, {} edges which took {} seconds",
            encoded_data_dir.to_str().unwrap(),
            g.count_nodes(),
            g.count_edges(),
            now.elapsed().as_secs()
        );

        g.encode(encoded_data_dir).expect("Failed to save graph");

        g
    };

    assert_eq!(graph.count_nodes(), 139);
    assert_eq!(graph.count_edges(), 701);

    let gandalf = "Gandalf".id();

    assert_eq!(gandalf, 2760374808085341115);
    assert!(graph.has_node(gandalf));
    assert_eq!(graph.node(gandalf).unwrap().name(), "Gandalf");

    let r: Vec<String> = temporally_reachable_nodes(&graph, None, 20, 31930, vec!["Gandalf"], None)
        .into_iter_values()
        .flatten()
        .map(|(_, s)| s)
        .collect();

    assert_eq!(r, vec!["Gandalf", "Saruman", "Wormtongue"])
}
