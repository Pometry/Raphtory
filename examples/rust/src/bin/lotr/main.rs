use raphtory::algorithms::triangle_count::{TriangleCountS1, TriangleCountS2, TriangleCountSlowS2};
use raphtory::core::Prop;
use raphtory::core::{state, utils};
use raphtory::db::graph::Graph;
use raphtory::db::program::{GlobalEvalState, Program};
use raphtory::db::view_api::*;
use raphtory::graph_loader::source::csv_loader::CsvLoader;
use serde::Deserialize;
use std::path::PathBuf;
use std::{env, path::Path, time::Instant};
use itertools::Itertools;
use raphtory::algorithms::generic_taint::generic_taint;
use raphtory::db::view_api::internal::GraphViewInternalOps;
use raphtory::graph_loader::example::lotr_graph::lotr_graph;

#[derive(Deserialize, std::fmt::Debug)]
pub struct Lotr {
    src_id: String,
    dst_id: String,
    time: i64,
}

fn main() {
    let args: Vec<String> = env::args().collect();

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
        let g = Graph::load_from_file(encoded_data_dir.as_path())
            .expect("Failed to load graph from encoded data files");

        println!(
            "Loaded graph from encoded data files {} with {} vertices, {} edges which took {} seconds",
            encoded_data_dir.to_str().unwrap(),
            g.num_vertices(),
            g.num_edges(),
            now.elapsed().as_secs()
        );

        g
    } else {
        let g = Graph::new(2);
        let now = Instant::now();

        CsvLoader::new(data_dir)
            .load_into_graph(&g, |lotr: Lotr, g: &Graph| {
                g.add_vertex(
                    lotr.time,
                    lotr.src_id.clone(),
                    &vec![
                        ("type".to_string(), Prop::Str("Character".to_string()))
                    ],
                ).expect("Failed to add vertex");

                g.add_vertex(
                    lotr.time,
                    lotr.dst_id.clone(),
                    &vec![
                        ("type".to_string(), Prop::Str("Character".to_string()))
                    ],
                ).expect("Failed to add vertex");

                g.add_edge(
                    lotr.time,
                    lotr.src_id.clone(),
                    lotr.dst_id.clone(),
                    &vec![(
                        "type".to_string(),
                        Prop::Str("Character Co-occurrence".to_string()),
                    )],
                    None,
                ).expect("Failed to add edge");
            })
            .expect("Failed to load graph from CSV data files");

        println!(
            "Loaded graph from CSV data files {} with {} vertices, {} edges which took {} seconds",
            encoded_data_dir.to_str().unwrap(),
            g.num_vertices(),
            g.num_edges(),
            now.elapsed().as_secs()
        );

        g.save_to_file(encoded_data_dir)
            .expect("Failed to save graph");

        g
    };

    assert_eq!(graph.num_vertices(), 139);
    assert_eq!(graph.num_edges(), 701);

    let gandalf = utils::calculate_hash(&"Gandalf");

    assert_eq!(gandalf, 2760374808085341115);
    assert!(graph.has_vertex(gandalf));
    assert_eq!(graph.vertex(gandalf).unwrap().name(), "Gandalf");

    let r = generic_taint(&graph, 20, 31930, vec!["Gandalf"], vec![]);
    assert_eq!(r.keys().sorted().collect_vec(), vec!["Gandalf", "Saruman", "Wormtongue"])
}
