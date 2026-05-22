#![cfg(feature = "io")]
//! Regenerate the v4 disk-graph fixtures used by `validate_v4_disk_graphs` in
//! `tests/test_saved_graphs.rs`. Use the following make command from the pometry-storage root:
//!
//!     make regen-disk-graphs
//!

use raphtory::prelude::*;
use std::fs;
use storage::{persist::strategy::PersistenceStrategy, Extension};

#[path = "../tests/common/mod.rs"]
mod common;

use common::{event_graph_disk_storage_dir, persistent_graph_disk_storage_dir, populate_graph};

fn main() {
    if !Extension::disk_storage_enabled() {
        eprintln!(
            "disk storage backend not enabled - run from the pometry-storage workspace \
             (where `storage = db4-disk-storage`)"
        );
        std::process::exit(1);
    }

    // event graph on disk
    let event_graph_path = event_graph_disk_storage_dir();
    let _ = fs::remove_dir_all(&event_graph_path);
    let event_graph =
        Graph::new_at_path(&event_graph_path).expect("event graph couldn't be created");
    populate_graph(&event_graph);
    drop(event_graph);
    println!(
        "Wrote event graph fixture to {}",
        event_graph_path.display()
    );

    // persistent graph on disk
    let persistent_graph_path = persistent_graph_disk_storage_dir();
    let _ = fs::remove_dir_all(&persistent_graph_path);
    let persistent_graph = PersistentGraph::new_at_path(&persistent_graph_path)
        .expect("persistent graph couldn't be created");
    populate_graph(&persistent_graph);
    drop(persistent_graph);
    println!(
        "Wrote persistent graph fixture to {}",
        persistent_graph_path.display()
    );
}
