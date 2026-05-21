//! Shared fixtures for `test_saved_graphs.rs` and the `regenerate_disk_graph_fixtures` example
//! used to regenerate disk-backed graphs when the format changes.

use raphtory::prelude::*;
use std::path::PathBuf;

pub fn event_graph_disk_storage_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("resources/test/disk_graphs/event_graph")
}

pub fn persistent_graph_disk_storage_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("resources/test/disk_graphs/persistent_graph")
}

/// Generates a small graph with a variety of writes (c_props, t_props, layers, node types, prop types, etc...)
/// We try to include as many things as possible to detect if their disk format changes
pub fn populate_graph<G: AdditionOps + PropertyAdditionOps + DeletionOps>(graph: &G) {
    // graph_c
    graph
        .add_metadata([
            ("dataset", Prop::str("v4_test")),
            ("schema_version", Prop::U64(1)),
            ("public", Prop::Bool(true)),
        ])
        .unwrap();

    // graph_t
    graph
        .add_properties(1, [("status", Prop::str("init")), ("count", Prop::I64(0))])
        .unwrap();
    graph
        .add_properties(
            5,
            [("status", Prop::str("active")), ("count", Prop::I64(3))],
        )
        .unwrap();
    graph
        .add_properties(10, [("status", Prop::str("done")), ("count", Prop::I64(5))])
        .unwrap();

    // nodes_c + nodes_t + node types
    // Two node types so we test the type column
    let alice = graph
        .add_node(
            1,
            "alice",
            [("score", Prop::I64(10)), ("active", Prop::Bool(true))],
            Some("Person"),
            None,
        )
        .unwrap();
    alice
        .add_metadata([("dept", Prop::str("eng")), ("hired", Prop::I64(2020))])
        .unwrap();

    let bob = graph
        .add_node(1, "bob", [("score", Prop::I64(7))], Some("Person"), None)
        .unwrap();
    bob.add_metadata([("dept", Prop::str("sales"))]).unwrap();

    let server = graph
        .add_node(
            1,
            "server-1",
            [("cpu", Prop::F64(0.1))],
            Some("Server"),
            None,
        )
        .unwrap();
    server
        .add_metadata([("region", Prop::str("us-west-2"))])
        .unwrap();

    // additional temporal updates to nodes with no node type
    graph
        .add_node(3, "alice", [("score", Prop::I64(15))], None, None)
        .unwrap();
    graph
        .add_node(5, "bob", [("score", Prop::I64(9))], None, None)
        .unwrap();
    graph
        .add_node(7, "server-1", [("cpu", Prop::F64(0.6))], None, None)
        .unwrap();

    // edges_t + edges_c
    let knows = graph
        .add_edge(
            2,
            "alice",
            "bob",
            [("weight", Prop::F64(1.0))],
            Some("knows"),
        )
        .unwrap();
    knows
        .add_metadata([("since", Prop::I64(2019))], Some("knows"))
        .unwrap();
    // second update on the same edge and layer
    graph
        .add_edge(
            6,
            "alice",
            "bob",
            [("weight", Prop::F64(2.5))],
            Some("knows"),
        )
        .unwrap();

    let uses = graph
        .add_edge(
            3,
            "alice",
            "server-1",
            [("requests", Prop::I64(3))],
            Some("uses"),
        )
        .unwrap();
    uses.add_metadata([("permission", Prop::str("admin"))], Some("uses"))
        .unwrap();
    graph
        .add_edge(
            4,
            "bob",
            "server-1",
            [("requests", Prop::I64(1))],
            Some("uses"),
        )
        .unwrap();

    // edge on the default layer
    graph
        .add_edge(5, "alice", "bob", [("msg", Prop::str("hi"))], None)
        .unwrap();

    // edges_d
    graph
        .delete_edge(8, "bob", "server-1", Some("uses"))
        .unwrap();
}
