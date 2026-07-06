//! Round-trip tests for the per-layer property presence bitset on `Meta`.

use raphtory::{errors::GraphError, prelude::*};
use raphtory_api::core::entities::{properties::meta::STATIC_GRAPH_LAYER_ID, LayerId};
use raphtory_storage::core_ops::CoreGraphOps;
use tempfile::TempDir;

const LAYER_A: &str = "layer_a";
const LAYER_B: &str = "layer_b";

/// Build a graph with properties confined to known layers:
/// - temporal edge prop `t_a` + edge metadata `m_a`: only on `LAYER_A`
/// - temporal edge prop `t_b`: only on `LAYER_B`
/// - `LAYER_B` also gets a property-less edge, so the layer exists regardless
/// - node temporal prop `np` + node metadata `nm`: nodes are unlayered, so these
///   land on the static layer
fn build_graph(dir: &TempDir) -> Result<Graph, GraphError> {
    let g = Graph::new_at_path(dir.path())?;

    g.add_edge(0, 1, 2, [("t_a", Prop::U64(1))], Some(LAYER_A))?;
    g.edge(1, 2)
        .unwrap()
        .add_metadata([("m_a", Prop::Bool(true))], Some(LAYER_A))?;

    g.add_edge(0, 2, 3, [("t_b", Prop::str("x"))], Some(LAYER_B))?;
    g.add_edge(1, 3, 4, NO_PROPS, Some(LAYER_B))?;

    let node = g.add_node(0, 5, [("np", Prop::I64(7))], None, None)?;
    node.add_metadata([("nm", Prop::str("meta"))])?;

    Ok(g)
}

/// Assert the presence bits for the fixture built by [`build_graph`] is still consistent after reload.
fn check_presence(g: &Graph) {
    let edge_meta = g.edge_meta();
    let node_meta = g.node_meta();

    let layer_a = edge_meta.get_layer_id(LAYER_A).expect("layer_a exists");
    let layer_b = edge_meta.get_layer_id(LAYER_B).expect("layer_b exists");

    let t_a = edge_meta
        .temporal_prop_mapper()
        .get_id("t_a")
        .expect("t_a registered");
    let t_b = edge_meta
        .temporal_prop_mapper()
        .get_id("t_b")
        .expect("t_b registered");
    let m_a = edge_meta
        .metadata_mapper()
        .get_id("m_a")
        .expect("m_a registered");

    // temporal edge props: present where written...
    assert!(edge_meta.temporal_layer_has(layer_a, t_a));
    assert!(edge_meta.temporal_layer_has(layer_b, t_b));
    // ...and absent where not (guards against null-column over-marking on load)
    assert!(!edge_meta.temporal_layer_has(layer_b, t_a));
    assert!(!edge_meta.temporal_layer_has(layer_a, t_b));

    // edge metadata: only on layer_a
    assert!(edge_meta.metadata_layer_has(layer_a, m_a));
    assert!(!edge_meta.metadata_layer_has(layer_b, m_a));

    // node props/metadata land on the static layer, not the edge layers
    let np = node_meta
        .temporal_prop_mapper()
        .get_id("np")
        .expect("np registered");
    let nm = node_meta
        .metadata_mapper()
        .get_id("nm")
        .expect("nm registered");
    assert!(node_meta.temporal_layer_has(STATIC_GRAPH_LAYER_ID, np));
    assert!(node_meta.metadata_layer_has(STATIC_GRAPH_LAYER_ID, nm));
    assert!(!node_meta.temporal_layer_has(LayerId(layer_a.0.max(layer_b.0) + 1), np));
}

/// The bitset must not break actual reads: values written to a layer must still
/// come back after a reload (guards against over-aggressive layer skipping).
fn check_values(g: &Graph) {
    let e_a = g.edge(1, 2).unwrap();
    assert_eq!(
        e_a.layers(LAYER_A).unwrap().properties().get("t_a"),
        Some(Prop::U64(1))
    );
    assert_eq!(
        e_a.layers(LAYER_A).unwrap().metadata().get("m_a"),
        Some(Prop::Bool(true))
    );

    let e_b = g.edge(2, 3).unwrap();
    assert_eq!(
        e_b.layers(LAYER_B).unwrap().properties().get("t_b"),
        Some(Prop::str("x"))
    );
    // t_a lives on a different layer: the layer-skip must hide it here
    assert_eq!(e_b.layers(LAYER_B).unwrap().properties().get("t_a"), None);

    let n = g.node(5).unwrap();
    assert_eq!(n.properties().get("np"), Some(Prop::I64(7)));
    assert_eq!(n.metadata().get("nm"), Some(Prop::str("meta")));
}

/// Write -> check -> flush (mem segments persisted to arrow columns) -> drop -> load -> check.
/// The reload rebuilds `Meta` by scanning the on-disk columns so this exercises the null-gated column-scan path.
#[test]
fn layer_prop_presence_survives_flush_and_reload() {
    let dir = TempDir::new().unwrap();

    let g = build_graph(&dir).unwrap();
    check_presence(&g);
    check_values(&g);

    g.core_graph().flush().unwrap();
    drop(g);

    let g = Graph::load(dir.path()).unwrap();
    check_presence(&g);
    check_values(&g);
}

/// Write -> check -> drop without an explicit flush -> load.
#[test]
fn layer_prop_presence_survives_wal_replay() {
    let dir = TempDir::new().unwrap();

    let g = build_graph(&dir).unwrap();
    check_presence(&g);
    drop(g);

    let g = Graph::load(dir.path()).unwrap();
    check_presence(&g);
    check_values(&g);
}
