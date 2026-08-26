use raphtory::{db::api::view::StaticGraphViewOps, prelude::*};

mod cached_view;
mod edge_property_filter;
mod exploded_edge_property_filter;
mod node_property_filter;
mod prop_index_equivalence;
mod subgraph_tests;
mod test_filters;
mod test_layers;
mod tests_node_type_filtered_subgraph;
mod views_test;

/// Whether [`init_graph`] adds nodes, and whether they carry a node type.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Nodes {
    /// Don't add any nodes.
    None,
    /// Add nodes without a node type.
    Untyped,
    /// Add nodes with their node type (`air_nomad` / `water_tribe` / `fire_nation`).
    Typed,
}

/// Whether [`init_graph`] adds edges, and whether they carry a layer.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Edges {
    /// Don't add any edges.
    None,
    /// Add edges without a layer.
    Unlayered,
    /// Add edges with their layer (`layer1` / `layer2`).
    Layered,
}

/// Builds the filter-test graph in the variations the different filter test modules use
pub fn init_graph<G: StaticGraphViewOps + AdditionOps>(graph: G, nodes: Nodes, edges: Edges) -> G {
    if edges != Edges::None {
        let edge_data = [
            (6, "N1", "N2", 2u64, "layer1"),
            (7, "N1", "N2", 1u64, "layer2"),
            (6, "N2", "N3", 1u64, "layer1"),
            (7, "N2", "N3", 2u64, "layer2"),
            (8, "N3", "N4", 1u64, "layer1"),
            (9, "N4", "N5", 1u64, "layer1"),
            (5, "N5", "N6", 1u64, "layer1"),
            (6, "N5", "N6", 2u64, "layer2"),
            (5, "N6", "N7", 1u64, "layer1"),
            (6, "N6", "N7", 1u64, "layer2"),
            (3, "N7", "N8", 1u64, "layer1"),
            (5, "N7", "N8", 1u64, "layer2"),
            (3, "N8", "N1", 1u64, "layer1"),
            (4, "N8", "N1", 2u64, "layer2"),
        ];

        for (ts, src, dst, p1, layer) in edge_data {
            let layer = (edges == Edges::Layered).then_some(layer);
            graph
                .add_edge(ts, src, dst, [("p1", Prop::U64(p1))], layer)
                .unwrap();
        }
    }

    if nodes != Nodes::None {
        let node_data = [
            (6, "N1", 2u64, "air_nomad"),
            (7, "N1", 1u64, "air_nomad"),
            (6, "N2", 1u64, "water_tribe"),
            (7, "N2", 2u64, "water_tribe"),
            (8, "N3", 1u64, "air_nomad"),
            (9, "N4", 1u64, "air_nomad"),
            (5, "N5", 1u64, "air_nomad"),
            (6, "N5", 2u64, "air_nomad"),
            (5, "N6", 1u64, "fire_nation"),
            (6, "N6", 1u64, "fire_nation"),
            (3, "N7", 1u64, "air_nomad"),
            (5, "N7", 1u64, "air_nomad"),
            (3, "N8", 1u64, "fire_nation"),
            (4, "N8", 2u64, "fire_nation"),
        ];

        for (ts, name, p1, node_type) in node_data {
            let node_type = (nodes == Nodes::Typed).then_some(node_type);
            graph
                .add_node(ts, name, [("p1", Prop::U64(p1))], node_type, None)
                .unwrap();
        }
    }

    graph
}
