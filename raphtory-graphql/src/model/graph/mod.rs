use itertools::Itertools;
use raphtory::{
    db::{
        api::view::internal::DynamicGraph,
        graph::{edge::EdgeView, vertex::VertexView},
    },
    prelude::{EdgeViewOps, VertexViewOps},
};
use std::collections::HashSet;

pub(crate) mod edge;
pub(crate) mod graph;
pub(crate) mod node;
pub(crate) mod property;
pub(crate) mod property_update;

fn get_expanded_edges(
    graph_nodes: HashSet<String>,
    vv: VertexView<DynamicGraph>,
) -> Vec<EdgeView<DynamicGraph>> {
    let node_found_in_graph_nodes =
        |node_name: String| -> bool { graph_nodes.iter().contains(&node_name) };

    let mut fetched_edges: Vec<EdgeView<DynamicGraph>> = vv
        .clone()
        .edges()
        .into_iter()
        .map(|ee| ee.clone())
        .collect_vec();

    let first_hop_edges = fetched_edges
        .clone()
        .into_iter()
        .filter(|e| {
            !node_found_in_graph_nodes((*e).src().name())
                || !node_found_in_graph_nodes((*e).dst().name())
        })
        .collect_vec();

    let mut first_hop_nodes: HashSet<String> = HashSet::new();
    first_hop_edges.clone().into_iter().for_each(|e| {
        first_hop_nodes.insert(e.src().name());
        first_hop_nodes.insert(e.dst().name());
    });

    let first_hop_nodes = first_hop_nodes
        .into_iter()
        .filter(|e| (*e).to_string() != *vv.name())
        .collect_vec();

    let node_found_in_first_hop_nodes =
        |node_name: String| -> bool { first_hop_nodes.contains(&node_name) };

    let mut first_hop_node_edges: Vec<EdgeView<DynamicGraph>> = vec![];

    first_hop_edges.into_iter().for_each(|e| {
        if node_found_in_graph_nodes(e.src().name()) {
            // Return only those edges whose either src or dst already exist
            let mut r = e
                .dst()
                .edges()
                .filter(|e| {
                    (node_found_in_first_hop_nodes(e.src().name())
                        && node_found_in_first_hop_nodes(e.dst().name()))
                        || node_found_in_graph_nodes(e.src().name())
                        || node_found_in_graph_nodes(e.dst().name())
                })
                .collect_vec();

            first_hop_node_edges.append(&mut r);
        } else {
            let mut r = e
                .src()
                .edges()
                .filter(|e| {
                    (node_found_in_first_hop_nodes(e.src().name())
                        && node_found_in_first_hop_nodes(e.dst().name()))
                        || node_found_in_graph_nodes(e.src().name())
                        || node_found_in_graph_nodes(e.dst().name())
                })
                .collect_vec();

            first_hop_node_edges.append(&mut r);
        }
    });

    fetched_edges.append(&mut first_hop_node_edges);

    fetched_edges
}
