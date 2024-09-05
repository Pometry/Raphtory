use raphtory_api::core::entities::GID;

use crate::{
    algorithms::{
        components::weakly_connected_components,
        layout::{fruchterman_reingold::fruchterman_reingold_unbounded, NodeVectors},
    },
    db::{api::view::MaterializedGraph, graph::node::NodeView},
    prelude::{AdditionOps, GraphViewOps, NodeViewOps, NO_PROPS},
};
use std::collections::HashMap;

/// Cohesive version of `fruchterman_reingold` that adds virtual edges between isolated nodes
pub fn cohesive_fruchterman_reingold<'graph, G: GraphViewOps<'graph>>(
    graph: &'graph G,
    iterations: u64,
    scale: f32,
    node_start_size: f32,
    cooloff_factor: f32,
    dt: f32,
) -> NodeVectors {
    let virtual_graph = graph.materialize().unwrap();

    let transform_map = |input_map: HashMap<String, GID>| -> HashMap<GID, Vec<NodeView<MaterializedGraph, MaterializedGraph>>> {
        let mut output_map: HashMap<GID, Vec<NodeView<MaterializedGraph, MaterializedGraph>>> = HashMap::new();

        for (key, value) in input_map.into_iter() {
            output_map
                .entry(value)
                .or_default()
                .push(virtual_graph.node(key.clone()).unwrap());
        }

        output_map
    };

    let connected_components = transform_map(
        weakly_connected_components(&virtual_graph, usize::MAX, None).get_all_with_names(),
    );

    if connected_components.len() > 1 {
        let max_degree = virtual_graph
            .nodes()
            .iter()
            .map(|node| node.degree())
            .max()
            .unwrap_or(0); // Default to 0 if there are no nodes

        let nodes = &virtual_graph.nodes();
        let nodes_with_max_degree: Vec<_> = nodes
            .iter()
            .filter(|node| node.degree() == max_degree)
            .collect();

        let bridge_node = &nodes_with_max_degree[0];

        for (_, isolated_nodes) in connected_components.iter() {
            virtual_graph
                .add_edge(0, bridge_node.id(), isolated_nodes[0].id(), NO_PROPS, None)
                .unwrap();
        }
    }

    fruchterman_reingold_unbounded(
        &virtual_graph,
        iterations,
        scale,
        node_start_size,
        cooloff_factor,
        dt,
    )
}
