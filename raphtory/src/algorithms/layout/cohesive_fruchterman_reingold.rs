use crate::{
    algorithms::layout::{
        fruchterman_reingold_unbounded::fruchterman_reingold_unbounded, NodeVectors,
    },
    prelude::{AdditionOps, GraphViewOps, NodeViewOps, NO_PROPS},
};
use itertools::Itertools;

pub fn cohesive_fruchterman_reingold<'graph, G: GraphViewOps<'graph>>(
    graph: &'graph G,
    iterations: u64,
    scale: f32,
    cooloff_factor: f32,
    dt: f32,
) -> NodeVectors {
    println!("cohesive_fruchterman_reingold");
    let virtual_graph = graph.materialize().unwrap();

    let isolated_nodes = virtual_graph
        .nodes()
        .iter()
        .filter(|node| node.degree() == 0)
        .collect_vec();

    if isolated_nodes.len() > 0 {
        let bridge_node = isolated_nodes.get(0).unwrap();
        let isolated_nodes = &isolated_nodes[1..];

        // if let Some(bridge_node) = index_dict.get(&bridge_node.name()) {
        for node in isolated_nodes {
            virtual_graph
                .add_edge(0, bridge_node.id(), node.id(), NO_PROPS, None)
                .unwrap();
        }

        let connection_to_main = virtual_graph
            .nodes()
            .iter()
            .filter(|node| node.degree() > 0)
            .map(|node| (node.degree(), node))
            .sorted_by(|(d1, _), (d2, _)| d1.cmp(d2))
            .next();

        if let Some((_, connection_to_main)) = connection_to_main {
            virtual_graph
                .add_edge(0, connection_to_main.id(), bridge_node.id(), NO_PROPS, None)
                .unwrap();
        }
    }

    fruchterman_reingold_unbounded(&virtual_graph, iterations, scale, cooloff_factor, dt)
}
