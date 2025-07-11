use crate::{
    algorithms::{
        components::weakly_connected_components,
        layout::fruchterman_reingold::fruchterman_reingold_unbounded,
    },
    db::api::state::NodeState,
    prelude::{NodeStateGroupBy, OrderedNodeStateOps, *},
};

/// Cohesive version of `fruchterman_reingold` that adds virtual edges between isolated nodes
///
/// # Arguments
///
/// - `g` - A reference to the graph
/// - `iter_count` - The number of iterations to run
/// - `scale`: Global scaling factor to control the overall spread of the graph
/// - `node_start_size`: Initial size or movement range for nodes
/// - `cooloff_factor`: Factor to reduce node movement in later iterations, helping stabilize the layout
/// - `dt`: Time step or movement factor in each iteration
///
/// # Returns
///
/// An [AlgorithmResult] containing a mapping between vertices and a [Vec2] of coordinates.
///
pub fn cohesive_fruchterman_reingold<'graph, G: GraphViewOps<'graph>>(
    g: &G,
    iter_count: u64,
    scale: f32,
    node_start_size: f32,
    cooloff_factor: f32,
    dt: f32,
) -> NodeState<'graph, [f32; 2], G> {
    let virtual_graph = g.materialize().unwrap();

    let connected_components = weakly_connected_components(&virtual_graph).groups();

    if connected_components.len() > 1 {
        let degrees = virtual_graph.nodes().degree();
        let bridge_node = degrees.max_item().unwrap().0;

        for (_, isolated_nodes) in connected_components.iter() {
            virtual_graph
                .add_edge(
                    0,
                    &bridge_node,
                    &isolated_nodes.degree().max_item().unwrap().0,
                    NO_PROPS,
                    None,
                )
                .unwrap();
        }
    }

    fruchterman_reingold_unbounded(g, iter_count, scale, node_start_size, cooloff_factor, dt)
}
