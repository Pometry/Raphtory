use crate::{
    algorithms::layout::NodeVectors,
    prelude::{GraphViewOps, NodeViewOps},
};
use glam::Vec2;
use quad_rand::RandomRange;
use raphtory_api::core::entities::GID;

/// Return the position of the nodes after running Fruchterman Reingold algorithm on the `graph`
pub fn fruchterman_reingold_unbounded<'graph, G: GraphViewOps<'graph>>(
    graph: &'graph G,
    iterations: u64,
    scale: f32,
    node_start_size: f32,
    cooloff_factor: f32,
    dt: f32,
) -> NodeVectors {
    let mut positions = init_positions(graph, node_start_size);
    let mut velocities = init_velocities(graph);

    for _index in 0..iterations {
        positions = update_positions(
            &positions,
            &mut velocities,
            graph,
            scale,
            cooloff_factor,
            dt,
        );
    }

    positions
}

fn update_positions<'graph, G: GraphViewOps<'graph>>(
    old_positions: &NodeVectors,
    velocities: &mut NodeVectors,
    graph: &G,
    scale: f32,
    cooloff_factor: f32,
    dt: f32,
) -> NodeVectors {
    let mut new_positions: NodeVectors = NodeVectors::default();

    for (id, old_position) in old_positions {
        // force that will be applied to the node
        let mut force = Vec2::ZERO;

        force += compute_repulsion(id, scale, old_positions);
        force += compute_attraction(id, scale, old_positions, graph);

        let velocity = velocities.get_mut(&id).unwrap();

        *velocity += force * dt;
        *velocity *= cooloff_factor;

        let new_position = *old_position + *velocity * dt;
        new_positions.insert(id.clone(), new_position);
    }
    new_positions
}

fn compute_repulsion(id: &GID, scale: f32, old_positions: &NodeVectors) -> Vec2 {
    let mut force = Vec2::ZERO;
    let position = old_positions.get(id).unwrap();

    for (alt_id, alt_position) in old_positions {
        if alt_id != id {
            force += -((scale * scale) / position.distance(*alt_position))
                * unit_vector(*position, *alt_position);
        }
    }

    force
}

fn compute_attraction<'graph, G: GraphViewOps<'graph>>(
    id: &GID,
    scale: f32,
    old_positions: &NodeVectors,
    graph: &G,
) -> Vec2 {
    let mut force = Vec2::ZERO;
    let node = graph.node(id).unwrap();
    let position = old_positions.get(id).unwrap();

    for alt_node in node.neighbours() {
        let alt_position = old_positions.get(&alt_node.id()).unwrap();
        force += (position.distance_squared(*alt_position) / scale)
            * unit_vector(*position, *alt_position);
    }

    force
}

fn unit_vector(a: Vec2, b: Vec2) -> Vec2 {
    (b - a).normalize_or_zero()
}

fn init_velocities<'graph, G: GraphViewOps<'graph>>(graph: &G) -> NodeVectors {
    graph
        .nodes()
        .iter()
        .map(|node| (node.id(), Vec2::ZERO))
        .collect()
}

fn init_positions<'graph, G: GraphViewOps<'graph>>(graph: &G, node_start_size: f32) -> NodeVectors {
    let half_node_start_width = node_start_size / 2.0;
    graph
        .nodes()
        .iter()
        .map(|node| {
            let position = Vec2::new(
                RandomRange::gen_range(-half_node_start_width, half_node_start_width),
                RandomRange::gen_range(-half_node_start_width, half_node_start_width),
            );
            (node.id(), position)
        })
        .collect()
}
