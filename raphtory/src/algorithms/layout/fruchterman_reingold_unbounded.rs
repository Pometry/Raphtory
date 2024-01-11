use crate::{
    algorithms::layout::NodeVectors,
    prelude::{GraphViewOps, NodeViewOps},
};
use glam::Vec2;

pub fn fruchterman_reingold_unbounded<'graph, G: GraphViewOps<'graph>>(
    graph: &'graph G,
    iterations: u64,
    scale: f32,
    cooloff_factor: f32,
    dt: f32,
) -> NodeVectors {
    println!("fruchterman_reingold_unbounded");
    let mut positions = init_node_vectors(graph);
    let mut velocities = init_node_vectors(graph);

    for index in 0..iterations {
        println!("iteration {index}");
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

    for (&id, old_position) in old_positions {
        // force that will be applied to the node
        let mut force = Vec2::ZERO;

        force += compute_repulsion(id, scale, old_positions);
        force += compute_attraction(id, scale, old_positions, graph);

        let mut velocity = velocities.get_mut(&id).unwrap();

        *velocity += force * dt;
        *velocity *= cooloff_factor;

        // node.location += node.velocity * dt;
        new_positions.insert(id, *old_position + *velocity * dt);
    }
    new_positions
}

fn compute_repulsion(id: u64, scale: f32, old_positions: &NodeVectors) -> Vec2 {
    let mut force = Vec2::ZERO;
    let position = old_positions.get(&id).unwrap();

    for (alt_id, alt_position) in old_positions {
        if *alt_id != id {
            force += -((scale * scale) / position.distance(*alt_position))
                * unit_vector(*position, *alt_position);
        }
    }

    force
}

fn compute_attraction<'graph, G: GraphViewOps<'graph>>(
    id: u64,
    scale: f32,
    old_positions: &NodeVectors,
    graph: &G,
) -> Vec2 {
    let mut force = Vec2::ZERO;
    let node = graph.node(id).unwrap();
    let position = old_positions.get(&id).unwrap();

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

fn init_node_vectors<'graph, G: GraphViewOps<'graph>>(graph: &G) -> NodeVectors {
    graph
        .nodes()
        .iter()
        .map(|node| (node.id(), Vec2::ZERO))
        .collect()
}
