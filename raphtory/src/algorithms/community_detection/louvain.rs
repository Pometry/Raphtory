use crate::{
    algorithms::community_detection::modularity::{ModularityFunction, Partition},
    core::entities::VID,
    db::api::state::{GenericNodeState, TypedNodeState},
    prelude::GraphViewOps,
};
use rand::{prelude::SliceRandom, rngs::StdRng, SeedableRng};
use serde::{Deserialize, Serialize};

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug, Default)]
pub struct LouvainState {
    community_id: usize,
}

/// Louvain algorithm for community detection
///
/// # Arguments
///
/// - `g` (GraphView): the graph view
/// - `resolution` (float): the resolution parameter for modularity
/// - `weight_prop` (str | None): the edge property to use for weights (has to be float)
/// - `tol` (None | float): the floating point tolerance for deciding if improvements are significant (default: 1e-8)
/// - `rng_seed` (None | u64): seed for the rng used to shuffle nodes; if `None`, the rng is seeded from the OS (default: None)
///
/// # Returns
///
///  An [AlgorithmResult] containing a mapping of vertices to cluster ID.
pub fn louvain<'graph, M: ModularityFunction, G: GraphViewOps<'graph>>(
    g: &G,
    resolution: f64,
    weight_prop: Option<&str>,
    tol: Option<f64>,
    rng_seed: Option<u64>,
) -> TypedNodeState<'graph, LouvainState, G> {
    let tol = tol.unwrap_or(1e-8);
    let mut rng = match rng_seed {
        Some(seed) => StdRng::seed_from_u64(seed),
        None => StdRng::from_os_rng(),
    };
    let mut modularity_state = M::new(
        g,
        weight_prop,
        resolution,
        Partition::new_singletons(g.count_nodes()),
        tol,
    );

    let mut global_partition: Vec<_> = (0..g.count_nodes()).collect();

    let mut outer_moved = true;
    while outer_moved {
        outer_moved = false;
        let mut inner_moved = true;
        let mut nodes: Vec<_> = modularity_state.nodes().collect();
        while inner_moved {
            inner_moved = false;
            nodes.shuffle(&mut rng);
            for v in nodes.iter() {
                if let Some((best_c, delta)) = modularity_state
                    .candidate_moves(v)
                    .map(|c| (c, modularity_state.move_delta(v, c)))
                    .max_by(|(com1, delta1), (com2, delta2)| {
                        delta1
                            .partial_cmp(delta2)
                            .unwrap()
                            .then_with(|| com1.0.cmp(&com2.0))
                    })
                {
                    let old_c = modularity_state.partition().com(v);
                    if best_c != old_c && delta > tol {
                        inner_moved = true;
                        outer_moved = true;
                        modularity_state.move_node(v, best_c);
                    }
                }
            }
        }
        let partition = modularity_state.aggregate();
        for c in global_partition.iter_mut() {
            *c = partition.com(&VID(*c)).index();
        }
    }
    TypedNodeState::new(GenericNodeState::new_from_eval_mapped(
        g.clone(),
        global_partition,
        |value| LouvainState {
            community_id: value,
        },
        None,
    ))
}
