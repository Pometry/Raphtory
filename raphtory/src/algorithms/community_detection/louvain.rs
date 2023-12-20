use crate::{
    algorithms::{
        algorithm_result::AlgorithmResult,
        community_detection::modularity::{ComID, ModularityFunction, Partition},
    },
    core::entities::VID,
    prelude::GraphViewOps,
};
use std::collections::HashMap;

pub fn louvain<'graph, M: ModularityFunction, G: GraphViewOps<'graph>>(
    graph: &G,
    resolution: f64,
    weight_prop: Option<&str>,
    tol: Option<f64>,
) -> AlgorithmResult<G, usize> {
    let tol = tol.unwrap_or(1e-8);
    let mut modularity_state = M::new(
        graph,
        weight_prop,
        resolution,
        Partition::new_singletons(graph.count_nodes()),
    );
    let mut global_partition: HashMap<_, _> = graph
        .nodes()
        .iter()
        .enumerate()
        .map(|(ci, node)| (node.node.index(), ci))
        .collect();

    let mut outer_moved = true;
    while outer_moved {
        outer_moved = false;
        let mut inner_moved = true;
        while inner_moved {
            inner_moved = false;
            for v in modularity_state.nodes() {
                if let Some((best_c, delta)) = modularity_state
                    .candidate_moves(&v)
                    .map(|c| (c, modularity_state.move_delta(&v, c)))
                    .max_by(|(_, delta1), (_, delta2)| delta1.total_cmp(delta2))
                {
                    let old_c = modularity_state.partition().com(&v);
                    if best_c != old_c && delta > tol {
                        inner_moved = true;
                        modularity_state.move_node(&v, best_c);
                    }
                }
            }
            let partition = modularity_state.aggregate();
            println!(
                "Finished outer iteration, num_coms={}, modularity={}",
                partition.num_coms(),
                modularity_state.value()
            );
            for c in global_partition.values_mut() {
                *c = partition.com(&VID(*c)).index();
            }
        }
    }
    AlgorithmResult::new(graph.clone(), "louvain", "usize", global_partition)
}

#[cfg(test)]
mod test {
    #[test]
    fn test_on_lfr() {}
}
