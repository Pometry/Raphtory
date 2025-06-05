use crate::{
    core::state::compute_state::ComputeStateVec,
    db::{
        api::{
            state::NodeState,
            view::{NodeViewOps, StaticGraphViewOps},
        },
        task::{
            context::Context,
            node::eval_node::EvalNodeView,
            task::{ATask, Job, Step},
            task_runner::TaskRunner,
        },
    },
    prelude::GraphViewOps,
};
use rand::prelude::*;
use std::sync::Arc;

#[derive(Clone, Debug, Default)]
struct FastRPState {
    embedding_state: Vec<f64>,
}

/// Computes the embeddings of each vertex of a graph using the Fast RP algorithm
///
/// # Arguments
///
/// - `g` - A reference to the graph
/// - `embedding_dim` - The size of the generated embeddings
/// - `normalization_strength` - The extent to which high-degree vertices should be discounted (range: 1-0)
/// - `iter_weights` - The scalar weights to apply to the results of each iteration
/// - `seed` - The seed for initialisation of random vectors
/// - `threads` - Number of threads to use
///
/// # Returns
///
/// An [AlgorithmResult] containing the mapping from the node to its embedding
///
pub fn fast_rp<G>(
    g: &G,
    embedding_dim: usize,
    normalization_strength: f64,
    iter_weights: Vec<f64>,
    seed: Option<u64>,
    threads: Option<usize>,
) -> NodeState<'static, Vec<f64>, G>
where
    G: StaticGraphViewOps,
{
    let ctx: Context<G, ComputeStateVec> = g.into();
    let m = g.count_nodes() as f64;
    let s = m.sqrt();
    let beta = normalization_strength - 1.0;
    let num_iters = iter_weights.len() - 1;
    let weights = Arc::new(iter_weights);
    let seed = seed.unwrap_or(rand::thread_rng().gen());

    // initialize each vertex with a random vector according to FastRP's construction rules
    let step1 = {
        let weights = Arc::clone(&weights);
        ATask::new(move |vv| {
            let l = ((vv.degree() as f64) / (m * 2.0)).powf(beta);
            let choices = [
                (l * s.sqrt(), 1.0 / (s * 2.0)),
                (0.0, 1.0 - (1.0 / s)),
                (-l * s.sqrt(), 1.0 / (s * 2.0)),
            ];
            let mut rng = SmallRng::seed_from_u64(vv.node.0 as u64 ^ seed);
            let state: &mut FastRPState = vv.get_mut();
            state.embedding_state = (0..embedding_dim)
                .map(|_| choices.choose_weighted(&mut rng, |item| item.1).unwrap().0 * weights[0])
                .collect();
            Step::Continue
        })
    };

    // sum each vector from neighbours and scale
    let step2 = ATask::new(move |vv: &mut EvalNodeView<G, FastRPState>| {
        // for neighbor, for i, add neighbors.prev[i] to current state
        // scale state by iteration weight
        let weights = Arc::clone(&weights);
        let ss = vv.eval_graph.ss;
        // TODO: rewrite using iters?
        for neighbour in vv.neighbours() {
            for i in 0..embedding_dim {
                vv.get_mut().embedding_state[i] += neighbour.prev().embedding_state[i];
            }
        }
        for value in vv.get_mut().embedding_state.iter_mut() {
            *value *= weights[ss];
        }

        Step::Continue
    });

    let mut runner: TaskRunner<G, _> = TaskRunner::new(ctx);

    runner.run(
        vec![Job::new(step1)],
        vec![Job::read_only(step2)],
        None,
        |_, _, _, local: Vec<FastRPState>| {
            NodeState::new_from_eval_mapped(g.clone(), local, |v| v.embedding_state)
        },
        threads,
        num_iters,
        None,
        None,
    )
}

#[cfg(test)]
mod fast_rp_test {
    use super::*;
    use crate::{db::api::mutation::AdditionOps, prelude::*, test_storage};
    use std::collections::HashMap;

    #[test]
    fn simple_fast_rp_test() {
        let graph = Graph::new();

        let edges = vec![
            (1, 2, 1),
            (1, 3, 1),
            (2, 3, 1),
            (4, 5, 1),
            (4, 6, 1),
            (4, 7, 1),
            (5, 6, 1),
            (5, 7, 1),
            (6, 7, 1),
            (6, 8, 1),
        ];

        for (src, dst, ts) in edges {
            graph.add_edge(ts, src, dst, NO_PROPS, None).unwrap();
        }

        let baseline = HashMap::from([
            (
                "7",
                [
                    0.0,
                    3.3635856610148585,
                    -1.6817928305074292,
                    -1.6817928305074292,
                    1.6817928305074292,
                    -1.6817928305074292,
                    1.6817928305074292,
                    0.0,
                    -1.6817928305074292,
                    0.0,
                    0.0,
                    -1.6817928305074292,
                    1.6817928305074292,
                    1.6817928305074292,
                    -1.6817928305074292,
                    -1.6817928305074292,
                ],
            ),
            (
                "6",
                [
                    -1.6817928305074292,
                    5.045378491522287,
                    -1.6817928305074292,
                    0.0,
                    1.6817928305074292,
                    -1.6817928305074292,
                    0.0,
                    0.0,
                    -1.6817928305074292,
                    0.0,
                    0.0,
                    -3.3635856610148585,
                    1.6817928305074292,
                    1.6817928305074292,
                    -1.6817928305074292,
                    -1.6817928305074292,
                ],
            ),
            (
                "5",
                [
                    0.0,
                    3.3635856610148585,
                    -1.6817928305074292,
                    -1.6817928305074292,
                    1.6817928305074292,
                    -1.6817928305074292,
                    1.6817928305074292,
                    0.0,
                    -1.6817928305074292,
                    0.0,
                    0.0,
                    -1.6817928305074292,
                    1.6817928305074292,
                    1.6817928305074292,
                    -1.6817928305074292,
                    -1.6817928305074292,
                ],
            ),
            (
                "2",
                [
                    1.6817928305074292,
                    1.6817928305074292,
                    -1.6817928305074292,
                    0.0,
                    0.0,
                    3.3635856610148585,
                    1.6817928305074292,
                    1.6817928305074292,
                    3.3635856610148585,
                    -3.3635856610148585,
                    0.0,
                    1.6817928305074292,
                    0.0,
                    -1.6817928305074292,
                    0.0,
                    -3.3635856610148585,
                ],
            ),
            (
                "8",
                [
                    -1.6817928305074292,
                    1.6817928305074292,
                    -1.6817928305074292,
                    0.0,
                    1.6817928305074292,
                    -1.6817928305074292,
                    -1.6817928305074292,
                    -1.6817928305074292,
                    0.0,
                    0.0,
                    0.0,
                    -1.6817928305074292,
                    1.6817928305074292,
                    0.0,
                    0.0,
                    0.0,
                ],
            ),
            (
                "3",
                [
                    1.6817928305074292,
                    1.6817928305074292,
                    -1.6817928305074292,
                    0.0,
                    0.0,
                    3.3635856610148585,
                    1.6817928305074292,
                    1.6817928305074292,
                    3.3635856610148585,
                    -3.3635856610148585,
                    0.0,
                    1.6817928305074292,
                    0.0,
                    -1.6817928305074292,
                    0.0,
                    -3.3635856610148585,
                ],
            ),
            (
                "1",
                [
                    1.6817928305074292,
                    1.6817928305074292,
                    -1.6817928305074292,
                    0.0,
                    0.0,
                    3.3635856610148585,
                    1.6817928305074292,
                    1.6817928305074292,
                    3.3635856610148585,
                    -3.3635856610148585,
                    0.0,
                    1.6817928305074292,
                    0.0,
                    -1.6817928305074292,
                    0.0,
                    -3.3635856610148585,
                ],
            ),
            (
                "4",
                [
                    0.0,
                    3.3635856610148585,
                    -1.6817928305074292,
                    -1.6817928305074292,
                    1.6817928305074292,
                    -1.6817928305074292,
                    1.6817928305074292,
                    0.0,
                    -1.6817928305074292,
                    0.0,
                    0.0,
                    -1.6817928305074292,
                    1.6817928305074292,
                    1.6817928305074292,
                    -1.6817928305074292,
                    -1.6817928305074292,
                ],
            ),
        ]);

        test_storage!(&graph, |graph| {
            let results = fast_rp(graph, 16, 1.0, vec![1.0, 1.0], Some(42), None);
            assert_eq!(results, baseline);
        });
    }

    // NOTE(Wyatt): the simple fast_rp test is more of a validation of idempotency than correctness (although the results are expected)
    // This test-- in progress-- is going to validate that the algorithm preserves the pairwise topological distances
    /*
    use crate::io::csv_loader::CsvLoader;
    use serde::{Deserialize, Serialize};
    use std::path::PathBuf;

    fn print_samples(map: &HashMap<String, Vec<f64>>, n: usize) {
        let mut count = 0;

        for (key, value) in map {
            println!("Key: {}, Value: {:#?}", key, value);

            count += 1;
            if count >= n {
                break;
            }
        }
    }

    fn top_k_neighbors(
        data: &HashMap<String, Vec<f64>>,
        k: usize,
    ) -> HashMap<String, Vec<String>> {
        let mut neighbors: HashMap<String, Vec<String>> = HashMap::new();

        // Iterate over each ID to find its top K neighbors
        for (id, vector) in data {
            // Collect distances to all other IDs
            let mut distances: Vec<(&String, f64)> = Vec::new();
            for (other_id, other_vector) in data {
                if id == other_id {
                    continue; // Skip self
                }
                // Compute Euclidean distance
                let distance = euclidean_distance(vector, other_vector);
                distances.push((other_id, distance));
            }
            // Sort the distances in ascending order
            distances.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
            // Collect top K neighbor IDs
            let top_k: Vec<String> = distances
                .iter()
                .take(k)
                .map(|(other_id, _)| (*other_id).clone())
                .collect();
            // Insert into the neighbors map
            neighbors.insert(id.clone(), top_k);
        }

        neighbors
    }

    fn euclidean_distance(a: &Vec<f64>, b: &Vec<f64>) -> f64 {
        assert_eq!(a.len(), b.len(), "Vectors must be of the same length");
        a.iter()
            .zip(b.iter())
            .map(|(&x, &y)| (x - y).powi(2))
            .sum::<f64>()
            .sqrt()
    }

    #[test]
    fn big_fast_rp_test() {
        let mut d = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        d.push("resources/test");
        let loader = CsvLoader::new(d.join("test.csv")).set_delimiter(",");
        let graph = Graph::new();

        #[derive(Deserialize, Serialize, Debug)]
        struct CsvEdge {
            src: u64,
            dst: u64,
        }

        loader
            .load_into_graph(&graph, |e: CsvEdge, g| {
                g.add_edge(1, e.src, e.dst, NO_PROPS, None).unwrap();
                g.add_edge(1, e.dst, e.src, NO_PROPS, None).unwrap();
            })
            .unwrap();

        test_storage!(&graph, |graph| {
            let results = fast_rp(
                graph,
                32,
                1.0,
                vec![1.0, 1.0, 0.5],
                Some(42),
                None,
            ).get_all_with_names();
            // println!("Result: {:#?}", results);
            print_samples(&results, 10);
        });
    }
     */
}
