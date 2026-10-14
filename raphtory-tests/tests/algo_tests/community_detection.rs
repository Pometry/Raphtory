use raphtory::{
    algorithms::community_detection::{
        label_propagation::label_propagation,
        louvain::louvain,
        modularity::{ComID, ModularityFunction, ModularityUnDir, Partition},
    },
    core::entities::VID,
    graphgen::random_attachment::random_attachment,
    logging::global_info_logger,
    prelude::*,
};
use raphtory_tests::test_storage;
use std::collections::{HashMap, HashSet};
use tracing::info;

fn group_by_value(map: &HashMap<String, usize>) -> Vec<HashSet<String>> {
    let mut grouped: HashMap<usize, HashSet<String>> = HashMap::new();

    for (key, &value) in map {
        grouped
            .entry(value)
            .or_insert_with(HashSet::new)
            .insert(key.clone());
    }

    grouped.into_values().collect()
}

#[test]
fn lpa_test() {
    let graph: Graph = Graph::new();
    let edges = vec![
        (1, "R1", "R2"),
        (1, "R1", "R3"),
        (1, "R2", "R3"),
        (1, "R3", "G"),
        (1, "G", "B1"),
        (1, "G", "B3"),
        (1, "B1", "B2"),
        (1, "B2", "B3"),
        (1, "B2", "B4"),
        (1, "B3", "B4"),
        (1, "B3", "B5"),
        (1, "B4", "B5"),
    ];
    for (ts, src, dst) in edges {
        graph.add_edge(ts, src, dst, NO_PROPS, None).unwrap();
    }
    test_storage!(&graph, |graph| {
        let seed = Some([5; 32]);
        let result =
            label_propagation(graph, 20, seed, None).to_hashmap(|value| value.community_id);
        println!("{:?}", result);
        let result = group_by_value(&result);

        let expected = vec![
            HashSet::from(["R1".to_string(), "R2".to_string(), "R3".to_string()]),
            HashSet::from([
                "G".to_string(),
                "B1".to_string(),
                "B2".to_string(),
                "B3".to_string(),
                "B4".to_string(),
                "B5".to_string(),
            ]),
        ];
        for hashset in expected {
            assert!(result.contains(&hashset));
        }
    });
}

use proptest::prelude::*;

#[test]
fn test_louvain() {
    let edges = vec![
        (100, 200, 2.0f64),
        (100, 300, 3.0f64),
        (200, 300, 8.5f64),
        (300, 400, 1.0f64),
        (400, 500, 1.5f64),
        (600, 800, 0.5f64),
        (700, 900, 3.5f64),
        (100, 600, 1.5f64),
    ];
    test_all_nodes_assigned_inner(edges)
}

fn test_all_nodes_assigned_inner(edges: Vec<(u64, u64, f64)>) {
    let graph = Graph::new();
    for (src, dst, weight) in edges {
        graph
            .add_edge(1, src, dst, [("weight", weight)], None)
            .unwrap();
        graph
            .add_edge(1, dst, src, [("weight", weight)], None)
            .unwrap();
    }

    test_storage!(&graph, |graph| {
        let result = louvain::<ModularityUnDir, _>(graph, 1.0, Some("weight"), None, Some(42));
        assert!(graph
            .nodes()
            .iter()
            .all(|n| result.get_by_node(n).is_some()));
    });
}

fn test_all_nodes_assigned_inner_unweighted(edges: Vec<(u64, u64)>) {
    let graph = Graph::new();
    for (src, dst) in edges {
        graph.add_edge(1, src, dst, NO_PROPS, None).unwrap();
        graph.add_edge(1, dst, src, NO_PROPS, None).unwrap();
    }

    test_storage!(&graph, |graph| {
        let result = louvain::<ModularityUnDir, _>(graph, 1.0, None, None, Some(42));
        assert!(graph
            .nodes()
            .iter()
            .all(|n| result.get_by_node(n).is_some()));
    });
}

proptest! {
    #[test]
    fn test_all_nodes_in_communities_proptest(edges in any::<Vec<(u64, u64, f64)>>().prop_map(|mut v| {v.iter_mut().for_each(|(_, _, w)| *w = w.abs()); v})) {
        test_all_nodes_assigned_inner(edges)
    }

    #[test]
    fn test_all_nodes_assigned_unweighted_proptest(edges in any::<Vec<(u8, u8)>>().prop_map(|v| v.into_iter().map(|(s, d)|  (s as u64, d as u64)).collect::<Vec<_>>())) {
        test_all_nodes_assigned_inner_unweighted(edges)
    }
}

#[test]
fn lfr_test() {
    use raphtory::io::csv_loader::CsvLoader;
    use raphtory_api::core::utils::logging::global_info_logger;
    use serde::{Deserialize, Serialize};
    use std::path::PathBuf;
    global_info_logger();
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
        })
        .unwrap();

    test_storage!(&graph, |graph| {
        let _ = louvain::<ModularityUnDir, _>(graph, 1.0, None, None, Some(42));
        // TODO: Add assertions
    });
}

#[test]
fn test_louvain_deterministic() {
    let graph = Graph::new();
    random_attachment(&graph, 10_000, 5, Some([7; 32]));

    test_storage!(&graph, |graph| {
        let seed = Some(42);
        let first = louvain::<ModularityUnDir, _>(graph, 1.0, None, None, seed);

        for _ in 0..100 {
            let result = louvain::<ModularityUnDir, _>(graph, 1.0, None, None, seed);
            assert!(
                result == first,
                "louvain produced different clusters across runs with the same seed"
            );
        }
    });
}

#[test]
fn test_delta() {
    global_info_logger();
    let graph = Graph::new();
    graph.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
    graph.add_edge(0, 2, 1, NO_PROPS, None).unwrap();

    test_storage!(&graph, |graph| {
        let mut m = ModularityUnDir::new(
            graph,
            None,
            1.0,
            Partition::new_singletons(graph.count_nodes()),
            1e-8,
        );
        let old_value = m.value();
        assert_eq!(old_value, -0.5);
        let delta = m.move_delta(&VID(0), ComID(1));
        info!("delta: {delta}");
        m.move_node(&VID(0), ComID(1));
        assert_eq!(m.value(), old_value + delta)
    });
}

#[test]
fn test_aggregation() {
    global_info_logger();
    let graph = Graph::new();
    graph.add_edge(0, 0, 1, NO_PROPS, None).unwrap();
    graph.add_edge(0, 1, 0, NO_PROPS, None).unwrap();
    graph.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
    graph.add_edge(0, 2, 1, NO_PROPS, None).unwrap();
    graph.add_edge(0, 0, 3, NO_PROPS, None).unwrap();
    graph.add_edge(0, 3, 0, NO_PROPS, None).unwrap();

    test_storage!(&graph, |graph| {
        let partition = Partition::from_iter([0usize, 0, 1, 1]);
        let mut m = ModularityUnDir::new(graph, None, 1.0, partition, 1e-8);
        let value_before = m.value();
        let _ = m.aggregate();
        let value_after = m.value();
        info!("before: {value_before}, after: {value_after}");
        assert_eq!(value_after, value_before);
        let delta = m.move_delta(&VID(0), ComID(1));
        m.move_node(&VID(0), ComID(1));
        let value_merged = m.value();
        assert_eq!(value_merged, 0.0);
        assert!((value_merged - (value_after + delta)).abs() < 1e-8);
    });
}

mod belief_propagation {
    use proptest::prelude::*;
    use raphtory::{
        algorithms::community_detection::belief_propagation::{
            belief_propagation, belief_propagation_probe_convergence, BeliefPropState,
        },
        db::api::view::{internal::FilterOps, StaticGraphViewOps},
        prelude::*,
    };
    use std::collections::HashMap;

    type Seeds = HashMap<String, Vec<(usize, f64)>>;
    /// Node name -> label -> D-belief.
    type Beliefs = HashMap<String, HashMap<usize, f64>>;

    // Small enough that the floor and the activation gate leave the fixed point untouched at the
    // precision the tests compare to.
    const EPSILON: f64 = 1e-12;
    const TOL: f64 = 1e-14;
    const MAX_ITER: usize = 10_000;

    fn run<G: StaticGraphViewOps>(
        g: &G,
        seeds: &Seeds,
        c: f64,
        epsilon: f64,
        activation_tol: f64,
        top_r: usize,
        threads: usize,
    ) -> HashMap<String, BeliefPropState> {
        belief_propagation(
            g,
            seeds.clone(),
            c,
            epsilon,
            Some(activation_tol),
            Some(MAX_ITER),
            Some(top_r),
            Some(threads),
        )
        .unwrap()
        .iter()
        .map(|(node, state)| (node.name(), state.clone()))
        .collect()
    }

    /// Every node's D-belief per label, read back from the top-r, so `top_r` must cover all labels.
    fn beliefs(states: &HashMap<String, BeliefPropState>) -> Beliefs {
        states
            .iter()
            .map(|(name, state)| {
                let per_label = state
                    .top_labels
                    .iter()
                    .copied()
                    .zip(state.top_values.iter().copied())
                    .collect();
                (name.clone(), per_label)
            })
            .collect()
    }

    fn belief(beliefs: &Beliefs, node: &str, label: usize) -> f64 {
        beliefs[node].get(&label).copied().unwrap_or(0.0)
    }

    fn prior(seeds: &Seeds, node: &str, label: usize) -> f64 {
        seeds
            .get(node)
            .and_then(|pairs| pairs.iter().find(|&&(l, _)| l == label))
            .map_or(0.0, |&(_, mass)| mass)
    }

    /// Undirected, deduplicated neighbours, without the node itself.
    fn neighbours(g: &Graph, node: &str) -> Vec<String> {
        g.node(node)
            .unwrap()
            .neighbours()
            .name()
            .filter(|nbor| nbor != node)
            .collect()
    }

    fn assert_close(actual: &Beliefs, expected: &Beliefs, rel_tol: f64) {
        assert_eq!(actual.len(), expected.len(), "different node sets");
        for (node, want) in expected {
            let got = &actual[node];
            for label in want.keys().chain(got.keys()) {
                let (a, e) = (belief(actual, node, *label), belief(expected, node, *label));
                assert!(
                    (a - e).abs() <= rel_tol * (1.0 + e.abs()),
                    "node {node}, label {label}: got {a}, expected {e}"
                );
            }
        }
    }

    /// The naive reference: the same update as full sweeps over every node, one label at a time,
    /// with no frontier, no floor and no activation gate, until no value moves.
    ///
    /// Stopping on exact equality is safe: floating-point rounding is monotone, so the iterates
    /// rise monotonically from zero and must settle.
    fn dense_reference(g: &Graph, seeds: &Seeds, c: f64) -> Beliefs {
        let names: Vec<String> = g.nodes().name().collect_vec();
        let pos: HashMap<&str, usize> = names
            .iter()
            .enumerate()
            .map(|(i, name)| (name.as_str(), i))
            .collect();
        let adj: Vec<Vec<usize>> = names
            .iter()
            .map(|name| {
                neighbours(g, name)
                    .iter()
                    .map(|nbor| pos[nbor.as_str()])
                    .collect()
            })
            .collect();
        let mut labels: Vec<usize> = seeds.values().flatten().map(|&(l, _)| l).collect();
        labels.sort_unstable();
        labels.dedup();

        let k = 1.0 - c * c;
        let mut out: Beliefs = names.iter().map(|n| (n.clone(), HashMap::new())).collect();
        for label in labels {
            let e: Vec<f64> = names.iter().map(|n| prior(seeds, n, label)).collect();
            let mut b = vec![0.0; names.len()];
            for _ in 0..MAX_ITER {
                let next: Vec<f64> = (0..names.len())
                    .map(|u| {
                        let sum: f64 = adj[u].iter().map(|&v| b[v]).sum();
                        (k * e[u] + c * sum) / (k + c * c * adj[u].len() as f64)
                    })
                    .collect();
                let moved = next != b;
                b = next;
                if !moved {
                    break;
                }
            }
            for (u, &value) in b.iter().enumerate() {
                if value > 0.0 {
                    out.get_mut(&names[u]).unwrap().insert(label, value);
                }
            }
        }
        out
    }

    /// A small random graph (self-loops and repeated edges included), seeds on some of its nodes,
    /// and the fraction of `1 / d_max` that `c` is set to.
    fn bp_inputs() -> impl Strategy<Value = (Vec<(u8, u8)>, Vec<(u8, usize, f64)>, f64)> {
        (
            prop::collection::vec((0u8..16, 0u8..16), 1..40),
            prop::collection::vec((0u8..16, 0usize..4, 0.1f64..10.0), 1..10),
            0.1f64..0.9,
        )
    }

    /// Builds the graph and the seeds, keeping the first mass for a repeated (node, label).
    ///
    /// `c < 1 / d_max <= 1 / ρ(B_nb)`, since `ρ(B_nb) <= d_max - 1`, so the update converges.
    fn build(
        edges: &[(u8, u8)],
        raw_seeds: &[(u8, usize, f64)],
        fraction: f64,
    ) -> (Graph, Seeds, f64) {
        let g = Graph::new();
        for &(src, dst) in edges {
            g.add_edge(0, src.to_string(), dst.to_string(), NO_PROPS, None)
                .unwrap();
        }
        let mut seeds: Seeds = HashMap::new();
        for &(node, label, mass) in raw_seeds {
            let name = node.to_string();
            if g.has_node(name.as_str()) {
                let pairs = seeds.entry(name).or_default();
                if pairs.iter().all(|&(l, _)| l != label) {
                    pairs.push((label, mass));
                }
            }
        }
        let d_max = g
            .nodes()
            .iter()
            .map(|n| neighbours(&g, &n.name()).len())
            .max()
            .unwrap_or(0)
            .max(1);
        (g, seeds, fraction / d_max as f64)
    }

    /// Exact answers on hand-solvable graphs.
    ///
    /// On a tree the echo cancellation is exact, so a single seed of mass `m` gives
    /// `b_v = m · c^dist(seed, v)`, and seeds of the same label add up. On a triangle seeded at `x`
    /// with mass `m`, at `c = 1/2`, the fixed point is `b_x = 9m/7` and `b_y = b_z = 6m/7`. A node
    /// whose only edge is a self-loop has no neighbours, so its belief is its own mass.
    #[test]
    fn bp_closed_form() {
        let c: f64 = 0.5;
        let g = Graph::new();
        // Tree: the path a-b-c-d with leaves e and f on b, plus a self-loop on b.
        // Triangle: x-y-z. And s, whose only edge is a self-loop.
        for (src, dst) in [
            ("a", "b"),
            ("b", "c"),
            ("c", "d"),
            ("b", "e"),
            ("b", "f"),
            ("b", "b"),
            ("x", "y"),
            ("y", "z"),
            ("z", "x"),
            ("s", "s"),
        ] {
            g.add_edge(0, src, dst, NO_PROPS, None).unwrap();
        }
        let seeds: Seeds = HashMap::from([
            ("a".to_string(), vec![(10, 2.0)]),
            ("d".to_string(), vec![(20, 1.0)]),
            ("f".to_string(), vec![(20, 3.0)]),
            ("x".to_string(), vec![(30, 7.0)]),
            ("s".to_string(), vec![(40, 1.5)]),
        ]);

        // Hops from a, d and f.
        let tree_hops = [
            ("a", [0, 3, 2]),
            ("b", [1, 2, 1]),
            ("c", [2, 1, 2]),
            ("d", [3, 0, 3]),
            ("e", [2, 3, 2]),
            ("f", [2, 3, 0]),
        ];
        let mut expected: Beliefs = tree_hops
            .iter()
            .map(|&(node, [from_a, from_d, from_f])| {
                let label_10 = 2.0 * c.powi(from_a);
                let label_20 = 1.0 * c.powi(from_d) + 3.0 * c.powi(from_f);
                (
                    node.to_string(),
                    HashMap::from([(10, label_10), (20, label_20)]),
                )
            })
            .collect();
        expected.insert("x".to_string(), HashMap::from([(30, 9.0)]));
        expected.insert("y".to_string(), HashMap::from([(30, 6.0)]));
        expected.insert("z".to_string(), HashMap::from([(30, 6.0)]));
        expected.insert("s".to_string(), HashMap::from([(40, 1.5)]));

        let actual = beliefs(&run(&g, &seeds, c, EPSILON, TOL, 4, 1));
        assert_close(&actual, &expected, 1e-9);
    }

    /// On the complete graph K5 (4-regular), the update's spectral radius is
    /// `ρ(W) = 4c / (1 − c² + 4c²)`, which is 1 at `c* = 1/3`. At `c = 0.5` it is `2/1.75 ≈ 1.14`,
    /// above the divergence threshold, so the run must stop with an error. At `c = 0.3` it is
    /// `≈ 0.945`: close to 1 but under the threshold, so the run must complete.
    #[test]
    fn bp_divergence_errors() {
        let g = Graph::new();
        add_clique(&g, &["a", "b", "c", "d", "e"]);
        let seeds: Seeds = HashMap::from([("a".to_string(), vec![(1, 1.0)])]);

        let diverged = belief_propagation(
            &g,
            seeds.clone(),
            0.5,
            EPSILON,
            Some(TOL),
            Some(MAX_ITER),
            None,
            Some(1),
        );
        let err = diverged.err().expect("c = 0.5 > c* must be an error");
        assert!(err.to_string().contains("diverges"), "{err}");

        run(&g, &seeds, 0.3, EPSILON, TOL, 1, 1);
    }

    /// Adds every edge between `names`: the complete graph on them.
    fn add_clique(g: &Graph, names: &[&str]) {
        for (i, src) in names.iter().enumerate() {
            for dst in &names[i + 1..] {
                g.add_edge(0, *src, *dst, NO_PROPS, None).unwrap();
            }
        }
    }

    fn probe<G: StaticGraphViewOps>(g: &G, seeds: &Seeds, c: f64, max_iter: usize) -> Option<f64> {
        belief_propagation_probe_convergence(
            g,
            seeds.clone(),
            c,
            Some(EPSILON),
            Some(max_iter),
            Some(1),
        )
        .unwrap()
    }

    /// On a d-regular graph the probe must return `ρ(W) = c·d / (1 − c² + c²·d)`: K5 (d = 4) at
    /// `c = 0.2` gives `0.8/1.12`, and K_{3,3} (d = 3, bipartite: spectrum `±ρ, 0`) gives `0.6/1.08`.
    /// Past `c* = 1/3`, K5 at `c = 0.5` diverges, which the probe returns as `ρ̂ = 2/1.75 > 1`, not an
    /// error.
    #[test]
    fn bp_probe_closed_form() {
        let k5 = Graph::new();
        add_clique(&k5, &["a", "b", "c", "d", "e"]);
        let seeds: Seeds = HashMap::from([("a".to_string(), vec![(1, 1.0)])]);
        let rho = probe(&k5, &seeds, 0.2, 30).unwrap();
        assert!((rho - 0.8 / 1.12).abs() < 1e-9, "K5 rho={rho}");
        let rho = probe(&k5, &seeds, 0.5, 30).unwrap();
        assert!((rho - 2.0 / 1.75).abs() < 1e-9, "K5 diverging rho={rho}");

        let k33 = Graph::new();
        for src in ["l1", "l2", "l3"] {
            for dst in ["r1", "r2", "r3"] {
                k33.add_edge(0, src, dst, NO_PROPS, None).unwrap();
            }
        }
        let seeds: Seeds = HashMap::from([("l1".to_string(), vec![(1, 1.0)])]);
        let rho = probe(&k33, &seeds, 0.2, 30).unwrap();
        assert!((rho - 0.6 / 1.08).abs() < 1e-9, "K3,3 rho={rho}");
    }

    /// `ρ` depends on the graph, not on the seeds: on K5 with a path hanging off it, a seed inside
    /// the clique and seeds at the far end of the path (two labels on one node, merged) give the
    /// same estimate. Capped at 40 sweeps: by then the transient has died out, while `dS` is still
    /// far above rounding noise (run to convergence, the last valid sweeps sit at `dS ≈ 1e-13`).
    #[test]
    fn bp_probe_seed_independent() {
        let g = Graph::new();
        add_clique(&g, &["a", "b", "c", "d", "e"]);
        for (src, dst) in [("e", "f"), ("f", "g"), ("g", "h")] {
            g.add_edge(0, src, dst, NO_PROPS, None).unwrap();
        }
        let near: Seeds = HashMap::from([("a".to_string(), vec![(1, 1.0)])]);
        let far: Seeds = HashMap::from([("h".to_string(), vec![(1, 0.5), (2, 2.0)])]);
        let rho_near = probe(&g, &near, 0.2, 40).unwrap();
        let rho_far = probe(&g, &far, 0.2, 40).unwrap();
        assert!(
            ((rho_near - rho_far) / rho_near).abs() < 1e-6,
            "near={rho_near} far={rho_far}"
        );
    }

    /// A seed whose only edge is a self-loop has no neighbours, so the probe dies out before any
    /// valid sweep and returns `None`, even at a `c` where the unreached K5 would diverge.
    #[test]
    fn bp_probe_dies_out() {
        let g = Graph::new();
        g.add_edge(0, "a", "a", NO_PROPS, None).unwrap();
        add_clique(&g, &["b", "c", "d", "e", "f"]);
        let seeds: Seeds = HashMap::from([("a".to_string(), vec![(1, 1.0)])]);
        assert_eq!(probe(&g, &seeds, 0.5, 30), None);
    }

    /// A layer view is not a trusted node list, so its index is built by the parallel scan. Its
    /// output, plain or cached, must match a graph holding only that layer's edges -- including
    /// with layer-`b`-only nodes (`x`, `y`, `z`) interleaved between layer `a`'s VIDs, and `b`
    /// edges among `a`'s nodes that must not count.
    #[test]
    fn bp_layered_view_matches_subgraph() {
        let a_edges = [
            ("p", "q"),
            ("q", "r"),
            ("r", "p"),
            ("r", "s"),
            ("s", "t"),
            ("t", "t"),
        ];
        let b_edges = [("x", "y"), ("p", "s"), ("y", "q"), ("q", "t")];
        let g = Graph::new();
        let sub = Graph::new();
        // `b_edges[0]` goes first, so the b-only nodes `x`, `y` take the lowest VIDs.
        g.add_edge(0, "x", "y", NO_PROPS, Some("b")).unwrap();
        for (i, (src, dst)) in a_edges.iter().enumerate() {
            if i == 3 {
                g.add_edge(0, "z", "x", NO_PROPS, Some("b")).unwrap();
            }
            g.add_edge(0, *src, *dst, NO_PROPS, Some("a")).unwrap();
            sub.add_edge(0, *src, *dst, NO_PROPS, None).unwrap();
        }
        for (src, dst) in &b_edges[1..] {
            g.add_edge(0, *src, *dst, NO_PROPS, Some("b")).unwrap();
        }
        let seeds: Seeds = HashMap::from([
            ("p".to_string(), vec![(1, 1.0)]),
            ("t".to_string(), vec![(2, 2.0)]),
        ]);
        let c = 0.3;

        let layer = g.layers("a").unwrap();
        let cached = layer.cache_view();
        assert!(!layer.node_list_trusted() && !cached.node_list_trusted());

        let expected = beliefs(&run(&sub, &seeds, c, EPSILON, TOL, 2, 1));
        for got in [
            run(&layer, &seeds, c, EPSILON, TOL, 2, 1),
            run(&cached, &seeds, c, EPSILON, TOL, 2, 1),
        ] {
            assert_close(&beliefs(&got), &expected, 1e-12);
        }

        let rho = probe(&sub, &seeds, c, 30).unwrap();
        for got in [probe(&layer, &seeds, c, 30), probe(&cached, &seeds, c, 30)] {
            let got = got.unwrap();
            assert!(
                ((got - rho) / rho).abs() < 1e-9,
                "rho={got}, expected {rho}"
            );
        }
    }

    /// The three things that must not change the output: the thread count, (up to the same scale)
    /// scaling all seed masses together with `epsilon` and `activation_tol`, and self-loops.
    fn check_invariances(edges: &[(u8, u8)], raw_seeds: &[(u8, usize, f64)], fraction: f64) {
        let (g, seeds, c) = build(edges, raw_seeds, fraction);
        // `top_r = 2` with up to 4 labels, so truncation is exercised too.
        let base = run(&g, &seeds, c, EPSILON, TOL, 2, 1);

        for threads in [2, 8] {
            assert_eq!(
                run(&g, &seeds, c, EPSILON, TOL, 2, threads),
                base,
                "threads = {threads}"
            );
        }

        // Doubling is exact in floating point, so every belief must double bit for bit. The
        // entropy sums are not linear in the masses and are not compared.
        let doubled: Seeds = seeds
            .iter()
            .map(|(node, pairs)| {
                let pairs = pairs.iter().map(|&(l, m)| (l, 2.0 * m)).collect();
                (node.clone(), pairs)
            })
            .collect();
        let scaled = run(&g, &doubled, c, 2.0 * EPSILON, 2.0 * TOL, 2, 1);
        for (node, state) in &base {
            let twice = &scaled[node];
            assert_eq!(twice.top_labels, state.top_labels, "node {node}");
            let values: Vec<f64> = state.top_values.iter().map(|v| 2.0 * v).collect();
            assert_eq!(twice.top_values, values, "node {node}");
            assert_eq!(twice.mass, 2.0 * state.mass, "node {node}");
        }

        // Last, since it changes `g`: a self-loop on every node.
        let names: Vec<String> = g.nodes().name().iter_values().collect();
        for name in &names {
            g.add_edge(0, name.as_str(), name.as_str(), NO_PROPS, None)
                .unwrap();
        }
        assert_eq!(run(&g, &seeds, c, EPSILON, TOL, 2, 1), base, "self-loops");
    }

    /// The output format of the top-r and the whole-run sums.
    #[test]
    fn bp_top_r_contract() {
        let c = 0.5;
        // Also checks that a huge label value comes back as given.
        let strongest = usize::MAX - 1;
        let g = Graph::new();
        // Tree: p-x-q and x-y-z.
        for (src, dst) in [("p", "x"), ("x", "q"), ("x", "y"), ("y", "z")] {
            g.add_edge(0, src, dst, NO_PROPS, None).unwrap();
        }
        // At x, by the tree formula: `strongest` 10 (its own seed); 3 and 5 tie exactly at
        // 2c = 1, one hop each by symmetry; 7 at c^2 = 0.25, two hops.
        let seeds: Seeds = HashMap::from([
            ("x".to_string(), vec![(strongest, 10.0)]),
            ("p".to_string(), vec![(5, 2.0)]),
            ("q".to_string(), vec![(3, 2.0)]),
            ("z".to_string(), vec![(7, 1.0)]),
        ]);
        let full = run(&g, &seeds, c, EPSILON, TOL, 10, 1);
        let top_2 = run(&g, &seeds, c, EPSILON, TOL, 2, 1);

        let x = &full["x"];
        assert_eq!(x.top_labels, vec![strongest, 3, 5, 7]);
        assert_eq!(
            x.top_values[1], x.top_values[2],
            "3 and 5 should tie exactly"
        );
        assert_eq!(top_2["x"].top_labels, vec![strongest, 3]);

        for (node, state) in &full {
            // Value descending, and a tie goes to the smaller label.
            for pair in state
                .top_values
                .iter()
                .zip(&state.top_labels)
                .collect::<Vec<_>>()
                .windows(2)
            {
                let ((v0, l0), (v1, l1)) = (pair[0], pair[1]);
                assert!(
                    v0 > v1 || (v0 == v1 && l0 < l1),
                    "node {node}: out of order"
                );
            }

            // Truncation keeps the head, and the sums still cover every label.
            let short = &top_2[node];
            let keep = state.top_labels.len().min(2);
            assert_eq!(short.top_labels, state.top_labels[..keep], "node {node}");
            assert_eq!(short.top_values, state.top_values[..keep], "node {node}");
            assert_eq!(short.mass, state.mass, "node {node}");
            assert_eq!(short.sum_b_ln_b, state.sum_b_ln_b, "node {node}");
            assert_eq!(short.sum_b1_ln_b1, state.sum_b1_ln_b1, "node {node}");

            // With `top_r` covering every label, the sums are the sums of the listed values.
            let values = &state.top_values;
            let close = |a: f64, b: f64| (a - b).abs() <= 1e-12 * (1.0 + b.abs());
            assert!(close(state.mass, values.iter().sum()), "node {node}: mass");
            assert!(
                close(state.sum_b_ln_b, values.iter().map(|v| v * v.ln()).sum()),
                "node {node}: sum_b_ln_b"
            );
            assert!(
                close(
                    state.sum_b1_ln_b1,
                    values.iter().map(|v| (v + 1.0) * v.ln_1p()).sum()
                ),
                "node {node}: sum_b1_ln_b1"
            );
        }
    }

    proptest! {
        /// The frontier implementation against the dense reference. Labels decouple too: the
        /// reference solves each label on its own.
        #[test]
        fn bp_matches_dense_reference((edges, raw_seeds, fraction) in bp_inputs()) {
            let (g, seeds, c) = build(&edges, &raw_seeds, fraction);
            // `top_r = 4` covers every label, and threads > 1 exercises the parallel sweep.
            let actual = beliefs(&run(&g, &seeds, c, EPSILON, TOL, 4, 4));
            assert_close(&actual, &dense_reference(&g, &seeds, c), 1e-9);
        }

        #[test]
        fn bp_invariances((edges, raw_seeds, fraction) in bp_inputs()) {
            check_invariances(&edges, &raw_seeds, fraction);
        }
    }
}
