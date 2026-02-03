
#[cfg(test)]
mod dominating_set_tests {
    use raphtory::{
        algorithms::covering::{dominating_set::{is_dominating_set, lazy_greedy_dominating_set}, fast_distributed_dominating_set::fast_distributed_dominating_set},
        graphgen::erdos_renyi::erdos_renyi,
        db::{api::{view::StaticGraphViewOps}, graph::graph::Graph},
        prelude::*,
    };

    fn graph() -> Graph {
        let nodes_to_add = 1000;
        let p = 0.5;
        let seed = 42;
        let g = erdos_renyi(nodes_to_add, p, Some(seed)).unwrap();
        g
    }

    #[test]
    fn test_lazy_greedy_dominating_set() {
        let g = graph();
        let n_nodes = g.count_nodes();
        let dominating_set = lazy_greedy_dominating_set(&g);
        assert!(is_dominating_set(&g, &dominating_set));
        assert!(dominating_set.len() <= (f64::ln(n_nodes as f64) as usize + 2))
    }

    #[test]
    fn test_fast_distributed_dominating_set() {
        let g = graph();
        let n_nodes = g.count_nodes();  
        let dominating_set = fast_distributed_dominating_set(&g);
        assert!(is_dominating_set(&g, &dominating_set));
        assert!(dominating_set.len() <= (6 * (f64::ln(n_nodes as f64) as usize) + 12))
    }
}