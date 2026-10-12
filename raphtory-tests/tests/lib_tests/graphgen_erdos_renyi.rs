#[cfg(test)]
mod tests {
    use raphtory::{graphgen::erdos_renyi::erdos_renyi, prelude::*};

    #[test]
    fn test_erdos_renyi_half_probability() {
        let n_nodes = 20;
        let p = 0.5;
        let seed = Some(42);
        let graph = erdos_renyi(n_nodes, p, seed).unwrap();
        let node_count = graph.nodes().id().iter_values().count();
        let edge_count = graph.edges().into_iter().count();
        assert_eq!(node_count, n_nodes);
        assert!(edge_count > 0);
        assert!(edge_count <= n_nodes * (n_nodes - 1));
    }

    #[test]
    fn test_erdos_renyi_zero_probability() {
        let n_nodes = 20;
        let p = 0.0;
        let seed = Some(42);
        let graph = erdos_renyi(n_nodes, p, seed).unwrap();
        let edge_count = graph.edges().into_iter().count();
        let node_count = graph.nodes().id().iter_values().count();
        assert_eq!(node_count, n_nodes);
        assert_eq!(edge_count, 0);
    }

    #[test]
    fn test_erdos_renyi_full_probability() {
        let n_nodes = 20;
        let p = 1.0;
        let seed = Some(42);
        let graph = erdos_renyi(n_nodes, p, seed).unwrap();
        let edge_count = graph.edges().into_iter().count();
        let node_count = graph.nodes().id().iter_values().count();
        assert_eq!(node_count, n_nodes);
        assert_eq!(edge_count, n_nodes * (n_nodes - 1));
    }
}
