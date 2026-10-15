#[cfg(test)]
mod test {
    use raphtory::{algorithms::centrality::degree_centrality::degree_centrality, prelude::*};

    #[test]
    fn test_empty_edges() {
        let g = Graph::new();
        for i in 0..10 {
            g.add_node(0, i, NO_PROPS, None, None).unwrap();
        }
        let c = degree_centrality(&g);
        assert_eq!(c.values_to_rows(), vec![0.0; 10])
    }
}
