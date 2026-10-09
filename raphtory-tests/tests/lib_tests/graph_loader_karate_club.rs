#[cfg(test)]
mod karate_test {
    use raphtory::{graph_loader::karate_club::*, prelude::*};

    #[test]
    fn test_graph_sizes() {
        let g = karate_club_graph();
        assert_eq!(g.count_nodes(), 34);
        assert_eq!(g.count_edges(), 155);
    }
}
