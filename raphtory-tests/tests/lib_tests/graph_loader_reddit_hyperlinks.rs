#[cfg(test)]
mod reddit_test {
    use raphtory::{
        db::api::view::*,
        graph_loader::reddit_hyperlinks::{reddit_file, reddit_graph},
    };

    #[test]
    fn check_data() {
        let file = reddit_file(100, Some(true));
        assert!(file.is_ok());
    }

    #[test]
    fn check_graph() {
        let graph = reddit_graph(100, true);
        assert_eq!(graph.count_nodes(), 16);
        assert_eq!(graph.count_edges(), 9);
    }
}
