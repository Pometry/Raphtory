#[cfg(test)]
mod random_graph_test {
    use raphtory::{
        graphgen::{preferential_attachment::ba_preferential_attachment, random_attachment::*},
        prelude::*,
    };
    use raphtory_api::core::utils::logging::global_info_logger;
    use tracing::error;
    #[test]
    fn blank_graph() {
        let graph = Graph::new();
        random_attachment(&graph, 100, 20, None);
        assert_eq!(graph.count_edges(), 2000);
        assert_eq!(graph.count_nodes(), 120);
    }

    #[test]
    fn only_nodes() {
        global_info_logger();
        let graph = Graph::new();
        for i in 0..10 {
            graph
                .add_node(i, i as u64, NO_PROPS, None, None)
                .map_err(|err| error!("{:?}", err))
                .ok();
        }

        random_attachment(&graph, 1000, 5, None);
        assert_eq!(graph.count_edges(), 5000);
        assert_eq!(graph.count_nodes(), 1010);
    }

    #[test]
    fn prior_graph() {
        let graph = Graph::new();
        ba_preferential_attachment(&graph, 300, 7, None);
        random_attachment(&graph, 4000, 12, None);
        assert_eq!(graph.count_edges(), 50106);
        assert_eq!(graph.count_nodes(), 4307);
    }
}
