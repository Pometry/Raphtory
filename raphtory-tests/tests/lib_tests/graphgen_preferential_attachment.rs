#[cfg(test)]
mod preferential_attachment_tests {
    use raphtory::prelude::*;
    use tracing::error;
    use raphtory::graphgen::preferential_attachment::*;
    use raphtory::graphgen::random_attachment::random_attachment;
    use raphtory_api::core::utils::logging::global_info_logger;
    #[test]
    fn blank_graph() {
        let graph = Graph::new();
        ba_preferential_attachment(&graph, 1000, 10, None);
        assert_eq!(graph.count_edges(), 10009);
        assert_eq!(graph.count_nodes(), 1010);
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

        ba_preferential_attachment(&graph, 1000, 5, None);
        assert_eq!(graph.count_edges(), 5009);
        assert_eq!(graph.count_nodes(), 1010);
    }

    #[test]
    fn prior_graph() {
        let graph = Graph::new();
        random_attachment(&graph, 1000, 3, None);
        ba_preferential_attachment(&graph, 500, 4, None);
        assert_eq!(graph.count_edges(), 5000);
        assert_eq!(graph.count_nodes(), 1503);
    }
}
