#[cfg(test)]
mod preferential_attachment_tests {
    use raphtory::{
        graphgen::{preferential_attachment::*, random_attachment::random_attachment},
        prelude::*,
    };
    use raphtory_api::core::utils::logging::global_info_logger;
    use tracing::error;
    #[test]
    fn blank_graph() {
        let graph = Graph::new();
        ba_preferential_attachment(&graph, 1000, 10, None);
        assert_eq!(graph.count_edges(), 10010);
        assert_eq!(graph.count_nodes(), 1011);
    }

    #[test]
    fn one_edge_per_step_from_an_empty_graph_or_a_single_node() {
        let graph = Graph::new();
        ba_preferential_attachment(&graph, 5, 1, None);
        assert_eq!(graph.count_nodes(), 7);
        assert_eq!(graph.count_edges(), 6);

        let graph = Graph::new();
        graph.add_node(0, 0u64, NO_PROPS, None, None).unwrap();
        ba_preferential_attachment(&graph, 5, 1, None);
        assert_eq!(graph.count_nodes(), 7);
        assert_eq!(graph.count_edges(), 6);
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
