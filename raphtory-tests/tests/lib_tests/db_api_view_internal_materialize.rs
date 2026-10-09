#[cfg(test)]
mod test_materialised_graph_dispatch {
    use raphtory::{
        core::entities::LayerIds,
        db::api::view::internal::{
            GraphTimeSemanticsOps, InternalEdgeFilterOps, InternalLayerOps, MaterializedGraph,
        },
        prelude::*,
    };
    use raphtory_api::core::entities::GID;
    use raphtory_storage::core_ops::CoreGraphOps;

    #[test]
    fn materialised_graph_has_core_ops() {
        let mg = MaterializedGraph::from(Graph::new());
        assert_eq!(mg.unfiltered_num_nodes(&LayerIds::All), 0);
    }

    #[test]
    fn materialised_graph_has_graph_ops() {
        let mg = MaterializedGraph::from(Graph::new());
        assert_eq!(mg.count_nodes(), 0);
    }

    #[test]
    fn materialised_graph_has_edge_filter_ops() {
        let mg = MaterializedGraph::from(Graph::new());
        assert!(!mg.internal_edge_filtered());
    }

    #[test]
    fn materialised_graph_has_layer_ops() {
        let mg = MaterializedGraph::from(Graph::new());
        assert!(matches!(mg.layer_ids(), LayerIds::All));
    }

    #[test]
    fn materialised_graph_has_time_semantics() {
        let mg = MaterializedGraph::from(Graph::new());
        assert!(mg.view_start().is_none());
    }

    #[test]
    fn materialised_graph_can_be_used_directly() {
        let g = Graph::new();

        let mg = g.materialize().unwrap();

        let v = mg.add_node(0, 1, NO_PROPS, None, None).unwrap();
        assert_eq!(v.id(), GID::U64(1))
    }
}
