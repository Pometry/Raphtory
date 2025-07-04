use crate::{
    db::{
        api::{
            properties::internal::InheritPropertiesOps,
            view::internal::{
                EdgeTimeSemanticsOps, Immutable, InheritEdgeFilterOps, InheritEdgeHistoryFilter,
                InheritExplodedEdgeFilterOps, InheritLayerOps, InheritListOps, InheritMaterialize,
                InheritNodeFilterOps, InheritNodeHistoryFilter, InheritStorageOps,
                InheritTimeSemantics, InternalEdgeLayerFilterOps, Static,
            },
        },
        graph::views::layer_graph::LayeredGraph,
    },
    prelude::GraphViewOps,
};
use raphtory_api::{core::entities::LayerIds, inherit::Base};
use raphtory_storage::{core_ops::InheritCoreGraphOps, graph::edges::edge_ref::EdgeStorageRef};

#[derive(Copy, Clone, Debug)]
pub struct ValidGraph<G> {
    graph: G,
}

impl<'graph, G: GraphViewOps<'graph>> Base for ValidGraph<G> {
    type Base = G;

    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl<'graph, G: GraphViewOps<'graph>> ValidGraph<G> {
    pub fn new(graph: G) -> Self {
        Self { graph }
    }
}

impl<G> Static for ValidGraph<G> {}
impl<G> Immutable for ValidGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritNodeHistoryFilter for ValidGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritEdgeHistoryFilter for ValidGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritStorageOps for ValidGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritCoreGraphOps for ValidGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritLayerOps for ValidGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritListOps for ValidGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize for ValidGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritPropertiesOps for ValidGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritNodeFilterOps for ValidGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritTimeSemantics for ValidGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritEdgeFilterOps for ValidGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritExplodedEdgeFilterOps for ValidGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InternalEdgeLayerFilterOps for ValidGraph<G> {
    fn internal_edge_layer_filtered(&self) -> bool {
        true
    }

    fn internal_layer_filter_edge_list_trusted(&self) -> bool {
        false
    }

    fn internal_filter_edge_layer(&self, edge: EdgeStorageRef, layer: usize) -> bool {
        let time_semantics = self.graph.edge_time_semantics();
        time_semantics.edge_is_valid(edge, LayeredGraph::new(&self.graph, LayerIds::One(layer)))
            && self.graph.internal_filter_edge_layer(edge, layer)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        db::graph::{
            graph::{assert_graph_equal, assert_persistent_materialize_graph_equal},
            views::deletion_graph::PersistentGraph,
        },
        errors::GraphError,
        prelude::*,
        test_utils::{build_graph, build_graph_strat},
    };
    use itertools::Itertools;
    use proptest::{arbitrary::any, proptest};
    use raphtory_storage::mutation::addition_ops::InternalAdditionOps;
    use std::ops::Range;

    #[test]
    fn test_valid_graph_persistent() -> Result<(), GraphError> {
        let g = PersistentGraph::new();
        g.add_edge(0, 0, 1, NO_PROPS, None)?;
        g.delete_edge(10, 0, 1, None)?;

        assert_eq!(
            g.window(0, 2).valid().edges().id().collect_vec(),
            [(GID::U64(0), GID::U64(1))]
        );
        assert!(g.valid().edges().is_empty());
        Ok(())
    }

    #[test]
    fn test_valid_graph_events() -> Result<(), GraphError> {
        let g = Graph::new();
        g.add_edge(0, 0, 1, NO_PROPS, None)?;
        g.delete_edge(10, 0, 1, None)?;

        assert_eq!(
            g.window(0, 2).valid().edges().id().collect_vec(),
            [(GID::U64(0), GID::U64(1))]
        );
        assert!(g.window(1, 20).valid().edges().is_empty()); // only deletion event in window, not valid
        assert_eq!(
            g.valid().edges().id().collect_vec(),
            [(GID::U64(0), GID::U64(1))]
        );
        Ok(())
    }

    #[test]
    fn materialize_prop_test_persistent() {
        proptest!(|(graph_f in build_graph_strat(10, 10, true))| {
            let g = PersistentGraph(build_graph(&graph_f)).valid();
            let gm = g.materialize().unwrap();
            assert_graph_equal(&g, &gm);
        })
    }

    #[test]
    fn materialize_prop_test_events() {
        proptest!(|(graph_f in build_graph_strat(10, 10, true))| {
            let g = Graph::from(build_graph(&graph_f)).valid();
            let gm = g.materialize().unwrap();
            assert_graph_equal(&g, &gm);
        })
    }

    #[test]
    fn test_single_deleted_edge_events() {
        let g = Graph::new();
        g.delete_edge(0, 0, 0, Some("a")).unwrap();
        let gv = g.valid();
        assert_eq!(gv.count_nodes(), 0);
        assert_eq!(gv.count_edges(), 0);
        assert_eq!(gv.count_temporal_edges(), 0);

        let expected = Graph::new();
        expected.resolve_layer(Some("a")).unwrap();
        assert_graph_equal(&gv, &expected);
        let gvm = gv.materialize().unwrap();
        assert_graph_equal(&gv, &gvm);
    }

    #[test]
    fn test_single_deleted_edge_persistent() {
        let g = PersistentGraph::new();
        g.delete_edge(0, 0, 0, Some("a")).unwrap();
        let gv = g.valid();
        assert_eq!(gv.count_nodes(), 0);
        assert_eq!(gv.count_edges(), 0);
        assert_eq!(gv.count_temporal_edges(), 0);

        let expected = PersistentGraph::new();
        expected.resolve_layer(Some("a")).unwrap();
        assert_graph_equal(&gv, &expected);
        let gvm = gv.materialize().unwrap();
        assert_graph_equal(&gv, &gvm);
    }

    #[test]
    fn materialize_valid_window_persistent_prop_test() {
        proptest!(|(graph_f in build_graph_strat(10, 10, true), w in any::<Range<i64>>())| {
            let g = PersistentGraph(build_graph(&graph_f));
            let gvw = g.valid().window(w.start, w.end);
            let gmw = gvw.materialize().unwrap();
            assert_persistent_materialize_graph_equal(&gvw, &gmw);
        })
    }

    #[test]
    fn test_deletions_in_window_but_edge_valid() {
        let g = PersistentGraph::new();
        g.delete_edge(0, 0, 0, None).unwrap();
        g.delete_edge(0, 0, 1, None).unwrap();
        g.add_edge(5, 0, 1, NO_PROPS, None).unwrap();
        let gvw = g.valid().window(-1, 1);
        assert_eq!(gvw.node(0).unwrap().out_degree(), 1);
    }

    #[test]
    fn materialize_valid_window_events_prop_test() {
        proptest!(|(graph_f in build_graph_strat(10, 10, true), w in any::<Range<i64>>())| {
            let g = Graph::from(build_graph(&graph_f));
            let gvw = g.valid().window(w.start, w.end);
            let gmw = gvw.materialize().unwrap();
            assert_persistent_materialize_graph_equal(&gvw, &gmw);
        })
    }

    #[test]
    fn materialize_window_valid_persistent_prop_test() {
        proptest!(|(graph_f in build_graph_strat(10, 10, true), w in any::<Range<i64>>())| {
            let g = PersistentGraph(build_graph(&graph_f));
            let gvw = g.window(w.start, w.end).valid();
            let gmw = gvw.materialize().unwrap();
            assert_persistent_materialize_graph_equal(&gvw, &gmw);
        })
    }

    #[test]
    fn materialize_window_valid_events_prop_test() {
        proptest!(|(graph_f in build_graph_strat(10, 10, true), w in any::<Range<i64>>())| {
            let g = Graph::from(build_graph(&graph_f));
            let gvw = g.window(w.start, w.end).valid();
            let gmw = gvw.materialize().unwrap();
            assert_persistent_materialize_graph_equal(&gvw, &gmw);
        })
    }

    #[test]
    fn broken_earliest_time() {
        let g = PersistentGraph::new();
        g.add_edge(0, 0, 1, NO_PROPS, None).unwrap();
        g.delete_edge(1, 0, 1, None).unwrap();
        let gv = g.valid();
        let expected = PersistentGraph::new();
        expected.resolve_layer(None).unwrap();
        assert_graph_equal(&gv, &expected);
        let gm = gv.materialize().unwrap();
        assert_graph_equal(&gv, &gm);
    }

    #[test]
    fn broken_earliest_time2() {
        let g = PersistentGraph::new();

        g.add_edge(10, 0, 0, NO_PROPS, None).unwrap();
        g.delete_edge(0, 0, 0, None).unwrap();

        let w = 1..11;

        let gv = g.valid();
        assert_eq!(gv.node(0).unwrap().earliest_time(), Some(0));

        let gvw = gv.window(w.start, w.end);
        assert_eq!(gvw.node(0).unwrap().earliest_time(), Some(10));

        assert_eq!(gvw.node(0).unwrap().history(), [10]);

        let gvwm = gvw.materialize().unwrap();
        assert_eq!(gvwm.node(0).unwrap().earliest_time(), Some(10));
    }

    #[test]
    fn broken_earliest_time3() {
        let g = PersistentGraph::new();
        g.add_edge(1, 0, 0, NO_PROPS, None).unwrap();
        g.add_edge(10, 1, 0, NO_PROPS, None).unwrap();
        g.delete_edge(100, 0, 0, None).unwrap();
        let gvw = g.valid().window(2, 20);
        assert_eq!(gvw.node(0).unwrap().earliest_time(), Some(10));
        let gvwm = gvw.materialize().unwrap();
        println!("{:?}", gvwm);
        assert_eq!(gvwm.node(0).unwrap().earliest_time(), Some(10));
    }

    #[test]
    fn missing_temporal_edge() {
        let g = PersistentGraph::new();
        g.add_edge(0, 0, 0, NO_PROPS, None).unwrap();
        g.add_edge(7658643179498972033, 0, 0, NO_PROPS, None)
            .unwrap();
        g.add_edge(781965068308597440, 0, 0, NO_PROPS, Some("b"))
            .unwrap();
        let gv = g.valid();
        assert_graph_equal(&gv, &g);
        let gvm = gv.materialize().unwrap();
        println!("{:?}", gvm);
        assert_graph_equal(&gv, &gvm);
    }

    #[test]
    fn wrong_temporal_edge_count() {
        let g = PersistentGraph::new();
        g.add_edge(10, 1, 0, NO_PROPS, None).unwrap();
        g.add_edge(0, 0, 1, NO_PROPS, None).unwrap();
        g.add_edge(1, 0, 1, NO_PROPS, None).unwrap();
        g.add_edge(2, 1, 0, NO_PROPS, None).unwrap();
        g.add_edge(3, 1, 0, NO_PROPS, Some("b")).unwrap();
        let gw = g.valid().window(0, 9);
        let gwm = gw.materialize().unwrap();
        assert_graph_equal(&gw, &gwm);
    }

    #[test]
    fn mismatched_edge_properties() {
        let g = PersistentGraph::new();
        g.add_edge(2, 1, 0, [("test", 1)], Some("b")).unwrap();
        g.add_edge(10, 1, 0, NO_PROPS, None).unwrap();
        g.add_edge(0, 0, 1, NO_PROPS, None)
            .unwrap()
            .add_constant_properties([("const_test", 2)], None)
            .unwrap();

        let gw = g.valid().window(-1, 1);
        assert_eq!(
            gw.edge(0, 1)
                .unwrap()
                .properties()
                .constant()
                .get("const_test")
                .unwrap(),
            Prop::map([("_default", 2)])
        );
        assert_eq!(
            gw.edge(0, 1)
                .unwrap()
                .default_layer()
                .properties()
                .constant()
                .get("const_test")
                .unwrap(),
            2.into()
        );
        assert_graph_equal(&gw, &gw.materialize().unwrap());
    }

    #[test]
    fn node_earliest_time() {
        let g = PersistentGraph::new();
        g.add_edge(0, 0, 1, NO_PROPS, None).unwrap();
        g.add_edge(0, 0, 2, NO_PROPS, None).unwrap();
        g.add_edge(2, 0, 2, NO_PROPS, None).unwrap();
        g.delete_edge(-10, 0, 1, None).unwrap();

        let gv = g.valid().window(-1, 10);
        let gvm = gv.materialize().unwrap();
        assert_graph_equal(&gv, &gvm);
        assert_eq!(gv.node(0).unwrap().earliest_time(), Some(0));
    }

    #[test]
    fn broken_degree() {
        let g = PersistentGraph::new();
        g.add_edge(0, 5, 4, NO_PROPS, None).unwrap();
        g.add_edge(0, 4, 9, NO_PROPS, None).unwrap();
        g.add_edge(0, 4, 6, NO_PROPS, None).unwrap();
        g.add_edge(0, 4, 6, NO_PROPS, Some("b")).unwrap();
        g.add_edge(1, 4, 9, NO_PROPS, Some("a")).unwrap();
        g.add_edge(2, 5, 4, NO_PROPS, Some("a")).unwrap();
        g.delete_edge(10, 5, 4, None).unwrap();

        let gv = g.valid().window(0, 20);
        assert!(!gv.default_layer().has_edge(5, 4));
        assert_eq!(gv.edge(5, 4).unwrap().latest_time(), Some(2));
        assert_eq!(gv.earliest_time(), Some(0));
        assert_eq!(gv.latest_time(), Some(2));
        assert_eq!(gv.node(6).unwrap().latest_time(), Some(0));
        let expected = PersistentGraph::new();
        expected.add_edge(0, 4, 9, NO_PROPS, None).unwrap();
        expected.add_edge(0, 4, 6, NO_PROPS, None).unwrap();
        expected.add_edge(0, 4, 6, NO_PROPS, Some("b")).unwrap();
        expected.add_edge(1, 4, 9, NO_PROPS, Some("a")).unwrap();
        expected.add_edge(2, 5, 4, NO_PROPS, Some("a")).unwrap();

        assert_graph_equal(&gv, &expected);

        let n4 = gv.node(4).unwrap();
        assert_eq!(n4.out_degree(), 2);
        assert_eq!(n4.in_degree(), 1);

        assert_graph_equal(&gv, &gv.materialize().unwrap());
    }

    #[test]
    fn weird_empty_graph() {
        let g = PersistentGraph::new();
        g.add_edge(10, 2, 1, NO_PROPS, Some("a")).unwrap();
        g.delete_edge(5, 2, 1, None).unwrap();
        g.add_edge(0, 2, 1, NO_PROPS, None).unwrap();
        let gvw = g.valid().window(0, 5);
        assert_eq!(gvw.count_nodes(), 0);
        let expected = PersistentGraph::new();
        expected.resolve_layer(None).unwrap();
        expected.resolve_layer(Some("a")).unwrap();
        assert_graph_equal(&gvw, &expected);
        let gvwm = gvw.materialize().unwrap();
        assert_graph_equal(&gvw, &gvwm);
    }

    #[test]
    fn mismatched_node_earliest_time_again() {
        let g = PersistentGraph::new();
        g.add_node(-2925244660385668056, 1, NO_PROPS, None).unwrap();
        g.add_edge(1116793271088085151, 2, 1, NO_PROPS, Some("a"))
            .unwrap();
        g.add_edge(0, 9, 1, NO_PROPS, None).unwrap();
        g.delete_edge(7891470373966857988, 9, 1, None).unwrap();
        g.add_edge(0, 2, 1, NO_PROPS, None).unwrap();

        let gv = g.window(-2925244660385668055, 7060945172792084486).valid();
        assert_persistent_materialize_graph_equal(&gv, &gv.materialize().unwrap());
    }

    #[test]
    fn test_single_edge() {
        let g = PersistentGraph::new();
        g.add_edge(0, 0, 1, NO_PROPS, None).unwrap();
        let gwv = g.window(10, 11).valid();
        let gm = gwv.materialize().unwrap();
        assert_persistent_materialize_graph_equal(&gwv, &gm);
    }
}
