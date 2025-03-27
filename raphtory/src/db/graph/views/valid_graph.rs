use crate::{
    db::{
        api::{
            properties::internal::InheritPropertiesOps,
            storage::graph::{
                edges::{edge_ref::EdgeStorageRef, edge_storage_ops::EdgeStorageOps},
                nodes::node_ref::NodeStorageRef,
            },
            view::{
                internal::{
                    CoreGraphOps, EdgeFilterOps, GraphTimeSemanticsOps, Immutable, InheritCoreOps,
                    InheritEdgeHistoryFilter, InheritLayerOps, InheritListOps, InheritMaterialize,
                    InheritNodeHistoryFilter, InheritStorageOps, InheritTimeSemantics,
                    NodeFilterOps, Static,
                },
                Base,
            },
        },
        graph::{edge::EdgeView, views::layer_graph::LayeredGraph},
    },
    prelude::{EdgeViewOps, GraphViewOps},
};
use raphtory_api::core::{
    entities::{LayerIds, ELID},
    storage::timeindex::{TimeIndexEntry, TimeIndexOps},
};
use std::ops::Range;

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

    fn valid_layer_ids(&self, edge: EdgeStorageRef, layers: &LayerIds) -> LayerIds {
        match layers {
            LayerIds::None => LayerIds::None,
            _ => {
                let valid_layers: Vec<_> = edge
                    .layer_ids_iter(&layers)
                    .filter(|l| self.filter_edge(edge, &LayerIds::One(*l)))
                    .collect();
                match valid_layers.len() {
                    0 => LayerIds::None,
                    1 => LayerIds::One(valid_layers[0]),
                    _ => LayerIds::Multiple(valid_layers.into()),
                }
            }
        }
    }

    fn valid_layer_ids_window(
        &self,
        edge: EdgeStorageRef,
        layers: &LayerIds,
        w: Range<i64>,
    ) -> LayerIds {
        match layers {
            LayerIds::None => LayerIds::None,
            _ => {
                let valid_layers: Vec<_> = edge
                    .layer_ids_iter(&layers)
                    .filter(|l| {
                        self.filter_edge(edge, &LayerIds::One(*l))
                            && self.include_edge_window(edge, w.clone(), &LayerIds::One(*l))
                    })
                    .collect();
                match valid_layers.len() {
                    0 => LayerIds::None,
                    1 => LayerIds::One(valid_layers[0]),
                    _ => LayerIds::Multiple(valid_layers.into()),
                }
            }
        }
    }
}

impl<G> Static for ValidGraph<G> {}
impl<G> Immutable for ValidGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritNodeHistoryFilter for ValidGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritEdgeHistoryFilter for ValidGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritStorageOps for ValidGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritCoreOps for ValidGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritLayerOps for ValidGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritListOps for ValidGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize for ValidGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritPropertiesOps for ValidGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> InheritTimeSemantics for ValidGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> NodeFilterOps for ValidGraph<G> {
    fn nodes_filtered(&self) -> bool {
        true
    }

    fn node_list_trusted(&self) -> bool {
        false
    }

    fn edge_filter_includes_node_filter(&self) -> bool {
        self.graph.edge_filter_includes_node_filter()
    }

    fn filter_node(&self, node: NodeStorageRef, layer_ids: &LayerIds) -> bool {
        self.graph.filter_node(node, layer_ids)
            && !node
                .history(LayeredGraph::new(self, layer_ids.clone()))
                .is_empty()
    }
}

impl<'graph, G: GraphViewOps<'graph>> EdgeFilterOps for ValidGraph<G> {
    fn edges_filtered(&self) -> bool {
        true
    }

    fn edge_list_trusted(&self) -> bool {
        false
    }

    fn filter_edge_history(&self, eid: ELID, t: TimeIndexEntry, layer_ids: &LayerIds) -> bool {
        self.graph.filter_edge_history(eid, t, layer_ids)
            && EdgeView::new(
                &self.graph,
                self.core_edge(eid.edge).out_ref().at_layer(eid.layer()),
            )
            .is_valid()
    }

    fn filter_edge(&self, edge: EdgeStorageRef, layer_ids: &LayerIds) -> bool {
        self.graph.edge_is_valid(edge.out_ref(), layer_ids)
            && self.graph.filter_edge(edge, layer_ids)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        core::utils::errors::GraphError,
        db::graph::{graph::assert_graph_equal, views::deletion_graph::PersistentGraph},
        prelude::*,
        test_utils::{build_graph, build_graph_strat},
    };
    use itertools::Itertools;
    use proptest::{arbitrary::any, proptest};
    use std::ops::Range;

    #[test]
    fn test_valid_graph() -> Result<(), GraphError> {
        let g = PersistentGraph::new();
        g.add_edge(0, 0, 1, NO_PROPS, None)?;
        g.delete_edge(10, 0, 1, None)?;

        let expected_window = PersistentGraph::new();
        expected_window.add_edge(0, 0, 1, NO_PROPS, None)?;

        assert_eq!(
            g.window(0, 2).valid()?.edges().id().collect_vec(),
            [(GID::U64(0), GID::U64(1))]
        );
        assert!(g.valid()?.edges().is_empty());
        Ok(())
    }

    #[test]
    fn materialize_prop_test() {
        proptest!(|(graph_f in build_graph_strat(10, 10, true))| {
            let g = build_graph(graph_f).persistent_graph().valid().unwrap();
            let gm = g.materialize().unwrap();
            assert_graph_equal(&g, &gm);
        })
    }

    #[test]
    fn materialize_valid_window_prop_test() {
        proptest!(|(graph_f in build_graph_strat(10, 10, true), w in any::<Range<i64>>())| {
            let g = build_graph(graph_f).persistent_graph();
            let gvw = g.valid().unwrap().window(w.start, w.end);
            let gmw = gvw.materialize().unwrap();
            assert_graph_equal(&gvw, &gmw);
        })
    }

    #[test]
    fn materialize_window_valid_prop_test() {
        proptest!(|(graph_f in build_graph_strat(10, 10, true), w in any::<Range<i64>>())| {
            let g = build_graph(graph_f).persistent_graph();
            let gvw = g.window(w.start, w.end).valid().unwrap();
            let gmw = gvw.materialize().unwrap();
            assert_graph_equal(&gvw, &gmw);
        })
    }

    #[test]
    fn broken_earliest_time() {
        let g = PersistentGraph::new();
        g.add_edge(0, 0, 1, NO_PROPS, None).unwrap();
        g.delete_edge(1, 0, 1, None).unwrap();
        let gv = g.valid().unwrap();
        assert_graph_equal(&gv, &PersistentGraph::new());
        let gm = gv.materialize().unwrap();
        assert_graph_equal(&gv, &gm);
    }

    #[test]
    fn broken_earliest_time2() {
        let g = PersistentGraph::new();

        g.add_edge(10, 0, 0, NO_PROPS, None).unwrap();
        g.delete_edge(0, 0, 0, None).unwrap();

        let w = 1..11;

        let gv = g.valid().unwrap();
        assert_eq!(gv.node(0).unwrap().earliest_time(), Some(0));

        let gvw = gv.window(w.start, w.end);
        assert_eq!(gvw.node(0).unwrap().earliest_time(), Some(1));

        assert_eq!(gvw.node(0).unwrap().history(), [10]);

        let gvwm = gvw.materialize().unwrap();
        assert_eq!(gvwm.node(0).unwrap().earliest_time(), Some(1));
    }

    #[test]
    fn broken_earliest_time3() {
        let g = PersistentGraph::new();
        g.add_edge(1, 0, 0, NO_PROPS, None).unwrap();
        g.add_edge(10, 1, 0, NO_PROPS, None).unwrap();
        g.delete_edge(100, 0, 0, None).unwrap();
        let gvw = g.valid().unwrap().window(2, 20);
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
        let gv = g.valid().unwrap();
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
        let gw = g.valid().unwrap().window(0, 9);
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

        let gw = g.valid().unwrap().window(-1, 1);
        assert_eq!(
            gw.edge(0, 1)
                .unwrap()
                .properties()
                .constant()
                .get("const_test")
                .unwrap(),
            Prop::map([("_default", 2)])
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

        let gv = g.valid().unwrap().window(-1, 10);
        let gvm = gv.materialize().unwrap();
        assert_graph_equal(&gv, &gvm);
        assert_eq!(gv.node(0).unwrap().earliest_time(), Some(-1));
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

        let gv = g.valid().unwrap().window(0, 20);
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
        let gvw = g.valid().unwrap().window(0, 5);
        assert_graph_equal(&gvw, &PersistentGraph::new());
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

        let gv = g
            .window(-2925244660385668055, 7060945172792084486)
            .valid()
            .unwrap();
        assert_graph_equal(&gv, &gv.materialize().unwrap());
    }

    #[test]
    fn test_single_edge() {
        let g = PersistentGraph::new();
        g.add_edge(0, 0, 1, NO_PROPS, None).unwrap();
        let gwv = g.window(10, 11).valid().unwrap();
        let gm = gwv.materialize().unwrap();
        assert_graph_equal(&gwv, &gm);
    }
}
