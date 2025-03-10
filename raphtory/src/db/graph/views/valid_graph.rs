use crate::{
    core::Prop,
    db::api::{
        properties::internal::InheritPropertiesOps,
        storage::graph::{
            edges::{edge_ref::EdgeStorageRef, edge_storage_ops::EdgeStorageOps},
            nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps},
            tprop_storage_ops::TPropOps,
        },
        view::{
            internal::{
                CoreGraphOps, EdgeFilterOps, Immutable, InheritCoreOps, InheritLayerOps,
                InheritListOps, InheritMaterialize, InternalLayerOps, NodeFilterOps, Static,
                TimeSemantics,
            },
            Base,
        },
    },
    prelude::{EdgeViewOps, GraphViewOps, NodeViewOps},
};
use itertools::Itertools;
use raphtory_api::{
    core::{
        entities::{edges::edge_ref::EdgeRef, LayerIds, VID},
        storage::timeindex::{AsTime, TimeIndexEntry, TimeIndexOps},
        Direction,
    },
    iter::{BoxedLIter, IntoDynBoxed},
};
use rayon::prelude::*;
use std::{borrow::Cow, iter, ops::Range};

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

impl<'graph, G: GraphViewOps<'graph>> InheritCoreOps for ValidGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritLayerOps for ValidGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritListOps for ValidGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritMaterialize for ValidGraph<G> {}
impl<'graph, G: GraphViewOps<'graph>> InheritPropertiesOps for ValidGraph<G> {}

impl<'graph, G: GraphViewOps<'graph>> TimeSemantics for ValidGraph<G> {
    fn node_earliest_time(&self, v: VID) -> Option<i64> {
        let cg = self.core_graph();
        self.graph
            .node_property_history(v, None)
            .next()
            .map(|t| t.t())
            .into_iter()
            .chain(
                cg.node_edges_iter(v, Direction::BOTH, self)
                    .filter_map(|e| self.edge_earliest_time(e, self.layer_ids())),
            )
            .min()
    }

    fn node_latest_time(&self, _v: VID) -> Option<i64> {
        self.graph.latest_time_global()
    }

    fn view_start(&self) -> Option<i64> {
        self.graph.view_start()
    }

    fn view_end(&self) -> Option<i64> {
        self.graph.view_end()
    }

    fn earliest_time_global(&self) -> Option<i64> {
        self.graph
            .temporal_prop_ids()
            .filter_map(|p| self.graph.temporal_prop_iter(p).next().map(|(t, _)| t))
            .chain(
                self.core_graph()
                    .nodes_par(self, None)
                    .filter_map(|n| self.node_earliest_time(n))
                    .min(),
            )
            .min()
    }

    fn latest_time_global(&self) -> Option<i64> {
        self.graph
            .temporal_prop_ids()
            .filter_map(|p| self.graph.temporal_prop_iter(p).last().map(|(t, _)| t))
            .chain(
                self.core_graph()
                    .nodes_par(self, None)
                    .filter_map(|n| self.node_latest_time(n))
                    .max(),
            )
            .max()
    }

    fn earliest_time_window(&self, start: i64, end: i64) -> Option<i64> {
        self.graph
            .temporal_prop_ids()
            .filter_map(|p| {
                self.graph
                    .temporal_prop_iter_window(p, start, end)
                    .next()
                    .map(|(t, _)| t)
            })
            .chain(
                self.core_graph()
                    .nodes_par(self, None)
                    .filter_map(|n| self.node_earliest_time_window(n, start, end))
                    .min(),
            )
            .min()
    }

    fn latest_time_window(&self, start: i64, end: i64) -> Option<i64> {
        self.graph
            .temporal_prop_ids()
            .filter_map(|p| {
                self.graph
                    .temporal_prop_iter_window(p, start, end)
                    .last()
                    .map(|(t, _)| t)
            })
            .chain(
                self.core_graph()
                    .nodes_par(self, None)
                    .filter_map(|n| self.node_latest_time_window(n, start, end))
                    .max(),
            )
            .max()
    }

    fn node_earliest_time_window(&self, v: VID, start: i64, end: i64) -> Option<i64> {
        let cg = self.core_graph();
        let from_props = self
            .graph
            .node_property_history(v, Some(i64::MIN..end))
            .next()
            .map(|t| t.t().max(start));
        from_props
            .into_iter()
            .chain(
                cg.node_edges_iter(v, Direction::BOTH, self)
                    .filter(|e| {
                        let edge = self.core_edge(e.pid());
                        self.filter_edge(edge.as_ref(), self.layer_ids())
                            && self.include_edge_window(edge.as_ref(), start..end, self.layer_ids())
                    })
                    .filter_map(|e| {
                        self.edge_earliest_time(e, self.layer_ids())
                            .filter(|&t| t < end)
                            .map(|t| t.max(start))
                    }),
            )
            .min()
    }

    fn node_latest_time_window(&self, v: VID, start: i64, end: i64) -> Option<i64> {
        self.include_node_window(
            self.core_node_entry(v).as_ref(),
            start..end,
            self.layer_ids(),
        )
        .then_some(end)
    }

    fn include_node_window(&self, v: NodeStorageRef, w: Range<i64>, layer_ids: &LayerIds) -> bool {
        self.graph.include_node_window(v, w.clone(), layer_ids) && {
            let cg = self.core_graph();
            self.graph
                .node_property_history(v.vid(), Some(i64::MIN..w.end))
                .next()
                .is_some()
                || v.edges_iter(layer_ids, Direction::BOTH).any(|e| {
                    let edge = cg.core_edge(e.pid());
                    self.filter_edge(edge.as_ref(), self.layer_ids())
                        && self.include_edge_window(edge.as_ref(), w.clone(), layer_ids)
                })
        }
    }

    fn include_edge_window(
        &self,
        edge: EdgeStorageRef,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> bool {
        let valid_layer_ids = self.valid_layer_ids(edge, layer_ids);
        self.graph
            .edge_is_valid_at_end(edge.out_ref(), &valid_layer_ids, w.end)
            && self.graph.include_edge_window(edge, w, &valid_layer_ids)
    }

    fn node_history(&self, v: VID) -> BoxedLIter<TimeIndexEntry> {
        let cn = self.core_node_entry(v);
        let edges = self.core_edges();
        let layer_ids = self.layer_ids();
        iter::once(self.graph.node_property_history(v, None))
            .chain(
                cn.into_edges_iter(layer_ids, Direction::BOTH)
                    .filter(move |e| self.filter_edge(edges.edge(e.pid()), layer_ids))
                    .map(|e| self.edge_history(e, Cow::Borrowed(layer_ids))),
            )
            .kmerge()
            .into_dyn_boxed()
    }

    fn node_history_window(&self, v: VID, w: Range<i64>) -> BoxedLIter<TimeIndexEntry> {
        let node = self.core_node_entry(v);
        let edges = self.core_edges();
        let layer_ids = self.layer_ids();
        iter::once(self.graph.node_property_history(v, Some(w.clone())))
            .chain(
                node.into_edges_iter(layer_ids, Direction::BOTH)
                    .filter(move |e| {
                        let edge = edges.edge(e.pid());
                        self.graph.filter_edge(edge, layer_ids)
                            && self.include_edge_window(edge, w.clone(), layer_ids)
                    })
                    .map(|e| self.edge_history(e, Cow::Borrowed(layer_ids))),
            )
            .kmerge()
            .into_dyn_boxed()
    }

    fn node_property_history(&self, v: VID, w: Option<Range<i64>>) -> BoxedLIter<TimeIndexEntry> {
        self.graph.node_property_history(v, w)
    }

    fn node_edge_history(&self, v: VID, w: Option<Range<i64>>) -> BoxedLIter<TimeIndexEntry> {
        let edge_iter = self
            .core_node_entry(v)
            .into_edges_iter(self.layer_ids(), Direction::BOTH);
        let edges = self.core_edges();
        let layer_ids = self.layer_ids();
        match w {
            None => edge_iter
                .filter(move |e| self.filter_edge(edges.edge(e.pid()), layer_ids))
                .map(|e| {
                    self.edge_history(e, Cow::Borrowed(layer_ids))
                        .merge(self.edge_deletion_history(e, Cow::Borrowed(layer_ids)))
                })
                .kmerge()
                .into_dyn_boxed(),
            Some(w) => edge_iter
                .filter(|e| {
                    let edge = edges.edge(e.pid());
                    self.graph.filter_edge(edge, layer_ids)
                        && self.include_edge_window(edge, w.clone(), layer_ids)
                })
                .map(|e| {
                    self.edge_history_window(e, Cow::Borrowed(layer_ids), w.clone())
                        .merge(self.edge_deletion_history_window(
                            e,
                            w.clone(),
                            Cow::Borrowed(layer_ids),
                        ))
                })
                .kmerge()
                .into_dyn_boxed(),
        }
    }

    fn node_history_rows(
        &self,
        v: VID,
        w: Option<Range<i64>>,
    ) -> BoxedLIter<(TimeIndexEntry, Vec<(usize, Prop)>)> {
        self.graph.node_history_rows(v, w)
    }

    fn edge_history<'a>(
        &'a self,
        e: EdgeRef,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, TimeIndexEntry> {
        let layer_ids = self.valid_layer_ids(self.core_edge(e.pid()).as_ref(), &layer_ids);
        self.graph.edge_history(e, Cow::Owned(layer_ids))
    }

    fn edge_history_window<'a>(
        &'a self,
        e: EdgeRef,
        layer_ids: Cow<'a, LayerIds>,
        w: Range<i64>,
    ) -> BoxedLIter<'a, TimeIndexEntry> {
        let layer_ids =
            self.valid_layer_ids_window(self.core_edge(e.pid()).as_ref(), &layer_ids, w.clone());
        self.graph.edge_history_window(e, Cow::Owned(layer_ids), w)
    }

    fn edge_exploded_count(&self, edge: EdgeStorageRef, layer_ids: &LayerIds) -> usize {
        let layer_ids = self.valid_layer_ids(edge, &layer_ids);
        self.graph.edge_exploded_count(edge, &layer_ids)
    }

    fn edge_exploded_count_window(
        &self,
        edge: EdgeStorageRef,
        layer_ids: &LayerIds,
        w: Range<i64>,
    ) -> usize {
        let layer_ids = self.valid_layer_ids_window(edge, &layer_ids, w.clone());
        self.graph.edge_exploded_count_window(edge, &layer_ids, w)
    }

    fn edge_exploded<'a>(
        &'a self,
        e: EdgeRef,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, EdgeRef> {
        let layer_ids = self.valid_layer_ids(self.core_edge(e.pid()).as_ref(), &layer_ids);
        self.graph.edge_exploded(e, Cow::Owned(layer_ids))
    }

    fn edge_layers<'a>(
        &'a self,
        e: EdgeRef,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, EdgeRef> {
        let layer_ids = self.valid_layer_ids(self.core_edge(e.pid()).as_ref(), &layer_ids);
        self.graph.edge_layers(e, Cow::Owned(layer_ids))
    }

    fn edge_window_exploded<'a>(
        &'a self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, EdgeRef> {
        let layer_ids =
            self.valid_layer_ids_window(self.core_edge(e.pid()).as_ref(), &layer_ids, w.clone());
        self.graph.edge_window_exploded(e, w, Cow::Owned(layer_ids))
    }

    fn edge_window_layers<'a>(
        &'a self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, EdgeRef> {
        let layer_ids =
            self.valid_layer_ids_window(self.core_edge(e.pid()).as_ref(), &layer_ids, w.clone());
        self.graph.edge_window_layers(e, w, Cow::Owned(layer_ids))
    }

    fn edge_earliest_time(&self, e: EdgeRef, layer_ids: &LayerIds) -> Option<i64> {
        let layer_ids = self.valid_layer_ids(self.core_edge(e.pid()).as_ref(), layer_ids);
        self.graph.edge_earliest_time(e, &layer_ids)
    }

    fn edge_earliest_time_window(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> Option<i64> {
        let layer_ids =
            self.valid_layer_ids_window(self.core_edge(e.pid()).as_ref(), layer_ids, w.clone());
        println!("{layer_ids:?}");
        let res = self.graph.edge_earliest_time_window(e, w, &layer_ids);
        println!("edge: {res:?}");
        res
    }

    fn edge_latest_time(&self, e: EdgeRef, layer_ids: &LayerIds) -> Option<i64> {
        let layer_ids = self.valid_layer_ids(self.core_edge(e.pid()).as_ref(), layer_ids);
        self.graph.edge_latest_time(e, &layer_ids)
    }

    fn edge_latest_time_window(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> Option<i64> {
        let layer_ids =
            self.valid_layer_ids_window(self.core_edge(e.pid()).as_ref(), layer_ids, w.clone());
        self.graph.edge_latest_time_window(e, w, &layer_ids)
    }

    fn edge_deletion_history<'a>(
        &'a self,
        e: EdgeRef,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, TimeIndexEntry> {
        let layer_ids = self.valid_layer_ids(self.core_edge(e.pid()).as_ref(), &layer_ids);
        self.graph.edge_deletion_history(e, Cow::Owned(layer_ids))
    }

    fn edge_deletion_history_window<'a>(
        &'a self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, TimeIndexEntry> {
        let layer_ids =
            self.valid_layer_ids_window(self.core_edge(e.pid()).as_ref(), &layer_ids, w.clone());
        self.graph
            .edge_deletion_history_window(e, w, Cow::Owned(layer_ids))
    }

    fn edge_is_valid(&self, e: EdgeRef, layer_ids: &LayerIds) -> bool {
        self.graph.edge_is_valid(e, layer_ids)
    }

    fn edge_is_valid_at_end(&self, e: EdgeRef, layer_ids: &LayerIds, t: i64) -> bool {
        let layer_ids =
            self.valid_layer_ids_window(self.core_edge(e.pid()).as_ref(), layer_ids, i64::MIN..t);
        !layer_ids.is_none()
    }

    fn has_temporal_prop(&self, prop_id: usize) -> bool {
        self.graph.has_temporal_prop(prop_id)
    }

    fn temporal_prop_vec(&self, prop_id: usize) -> Vec<(i64, Prop)> {
        self.graph.temporal_prop_vec(prop_id)
    }

    fn has_temporal_prop_window(&self, prop_id: usize, w: Range<i64>) -> bool {
        self.graph.has_temporal_prop_window(prop_id, w)
    }

    fn temporal_prop_vec_window(&self, prop_id: usize, start: i64, end: i64) -> Vec<(i64, Prop)> {
        self.graph.temporal_prop_vec_window(prop_id, start, end)
    }

    fn temporal_node_prop_hist(&self, v: VID, id: usize) -> BoxedLIter<(TimeIndexEntry, Prop)> {
        self.graph.temporal_node_prop_hist(v, id)
    }

    fn temporal_node_prop_hist_window(
        &self,
        v: VID,
        id: usize,
        start: i64,
        end: i64,
    ) -> BoxedLIter<(TimeIndexEntry, Prop)> {
        self.graph.temporal_node_prop_hist_window(v, id, start, end)
    }

    fn temporal_edge_prop_hist_window<'a>(
        &'a self,
        e: EdgeRef,
        id: usize,
        start: i64,
        end: i64,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, (TimeIndexEntry, Prop)> {
        let layer_ids =
            self.valid_layer_ids_window(self.core_edge(e.pid()).as_ref(), &layer_ids, start..end);
        self.graph
            .temporal_edge_prop_hist_window(e, id, start, end, Cow::Owned(layer_ids))
    }

    fn temporal_edge_prop_at(
        &self,
        e: EdgeRef,
        id: usize,
        t: TimeIndexEntry,
        layer_ids: &LayerIds,
    ) -> Option<Prop> {
        let layer_ids = self.valid_layer_ids(self.core_edge(e.pid()).as_ref(), layer_ids);
        self.graph.temporal_edge_prop_at(e, id, t, &layer_ids)
    }

    fn temporal_edge_prop_hist<'a>(
        &'a self,
        e: EdgeRef,
        id: usize,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, (TimeIndexEntry, Prop)> {
        let layer_ids = self.valid_layer_ids(self.core_edge(e.pid()).as_ref(), &layer_ids);
        self.graph
            .temporal_edge_prop_hist(e, id, Cow::Owned(layer_ids))
    }

    fn constant_edge_prop(&self, e: EdgeRef, id: usize, layer_ids: &LayerIds) -> Option<Prop> {
        let layer_ids = layer_ids.constrain_from_edge(e);
        let restricted_layer_ids =
            self.valid_layer_ids(self.core_edge(e.pid()).as_ref(), &layer_ids);
        let prop = self
            .graph
            .constant_edge_prop(e, id, &restricted_layer_ids)?;
        if self.unfiltered_num_layers() > 1 && !layer_ids.is_single() {
            match restricted_layer_ids {
                LayerIds::One(id) => {
                    let name = self.get_layer_name(id);
                    Some(Prop::map([(name, prop)]))
                }
                _ => Some(prop),
            }
        } else {
            Some(prop)
        }
    }

    fn constant_edge_prop_window(
        &self,
        e: EdgeRef,
        id: usize,
        layer_ids: &LayerIds,
        w: Range<i64>,
    ) -> Option<Prop> {
        let layer_ids = layer_ids.constrain_from_edge(e);
        let restricted_layer_ids =
            self.valid_layer_ids_window(self.core_edge(e.pid()).as_ref(), &layer_ids, w.clone());
        let prop = self
            .graph
            .constant_edge_prop_window(e, id, &restricted_layer_ids, w)?;
        if self.unfiltered_num_layers() > 1 && !layer_ids.is_single() {
            match restricted_layer_ids {
                LayerIds::One(id) => {
                    let name = self.get_layer_name(id);
                    Some(Prop::map([(name, prop)]))
                }
                _ => Some(prop),
            }
        } else {
            Some(prop)
        }
    }
}

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
            && (self
                .graph
                .node_property_history(node.vid(), None)
                .next()
                .is_some()
                || (&&self.graph).node(node.vid()).map_or(false, |node| {
                    node.edges().into_iter().any(|edge| edge.is_valid())
                }))
    }
}

impl<'graph, G: GraphViewOps<'graph>> EdgeFilterOps for ValidGraph<G> {
    fn edges_filtered(&self) -> bool {
        true
    }

    fn edge_list_trusted(&self) -> bool {
        false
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
        println!("{:?}", gvm);
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
}
