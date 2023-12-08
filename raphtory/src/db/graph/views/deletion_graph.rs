use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, nodes::node_store::NodeStore, LayerIds, VID},
        storage::timeindex::{AsTime, TimeIndexEntry, TimeIndexOps},
        utils::errors::GraphError,
        Direction, Prop,
    },
    db::{
        api::{
            mutation::internal::InheritMutationOps,
            properties::internal::InheritPropertiesOps,
            view::{internal::*, BoxedIter},
        },
        graph::graph::{graph_equal, InternalGraph},
    },
    prelude::*,
};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use std::{
    cmp::min,
    fmt::{Display, Formatter},
    iter,
    ops::{Deref, Range},
    path::Path,
    sync::Arc,
};

/// A graph view where an edge remains active from the time it is added until it is explicitly marked as deleted.
///
/// Note that the graph will give you access to all edges that were added at any point in time, even those that are marked as deleted.
/// The deletion only has an effect on the exploded edge view that are returned. An edge is included in a windowed view of the graph if
/// it is considered active at any point in the window.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GraphWithDeletions {
    graph: Arc<InternalGraph>,
}

impl Static for GraphWithDeletions {}

impl From<InternalGraph> for GraphWithDeletions {
    fn from(value: InternalGraph) -> Self {
        Self {
            graph: Arc::new(value),
        }
    }
}

impl IntoDynamic for GraphWithDeletions {
    fn into_dynamic(self) -> DynamicGraph {
        DynamicGraph::new(self)
    }
}

impl Display for GraphWithDeletions {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.graph, f)
    }
}

fn edge_alive_at(e: &dyn EdgeLike, t: i64, layer_ids: &LayerIds) -> bool {
    let range = i64::MIN..t.saturating_add(1);
    let (first_addition, first_deletion, last_addition_before_start, last_deletion_before_start) =
        match layer_ids {
            LayerIds::None => return false,
            LayerIds::All => (
                e.additions_iter().flat_map(|v| v.first()).min(),
                e.deletions_iter().flat_map(|v| v.first()).min(),
                e.additions_iter()
                    .flat_map(|v| v.range(range.clone()).last())
                    .max(),
                e.deletions_iter()
                    .flat_map(|v| v.range(range.clone()).last())
                    .max(),
            ),
            LayerIds::One(l_id) => (
                e.additions(*l_id).and_then(|v| v.first()),
                e.deletions(*l_id).and_then(|v| v.first()),
                e.additions(*l_id)
                    .and_then(|v| v.range(range.clone()).last()),
                e.deletions(*l_id)
                    .and_then(|v| v.range(range.clone()).last()),
            ),
            LayerIds::Multiple(ids) => (
                ids.iter()
                    .flat_map(|l_id| e.additions(*l_id).and_then(|v| v.first()))
                    .min(),
                ids.iter()
                    .flat_map(|l_id| e.deletions(*l_id).and_then(|v| v.first()))
                    .min(),
                ids.iter()
                    .flat_map(|l_id| {
                        e.additions(*l_id)
                            .and_then(|v| v.range(range.clone()).last())
                    })
                    .max(),
                ids.iter()
                    .flat_map(|l_id| {
                        e.deletions(*l_id)
                            .and_then(|v| v.range(range.clone()).last())
                    })
                    .max(),
            ),
        };

    // None is less than any value (see test below)
    (first_deletion < first_addition
        && first_deletion
            .filter(|&v| v >= TimeIndexEntry::start(t))
            .is_some())
        || last_addition_before_start > last_deletion_before_start
}

static WINDOW_FILTER: Lazy<EdgeWindowFilter> = Lazy::new(|| {
    Arc::new(|e, layer_ids, w| {
        e.active(layer_ids, w.clone()) || edge_alive_at(e, w.start, layer_ids)
    })
});

impl GraphWithDeletions {
    fn node_alive_at(
        &self,
        v: &NodeStore,
        t: i64,
        layers: &LayerIds,
        edge_filter: Option<&EdgeFilter>,
    ) -> bool {
        let edges = self.graph.inner().storage.edges.read_lock();
        v.edge_tuples(layers, Direction::BOTH)
            .map(|eref| edges.get(eref.pid().into()))
            .find(|&e| {
                edge_filter.map(|f| f(e, layers)).unwrap_or(true) && edge_alive_at(e, t, layers)
            })
            .is_some()
    }

    pub fn new() -> Self {
        Self {
            graph: Arc::new(InternalGraph::default()),
        }
    }

    /// Save a graph to a directory
    ///
    /// # Arguments
    ///
    /// * `path` - The path to the directory
    ///
    /// Returns:
    ///
    /// A raphtory graph
    ///
    /// # Example
    ///
    /// ```no_run
    /// use std::fs::File;
    /// use raphtory::prelude::*;
    /// let g = Graph::new();
    /// g.add_node(1, 1, NO_PROPS).unwrap();
    /// g.save_to_file("path_str").expect("failed to save file");
    /// ```
    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), GraphError> {
        MaterializedGraph::from(self.clone()).save_to_file(path)
    }

    /// Load a graph from a directory
    ///
    /// # Arguments
    ///
    /// * `path` - The path to the directory
    ///
    /// Returns:
    ///
    /// A raphtory graph
    ///
    /// # Example
    ///
    /// ```no_run
    /// use raphtory::prelude::*;
    /// let g = Graph::load_from_file("path/to/graph");
    /// ```
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self, GraphError> {
        let g = MaterializedGraph::load_from_file(path)?;
        g.into_persistent().ok_or(GraphError::GraphLoadError)
    }
}

impl<'graph, G: GraphViewOps<'graph>> PartialEq<G> for GraphWithDeletions {
    fn eq(&self, other: &G) -> bool {
        graph_equal(self, other)
    }
}

impl Base for GraphWithDeletions {
    type Base = InternalGraph;
    #[inline(always)]
    fn base(&self) -> &Self::Base {
        &self.graph
    }
}

impl InternalMaterialize for GraphWithDeletions {
    fn new_base_graph(&self, graph: InternalGraph) -> MaterializedGraph {
        MaterializedGraph::PersistentGraph(GraphWithDeletions {
            graph: Arc::new(graph),
        })
    }

    fn include_deletions(&self) -> bool {
        true
    }
}

impl InheritMutationOps for GraphWithDeletions {}

impl InheritCoreOps for GraphWithDeletions {}

impl InheritCoreDeletionOps for GraphWithDeletions {}

impl InheritGraphOps for GraphWithDeletions {}

impl InheritPropertiesOps for GraphWithDeletions {}

impl InheritLayerOps for GraphWithDeletions {}

impl InheritEdgeFilterOps for GraphWithDeletions {}

impl TimeSemantics for GraphWithDeletions {
    fn view_start(&self) -> Option<i64> {
        self.graph.view_start()
    }

    fn view_end(&self) -> Option<i64> {
        self.graph.view_end()
    }

    fn earliest_time_global(&self) -> Option<i64> {
        self.graph.earliest_time_global()
    }

    fn latest_time_global(&self) -> Option<i64> {
        self.graph.latest_time_global()
    }

    fn earliest_time_window(&self, start: i64, end: i64) -> Option<i64> {
        self.graph.earliest_time_window(start, end)
    }

    fn latest_time_window(&self, start: i64, end: i64) -> Option<i64> {
        self.graph.latest_time_window(start, end)
    }

    fn include_node_window(
        &self,
        v: VID,
        w: Range<i64>,
        _layer_ids: &LayerIds,
        _edge_filter: Option<&EdgeFilter>,
    ) -> bool {
        // FIXME: Think about node deletions
        let v = self.graph.inner().storage.get_node(v);
        v.timestamps().first_t().filter(|&t| t <= w.end).is_some()
    }

    fn node_earliest_time(&self, v: VID) -> Option<i64> {
        self.graph.node_earliest_time(v)
    }

    fn node_latest_time(&self, _v: VID) -> Option<i64> {
        Some(i64::MAX)
    }

    fn node_earliest_time_window(&self, v: VID, start: i64, end: i64) -> Option<i64> {
        let v = self.core_node(v);
        if v.timestamps().first_t()? <= start {
            Some(v.timestamps().range(start..end).first_t().unwrap_or(start))
        } else {
            None
        }
    }

    fn node_latest_time_window(&self, v: VID, _start: i64, end: i64) -> Option<i64> {
        let v = self.core_node(v);
        if v.timestamps().first_t()? < end {
            Some(end - 1)
        } else {
            None
        }
    }

    fn include_edge_window(&self) -> &EdgeWindowFilter {
        // includes edge if it is alive at the start of the window or added during the window
        &WINDOW_FILTER
    }

    fn node_history(&self, v: VID) -> Vec<i64> {
        self.graph.node_history(v)
    }

    fn node_history_window(&self, v: VID, w: Range<i64>) -> Vec<i64> {
        self.graph.node_history_window(v, w)
    }

    fn edge_exploded(&self, e: EdgeRef, layer_ids: LayerIds) -> BoxedIter<EdgeRef> {
        //Fixme: Need support for duration on exploded edges
        if edge_alive_at(self.core_edge(e.pid()).deref(), i64::MIN, &layer_ids) {
            Box::new(
                iter::once(e.at(i64::MIN.into())).chain(self.graph.edge_window_exploded(
                    e,
                    (i64::MIN + 1)..i64::MAX,
                    layer_ids,
                )),
            )
        } else {
            self.graph.edge_exploded(e, layer_ids)
        }
    }

    fn edge_layers(&self, e: EdgeRef, layer_ids: LayerIds) -> BoxedIter<EdgeRef> {
        self.graph.edge_layers(e, layer_ids)
    }

    fn edge_window_exploded(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: LayerIds,
    ) -> BoxedIter<EdgeRef> {
        // FIXME: Need better iterators on LockedView that capture the guard
        let entry = self.core_edge(e.pid());
        if edge_alive_at(entry.deref(), w.start, &layer_ids) {
            Box::new(
                iter::once(e.at(w.start.into())).chain(self.graph.edge_window_exploded(
                    e,
                    w.start.saturating_add(1)..w.end,
                    layer_ids,
                )),
            )
        } else {
            self.graph.edge_window_exploded(e, w, layer_ids)
        }
    }

    fn edge_window_layers(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: LayerIds,
    ) -> BoxedIter<EdgeRef> {
        let g = self.clone();
        let window_filter = self.include_edge_window().clone();
        Box::new(
            self.graph
                .edge_layers(e, layer_ids.clone())
                .filter(move |&e| {
                    let entry = g.core_edge(e.pid());
                    window_filter(
                        entry.deref(),
                        &layer_ids.clone().constrain_from_edge(e),
                        w.clone(),
                    )
                }),
        )
    }

    fn edge_earliest_time(&self, e: EdgeRef, layer_ids: LayerIds) -> Option<i64> {
        e.time().map(|ti| *ti.t()).or_else(|| {
            let entry = self.core_edge(e.pid());
            if edge_alive_at(entry.deref(), i64::MIN, &layer_ids) {
                Some(i64::MIN)
            } else {
                self.edge_additions(e, layer_ids).first().map(|ti| *ti.t())
            }
        })
    }

    fn edge_earliest_time_window(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: LayerIds,
    ) -> Option<i64> {
        let entry = self.core_edge(e.pid());
        if edge_alive_at(entry.deref(), w.start, &layer_ids) {
            Some(w.start)
        } else {
            self.edge_additions(e, layer_ids).range(w).first_t()
        }
    }

    fn edge_latest_time(&self, e: EdgeRef, layer_ids: LayerIds) -> Option<i64> {
        match e.time().map(|ti| *ti.t()) {
            Some(t) => Some(min(
                self.edge_additions(e, layer_ids.clone())
                    .range(t.saturating_add(1)..i64::MAX)
                    .first_t()
                    .unwrap_or(i64::MAX),
                self.edge_deletions(e, layer_ids)
                    .range(t.saturating_add(1)..i64::MAX)
                    .first_t()
                    .unwrap_or(i64::MAX),
            )),
            None => {
                let entry = self.core_edge(e.pid());
                if edge_alive_at(entry.deref(), i64::MAX, &layer_ids) {
                    Some(i64::MAX)
                } else {
                    self.edge_deletions(e, layer_ids).last_t()
                }
            }
        }
    }

    fn edge_latest_time_window(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: LayerIds,
    ) -> Option<i64> {
        match e.time().map(|ti| *ti.t()) {
            Some(t) => Some(min(
                self.edge_additions(e, layer_ids.clone())
                    .range(t.saturating_add(1)..w.end)
                    .first_t()
                    .unwrap_or(w.end - 1),
                self.edge_deletions(e, layer_ids)
                    .range(t.saturating_add(1)..w.end)
                    .first_t()
                    .unwrap_or(w.end - 1),
            )),
            None => {
                let entry = self.core_edge(e.pid());
                if edge_alive_at(entry.deref(), w.end - 1, &layer_ids) {
                    Some(w.end - 1)
                } else {
                    self.edge_deletions(e, layer_ids).range(w).last_t()
                }
            }
        }
    }

    fn edge_deletion_history(&self, e: EdgeRef, layer_ids: LayerIds) -> Vec<i64> {
        self.edge_deletions(e, layer_ids)
            .iter_t()
            .copied()
            .collect()
    }

    fn edge_deletion_history_window(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: LayerIds,
    ) -> Vec<i64> {
        self.edge_deletions(e, layer_ids)
            .range(w)
            .iter_t()
            .copied()
            .collect()
    }

    #[inline]
    fn has_temporal_prop(&self, prop_id: usize) -> bool {
        self.graph.has_temporal_prop(prop_id)
    }

    fn temporal_prop_vec(&self, prop_id: usize) -> Vec<(i64, Prop)> {
        self.graph.temporal_prop_vec(prop_id)
    }

    #[inline]
    fn has_temporal_prop_window(&self, prop_id: usize, w: Range<i64>) -> bool {
        self.graph.has_temporal_prop_window(prop_id, w)
    }

    fn temporal_prop_vec_window(&self, prop_id: usize, start: i64, end: i64) -> Vec<(i64, Prop)> {
        self.graph.temporal_prop_vec_window(prop_id, start, end)
    }

    #[inline]
    fn has_temporal_node_prop(&self, v: VID, prop_id: usize) -> bool {
        self.graph.has_temporal_node_prop(v, prop_id)
    }

    fn temporal_node_prop_vec(&self, v: VID, prop_id: usize) -> Vec<(i64, Prop)> {
        self.graph.temporal_node_prop_vec(v, prop_id)
    }

    fn has_temporal_node_prop_window(&self, v: VID, prop_id: usize, w: Range<i64>) -> bool {
        self.graph
            .has_temporal_node_prop_window(v, prop_id, i64::MIN..w.end)
    }

    fn temporal_node_prop_vec_window(
        &self,
        v: VID,
        prop_id: usize,
        start: i64,
        end: i64,
    ) -> Vec<(i64, Prop)> {
        let prop = self.temporal_node_prop(v, prop_id);
        match prop {
            Some(p) => p
                .last_before(start.saturating_add(1))
                .into_iter()
                .map(|(_, v)| (start, v))
                .chain(p.iter_window_t(start.saturating_add(1)..end))
                .collect(),
            None => Default::default(),
        }
    }

    fn has_temporal_edge_prop_window(
        &self,
        e: EdgeRef,
        prop_id: usize,
        w: Range<i64>,
        layer_ids: LayerIds,
    ) -> bool {
        let entry = self.core_edge(e.pid());

        if entry.has_temporal_prop(&layer_ids, prop_id) {
            // if property was added at any point since the last deletion, it is still there,
            // if deleted at the start of the window, we still need to check for any additions
            // that happened at the same time
            let search_start = min(
                entry
                    .last_deletion_before(&layer_ids, w.start.saturating_add(1))
                    .unwrap_or(TimeIndexEntry::MIN),
                TimeIndexEntry::start(w.start),
            );
            let search_end = TimeIndexEntry::start(w.end);
            match layer_ids {
                LayerIds::None => false,
                LayerIds::All => entry.layer_ids_iter().any(|id| {
                    entry
                        .temporal_prop_layer(id, prop_id)
                        .filter(|prop| prop.iter_window(search_start..search_end).next().is_some())
                        .is_some()
                }),
                LayerIds::One(id) => entry
                    .temporal_prop_layer(id, prop_id)
                    .filter(|prop| prop.iter_window(search_start..search_end).next().is_some())
                    .is_some(),
                LayerIds::Multiple(ids) => ids.iter().any(|&id| {
                    entry
                        .temporal_prop_layer(id, prop_id)
                        .filter(|prop| prop.iter_window(search_start..search_end).next().is_some())
                        .is_some()
                }),
            }
        } else {
            false
        }
    }

    fn temporal_edge_prop_vec_window(
        &self,
        e: EdgeRef,
        prop_id: usize,
        start: i64,
        end: i64,
        layer_ids: LayerIds,
    ) -> Vec<(i64, Prop)> {
        let prop = self.temporal_edge_prop(e, prop_id, layer_ids.clone());
        match prop {
            Some(p) => {
                let entry = self.core_edge(e.pid());
                if edge_alive_at(entry.deref(), start, &layer_ids) {
                    p.last_before(start.saturating_add(1))
                        .into_iter()
                        .map(|(_, v)| (start, v))
                        .chain(p.iter_window(start.saturating_add(1)..end))
                        .collect()
                } else {
                    p.iter_window(start..end).collect()
                }
            }
            None => Default::default(),
        }
    }

    fn has_temporal_edge_prop(&self, e: EdgeRef, prop_id: usize, layer_ids: LayerIds) -> bool {
        self.graph.has_temporal_edge_prop(e, prop_id, layer_ids)
    }

    fn temporal_edge_prop_vec(
        &self,
        e: EdgeRef,
        prop_id: usize,
        layer_ids: LayerIds,
    ) -> Vec<(i64, Prop)> {
        self.graph.temporal_edge_prop_vec(e, prop_id, layer_ids)
    }
}

#[cfg(test)]
mod test_deletions {
    use crate::{db::graph::views::deletion_graph::GraphWithDeletions, prelude::*};
    use itertools::Itertools;

    #[test]
    fn test_nodes() {
        let g = GraphWithDeletions::new();

        g.add_edge(0, 1, 2, [("added", Prop::I64(0))], Some("assigned"))
            .unwrap();
        g.add_edge(1, 1, 3, [("added", Prop::I64(0))], Some("assigned"))
            .unwrap();
        g.add_edge(2, 4, 2, [("added", Prop::I64(0))], Some("has"))
            .unwrap();
        g.add_edge(3, 4, 2, [("added", Prop::I64(0))], Some("has"))
            .unwrap();
        g.add_edge(4, 5, 2, [("added", Prop::I64(0))], Some("blocks"))
            .unwrap();
        g.add_edge(5, 4, 5, [("added", Prop::I64(0))], Some("has"))
            .unwrap();
        g.add_edge(6, 6, 5, [("added", Prop::I64(0))], Some("assigned"))
            .unwrap();

        let nodes = g
            .window(0, 1701786285758)
            .layer(vec!["assigned", "has", "blocks"])
            .unwrap()
            .nodes()
            .into_iter()
            .map(|vv| vv.name())
            .collect_vec();

        assert_eq!(nodes, vec!["1", "2", "3", "4", "5", "6"]);

        let nodes = g
            .at(1701786285758)
            .layer(vec!["assigned", "has", "blocks"])
            .unwrap()
            .nodes()
            .into_iter()
            .map(|vv| vv.name())
            .collect_vec();

        assert_eq!(nodes, vec!["1", "2", "3", "4", "5", "6"]);
    }

    #[test]
    fn test_edge_deletions() {
        let g = GraphWithDeletions::new();

        g.add_edge(0, 0, 1, [("added", Prop::I64(0))], None)
            .unwrap();
        g.delete_edge(10, 0, 1, None).unwrap();

        assert_eq!(g.edges().id().collect::<Vec<_>>(), vec![(0, 1)]);

        assert_eq!(
            g.window(1, 2).edges().id().collect::<Vec<_>>(),
            vec![(0, 1)]
        );

        assert_eq!(g.window(1, 2).count_edges(), 1);

        assert_eq!(g.window(11, 12).count_edges(), 0);

        assert_eq!(
            g.window(1, 2)
                .edge(0, 1)
                .unwrap()
                .properties()
                .get("added")
                .unwrap_i64(),
            0
        );

        assert!(g.window(11, 12).edge(0, 1).is_none());

        assert_eq!(
            g.window(1, 2)
                .edge(0, 1)
                .unwrap()
                .properties()
                .temporal()
                .get("added")
                .unwrap()
                .iter()
                .collect_vec(),
            vec![(1, Prop::I64(0))]
        );

        assert_eq!(g.window(1, 2).node(0).unwrap().out_degree(), 1)
    }

    #[test]
    fn test_window_semantics() {
        let g = GraphWithDeletions::new();
        g.add_edge(1, 1, 2, [("test", "test")], None).unwrap();
        g.delete_edge(10, 1, 2, None).unwrap();

        assert_eq!(g.count_edges(), 1);

        assert_eq!(g.at(12).count_edges(), 0);
        assert_eq!(g.at(11).count_edges(), 0);
        assert_eq!(g.at(10).count_edges(), 0);
        assert_eq!(g.at(9).count_edges(), 1);
        assert_eq!(g.window(5, 9).count_edges(), 1);
        assert_eq!(g.window(5, 10).count_edges(), 1);
        assert_eq!(g.window(5, 11).count_edges(), 1);
        assert_eq!(g.window(10, 12).count_edges(), 0);
        assert_eq!(g.before(10).count_edges(), 1);
        assert_eq!(g.after(10).count_edges(), 0);
    }

    #[test]
    fn test_timestamps() {
        let g = GraphWithDeletions::new();
        let e = g.add_edge(1, 1, 2, [("test", "test")], None).unwrap();
        assert_eq!(e.earliest_time().unwrap(), 1);
        assert_eq!(e.latest_time(), Some(i64::MAX));
        g.delete_edge(10, 1, 2, None).unwrap();
        assert_eq!(e.latest_time().unwrap(), 10);

        g.delete_edge(10, 3, 4, None).unwrap();
        let e = g.edge(3, 4).unwrap();
        assert_eq!(e.earliest_time(), None);
        assert_eq!(e.latest_time().unwrap(), 10);
        g.add_edge(1, 3, 4, [("test", "test")], None).unwrap();
        assert_eq!(e.latest_time().unwrap(), 10);
        assert_eq!(e.earliest_time().unwrap(), 1);
    }

    #[test]
    fn test_materialize_only_deletion() {
        let g = GraphWithDeletions::new();
        g.delete_edge(1, 1, 2, None).unwrap();

        assert_eq!(g.materialize().unwrap().into_persistent().unwrap(), g);
    }

    #[test]
    fn test_materialize_window() {
        let g = GraphWithDeletions::new();
        g.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        g.delete_edge(10, 1, 2, None).unwrap();

        let gm = g
            .window(3, 5)
            .materialize()
            .unwrap()
            .into_persistent()
            .unwrap();
        assert_eq!(gm, g.window(3, 5))
    }

    #[test]
    fn test_exploded_latest_time() {
        let g = GraphWithDeletions::new();
        let e = g.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        g.delete_edge(10, 1, 2, None).unwrap();
        assert_eq!(e.latest_time(), Some(10));
        assert_eq!(e.explode().latest_time().collect_vec(), vec![Some(10)]);
    }

    #[test]
    fn test_edge_properties() {
        let g = GraphWithDeletions::new();
        let e = g.add_edge(0, 1, 2, [("test", "test")], None).unwrap();
        assert_eq!(e.properties().get("test").unwrap_str(), "test");
        e.delete(10, None).unwrap();
        assert_eq!(e.properties().get("test").unwrap_str(), "test");
        assert_eq!(e.at(10).properties().get("test"), None);
        e.add_updates(11, [("test", "test11")], None).unwrap();
        assert_eq!(
            e.window(10, 12).properties().get("test").unwrap_str(),
            "test11"
        );
        assert_eq!(
            e.window(5, 12)
                .properties()
                .temporal()
                .get("test")
                .unwrap()
                .iter()
                .collect_vec(),
            vec![(5, Prop::str("test")), (11i64, Prop::str("test11"))],
        );
    }

    #[test]
    fn test_ordering_of_addition_and_deletion() {
        let g = GraphWithDeletions::new();

        //deletion before addition (edge exists from (-inf,1) and (1, inf)
        g.delete_edge(1, 1, 2, None).unwrap();
        let e_1_2 = g.add_edge(1, 1, 2, [("test", "test")], None).unwrap();

        //deletion after addition (edge exists only at 2)
        let e_3_4 = g.add_edge(2, 3, 4, [("test", "test")], None).unwrap();
        g.delete_edge(2, 3, 4, None).unwrap();

        assert_eq!(e_1_2.at(0).properties().get("test"), None);
        assert_eq!(e_1_2.at(1).properties().get("test").unwrap_str(), "test");
        assert_eq!(e_1_2.at(2).properties().get("test").unwrap_str(), "test");

        assert_eq!(e_3_4.at(0).properties().get("test"), None);
        assert_eq!(e_3_4.at(2).properties().get("test").unwrap_str(), "test");
        assert_eq!(e_3_4.at(3).properties().get("test"), None);

        assert!(g.window(0, 1).has_edge(1, 2, Layer::Default));
        assert!(!g.window(0, 2).has_edge(3, 4, Layer::Default));
        assert!(g.window(1, 2).has_edge(1, 2, Layer::Default));
        assert!(g.window(2, 3).has_edge(3, 4, Layer::Default));
        assert!(!g.window(3, 4).has_edge(3, 4, Layer::Default));
    }

    #[test]
    fn test_edge_latest_time() {
        let g = GraphWithDeletions::new();
        let e = g.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        e.delete(2, None).unwrap();
        assert_eq!(e.latest_time(), Some(2));
        e.add_updates(4, NO_PROPS, None).unwrap();
        assert_eq!(e.latest_time(), Some(i64::MAX));

        assert_eq!(e.window(0, 3).latest_time(), Some(2));
    }

    #[test]
    fn test_view_start_end() {
        let g = GraphWithDeletions::new();
        let e = g.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        assert_eq!(g.start(), Some(0));
        assert_eq!(g.end(), Some(1));
        e.delete(2, None).unwrap();
        assert_eq!(g.start(), Some(0));
        assert_eq!(g.end(), Some(3));
        let w = g.window(g.start().unwrap(), g.end().unwrap());
        assert!(g.has_edge(1, 2, Layer::All));
        assert!(w.has_edge(1, 2, Layer::All));
        assert_eq!(w.start(), Some(0));
        assert_eq!(w.end(), Some(3));

        e.add_updates(4, NO_PROPS, None).unwrap();
        assert_eq!(g.start(), Some(0));
        assert_eq!(g.end(), Some(5));
    }

    #[test]
    fn test_node_property_semantics() {
        let g = GraphWithDeletions::new();
        let _v = g.add_node(1, 1, [("test_prop", "test value")]).unwrap();
        let v = g.add_node(11, 1, [("test_prop", "test value 2")]).unwrap();
        let v_from_graph = g.at(10).node(1).unwrap();
        assert_eq!(v.properties().get("test_prop").unwrap_str(), "test value 2");
        assert_eq!(
            v.at(10).properties().get("test_prop").unwrap_str(),
            "test value"
        );
        assert_eq!(
            v.at(11).properties().get("test_prop").unwrap_str(),
            "test value 2"
        );
        assert_eq!(
            v_from_graph.properties().get("test_prop").unwrap_str(),
            "test value"
        );

        assert_eq!(
            v.before(11).properties().get("test_prop").unwrap_str(),
            "test value"
        );

        assert_eq!(
            v.properties()
                .temporal()
                .get("test_prop")
                .unwrap()
                .history(),
            [1, 11]
        );
        assert_eq!(
            v_from_graph
                .properties()
                .temporal()
                .get("test_prop")
                .unwrap()
                .history(),
            [10]
        );

        assert_eq!(v_from_graph.earliest_time(), Some(10));
        assert_eq!(v.earliest_time(), Some(1));
        assert_eq!(v.at(10).earliest_time(), Some(10));
        assert_eq!(v.at(10).latest_time(), Some(10));
        assert_eq!(v.latest_time(), Some(i64::MAX));
    }

    #[test]
    fn test_jira() {
        let g = GraphWithDeletions::new();

        g.add_edge(0, 1, 2, [("added", Prop::I64(0))], Some("assigned"))
            .unwrap();
        g.add_edge(1, 1, 3, [("added", Prop::I64(0))], Some("assigned"))
            .unwrap();
        g.add_edge(2, 4, 2, [("added", Prop::I64(0))], Some("has"))
            .unwrap();
        g.add_edge(3, 4, 2, [("added", Prop::I64(0))], Some("has"))
            .unwrap();
        g.add_edge(4, 5, 2, [("added", Prop::I64(0))], Some("blocks"))
            .unwrap();
        g.add_edge(5, 4, 5, [("added", Prop::I64(0))], Some("has"))
            .unwrap();
        g.add_edge(6, 6, 5, [("added", Prop::I64(0))], Some("assigned"))
            .unwrap();

        let nodes = g
            .window(0, 1701786285758)
            .layer(vec!["assigned", "has", "blocks"])
            .unwrap()
            .edges()
            .into_iter()
            .map(|vv| vv.id())
            .collect_vec();

        println!("windowed edges = {:?}", nodes);

        let nodes = g
            .window(0, 1701786285758)
            .layer(vec!["assigned", "has", "blocks"])
            .unwrap()
            .nodes()
            .into_iter()
            .map(|vv| vv.name())
            .collect_vec();

        println!("windowed nodes = {:?}", nodes);

        let nodes = g
            .at(1701786285758)
            .layer(vec!["assigned", "has", "blocks"])
            .unwrap()
            .edges()
            .into_iter()
            .map(|vv| vv.id())
            .collect_vec();
        println!("at edges = {:?}", nodes);

        let nodes = g
            .at(1701786285758)
            .layer(vec!["assigned", "has", "blocks"])
            .unwrap()
            .nodes()
            .into_iter()
            .map(|vv| vv.id())
            .collect_vec();

        println!("at nodes = {:?}", nodes);
        // assert_eq!(g.window(1, 2).node(0).unwrap().out_degree(), 1)
    }
}
