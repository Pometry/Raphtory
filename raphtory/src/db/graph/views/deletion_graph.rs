use crate::{
    core::{
        entities::{
            edges::{edge_ref::EdgeRef, edge_store::EdgeStore},
            vertices::vertex_store::VertexStore,
            LayerIds, VID,
        },
        storage::timeindex::{AsTime, TimeIndexOps},
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
use serde::{Deserialize, Serialize};
use std::{
    cmp::min,
    fmt::{Display, Formatter},
    iter,
    ops::Range,
    path::Path,
    sync::Arc,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GraphWithDeletions {
    graph: Arc<InternalGraph>,
}

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

impl GraphWithDeletions {
    fn edge_alive_at(&self, e: &EdgeStore, t: i64, layer_ids: &LayerIds) -> bool {
        // FIXME: assumes additions are before deletions if at the same timestamp (need to have strict ordering/secondary index)
        let (
            first_addition,
            first_deletion,
            last_addition_before_start,
            last_deletion_before_start,
        ) = match layer_ids {
            LayerIds::None => return false,
            LayerIds::All => (
                e.additions().iter().flat_map(|v| v.first_t()).min(),
                e.deletions().iter().flat_map(|v| v.first_t()).min(),
                e.additions()
                    .iter()
                    .flat_map(|v| v.range(i64::MIN..t.saturating_add(1)).last_t())
                    .max(),
                e.deletions()
                    .iter()
                    .flat_map(|v| v.range(i64::MIN..t).last_t())
                    .max(),
            ),
            LayerIds::One(l_id) => (
                e.additions().get(*l_id).and_then(|v| v.first_t()),
                e.deletions().get(*l_id).and_then(|v| v.first_t()),
                e.additions()
                    .get(*l_id)
                    .and_then(|v| v.range(i64::MIN..t.saturating_add(1)).last_t()),
                e.deletions()
                    .get(*l_id)
                    .and_then(|v| v.range(i64::MIN..t).last_t()),
            ),
            LayerIds::Multiple(ids) => (
                ids.iter()
                    .flat_map(|l_id| e.additions().get(*l_id).and_then(|v| v.first_t()))
                    .min(),
                ids.iter()
                    .flat_map(|l_id| e.deletions().get(*l_id).and_then(|v| v.first_t()))
                    .min(),
                ids.iter()
                    .flat_map(|l_id| {
                        e.additions()
                            .get(*l_id)
                            .and_then(|v| v.range(i64::MIN..t.saturating_add(1)).last_t())
                    })
                    .max(),
                ids.iter()
                    .flat_map(|l_id| {
                        e.deletions()
                            .get(*l_id)
                            .and_then(|v| v.range(i64::MIN..t).last_t())
                    })
                    .max(),
            ),
        };

        // None is less than any value (see test below)
        (first_deletion < first_addition && first_deletion.filter(|v| *v >= t).is_some())
            || last_addition_before_start > last_deletion_before_start
    }

    fn vertex_alive_at(
        &self,
        v: &VertexStore,
        t: i64,
        layers: &LayerIds,
        edge_filter: Option<&EdgeFilter>,
    ) -> bool {
        let edges = self.graph.inner().storage.edges.read_lock();
        v.edge_tuples(layers, Direction::BOTH)
            .map(|eref| edges.get(eref.pid().into()))
            .filter(|e| {
                edge_filter.map(|f| f(e, layers)).unwrap_or(true)
                    && self.edge_alive_at(e, t, layers)
            })
            .next()
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
    /// # Returns
    ///
    /// A raphtory graph
    ///
    /// # Example
    ///
    /// ```no_run
    /// use std::fs::File;
    /// use raphtory::prelude::*;
    /// let g = Graph::new();
    /// g.add_vertex(1, 1, NO_PROPS).unwrap();
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
    /// # Returns
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

impl<G: GraphViewOps> PartialEq<G> for GraphWithDeletions {
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
    fn vertex_earliest_time(&self, v: VID) -> Option<i64> {
        self.graph.vertex_earliest_time(v)
    }

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

    fn earliest_time_window(&self, t_start: i64, t_end: i64) -> Option<i64> {
        self.graph.earliest_time_window(t_start, t_end)
    }

    fn latest_time_window(&self, t_start: i64, t_end: i64) -> Option<i64> {
        self.graph.latest_time_window(t_start, t_end)
    }

    fn include_vertex_window(
        &self,
        v: VID,
        w: Range<i64>,
        layer_ids: &LayerIds,
        edge_filter: Option<&EdgeFilter>,
    ) -> bool {
        let v = self.graph.inner().storage.get_node(v);
        v.active(w.clone()) || self.vertex_alive_at(&v, w.start, layer_ids, edge_filter)
    }

    fn include_edge_window(&self, e: &EdgeStore, w: Range<i64>, layer_ids: &LayerIds) -> bool {
        // includes edge if it is alive at the start of the window or added during the window
        e.active(layer_ids, w.clone()) || self.edge_alive_at(e, w.start, layer_ids)
    }

    fn vertex_history(&self, v: VID) -> Vec<i64> {
        self.graph.vertex_history(v)
    }

    fn vertex_history_window(&self, v: VID, w: Range<i64>) -> Vec<i64> {
        self.graph.vertex_history_window(v, w)
    }

    fn edge_exploded(&self, e: EdgeRef, layer_ids: LayerIds) -> BoxedIter<EdgeRef> {
        //Fixme: Need support for duration on exploded edges
        if self.edge_alive_at(&self.core_edge(e.pid()), i64::MIN, &layer_ids) {
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
        if self.edge_alive_at(&entry, w.start, &layer_ids) {
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
        Box::new(
            self.graph
                .edge_layers(e, layer_ids.clone())
                .filter(move |&e| {
                    let entry = g.core_edge(e.pid());
                    g.include_edge_window(
                        &entry,
                        w.clone(),
                        &layer_ids.clone().constrain_from_edge(e),
                    )
                }),
        )
    }

    fn edge_earliest_time(&self, e: EdgeRef, layer_ids: LayerIds) -> Option<i64> {
        e.time().map(|ti| *ti.t()).or_else(|| {
            let entry = self.core_edge(e.pid());
            if self.edge_alive_at(&entry, i64::MIN, &layer_ids.clone()) {
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
        if self.edge_alive_at(&entry, w.start, &layer_ids) {
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
                if self.edge_alive_at(&entry, i64::MAX, &layer_ids) {
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
                if self.edge_alive_at(&entry, w.end - 1, &layer_ids) {
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

    fn temporal_prop_vec(&self, name: &str) -> Vec<(i64, Prop)> {
        self.graph.temporal_prop_vec(name)
    }

    fn temporal_prop_vec_window(&self, name: &str, t_start: i64, t_end: i64) -> Vec<(i64, Prop)> {
        self.graph.temporal_prop_vec_window(name, t_start, t_end)
    }

    fn temporal_vertex_prop_vec(&self, v: VID, name: &str) -> Vec<(i64, Prop)> {
        self.graph.temporal_vertex_prop_vec(v, name)
    }

    fn temporal_vertex_prop_vec_window(
        &self,
        v: VID,
        name: &str,
        t_start: i64,
        t_end: i64,
    ) -> Vec<(i64, Prop)> {
        self.graph
            .temporal_vertex_prop_vec_window(v, name, t_start, t_end)
    }

    fn temporal_edge_prop_vec_window(
        &self,
        e: EdgeRef,
        name: &str,
        t_start: i64,
        t_end: i64,
        layer_ids: LayerIds,
    ) -> Vec<(i64, Prop)> {
        let prop = self.temporal_edge_prop(e, name, layer_ids.clone());
        match prop {
            Some(p) => {
                let entry = self.core_edge(e.pid());
                if self.edge_alive_at(&entry, t_start, &layer_ids) {
                    p.last_before(t_start.saturating_add(1))
                        .into_iter()
                        .map(|(_, v)| (t_start, v))
                        .chain(p.iter_window(t_start.saturating_add(1)..t_end))
                        .collect()
                } else {
                    p.iter_window(t_start..t_end).collect()
                }
            }
            None => Default::default(),
        }
    }

    fn temporal_edge_prop_vec(
        &self,
        e: EdgeRef,
        name: &str,
        layer_ids: LayerIds,
    ) -> Vec<(i64, Prop)> {
        self.graph.temporal_edge_prop_vec(e, name, layer_ids)
    }
}

#[cfg(test)]
mod test_deletions {
    use crate::{db::graph::views::deletion_graph::GraphWithDeletions, prelude::*};
    use itertools::Itertools;

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

        assert_eq!(g.window(1, 2).num_edges(), 1);

        assert!(g.window(11, 12).is_empty());

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

        assert_eq!(g.window(1, 2).vertex(0).unwrap().out_degree(), 1)
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
        g.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        g.delete_edge(10, 1, 2, None).unwrap();
        let e = g.edge(1, 2).unwrap();
        assert_eq!(e.latest_time(), Some(10));
        assert_eq!(e.explode().latest_time().collect_vec(), vec![Some(10)]);
    }
}
