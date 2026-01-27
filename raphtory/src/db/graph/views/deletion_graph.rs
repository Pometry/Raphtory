#[cfg(feature = "io")]
use crate::serialise::GraphPaths;
use crate::{
    core::storage::timeindex::{AsTime, EventTime, TimeIndex, TimeIndexOps},
    db::{
        api::{
            properties::internal::InheritPropertiesOps, storage::storage::Storage,
            view::internal::*,
        },
        graph::graph::graph_equal,
    },
    errors::GraphError,
    prelude::*,
};
use raphtory_api::{
    core::entities::properties::tprop::TPropOps,
    inherit::Base,
    iter::{BoxedLIter, IntoDynBoxed},
    GraphType,
};
use raphtory_core::utils::iter::GenLockedIter;
use raphtory_storage::{graph::graph::GraphStorage, mutation::InheritMutationOps};
use std::{
    fmt::{Display, Formatter},
    ops::Range,
    sync::Arc,
};
use storage::{
    api::graph_props::{GraphPropEntryOps, GraphPropRefOps},
    persist::strategy::PersistentStrategy,
    Extension,
};

/// A graph view where an edge remains active from the time it is added until it is explicitly marked as deleted.
///
/// Note that the graph will give you access to all edges that were added at any point in time, even those that are marked as deleted.
/// The deletion only has an effect on the exploded edge view that are returned. An edge is included in a windowed view of the graph if
/// it is considered active at any point in the window. Note that this means that if the last event at the start of the window (by event id) is a deletion,
/// the edge is not considered active at the start of the window, even if there are simultaneous addition events.
///
///
#[derive(Clone, Debug, Default)]
pub struct PersistentGraph(pub(crate) Arc<Storage>);

impl Static for PersistentGraph {}

impl From<GraphStorage> for PersistentGraph {
    fn from(value: GraphStorage) -> Self {
        Self(Arc::new(Storage::from_inner(value)))
    }
}

impl From<Arc<Storage>> for PersistentGraph {
    fn from(value: Arc<Storage>) -> Self {
        Self(value)
    }
}

impl Display for PersistentGraph {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

/// Get the last update of a property before `t` (exclusive), taking deletions into account.
/// The update is only returned if the edge was not deleted since.
fn last_prop_value_before<'a>(
    t: EventTime,
    props: impl TPropOps<'a>,
    deletions: impl TimeIndexOps<'a, IndexType = EventTime>,
) -> Option<(EventTime, Prop)> {
    props
        .last_before(t) // inclusive
        .filter(|(last_t, _)| !deletions.active(*last_t..t))
}

/// Gets the potentially persisted property value at a point in time
///
/// Persisted value can only exist if there is no update at time `t` and the edge is not deleted at time `t`
/// and if it exists it is the last value of the property before `t` as computed by `last_prop_value_before`.
fn persisted_prop_value_at<'a>(
    t: i64,
    props: impl TPropOps<'a>,
    deletions: impl TimeIndexOps<'a, IndexType = EventTime>,
) -> Option<Prop> {
    if props.active_t(t..t.saturating_add(1)) || deletions.active_t(t..t.saturating_add(1)) {
        None
    } else {
        last_prop_value_before(EventTime::start(t), props, deletions).map(|(_, v)| v)
    }
}

impl PersistentGraph {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new persistent graph at a specific path
    ///
    /// # Arguments
    /// * `path` - The path to the storage location
    /// # Returns
    /// A raphtory graph with storage at the specified path
    /// # Example
    /// ```no_run
    /// use raphtory::prelude::PersistentGraph;
    /// let g = PersistentGraph::new_at_path("/path/to/storage");
    /// ```
    #[cfg(feature = "io")]
    pub fn new_at_path(path: &(impl GraphPaths + ?Sized)) -> Result<Self, GraphError> {
        if !Extension::disk_storage_enabled() {
            return Err(GraphError::DiskGraphNotEnabled);
        }
        path.init()?;
        let graph = Self(Arc::new(Storage::new_at_path(path.graph_path()?)?));
        path.write_metadata(&graph)?;
        Ok(graph)
    }

    /// Load a graph from a specific path
    /// # Arguments
    /// * `path` - The path to the storage location
    /// # Returns
    /// A raphtory graph loaded from the specified path
    /// # Example
    /// ```no_run
    /// use raphtory::prelude::Graph;
    /// let g = Graph::load_from_path("/path/to/storage");
    ///
    #[cfg(feature = "io")]
    pub fn load_from_path(path: &(impl GraphPaths + ?Sized)) -> Result<Self, GraphError> {
        Ok(Self(Arc::new(Storage::load_from(path.graph_path()?)?)))
    }

    pub fn from_storage(storage: Arc<Storage>) -> Self {
        Self(storage)
    }

    pub fn from_internal_graph(internal_graph: GraphStorage) -> Self {
        Self(Arc::new(Storage::from_inner(internal_graph)))
    }

    /// Get event graph
    pub fn event_graph(&self) -> Graph {
        Graph::from_storage(self.0.clone())
    }
    pub fn persistent_graph(&self) -> PersistentGraph {
        self.clone()
    }
}

impl<'graph, G: GraphViewOps<'graph>> PartialEq<G> for PersistentGraph {
    fn eq(&self, other: &G) -> bool {
        graph_equal(self, other)
    }
}

impl Base for PersistentGraph {
    type Base = Storage;

    #[inline(always)]
    fn base(&self) -> &Self::Base {
        &self.0
    }
}

impl InheritStorageOps for PersistentGraph {}

impl InternalMaterialize for PersistentGraph {
    fn graph_type(&self) -> GraphType {
        GraphType::PersistentGraph
    }
}

impl InheritMutationOps for PersistentGraph {}

impl InheritListOps for PersistentGraph {}

impl InheritCoreGraphOps for PersistentGraph {}

impl InheritPropertiesOps for PersistentGraph {}

impl InheritLayerOps for PersistentGraph {}

impl InheritAllEdgeFilterOps for PersistentGraph {}

impl InheritNodeFilterOps for PersistentGraph {}

impl GraphTimeSemanticsOps for PersistentGraph {
    fn node_time_semantics(&self) -> TimeSemantics {
        TimeSemantics::persistent()
    }

    fn edge_time_semantics(&self) -> TimeSemantics {
        TimeSemantics::persistent()
    }

    fn view_start(&self) -> Option<EventTime> {
        self.0.view_start()
    }

    fn view_end(&self) -> Option<EventTime> {
        self.0.view_end()
    }

    fn earliest_time_global(&self) -> Option<i64> {
        self.0.earliest_time_global()
    }

    fn latest_time_global(&self) -> Option<i64> {
        self.0.latest_time_global()
    }

    fn earliest_time_window(&self, start: EventTime, end: EventTime) -> Option<i64> {
        self.earliest_time_global()
            .map(|t| t.max(start.t()))
            .filter(|&t| t < end.t())
    }

    fn latest_time_window(&self, start: EventTime, end: EventTime) -> Option<i64> {
        if self.0.earliest_time_global()? >= end.t() {
            return None;
        }
        self.latest_time_global()
            .map(|t| t.min(end.t().saturating_sub(1)).max(start.t()))
    }

    #[inline]
    fn has_temporal_prop(&self, prop_id: usize) -> bool {
        self.0.has_temporal_prop(prop_id)
    }

    fn temporal_prop_iter(&self, prop_id: usize) -> BoxedLIter<'_, (EventTime, Prop)> {
        self.0.temporal_prop_iter(prop_id)
    }

    #[inline]
    fn has_temporal_prop_window(&self, prop_id: usize, w: Range<EventTime>) -> bool {
        self.temporal_prop_iter_window(prop_id, w.start, w.end)
            .next()
            .is_some()
    }

    /// Iterates over temporal property values within a time window `[start, end)`.
    ///
    /// # Returns
    /// A boxed iterator yielding `(TimeIndexEntry, Prop)` tuples.
    fn temporal_prop_iter_window(
        &self,
        prop_id: usize,
        start: EventTime,
        end: EventTime,
    ) -> BoxedLIter<'_, (EventTime, Prop)> {
        let graph_entry = self.core_graph().graph_entry();

        GenLockedIter::from(graph_entry, move |entry| {
            let tprop = entry.as_ref().get_temporal_prop(prop_id);

            // Get the property value that was active at the start of the window.
            let first = persisted_prop_value_at(start.t(), tprop, &TimeIndex::Empty)
                .map(|prop_value| (start, prop_value));

            // Chain the initial prop with the rest of the props that occur
            // within the window.
            first
                .into_iter()
                .chain(tprop.iter_window(start..end))
                .into_dyn_boxed()
        })
        .into_dyn_boxed()
    }

    fn temporal_prop_iter_window_rev(
        &self,
        prop_id: usize,
        start: EventTime,
        end: EventTime,
    ) -> BoxedLIter<'_, (EventTime, Prop)> {
        let graph_entry = self.core_graph().graph_entry();

        GenLockedIter::from(graph_entry, move |entry| {
            let tprop = entry.as_ref().get_temporal_prop(prop_id);

            // Get the property value that was active at the start of the window.
            let first = persisted_prop_value_at(start.t(), tprop, &TimeIndex::Empty)
                .map(|prop_value| (start, prop_value));

            // Chain the initial prop with the rest of the props that occur
            // within the window, in reverse order.
            tprop
                .iter_window_rev(start..end)
                .chain(first)
                .into_dyn_boxed()
        })
        .into_dyn_boxed()
    }

    fn temporal_prop_last_at(&self, prop_id: usize, t: EventTime) -> Option<(EventTime, Prop)> {
        self.0.temporal_prop_last_at(prop_id, t)
    }

    fn temporal_prop_last_at_window(
        &self,
        prop_id: usize,
        t: EventTime,
        w: Range<EventTime>,
    ) -> Option<(EventTime, Prop)> {
        if w.contains(&t) {
            self.0
                .temporal_prop_last_at(prop_id, t)
                .map(|(t, v)| (t.max(w.start), v))
        } else {
            None
        }
    }
}

#[cfg(test)]
mod test {
    use crate::db::api::view::time::internal::InternalTimeOps;

    use super::*;

    #[test]
    fn test_view_start_end() {
        let g = PersistentGraph::new();
        let e = g.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        assert_eq!(g.start(), None);
        assert_eq!(g.timeline_start().map(|t| t.t()), Some(0));
        assert_eq!(g.end(), None);
        assert_eq!(g.timeline_end().map(|t| t.t()), Some(1));
        e.delete(2, None).unwrap();
        assert_eq!(g.timeline_start().map(|t| t.t()), Some(0));
        assert_eq!(g.timeline_end().map(|t| t.t()), Some(3));
        let w = g.window(g.timeline_start().unwrap(), g.timeline_end().unwrap());
        assert!(g.has_edge(1, 2));
        assert!(w.has_edge(1, 2));
        assert_eq!(w.start().map(|t| t.t()), Some(0));
        assert_eq!(w.timeline_start().map(|t| t.t()), Some(0));
        assert_eq!(w.end().map(|t| t.t()), Some(3));
        assert_eq!(w.timeline_end().map(|t| t.t()), Some(3));

        e.add_updates(4, NO_PROPS, None).unwrap();
        assert_eq!(g.timeline_start().map(|t| t.t()), Some(0));
        assert_eq!(g.timeline_end().map(|t| t.t()), Some(5));
    }
    #[test]
    fn test_materialize_window_earliest_time() {
        let g = PersistentGraph::new();
        g.add_edge(0, 1, 2, NO_PROPS, None).unwrap();
        g.delete_edge(10, 1, 2, None).unwrap();

        let ltg = g.latest_time_global();
        assert_eq!(ltg, Some(10));

        let wg = g.window(3, 5);

        let e = wg.edge(1, 2).unwrap();
        assert_eq!(e.earliest_time().map(|t| t.t()), Some(3));
        assert_eq!(e.latest_time().map(|t| t.t()), Some(3));
        let n1 = wg.node(1).unwrap();
        assert_eq!(n1.earliest_time().unwrap().t(), 3);
        assert_eq!(n1.latest_time().unwrap().t(), 3);
        let n2 = wg.node(2).unwrap();
        assert_eq!(n2.earliest_time().unwrap().t(), 3);
        assert_eq!(n2.latest_time().unwrap().t(), 3);

        let actual_lt = wg.latest_time();
        assert_eq!(actual_lt.unwrap().t(), 3);

        let actual_et = wg.earliest_time();
        assert_eq!(actual_et.unwrap().t(), 3);

        let gm = g
            .window(3, 5)
            .materialize()
            .unwrap()
            .into_persistent()
            .unwrap();

        let expected_et = gm.earliest_time();
        assert_eq!(actual_et, expected_et);
    }
}
