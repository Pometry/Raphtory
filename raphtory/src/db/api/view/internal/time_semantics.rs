use crate::{
    core::{
        entities::{edges::edge_ref::EdgeRef, LayerIds, VID},
        Prop,
    },
    db::api::{
        storage::graph::{
            edges::edge_ref::EdgeStorageRef,
            nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps},
            tprop_storage_ops::TPropOps,
        },
        view::{internal::Base, BoxedLDIter, BoxedLIter, MaterializedGraph},
    },
    prelude::GraphViewOps,
};
use enum_dispatch::enum_dispatch;
use raphtory_api::{
    core::storage::timeindex::{AsTime, TimeIndexEntry, TimeIndexOps},
    iter::IntoDynBoxed,
};
use std::{borrow::Cow, ops::Range};

#[enum_dispatch(NodeTimeSemanticsOps)]
pub enum BaseTimeSemantics {
    Persistent(PersistentSemantics),
    Event(EventSemantics),
}

pub enum TimeSemantics {
    Base(BaseTimeSemantics),
    Window(WindowTimeSemantics),
}

macro_rules! for_all {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            TimeSemantics::Base($pattern) => $result,
            TimeSemantics::Window($pattern) => $result,
        }
    };
}

impl NodeTimeSemanticsOps for TimeSemantics {
    fn earliest_time<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> Option<i64> {
        for_all!(self, semantics => semantics.earliest_time(node, view))
    }

    fn latest_time<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> Option<i64> {
        for_all!(self, semantics => semantics.latest_time(node, view))
    }

    fn earliest_time_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> Option<i64> {
        for_all!(self, semantics => semantics.earliest_time_window(node, view, w))
    }

    fn latest_time_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> Option<i64> {
        for_all!(self, semantics => semantics.latest_time_window(node, view, w))
    }

    fn history<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> BoxedLIter<'graph, i64> {
        for_all!(self, semantics => semantics.history(node, view))
    }

    fn history_window<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, i64> {
        for_all!(self, semantics => semantics.history_window(node, view, w))
    }

    fn valid_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> bool {
        for_all!(self, semantics => semantics.valid_window(node, view, w))
    }

    fn node_updates<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, Vec<(usize, Prop)>)> {
        for_all!(self, semantics => semantics.node_updates(node, view))
    }

    fn node_updates_window<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, Vec<(usize, Prop)>)> {
        for_all!(self, semantics => semantics.node_updates_window(node, view, w))
    }

    fn valid<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> bool {
        for_all!(self, semantics => semantics.valid(node, view))
    }
}

impl TimeSemantics {
    pub fn persistent() -> Self {
        TimeSemantics::Base(BaseTimeSemantics::Persistent(PersistentSemantics()))
    }

    pub fn event() -> Self {
        TimeSemantics::Base(BaseTimeSemantics::Event(EventSemantics()))
    }

    pub fn window(self, w: Range<i64>) -> Self {
        match self {
            TimeSemantics::Base(semantics) => TimeSemantics::Window(WindowTimeSemantics {
                semantics,
                window: w,
            }),
            TimeSemantics::Window(semantics) => TimeSemantics::Window(semantics.window(w)),
        }
    }
}

#[enum_dispatch]
pub trait NodeTimeSemanticsOps {
    fn earliest_time<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> Option<i64>;

    fn latest_time<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> Option<i64>;

    fn earliest_time_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> Option<i64>;

    fn latest_time_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> Option<i64>;

    fn history<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> BoxedLIter<'graph, i64>;

    fn history_window<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, i64>;

    fn node_updates<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, Vec<(usize, Prop)>)>;

    fn node_updates_window<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, Vec<(usize, Prop)>)>;

    /// Check if the node is part of the graph based on the history
    fn valid<'graph, G: GraphViewOps<'graph>>(&self, node: NodeStorageRef<'graph>, view: G)
        -> bool;

    fn valid_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> bool;
}

pub struct PersistentSemantics();

impl NodeTimeSemanticsOps for PersistentSemantics {
    fn earliest_time<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> Option<i64> {
        node.history(view).first_t()
    }

    fn latest_time<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> Option<i64> {
        node.history(view).last_t()
    }

    fn earliest_time_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> Option<i64> {
        let history = node.history(view);
        if history.active_t(i64::MIN..w.start) {
            Some(w.start)
        } else {
            history.range_t(w).first_t()
        }
    }

    fn latest_time_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> Option<i64> {
        node.history(view)
            .range_t(i64::MIN..w.end)
            .last_t()
            .map(|t| t.max(w.start))
    }

    fn history<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> BoxedLIter<'graph, i64> {
        node.history(view).iter_t()
    }

    fn history_window<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, i64> {
        node.history(view).range_t(w).iter_t()
    }

    fn node_updates<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        _view: G,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, Vec<(usize, Prop)>)> {
        node.temp_prop_rows()
            .map(|(t, row)| {
                (
                    t,
                    row.into_iter().filter_map(|(i, v)| Some((i, v?))).collect(),
                )
            })
            .into_dyn_boxed()
    }

    fn node_updates_window<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, Vec<(usize, Prop)>)> {
        let start = w.start;
        let first_row = if node.history(view).active_t(i64::MIN..start) {
            Some(
                node.tprops()
                    .filter_map(|(i, tprop)| {
                        if tprop.active(start..start.saturating_add(1)) {
                            None
                        } else {
                            tprop
                                .last_before(TimeIndexEntry::start(start))
                                .map(|(_, v)| (i, v))
                        }
                    })
                    .collect(),
            )
        } else {
            None
        };
        first_row
            .into_iter()
            .map(move |row| (TimeIndexEntry::start(start), row))
            .chain(
                node.temp_prop_rows_window(TimeIndexEntry::range(w))
                    .map(|(t, row)| {
                        (
                            t,
                            row.into_iter().filter_map(|(i, v)| Some((i, v?))).collect(),
                        )
                    }),
            )
            .into_dyn_boxed()
    }

    fn valid<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> bool {
        !node.history(view).is_empty()
    }

    fn valid_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> bool {
        node.history(view).active_t(i64::MIN..w.end)
    }
}

pub struct EventSemantics();

impl NodeTimeSemanticsOps for EventSemantics {
    fn earliest_time<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> Option<i64> {
        node.history(view).first_t()
    }

    fn latest_time<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> Option<i64> {
        node.history(view).last_t()
    }

    fn earliest_time_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> Option<i64> {
        node.history(view).range_t(w).first_t()
    }

    fn latest_time_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> Option<i64> {
        node.history(view).range_t(w).last_t()
    }

    fn history<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> BoxedLIter<'graph, i64> {
        node.history(view).iter_t()
    }

    fn history_window<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, i64> {
        node.history(view).range_t(w).iter_t()
    }

    fn node_updates<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        _view: G,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, Vec<(usize, Prop)>)> {
        node.temp_prop_rows()
            .map(|(t, row)| {
                (
                    t,
                    row.into_iter()
                        .filter_map(|(id, prop)| Some((id, prop?)))
                        .collect(),
                )
            })
            .into_dyn_boxed()
    }

    fn node_updates_window<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        _view: G,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, Vec<(usize, Prop)>)> {
        node.temp_prop_rows_window(TimeIndexEntry::range(w))
            .map(|(t, row)| {
                (
                    t,
                    row.into_iter()
                        .filter_map(|(id, prop)| Some((id, prop?)))
                        .collect(),
                )
            })
            .into_dyn_boxed()
    }

    fn valid<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> bool {
        !node.history(view).is_empty()
    }

    fn valid_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> bool {
        node.history(view).active_t(w)
    }
}

pub struct WindowTimeSemantics {
    semantics: BaseTimeSemantics,
    window: Range<i64>,
}

impl WindowTimeSemantics {
    pub fn window(self, w: Range<i64>) -> Self {
        let start = self.window.start.max(w.start);
        let end = self.window.end.min(w.end).max(start);
        WindowTimeSemantics {
            window: start..end,
            ..self
        }
    }
}

impl NodeTimeSemanticsOps for WindowTimeSemantics {
    fn earliest_time<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> Option<i64> {
        self.semantics
            .earliest_time_window(node, view, self.window.clone())
    }

    fn latest_time<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> Option<i64> {
        self.semantics
            .latest_time_window(node, view, self.window.clone())
    }

    fn earliest_time_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> Option<i64> {
        self.semantics.earliest_time_window(node, view, w)
    }

    fn latest_time_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> Option<i64> {
        self.semantics.latest_time_window(node, view, w)
    }

    fn history<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> BoxedLIter<'graph, i64> {
        self.semantics
            .history_window(node, view, self.window.clone())
    }

    fn history_window<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, i64> {
        self.semantics.history_window(node, view, w)
    }

    fn node_updates<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, Vec<(usize, Prop)>)> {
        self.semantics
            .node_updates_window(node, view, self.window.clone())
    }

    fn node_updates_window<'graph, G: GraphViewOps<'graph>>(
        self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> BoxedLIter<'graph, (TimeIndexEntry, Vec<(usize, Prop)>)> {
        self.semantics.node_updates_window(node, view, w)
    }

    fn valid<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
    ) -> bool {
        self.semantics.valid_window(node, view, self.window.clone())
    }

    fn valid_window<'graph, G: GraphViewOps<'graph>>(
        &self,
        node: NodeStorageRef<'graph>,
        view: G,
        w: Range<i64>,
    ) -> bool {
        self.semantics.valid_window(node, view, w)
    }
}
/// Methods for defining time windowing semantics for a graph
#[enum_dispatch]
pub trait GraphTimeSemanticsOps {
    fn node_time_semantics(&self) -> TimeSemantics;

    /// Returns the start of the current view or `None` if unbounded
    fn view_start(&self) -> Option<i64>;

    /// Returns the end of the current view or `None` if unbounded
    fn view_end(&self) -> Option<i64>;

    /// Returns the timestamp for the earliest activity
    fn earliest_time_global(&self) -> Option<i64>;
    /// Returns the timestamp for the latest activity
    fn latest_time_global(&self) -> Option<i64>;
    /// Returns the timestamp for the earliest activity in the window
    fn earliest_time_window(&self, start: i64, end: i64) -> Option<i64>;

    /// Returns the timestamp for the latest activity in the window
    fn latest_time_window(&self, start: i64, end: i64) -> Option<i64>;

    /// check if edge `e` should be included in window `w`
    fn include_edge_window(
        &self,
        edge: EdgeStorageRef,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> bool;

    fn edge_history<'a>(
        &'a self,
        e: EdgeRef,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, TimeIndexEntry>;

    fn edge_history_window<'a>(
        &'a self,
        e: EdgeRef,
        layer_ids: Cow<'a, LayerIds>,
        w: Range<i64>,
    ) -> BoxedLIter<'a, TimeIndexEntry>;

    /// The number of exploded edge events for the `edge`
    fn edge_exploded_count(&self, edge: EdgeStorageRef, layer_ids: &LayerIds) -> usize;

    /// The number of exploded edge events for the edge in the window `w`
    fn edge_exploded_count_window(
        &self,
        edge: EdgeStorageRef,
        layer_ids: &LayerIds,
        w: Range<i64>,
    ) -> usize;

    /// Exploded edge iterator for edge `e`
    fn edge_exploded<'a>(
        &'a self,
        e: EdgeRef,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, EdgeRef>;

    /// Explode edge iterator for edge `e` for every layer
    fn edge_layers<'a>(
        &'a self,
        e: EdgeRef,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, EdgeRef>;

    /// Exploded edge iterator for edge`e` over window `w`
    fn edge_window_exploded<'a>(
        &'a self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, EdgeRef>;

    /// Exploded edge iterator for edge `e` over window `w` for every layer
    fn edge_window_layers<'a>(
        &'a self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, EdgeRef>;

    /// Get the time of the earliest activity of an edge
    fn edge_earliest_time(&self, e: EdgeRef, layer_ids: &LayerIds) -> Option<i64>;

    /// Get the time of the earliest activity of an edge `e` in window `w`
    fn edge_earliest_time_window(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> Option<i64>;

    /// Get the time of the latest activity of an edge
    fn edge_latest_time(&self, e: EdgeRef, layer_ids: &LayerIds) -> Option<i64>;

    /// Get the time of the latest activity of an edge `e` in window `w`
    fn edge_latest_time_window(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> Option<i64>;

    /// Get the edge deletions for use with materialize
    fn edge_deletion_history<'a>(
        &'a self,
        e: EdgeRef,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, TimeIndexEntry>;

    /// Get the edge deletions for use with materialize restricted to window `w`
    fn edge_deletion_history_window<'a>(
        &'a self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, TimeIndexEntry>;

    /// Check if  edge `e` is currently valid in any layer included in `layer_ids`
    fn edge_is_valid(&self, e: EdgeRef, layer_ids: &LayerIds) -> bool;

    /// Check if edge `e` is valid at the end of a window with exclusive end time `t` in all layers included in `layer_ids`
    fn edge_is_valid_at_end(&self, e: EdgeRef, layer_ids: &LayerIds, t: i64) -> bool;

    /// Check if graph has temporal property with the given id
    ///
    /// # Arguments
    ///
    /// * `prop_id` - The id of the property to retrieve.
    fn has_temporal_prop(&self, prop_id: usize) -> bool;

    /// Returns a vector of all temporal values of the graph property with the given id
    ///
    /// # Arguments
    ///
    /// * `prop_id` - The id of the property to retrieve.
    ///
    /// Returns:
    ///
    /// A vector of tuples representing the temporal values of the property
    /// that fall within the specified time window, where the first element of each tuple is the timestamp
    /// and the second element is the property value.
    fn temporal_prop_vec(&self, prop_id: usize) -> Vec<(i64, Prop)>;

    fn temporal_prop_iter(&self, prop_id: usize) -> BoxedLIter<(i64, Prop)> {
        Box::new(self.temporal_prop_vec(prop_id).into_iter())
    }

    /// Check if graph has temporal property with the given id in the window
    ///
    /// # Arguments
    ///
    /// * `prop_id` - The id of the property to retrieve.
    /// * `w` - time window
    fn has_temporal_prop_window(&self, prop_id: usize, w: Range<i64>) -> bool;

    /// Returns a vector of all temporal values of the graph property with the given name
    /// that fall within the specified time window.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the property to retrieve.
    /// * `start` - The start time of the window to consider.
    /// * `end` - The end time of the window to consider.
    ///
    /// Returns:
    ///
    /// A vector of tuples representing the temporal values of the property
    /// that fall within the specified time window, where the first element of each tuple is the timestamp
    /// and the second element is the property value.
    fn temporal_prop_vec_window(&self, prop_id: usize, start: i64, end: i64) -> Vec<(i64, Prop)>;

    fn temporal_prop_iter_window(
        &self,
        prop_id: usize,
        start: i64,
        end: i64,
    ) -> BoxedLIter<(i64, Prop)> {
        Box::new(
            self.temporal_prop_vec_window(prop_id, start, end)
                .into_iter(),
        )
    }

    /// Returns a vector of all temporal values of the node property with the given name for the
    /// given node
    ///
    /// # Arguments
    ///
    /// * `v` - A reference to the node for which to retrieve the temporal property vector.
    /// * `name` - The name of the property to retrieve.
    ///
    /// Returns:
    ///
    /// A vector of tuples representing the temporal values of the property for the given node
    /// that fall within the specified time window, where the first element of each tuple is the timestamp
    /// and the second element is the property value.
    fn temporal_node_prop_hist(&self, v: VID, id: usize) -> BoxedLDIter<(TimeIndexEntry, Prop)>;

    /// Returns a vector of all temporal values of the node property with the given name for the given node
    /// that fall within the specified time window.
    ///
    /// # Arguments
    ///
    /// * `v` - A reference to the node for which to retrieve the temporal property vector.
    /// * `name` - The name of the property to retrieve.
    /// * `start` - The start time of the window to consider.
    /// * `end` - The end time of the window to consider.
    ///
    /// Returns:
    ///
    /// A vector of tuples representing the temporal values of the property for the given node
    /// that fall within the specified time window, where the first element of each tuple is the timestamp
    /// and the second element is the property value.
    fn temporal_node_prop_hist_window(
        &self,
        v: VID,
        id: usize,
        start: i64,
        end: i64,
    ) -> BoxedLDIter<(TimeIndexEntry, Prop)>;

    /// Returns a vector of tuples containing the values of the temporal property with the given name
    /// for the given edge reference within the specified time window.
    ///
    /// # Arguments
    ///
    /// * `e` - An `EdgeRef` reference to the edge of interest.
    /// * `name` - A `String` containing the name of the temporal property.
    /// * `start` - An `i64` containing the start time of the time window (inclusive).
    /// * `end` - An `i64` containing the end time of the time window (exclusive).
    ///
    /// Returns:
    ///
    /// * A `Vec` of tuples containing the values of the temporal property with the given name for the given edge
    /// within the specified time window.
    ///
    fn temporal_edge_prop_hist_window<'a>(
        &'a self,
        e: EdgeRef,
        id: usize,
        start: i64,
        end: i64,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, (TimeIndexEntry, Prop)>;

    /// Return the value of an edge temporal property at a given point in time if it exists
    fn temporal_edge_prop_at(
        &self,
        e: EdgeRef,
        id: usize,
        t: TimeIndexEntry,
        layer_ids: &LayerIds,
    ) -> Option<Prop>;

    /// Returns a vector of tuples containing the values of the temporal property with the given name
    /// for the given edge reference.
    ///
    /// # Arguments
    ///
    /// * `e` - An `EdgeRef` reference to the edge of interest.
    /// * `name` - A `String` containing the name of the temporal property.
    ///
    /// Returns:
    ///
    /// * A `Vec` of tuples containing the values of the temporal property with the given name for the given edge.
    fn temporal_edge_prop_hist<'a>(
        &'a self,
        e: EdgeRef,
        id: usize,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, (TimeIndexEntry, Prop)>;

    /// Get constant edge property
    fn constant_edge_prop(&self, e: EdgeRef, id: usize, layer_ids: &LayerIds) -> Option<Prop>;

    /// Get constant edge property for a window
    ///
    /// Should only return the property for a layer if the edge exists in the window in that layer
    fn constant_edge_prop_window(
        &self,
        e: EdgeRef,
        id: usize,
        layer_ids: &LayerIds,
        w: Range<i64>,
    ) -> Option<Prop>;
}

pub trait InheritTimeSemantics: Base {}

impl<G: InheritTimeSemantics> DelegateTimeSemantics for G
where
    <G as Base>::Base: GraphTimeSemanticsOps,
{
    type Internal = <G as Base>::Base;

    fn graph(&self) -> &Self::Internal {
        self.base()
    }
}

pub trait DelegateTimeSemantics {
    type Internal: GraphTimeSemanticsOps + ?Sized;

    fn graph(&self) -> &Self::Internal;
}

impl<G: DelegateTimeSemantics + ?Sized> GraphTimeSemanticsOps for G {
    #[inline]
    fn node_time_semantics(&self) -> TimeSemantics {
        self.graph().node_time_semantics()
    }
    #[inline]
    fn view_start(&self) -> Option<i64> {
        self.graph().view_start()
    }
    #[inline]
    fn view_end(&self) -> Option<i64> {
        self.graph().view_end()
    }
    #[inline]
    fn earliest_time_global(&self) -> Option<i64> {
        self.graph().earliest_time_global()
    }
    #[inline]
    fn latest_time_global(&self) -> Option<i64> {
        self.graph().latest_time_global()
    }
    #[inline]
    fn earliest_time_window(&self, start: i64, end: i64) -> Option<i64> {
        self.graph().earliest_time_window(start, end)
    }

    #[inline]
    fn latest_time_window(&self, start: i64, end: i64) -> Option<i64> {
        self.graph().latest_time_window(start, end)
    }

    #[inline]
    fn include_edge_window(
        &self,
        edge: EdgeStorageRef,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> bool {
        self.graph().include_edge_window(edge, w, layer_ids)
    }

    #[inline]
    fn edge_history<'a>(
        &'a self,
        e: EdgeRef,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, TimeIndexEntry> {
        self.graph().edge_history(e, layer_ids)
    }

    #[inline]
    fn edge_history_window<'a>(
        &'a self,
        e: EdgeRef,
        layer_ids: Cow<'a, LayerIds>,
        w: Range<i64>,
    ) -> BoxedLIter<'a, TimeIndexEntry> {
        self.graph().edge_history_window(e, layer_ids, w)
    }

    #[inline]
    fn edge_exploded_count(&self, edge: EdgeStorageRef, layer_ids: &LayerIds) -> usize {
        self.graph().edge_exploded_count(edge, layer_ids)
    }

    #[inline]
    fn edge_exploded_count_window(
        &self,
        edge: EdgeStorageRef,
        layer_ids: &LayerIds,
        w: Range<i64>,
    ) -> usize {
        self.graph().edge_exploded_count_window(edge, layer_ids, w)
    }

    #[inline]
    fn edge_exploded<'a>(
        &'a self,
        e: EdgeRef,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, EdgeRef> {
        self.graph().edge_exploded(e, layer_ids)
    }

    #[inline]
    fn edge_layers<'a>(
        &'a self,
        e: EdgeRef,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, EdgeRef> {
        self.graph().edge_layers(e, layer_ids)
    }

    #[inline]
    fn edge_window_exploded<'a>(
        &'a self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, EdgeRef> {
        self.graph().edge_window_exploded(e, w, layer_ids)
    }

    #[inline]
    fn edge_window_layers<'a>(
        &'a self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, EdgeRef> {
        self.graph().edge_window_layers(e, w, layer_ids)
    }

    #[inline]
    fn edge_earliest_time(&self, e: EdgeRef, layer_ids: &LayerIds) -> Option<i64> {
        self.graph().edge_earliest_time(e, layer_ids)
    }

    #[inline]
    fn edge_earliest_time_window(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> Option<i64> {
        self.graph().edge_earliest_time_window(e, w, layer_ids)
    }

    #[inline]
    fn edge_latest_time(&self, e: EdgeRef, layer_ids: &LayerIds) -> Option<i64> {
        self.graph().edge_latest_time(e, layer_ids)
    }

    #[inline]
    fn edge_latest_time_window(
        &self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: &LayerIds,
    ) -> Option<i64> {
        self.graph().edge_latest_time_window(e, w, layer_ids)
    }

    #[inline]
    fn edge_deletion_history<'a>(
        &'a self,
        e: EdgeRef,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, TimeIndexEntry> {
        self.graph().edge_deletion_history(e, layer_ids)
    }

    #[inline]
    fn edge_deletion_history_window<'a>(
        &'a self,
        e: EdgeRef,
        w: Range<i64>,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, TimeIndexEntry> {
        self.graph().edge_deletion_history_window(e, w, layer_ids)
    }

    #[inline]
    fn edge_is_valid(&self, e: EdgeRef, layer_ids: &LayerIds) -> bool {
        self.graph().edge_is_valid(e, layer_ids)
    }

    #[inline]
    fn edge_is_valid_at_end(&self, e: EdgeRef, layer_ids: &LayerIds, t: i64) -> bool {
        self.graph().edge_is_valid_at_end(e, layer_ids, t)
    }

    #[inline]
    fn has_temporal_prop(&self, prop_id: usize) -> bool {
        self.graph().has_temporal_prop(prop_id)
    }

    #[inline]
    fn temporal_prop_vec(&self, prop_id: usize) -> Vec<(i64, Prop)> {
        self.graph().temporal_prop_vec(prop_id)
    }

    #[inline]
    fn has_temporal_prop_window(&self, prop_id: usize, w: Range<i64>) -> bool {
        self.graph().has_temporal_prop_window(prop_id, w)
    }

    #[inline]
    fn temporal_prop_vec_window(&self, prop_id: usize, start: i64, end: i64) -> Vec<(i64, Prop)> {
        self.graph().temporal_prop_vec_window(prop_id, start, end)
    }

    #[inline]
    fn temporal_node_prop_hist(
        &self,
        v: VID,
        prop_id: usize,
    ) -> BoxedLDIter<(TimeIndexEntry, Prop)> {
        self.graph().temporal_node_prop_hist(v, prop_id)
    }

    #[inline]
    fn temporal_node_prop_hist_window(
        &self,
        v: VID,
        prop_id: usize,
        start: i64,
        end: i64,
    ) -> BoxedLDIter<(TimeIndexEntry, Prop)> {
        self.graph()
            .temporal_node_prop_hist_window(v, prop_id, start, end)
    }

    #[inline]
    fn temporal_edge_prop_hist_window<'a>(
        &'a self,
        e: EdgeRef,
        prop_id: usize,
        start: i64,
        end: i64,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, (TimeIndexEntry, Prop)> {
        self.graph()
            .temporal_edge_prop_hist_window(e, prop_id, start, end, layer_ids)
    }

    #[inline]
    fn temporal_edge_prop_at(
        &self,
        e: EdgeRef,
        id: usize,
        t: TimeIndexEntry,
        layer_ids: &LayerIds,
    ) -> Option<Prop> {
        self.graph().temporal_edge_prop_at(e, id, t, layer_ids)
    }

    #[inline]
    fn temporal_edge_prop_hist<'a>(
        &'a self,
        e: EdgeRef,
        prop_id: usize,
        layer_ids: Cow<'a, LayerIds>,
    ) -> BoxedLIter<'a, (TimeIndexEntry, Prop)> {
        self.graph().temporal_edge_prop_hist(e, prop_id, layer_ids)
    }

    #[inline]
    fn constant_edge_prop(&self, e: EdgeRef, id: usize, layer_ids: &LayerIds) -> Option<Prop> {
        self.graph().constant_edge_prop(e, id, layer_ids)
    }

    #[inline]
    fn constant_edge_prop_window(
        &self,
        e: EdgeRef,
        id: usize,
        layer_ids: &LayerIds,
        w: Range<i64>,
    ) -> Option<Prop> {
        self.graph().constant_edge_prop_window(e, id, layer_ids, w)
    }
}
