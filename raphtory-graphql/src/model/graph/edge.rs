use crate::{
    model::graph::{
        edges::GqlEdges,
        filtering::{EdgeViewCollection, GqlEdgeFilter},
        history::GqlHistory,
        node::GqlNode,
        node_id::GqlNodeId,
        property::{GqlMetadata, GqlProperties},
        timeindex::{GqlEventTime, GqlTimeInput},
        windowset::GqlEdgeWindowSet,
        GqlAlignmentUnit, WindowDuration,
    },
    rayon::blocking_compute,
};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use raphtory::{
    core::utils::time::TryIntoInterval,
    db::{
        api::view::{DynamicGraph, EdgeViewOps, Filter, IntoDynamic, StaticGraphViewOps},
        graph::{edge::EdgeView, views::filter::model::edge_filter::CompositeEdgeFilter},
    },
    errors::GraphError,
    prelude::{LayerOps, TimeOps},
};
use raphtory_api::core::utils::time::IntoTime;

/// Raphtory graph edge.
#[derive(ResolvedObject, Clone)]
#[graphql(name = "Edge")]
pub struct GqlEdge {
    pub(crate) ee: EdgeView<DynamicGraph>,
}

impl<G: StaticGraphViewOps + IntoDynamic> From<EdgeView<G>> for GqlEdge {
    fn from(value: EdgeView<G>) -> Self {
        Self {
            ee: EdgeView {
                graph: value.graph.into_dynamic(),
                edge: value.edge,
            },
        }
    }
}

impl GqlEdge {
    pub(crate) fn from_ref<G: StaticGraphViewOps + IntoDynamic>(value: EdgeView<&G>) -> Self {
        value.cloned().into()
    }
}

#[ResolvedObjectFields]
impl GqlEdge {
    ////////////////////////
    // LAYERS AND WINDOWS //
    ////////////////////////

    /// Return a view of Edge containing only the default edge layer.
    async fn default_layer(&self) -> GqlEdge {
        self.ee.default_layer().into()
    }

    /// Returns a view of Edge containing all layers in the list of names.
    ///
    /// Errors if any of the layers do not exist.

    async fn layers(
        &self,
        #[graphql(desc = "Layer names to include.")] names: Vec<String>,
    ) -> GqlEdge {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.ee.valid_layers(names).into()).await
    }

    /// Returns a view of Edge containing all layers except the excluded list of names.
    ///
    /// Errors if any of the layers do not exist.

    async fn exclude_layers(
        &self,
        #[graphql(desc = "Layer names to exclude.")] names: Vec<String>,
    ) -> GqlEdge {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.ee.exclude_valid_layers(names).into()).await
    }

    /// Returns a view of Edge containing the specified layer.
    ///
    /// Errors if any of the layers do not exist.

    async fn layer(&self, #[graphql(desc = "Layer name to include.")] name: String) -> GqlEdge {
        self.ee.valid_layers(name).into()
    }

    /// Returns a view of Edge containing all layers except the excluded layer specified.
    ///
    /// Errors if any of the layers do not exist.

    async fn exclude_layer(
        &self,
        #[graphql(desc = "Layer name to exclude.")] name: String,
    ) -> GqlEdge {
        self.ee.exclude_valid_layers(name).into()
    }

    /// Creates a WindowSet with the given window duration and optional step using a rolling window.
    ///
    /// A rolling window is a window that moves forward by step size at each iteration.
    ///
    /// alignment_unit optionally aligns the windows to the specified unit. "Unaligned" can be passed for no alignment.
    /// If unspecified (i.e. by default), alignment is done on the smallest unit of time in the step (or window if no step is passed).
    /// e.g. "1 month and 1 day" will align at the start of the day.
    /// Note that passing a step larger than window while alignment_unit is not "Unaligned" may lead to some entries appearing before
    /// the start of the first window and/or after the end of the last window (i.e. not included in any window).

    async fn rolling(
        &self,
        #[graphql(
            desc = "Width of each window. Pass either `{epoch: <ms>}` for a discrete number of milliseconds (e.g. `{epoch: 1000}` for 1 second), or `{duration: <text>}` for a calendar duration (e.g. `{duration: 1 day}` or `{duration: 2 hours and 30 minutes}`)."
        )]
        window: WindowDuration,
        #[graphql(
            desc = "Optional gap between the start of one window and the start of the next. Accepts the same `{epoch: <ms>}` or `{duration: <text>}` values as `window`. Defaults to `window` — i.e. windows touch end-to-end with no overlap and no gap."
        )]
        step: Option<WindowDuration>,
        #[graphql(
            desc = "Optional anchor for window boundaries — pass `Unaligned` to disable, or one of the unit values (e.g. `Day`, `Hour`, `Minute`) to align edges to that calendar unit. Defaults to the smallest unit present in `step` (or `window` if no step is set)."
        )]
        alignment_unit: Option<GqlAlignmentUnit>,
    ) -> Result<GqlEdgeWindowSet, GraphError> {
        let window = window.try_into_interval()?;
        let step = step.map(|x| x.try_into_interval()).transpose()?;
        let ws = if let Some(unit) = alignment_unit {
            self.ee.rolling_aligned(window, step, unit.into())?
        } else {
            self.ee.rolling(window, step)?
        };
        Ok(GqlEdgeWindowSet::new(ws))
    }

    /// Creates a WindowSet with the given step size using an expanding window.
    ///
    /// An expanding window is a window that grows by step size at each iteration.
    ///
    /// alignment_unit optionally aligns the windows to the specified unit. "Unaligned" can be passed for no alignment.
    /// If unspecified (i.e. by default), alignment is done on the smallest unit of time in the step.
    /// e.g. "1 month and 1 day" will align at the start of the day.

    async fn expanding(
        &self,
        #[graphql(
            desc = "How much the window grows by on each step. Pass either `{epoch: <ms>}` for a discrete number of milliseconds, or `{duration: <text>}` for a calendar duration (e.g. `{duration: 1 day}`)."
        )]
        step: WindowDuration,
        #[graphql(
            desc = "Optional anchor for window boundaries — pass `Unaligned` to disable, or one of the unit values (e.g. `Day`, `Hour`, `Minute`) to align edges to that calendar unit. Defaults to the smallest unit present in `step`."
        )]
        alignment_unit: Option<GqlAlignmentUnit>,
    ) -> Result<GqlEdgeWindowSet, GraphError> {
        let step = step.try_into_interval()?;
        let ws = if let Some(unit) = alignment_unit {
            self.ee.expanding_aligned(step, unit.into())?
        } else {
            self.ee.expanding(step)?
        };
        Ok(GqlEdgeWindowSet::new(ws))
    }

    /// Creates a view of the Edge including all events between the specified start (inclusive) and end (exclusive).
    ///
    /// For persistent graphs, any edge which exists at any point during the window will be included. You may want to restrict this to only edges that are present at the end of the window using the is_valid function.

    async fn window(
        &self,
        #[graphql(desc = "Inclusive lower bound.")] start: GqlTimeInput,
        #[graphql(desc = "Exclusive upper bound.")] end: GqlTimeInput,
    ) -> GqlEdge {
        self.ee.window(start.into_time(), end.into_time()).into()
    }

    /// Creates a view of the Edge including all events at a specified time.

    async fn at(
        &self,
        #[graphql(desc = "Instant to pin the view to.")] time: GqlTimeInput,
    ) -> GqlEdge {
        self.ee.at(time.into_time()).into()
    }

    /// View of this edge pinned to the graph's latest time — equivalent to
    /// `at(graph.latestTime)`. The edge's properties and metadata show their
    /// most recent values, and (for persistent graphs) validity is evaluated
    /// at that instant.
    async fn latest(&self) -> GqlEdge {
        self.ee.latest().into()
    }

    /// Creates a view of the Edge including all events that are valid at time.
    ///
    /// This is equivalent to before(time + 1) for Graph and at(time) for PersistentGraph.

    async fn snapshot_at(
        &self,
        #[graphql(desc = "Instant at which entities must be valid.")] time: GqlTimeInput,
    ) -> GqlEdge {
        self.ee.snapshot_at(time.into_time()).into()
    }

    /// Creates a view of the Edge including all events that are valid at the latest time.
    ///
    /// This is equivalent to a no-op for Graph and latest() for PersistentGraph.
    async fn snapshot_latest(&self) -> GqlEdge {
        self.ee.snapshot_latest().into()
    }

    /// Creates a view of the Edge including all events before a specified end (exclusive).

    async fn before(
        &self,
        #[graphql(desc = "Exclusive upper bound.")] time: GqlTimeInput,
    ) -> GqlEdge {
        self.ee.before(time.into_time()).into()
    }

    /// Creates a view of the Edge including all events after a specified start (exclusive).

    async fn after(
        &self,
        #[graphql(desc = "Exclusive lower bound.")] time: GqlTimeInput,
    ) -> GqlEdge {
        self.ee.after(time.into_time()).into()
    }

    /// Shrinks both the start and end of the window.

    async fn shrink_window(
        &self,
        #[graphql(desc = "Proposed new start (TimeInput); ignored if it would widen the window.")]
        start: GqlTimeInput,
        #[graphql(desc = "Proposed new end (TimeInput); ignored if it would widen the window.")]
        end: GqlTimeInput,
    ) -> Self {
        self.ee
            .shrink_window(start.into_time(), end.into_time())
            .into()
    }

    /// Set the start of the window.

    async fn shrink_start(
        &self,
        #[graphql(desc = "Proposed new start (TimeInput); ignored if it would widen the window.")]
        start: GqlTimeInput,
    ) -> Self {
        self.ee.shrink_start(start.into_time()).into()
    }

    /// Set the end of the window.

    async fn shrink_end(
        &self,
        #[graphql(desc = "Proposed new end (TimeInput); ignored if it would widen the window.")]
        end: GqlTimeInput,
    ) -> Self {
        self.ee.shrink_end(end.into_time()).into()
    }

    /// Takes a specified selection of views and applies them in given order.

    async fn apply_views(
        &self,
        #[graphql(
            desc = "Ordered list of view operations; each entry is a one-of variant (`window`, `layer`, `filter`, ...) applied to the running result."
        )]
        views: Vec<EdgeViewCollection>,
    ) -> Result<GqlEdge, GraphError> {
        let mut return_view: GqlEdge = self.ee.clone().into();
        for view in views {
            return_view = match view {
                EdgeViewCollection::DefaultLayer(apply) => {
                    if apply {
                        return_view.default_layer().await
                    } else {
                        return_view
                    }
                }
                EdgeViewCollection::Layers(layers) => return_view.layers(layers).await,

                EdgeViewCollection::ExcludeLayers(layers) => {
                    return_view.exclude_layers(layers).await
                }
                EdgeViewCollection::ExcludeLayer(layer) => return_view.exclude_layer(layer).await,

                EdgeViewCollection::Latest(apply) => {
                    if apply {
                        return_view.latest().await
                    } else {
                        return_view
                    }
                }
                EdgeViewCollection::SnapshotLatest(apply) => {
                    if apply {
                        return_view.snapshot_latest().await
                    } else {
                        return_view
                    }
                }
                EdgeViewCollection::SnapshotAt(at) => return_view.snapshot_at(at).await,
                EdgeViewCollection::Window(window) => {
                    return_view.window(window.start, window.end).await
                }
                EdgeViewCollection::At(at) => return_view.at(at).await,
                EdgeViewCollection::Before(time) => return_view.before(time).await,
                EdgeViewCollection::After(time) => return_view.after(time).await,
                EdgeViewCollection::ShrinkWindow(window) => {
                    return_view.shrink_window(window.start, window.end).await
                }
                EdgeViewCollection::ShrinkStart(time) => return_view.shrink_start(time).await,
                EdgeViewCollection::ShrinkEnd(time) => return_view.shrink_end(time).await,
                EdgeViewCollection::EdgeFilter(filter) => return_view.filter(filter).await?,
            }
        }
        Ok(return_view)
    }

    /// Returns the earliest time of an edge.
    async fn earliest_time(&self) -> GqlEventTime {
        self.ee.earliest_time().into()
    }

    /// The timestamp of the first event in this edge's history (first update, first
    /// deletion, or anything in between). Differs from `earliestTime` in that
    /// `earliestTime` reports when the edge is first *valid*; `firstUpdate` reports
    /// when its history actually begins.
    async fn first_update(&self) -> GqlEventTime {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.ee.history().earliest_time().into()).await
    }

    /// Returns the latest time of an edge.
    async fn latest_time(&self) -> GqlEventTime {
        self.ee.latest_time().into()
    }

    /// The timestamp of the last event in this edge's history (last update, last
    /// deletion, or anything in between). Differs from `latestTime` in that
    /// `latestTime` reports when the edge is last *valid*; `lastUpdate` reports
    /// when its history actually ends.
    async fn last_update(&self) -> GqlEventTime {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.ee.history().latest_time().into()).await
    }

    /// Returns the time of an exploded edge. Errors on an unexploded edge.
    async fn time(&self) -> Result<GqlEventTime, GraphError> {
        self.ee.time().map(|t| t.into())
    }

    /// Returns the start time for rolling and expanding windows for this edge. Returns none if no window is applied.
    async fn start(&self) -> GqlEventTime {
        self.ee.start().into()
    }

    /// Returns the end time of the window. Returns none if no window is applied.
    async fn end(&self) -> GqlEventTime {
        self.ee.end().into()
    }

    /// Returns the source node of the edge.
    ///
    /// Returns:
    ///     Node:
    async fn src(&self) -> GqlNode {
        self.ee.src().into()
    }

    /// Returns the destination node of the edge.
    ///
    /// Returns:
    ///     Node:
    async fn dst(&self) -> GqlNode {
        self.ee.dst().into()
    }

    /// Returns the node at the other end of the edge (same as dst() for out-edges and src() for in-edges).
    ///
    /// Returns:
    ///     Node:
    async fn nbr(&self) -> GqlNode {
        self.ee.nbr().into()
    }

    /// Returns the `[src, dst]` id pair of the edge. Each id is a `String`
    /// for string-indexed graphs or a non-negative `Int` for integer-indexed
    /// graphs.
    async fn id(&self) -> Vec<GqlNodeId> {
        let (src_id, dst_id) = self.ee.id();
        vec![GqlNodeId(src_id), GqlNodeId(dst_id)]
    }

    /// Returns a view of the properties of the edge.
    async fn properties(&self) -> GqlProperties {
        self.ee.properties().into()
    }

    /// Returns the metadata of an edge.
    async fn metadata(&self) -> GqlMetadata {
        self.ee.metadata().into()
    }

    /// Returns the names of the layers that have this edge as a member.
    async fn layer_names(&self) -> Vec<String> {
        self.ee
            .layer_names()
            .into_iter()
            .map(|x| x.into())
            .collect()
    }

    /// Returns the layer name of an exploded edge, errors on an edge.
    async fn layer_name(&self) -> Result<String, GraphError> {
        self.ee.layer_name().map(|x| x.into())
    }

    /// Returns an edge object for each update within the original edge.
    async fn explode(&self) -> GqlEdges {
        GqlEdges::new(self.ee.explode())
    }

    /// Returns an edge object for each layer within the original edge.
    ///
    /// Each new edge object contains only updates from the respective layers.
    async fn explode_layers(&self) -> GqlEdges {
        GqlEdges::new(self.ee.explode_layers())
    }

    /// Returns a History object with time entries for when an edge is added or change to an edge is made.
    ///
    /// Returns:
    ///     History:
    async fn history(&self) -> GqlHistory {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.ee.history().into()).await
    }

    /// Returns a history object with time entries for an edge's deletion times.
    ///
    /// Returns:
    ///     History:
    async fn deletions(&self) -> GqlHistory {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.ee.deletions().into()).await
    }

    /// Checks if the edge is currently valid and exists at the current time.
    ///
    /// Returns: boolean
    async fn is_valid(&self) -> bool {
        self.ee.is_valid()
    }

    /// Checks if the edge is currently active and has at least one update within the current period.
    ///
    /// Returns: boolean
    async fn is_active(&self) -> bool {
        self.ee.is_active()
    }

    /// Checks if the edge is deleted at the current time.
    ///
    /// Returns: boolean
    async fn is_deleted(&self) -> bool {
        self.ee.is_deleted()
    }

    /// Returns true if the edge source and destination nodes are the same.
    ///
    /// Returns: boolean
    async fn is_self_loop(&self) -> bool {
        self.ee.is_self_loop()
    }

    /// Apply an edge filter in place, returning an edge view whose properties /
    /// metadata / history are restricted to the matching subset.

    async fn filter(
        &self,
        #[graphql(desc = "Composite edge filter (by property, layer, src/dst, etc.).")]
        expr: GqlEdgeFilter,
    ) -> Result<Self, GraphError> {
        let self_clone = self.clone();
        blocking_compute(move || {
            let filter: CompositeEdgeFilter = expr.try_into()?;
            let filtered = self_clone.ee.filter(filter)?;
            Ok(self_clone.update(filtered.into_dynamic()))
        })
        .await
    }
}

impl GqlEdge {
    fn update<E: Into<EdgeView<DynamicGraph>>>(&self, edge: E) -> Self {
        Self { ee: edge.into() }
    }
}
