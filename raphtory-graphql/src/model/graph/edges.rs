use crate::{
    model::{
        graph::{
            collection::{check_list_allowed, check_page_limit},
            edge::GqlEdge,
            filtering::EdgesViewCollection,
            timeindex::{GqlEventTime, GqlTimeInput},
            windowset::GqlEdgesWindowSet,
            GqlAlignmentUnit, WindowDuration,
        },
        sorting::{EdgeSortBy, SortByTime},
    },
    rayon::blocking_compute,
};
use async_graphql::Context;
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use itertools::Itertools;
use raphtory::{
    core::utils::time::TryIntoInterval,
    db::{
        api::view::{internal::InternalFilter, DynamicGraph, EdgeSelect},
        graph::edges::Edges,
    },
    errors::GraphError,
    prelude::*,
};
use raphtory_api::{core::utils::time::IntoTime, iter::IntoDynBoxed};
use std::{cmp::Ordering, sync::Arc};

use crate::model::graph::filtering::GqlEdgeFilter;
use raphtory::db::{
    api::view::Filter, graph::views::filter::model::edge_filter::CompositeEdgeFilter,
};

/// A lazy collection of edges from a graph view. Supports the usual view
/// transforms (window, layer, filter, ...), plus edge-specific ones like
/// `explode` and `explodeLayers`, pagination, and sorting.
#[derive(ResolvedObject, Clone)]
#[graphql(name = "Edges")]
pub(crate) struct GqlEdges {
    pub(crate) ee: Edges<'static, DynamicGraph>,
}

impl GqlEdges {
    fn update<E: Into<Edges<'static, DynamicGraph>>>(&self, edges: E) -> Self {
        Self::new(edges)
    }
}

impl GqlEdges {
    pub(crate) fn new<E: Into<Edges<'static, DynamicGraph>>>(edges: E) -> Self {
        Self { ee: edges.into() }
    }

    fn iter(&self) -> Box<dyn Iterator<Item = GqlEdge> + '_> {
        let iter = self.ee.iter().map(GqlEdge::from_ref);
        Box::new(iter)
    }
}

/// A collection of edges.
///
/// Collections can be filtered and used to create lists.
#[ResolvedObjectFields]
impl GqlEdges {
    ////////////////////////
    // LAYERS AND WINDOWS //
    ////////////////////////

    /// Returns a collection containing only edges in the default edge layer.
    async fn default_layer(&self) -> Self {
        self.update(self.ee.default_layer())
    }

    /// Returns a collection containing only edges belonging to the listed layers.

    async fn layers(
        &self,
        #[graphql(desc = "Layer names to include.")] names: Vec<String>,
    ) -> Self {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.update(self_clone.ee.valid_layers(names))).await
    }

    /// Returns a collection containing edges belonging to all layers except the excluded list of layers.

    async fn exclude_layers(
        &self,
        #[graphql(desc = "Layer names to exclude.")] names: Vec<String>,
    ) -> Self {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.update(self_clone.ee.exclude_valid_layers(names))).await
    }

    /// Returns a collection containing edges belonging to the specified layer.

    async fn layer(&self, #[graphql(desc = "Layer name to include.")] name: String) -> Self {
        self.update(self.ee.valid_layers(name))
    }

    /// Returns a collection containing edges belonging to all layers except the excluded layer specified.

    async fn exclude_layer(
        &self,
        #[graphql(desc = "Layer name to exclude.")] name: String,
    ) -> Self {
        self.update(self.ee.exclude_valid_layers(name))
    }

    /// Creates a WindowSet with the given window duration and optional step using a rolling window. A rolling window is a window that moves forward by step size at each iteration.
    ///
    /// Returns a collection of collections. This means that item in the window set is a collection of edges.
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
    ) -> Result<GqlEdgesWindowSet, GraphError> {
        let window = window.try_into_interval()?;
        let step = step.map(|x| x.try_into_interval()).transpose()?;
        let ws = if let Some(unit) = alignment_unit {
            self.ee.rolling_aligned(window, step, unit.into())?
        } else {
            self.ee.rolling(window, step)?
        };
        Ok(GqlEdgesWindowSet::new(ws))
    }

    /// Creates a WindowSet with the given step size using an expanding window. An expanding window is a window that grows by step size at each iteration.
    ///
    /// Returns a collection of collections. This means that item in the window set is a collection of edges.
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
    ) -> Result<GqlEdgesWindowSet, GraphError> {
        let step = step.try_into_interval()?;
        let ws = if let Some(unit) = alignment_unit {
            self.ee.expanding_aligned(step, unit.into())?
        } else {
            self.ee.expanding(step)?
        };
        Ok(GqlEdgesWindowSet::new(ws))
    }

    /// Creates a view of the Edge including all events between the specified start (inclusive) and end (exclusive).

    async fn window(
        &self,
        #[graphql(desc = "Inclusive lower bound.")] start: GqlTimeInput,
        #[graphql(desc = "Exclusive upper bound.")] end: GqlTimeInput,
    ) -> Self {
        self.update(self.ee.window(start.into_time(), end.into_time()))
    }

    /// Creates a view of the Edge including all events at a specified time.

    async fn at(
        &self,
        #[graphql(desc = "Instant to pin the view to.")] time: GqlTimeInput,
    ) -> Self {
        self.update(self.ee.at(time.into_time()))
    }

    /// View showing only the latest state of each edge (equivalent to `at(latestTime)`).
    async fn latest(&self) -> Self {
        let e = self.ee.clone();
        let latest = blocking_compute(move || e.latest()).await;
        self.update(latest)
    }

    /// Creates a view of the Edge including all events that are valid at time. This is equivalent to before(time + 1) for Graph and at(time) for PersistentGraph.

    async fn snapshot_at(
        &self,
        #[graphql(desc = "Instant at which entities must be valid.")] time: GqlTimeInput,
    ) -> Self {
        self.update(self.ee.snapshot_at(time.into_time()))
    }

    /// Creates a view of the Edge including all events that are valid at the latest time. This is equivalent to a no-op for Graph and latest() for PersistentGraph.
    async fn snapshot_latest(&self) -> Self {
        self.update(self.ee.snapshot_latest())
    }

    /// Creates a view of the Edge including all events before a specified end (exclusive).

    async fn before(&self, #[graphql(desc = "Exclusive upper bound.")] time: GqlTimeInput) -> Self {
        self.update(self.ee.before(time.into_time()))
    }

    /// Creates a view of the Edge including all events after a specified start (exclusive).

    async fn after(&self, #[graphql(desc = "Exclusive lower bound.")] time: GqlTimeInput) -> Self {
        self.update(self.ee.after(time.into_time()))
    }

    /// Shrinks both the start and end of the window.

    async fn shrink_window(
        &self,
        #[graphql(desc = "Proposed new start (TimeInput); ignored if it would widen the window.")]
        start: GqlTimeInput,
        #[graphql(desc = "Proposed new end (TimeInput); ignored if it would widen the window.")]
        end: GqlTimeInput,
    ) -> Self {
        self.update(self.ee.shrink_window(start.into_time(), end.into_time()))
    }

    /// Set the start of the window.

    async fn shrink_start(
        &self,
        #[graphql(desc = "Proposed new start (TimeInput); ignored if it would widen the window.")]
        start: GqlTimeInput,
    ) -> Self {
        self.update(self.ee.shrink_start(start.into_time()))
    }

    /// Set the end of the window.

    async fn shrink_end(
        &self,
        #[graphql(desc = "Proposed new end (TimeInput); ignored if it would widen the window.")]
        end: GqlTimeInput,
    ) -> Self {
        self.update(self.ee.shrink_end(end.into_time()))
    }

    /// Takes a specified selection of views and applies them in order given.

    async fn apply_views(
        &self,
        #[graphql(
            desc = "Ordered list of view operations; each entry is a one-of variant (`window`, `layer`, `filter`, ...) applied to the running result."
        )]
        views: Vec<EdgesViewCollection>,
    ) -> Result<GqlEdges, GraphError> {
        let mut return_view: GqlEdges = self.update(self.ee.clone());
        for view in views {
            return_view = match view {
                EdgesViewCollection::DefaultLayer(apply) => {
                    if apply {
                        return_view.default_layer().await
                    } else {
                        return_view
                    }
                }
                EdgesViewCollection::Latest(apply) => {
                    if apply {
                        return_view.latest().await
                    } else {
                        return_view
                    }
                }
                EdgesViewCollection::SnapshotLatest(apply) => {
                    if apply {
                        return_view.snapshot_latest().await
                    } else {
                        return_view
                    }
                }
                EdgesViewCollection::SnapshotAt(at) => return_view.snapshot_at(at).await,
                EdgesViewCollection::Layers(layers) => return_view.layers(layers).await,
                EdgesViewCollection::ExcludeLayers(layers) => {
                    return_view.exclude_layers(layers).await
                }
                EdgesViewCollection::ExcludeLayer(layer) => return_view.exclude_layer(layer).await,
                EdgesViewCollection::Window(window) => {
                    return_view.window(window.start, window.end).await
                }
                EdgesViewCollection::At(at) => return_view.at(at).await,
                EdgesViewCollection::Before(time) => return_view.before(time).await,
                EdgesViewCollection::After(time) => return_view.after(time).await,
                EdgesViewCollection::ShrinkWindow(window) => {
                    return_view.shrink_window(window.start, window.end).await
                }
                EdgesViewCollection::ShrinkStart(time) => return_view.shrink_start(time).await,
                EdgesViewCollection::ShrinkEnd(time) => return_view.shrink_end(time).await,
                EdgesViewCollection::EdgeFilter(filter) => return_view.filter(filter).await?,
            }
        }

        Ok(return_view)
    }

    /// Expand each edge into one edge per update: if `A->B` has three updates, it
    /// becomes three `A->B` entries each at a distinct timestamp. Use this to
    /// iterate per-event rather than per-edge.
    async fn explode(&self) -> Self {
        self.update(self.ee.explode())
    }

    /// Returns an edge object for each layer within the original edge.
    ///
    /// Each new edge object contains only updates from the respective layers.
    async fn explode_layers(&self) -> Self {
        self.update(self.ee.explode_layers())
    }

    /// Sort the edges. Multiple criteria are applied lexicographically (ties
    /// on the first key break to the second, etc.).

    async fn sorted(
        &self,
        #[graphql(
            desc = "Ordered list of sort keys. Each entry chooses exactly one of `src` / `dst` / `time` / `property`, with an optional `reverse: true` to flip order."
        )]
        sort_bys: Vec<EdgeSortBy>,
    ) -> Self {
        let self_clone = self.clone();
        blocking_compute(move || {
            let sorted: Arc<[_]> = self_clone
                .ee
                .iter()
                .sorted_by(|first_edge, second_edge| {
                    sort_bys.clone().into_iter().fold(
                        Ordering::Equal,
                        |current_ordering, sort_by| {
                            current_ordering.then_with(|| {
                                let ordering = if sort_by.src == Some(true) {
                                    first_edge.src().id().partial_cmp(&second_edge.src().id())
                                } else if sort_by.dst == Some(true) {
                                    first_edge.dst().id().partial_cmp(&second_edge.dst().id())
                                } else if let Some(sort_by_time) = sort_by.time {
                                    let (first_time, second_time) = match sort_by_time {
                                        SortByTime::Latest => {
                                            (first_edge.latest_time(), second_edge.latest_time())
                                        }
                                        SortByTime::Earliest => (
                                            first_edge.earliest_time(),
                                            second_edge.earliest_time(),
                                        ),
                                    };
                                    first_time.partial_cmp(&second_time)
                                } else if let Some(sort_by_property) = sort_by.property {
                                    let first_prop_maybe =
                                        first_edge.properties().get(&*sort_by_property);
                                    let second_prop_maybe =
                                        second_edge.properties().get(&*sort_by_property);
                                    first_prop_maybe.partial_cmp(&second_prop_maybe)
                                } else {
                                    None
                                };
                                if let Some(ordering) = ordering {
                                    if sort_by.reverse == Some(true) {
                                        ordering.reverse()
                                    } else {
                                        ordering
                                    }
                                } else {
                                    Ordering::Equal
                                }
                            })
                        },
                    )
                })
                .map(|edge_view| edge_view.edge)
                .collect();
            self_clone.update(Edges::new(
                self_clone.ee.base_graph().clone(),
                Arc::new(move || {
                    let sorted = sorted.clone();
                    (0..sorted.len()).map(move |i| sorted[i]).into_dyn_boxed()
                }),
            ))
        })
        .await
    }

    ////////////////////////
    //// TIME QUERIES //////
    ////////////////////////

    /// Returns the start time of the window or none if there is no window.
    async fn start(&self) -> GqlEventTime {
        self.ee.start().into()
    }

    /// Returns the end time of the window or none if there is no window.
    async fn end(&self) -> GqlEventTime {
        self.ee.end().into()
    }

    /////////////////
    //// List ///////
    /////////////////

    /// Returns the number of edges.
    ///
    /// Returns:
    ///     int:
    async fn count(&self) -> usize {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.ee.len()).await
    }

    /// Fetch one page with a number of items up to a specified limit, optionally offset by a specified amount.
    /// The page_index sets the number of pages to skip (defaults to 0).
    ///
    /// For example, if page(5, 2, 1) is called, a page with 5 items, offset by 11 items (2 pages of 5 + 1),
    /// will be returned.

    async fn page(
        &self,
        ctx: &Context<'_>,
        #[graphql(desc = "Maximum number of items to return on this page.")] limit: usize,
        #[graphql(desc = "Extra items to skip on top of `pageIndex` paging (default 0).")]
        offset: Option<usize>,
        #[graphql(
            desc = "Zero-based page number; multiplies `limit` to determine where to start (default 0)."
        )]
        page_index: Option<usize>,
    ) -> async_graphql::Result<Vec<GqlEdge>> {
        check_page_limit(ctx, limit)?;
        let self_clone = self.clone();
        Ok(blocking_compute(move || {
            let start = page_index.unwrap_or(0) * limit + offset.unwrap_or(0);
            self_clone.iter().skip(start).take(limit).collect()
        })
        .await)
    }

    /// Returns a list of all objects in the current selection of the collection. You should filter the collection first then call list.
    async fn list(&self, ctx: &Context<'_>) -> async_graphql::Result<Vec<GqlEdge>> {
        check_list_allowed(ctx)?;
        let self_clone = self.clone();
        Ok(blocking_compute(move || self_clone.iter().collect()).await)
    }

    /// Narrow the collection to edges matching `expr`. The filter sticks to the
    /// returned view — every subsequent traversal through these edges (their
    /// properties, their endpoints' neighbours, etc.) continues to see the
    /// filtered scope.
    ///
    /// Useful when you want one scoping rule to apply across the whole query.
    /// E.g. restricting everything to a specific week:
    ///
    /// ```text
    /// edges { filter(expr: {window: {start: 1234, end: 5678}}) {
    ///   list { src { neighbours { list { name } } } }   # neighbours still windowed
    /// } }
    /// ```
    ///
    /// Contrast with `select`, which applies here and is not carried through.

    async fn filter(
        &self,
        #[graphql(desc = "Composite edge filter (by property, layer, src/dst, etc.).")]
        expr: GqlEdgeFilter,
    ) -> Result<Self, GraphError> {
        let self_clone = self.clone();
        blocking_compute(move || {
            let filter: CompositeEdgeFilter = expr.try_into()?;
            let filtered = self_clone.ee.filter(filter)?;
            Ok(self_clone.update(filtered.into_dyn()))
        })
        .await
    }

    /// Narrow the collection to edges matching `expr`, but only at this step —
    /// subsequent traversals out of these edges see the unfiltered graph again.
    ///
    /// Useful when you want different scopes at different hops. E.g. Monday's
    /// edges, then the neighbours of their endpoints on Tuesday, then *those*
    /// neighbours on Wednesday:
    ///
    /// ```text
    /// edges { select(expr: {window: {...monday...}}) {
    ///   list { src { select(expr: {window: {...tuesday...}}) {
    ///     neighbours { select(expr: {window: {...wednesday...}}) {
    ///       neighbours { list { name } }
    ///     } }
    ///   } } }
    /// } }
    /// ```
    ///
    /// Contrast with `filter`, which persists the scope through subsequent ops.

    async fn select(
        &self,
        #[graphql(desc = "Composite edge filter (by property, layer, src/dst, etc.).")]
        expr: GqlEdgeFilter,
    ) -> Result<Self, GraphError> {
        let self_clone = self.clone();
        blocking_compute(move || {
            let filter: CompositeEdgeFilter = expr.try_into()?;
            let filtered = self_clone.ee.select(filter)?;
            Ok(self_clone.update(filtered))
        })
        .await
    }
}
