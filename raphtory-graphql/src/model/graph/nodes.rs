use crate::{
    model::{
        graph::{
            collection::{check_list_allowed, check_page_limit},
            filtering::{GqlNodeFilter, NodesViewCollection},
            node::GqlNode,
            timeindex::{GqlEventTime, GqlTimeInput},
            windowset::GqlNodesWindowSet,
            GqlAlignmentUnit, WindowDuration,
        },
        sorting::{NodeSortBy, SortByTime},
    },
    rayon::blocking_compute,
};
use async_graphql::{Context, Result};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use itertools::Itertools;
use raphtory::{
    core::utils::time::TryIntoInterval,
    db::{
        api::{
            state::{ops::DynNodeFilter, Index},
            view::{filter_ops::NodeSelect, DynamicGraph, Filter},
        },
        graph::{
            nodes::{IntoDynNodes, Nodes},
            views::filter::model::node_filter::CompositeNodeFilter,
        },
    },
    errors::GraphError,
    prelude::*,
};
use raphtory_api::core::{entities::VID, utils::time::IntoTime};
use std::cmp::Ordering;

/// A lazy collection of nodes from a graph view. Supports all the same view
/// transforms as `Graph` (window, layer, filter, ...) plus pagination and
/// sorting. Iterated via `list` / `page` / `ids` / `count`.
#[derive(ResolvedObject, Clone)]
#[graphql(name = "Nodes")]
pub(crate) struct GqlNodes {
    pub(crate) nn: Nodes<'static, DynamicGraph, DynamicGraph, DynNodeFilter>,
}

impl GqlNodes {
    fn update<N: IntoDynNodes>(&self, nodes: N) -> Self {
        GqlNodes::new(nodes)
    }
}

impl GqlNodes {
    pub(crate) fn new<N: IntoDynNodes>(nodes: N) -> Self {
        Self {
            nn: nodes.into_dyn(),
        }
    }

    fn iter(&self) -> Box<dyn Iterator<Item = GqlNode> + '_> {
        let iter = self.nn.iter_owned_unlocked().map(GqlNode::from);
        Box::new(iter)
    }
}

#[ResolvedObjectFields]
impl GqlNodes {
    ////////////////////////
    // LAYERS AND WINDOWS //
    ////////////////////////

    /// Return a view of the nodes containing only the default edge layer.
    async fn default_layer(&self) -> Self {
        self.update(self.nn.default_layer())
    }

    /// Return a view of the nodes containing all layers specified.

    async fn layers(
        &self,
        #[graphql(desc = "Layer names to include.")] names: Vec<String>,
    ) -> Self {
        self.update(self.nn.valid_layers(names))
    }

    /// Return a view of the nodes containing all layers except those specified.

    async fn exclude_layers(
        &self,
        #[graphql(desc = "Layer names to exclude.")] names: Vec<String>,
    ) -> Self {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.update(self_clone.nn.exclude_valid_layers(names))).await
    }

    /// Return a view of the nodes containing the specified layer.

    async fn layer(&self, #[graphql(desc = "Layer name to include.")] name: String) -> Self {
        self.update(self.nn.valid_layers(name))
    }

    /// Return a view of the nodes containing all layers except those specified.

    async fn exclude_layer(
        &self,
        #[graphql(desc = "Layer name to exclude.")] name: String,
    ) -> Self {
        self.update(self.nn.exclude_valid_layers(name))
    }

    /// Creates a WindowSet with the specified window size and optional step using a rolling window.
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
    ) -> Result<GqlNodesWindowSet, GraphError> {
        let window = window.try_into_interval()?;
        let step = step.map(|x| x.try_into_interval()).transpose()?;
        let ws = if let Some(unit) = alignment_unit {
            self.nn.rolling_aligned(window, step, unit.into())?
        } else {
            self.nn.rolling(window, step)?
        };
        Ok(GqlNodesWindowSet::new(ws))
    }

    /// Creates a WindowSet with the specified step size using an expanding window.
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
    ) -> Result<GqlNodesWindowSet, GraphError> {
        let step = step.try_into_interval()?;
        let ws = if let Some(unit) = alignment_unit {
            self.nn.expanding_aligned(step, unit.into())?
        } else {
            self.nn.expanding(step)?
        };
        Ok(GqlNodesWindowSet::new(ws))
    }

    /// Create a view of the node including all events between the specified start (inclusive) and end (exclusive).

    async fn window(
        &self,
        #[graphql(desc = "Inclusive lower bound.")] start: GqlTimeInput,
        #[graphql(desc = "Exclusive upper bound.")] end: GqlTimeInput,
    ) -> Self {
        self.update(self.nn.window(start.into_time(), end.into_time()))
    }

    /// Create a view of the nodes including all events at a specified time.

    async fn at(
        &self,
        #[graphql(desc = "Instant to pin the view to.")] time: GqlTimeInput,
    ) -> Self {
        self.update(self.nn.at(time.into_time()))
    }

    /// Create a view of the nodes including all events at the latest time.
    async fn latest(&self) -> Self {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.update(self_clone.nn.latest())).await
    }

    /// Create a view of the nodes including all events that are valid at the specified time.

    async fn snapshot_at(
        &self,
        #[graphql(desc = "Instant at which entities must be valid.")] time: GqlTimeInput,
    ) -> Self {
        self.update(self.nn.snapshot_at(time.into_time()))
    }

    /// Create a view of the nodes including all events that are valid at the latest time.
    async fn snapshot_latest(&self) -> Self {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.update(self_clone.nn.snapshot_latest())).await
    }

    /// Create a view of the nodes including all events before specified end time (exclusive).

    async fn before(&self, #[graphql(desc = "Exclusive upper bound.")] time: GqlTimeInput) -> Self {
        self.update(self.nn.before(time.into_time()))
    }

    /// Create a view of the nodes including all events after the specified start time (exclusive).

    async fn after(&self, #[graphql(desc = "Exclusive lower bound.")] time: GqlTimeInput) -> Self {
        self.update(self.nn.after(time.into_time()))
    }

    /// Shrink both the start and end of the window.

    async fn shrink_window(
        &self,
        #[graphql(desc = "Proposed new start (TimeInput); ignored if it would widen the window.")]
        start: GqlTimeInput,
        #[graphql(desc = "Proposed new end (TimeInput); ignored if it would widen the window.")]
        end: GqlTimeInput,
    ) -> Self {
        self.update(self.nn.shrink_window(start.into_time(), end.into_time()))
    }

    /// Set the start of the window to the larger of a specified start time and self.start().

    async fn shrink_start(
        &self,
        #[graphql(desc = "Proposed new start (TimeInput); ignored if it would widen the window.")]
        start: GqlTimeInput,
    ) -> Self {
        self.update(self.nn.shrink_start(start.into_time()))
    }

    /// Set the end of the window to the smaller of a specified end and self.end().

    async fn shrink_end(
        &self,
        #[graphql(desc = "Proposed new end (TimeInput); ignored if it would widen the window.")]
        end: GqlTimeInput,
    ) -> Self {
        self.update(self.nn.shrink_end(end.into_time()))
    }

    /// Filter nodes by node type.

    async fn type_filter(
        &self,
        #[graphql(desc = "Node-type names to keep.")] node_types: Vec<String>,
    ) -> Self {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.update(self_clone.nn.type_filter(&node_types))).await
    }

    /// Apply a list of views in the given order and return the resulting nodes
    /// collection. Lets callers compose window, layer, filter, and snapshot
    /// operations in a single call.

    async fn apply_views(
        &self,
        #[graphql(
            desc = "Ordered list of view operations; each entry is a one-of variant (`window`, `layer`, `filter`, etc.) applied to the running result."
        )]
        views: Vec<NodesViewCollection>,
    ) -> Result<GqlNodes, GraphError> {
        let mut return_view: GqlNodes = GqlNodes::new(self.nn.clone());
        for view in views {
            return_view = match view {
                NodesViewCollection::DefaultLayer(apply) => {
                    if apply {
                        return_view.default_layer().await
                    } else {
                        return_view
                    }
                }
                NodesViewCollection::ExcludeLayer(layer) => return_view.exclude_layer(layer).await,
                NodesViewCollection::Layers(layers) => return_view.layers(layers).await,
                NodesViewCollection::ExcludeLayers(layers) => {
                    return_view.exclude_layers(layers).await
                }
                NodesViewCollection::Window(window) => {
                    return_view.window(window.start, window.end).await
                }
                NodesViewCollection::At(at) => return_view.at(at).await,
                NodesViewCollection::Latest(apply) => {
                    if apply {
                        return_view.latest().await
                    } else {
                        return_view
                    }
                }
                NodesViewCollection::SnapshotLatest(apply) => {
                    if apply {
                        return_view.snapshot_latest().await
                    } else {
                        return_view
                    }
                }
                NodesViewCollection::SnapshotAt(at) => return_view.snapshot_at(at).await,
                NodesViewCollection::Before(time) => return_view.before(time).await,
                NodesViewCollection::After(time) => return_view.after(time).await,
                NodesViewCollection::ShrinkWindow(window) => {
                    return_view.shrink_window(window.start, window.end).await
                }
                NodesViewCollection::ShrinkStart(time) => return_view.shrink_start(time).await,
                NodesViewCollection::ShrinkEnd(time) => return_view.shrink_end(time).await,
                NodesViewCollection::NodeFilter(node_filter) => {
                    return_view.filter(node_filter).await?
                }
                NodesViewCollection::TypeFilter(types) => return_view.type_filter(types).await,
            }
        }

        Ok(return_view)
    }

    /////////////////
    //// Sorting ////
    /////////////////

    /// Sort the nodes. Multiple criteria are applied lexicographically (ties on the
    /// first key break to the second, etc.).

    async fn sorted(
        &self,
        #[graphql(
            desc = "Ordered list of sort keys. Each entry chooses exactly one of `id` / `time` / `property`, with an optional `reverse: true` to flip order."
        )]
        sort_bys: Vec<NodeSortBy>,
    ) -> Self {
        let self_clone = self.clone();
        blocking_compute(move || {
            let sorted: Index<VID> = self_clone
                .nn
                .iter()
                .sorted_by(|first_node, second_node| {
                    sort_bys
                        .iter()
                        .fold(Ordering::Equal, |current_ordering, sort_by| {
                            current_ordering.then_with(|| {
                                let ordering = if sort_by.id == Some(true) {
                                    first_node.id().partial_cmp(&second_node.id())
                                } else if let Some(sort_by_time) = sort_by.time.as_ref() {
                                    let (first_time, second_time) = match sort_by_time {
                                        SortByTime::Latest => {
                                            (first_node.latest_time(), second_node.latest_time())
                                        }
                                        SortByTime::Earliest => (
                                            first_node.earliest_time(),
                                            second_node.earliest_time(),
                                        ),
                                    };
                                    first_time.partial_cmp(&second_time)
                                } else if let Some(sort_by_property) = sort_by.property.as_ref() {
                                    let first_prop_maybe =
                                        first_node.properties().get(sort_by_property);
                                    let second_prop_maybe =
                                        second_node.properties().get(sort_by_property);
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
                        })
                })
                .map(|node_view| node_view.node)
                .collect();
            GqlNodes::new(self_clone.nn.indexed(sorted))
        })
        .await
    }

    ////////////////////////
    //// TIME QUERIES //////
    ////////////////////////

    /// Returns the start time of the window. Errors if there is no window.
    async fn start(&self) -> GqlEventTime {
        self.nn.start().into()
    }

    /// Returns the end time of the window. Errors if there is no window.
    async fn end(&self) -> GqlEventTime {
        self.nn.end().into()
    }

    /////////////////
    //// List ///////
    /////////////////

    /// Number of nodes in the current view.
    async fn count(&self) -> usize {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.nn.len()).await
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
    ) -> Result<Vec<GqlNode>> {
        check_page_limit(ctx, limit)?;
        let self_clone = self.clone();
        Ok(blocking_compute(move || {
            let start = page_index.unwrap_or(0) * limit + offset.unwrap_or(0);
            self_clone.iter().skip(start).take(limit).collect()
        })
        .await)
    }

    /// Materialise every node in the view. Rejected by the server when bulk list
    /// endpoints are disabled; use `page` for paginated access instead.
    async fn list(&self, ctx: &Context<'_>) -> Result<Vec<GqlNode>> {
        check_list_allowed(ctx)?;
        let self_clone = self.clone();
        Ok(blocking_compute(move || self_clone.iter().collect()).await)
    }

    /// Every node's id (name) as a flat list of strings. Rejected by the server when
    /// bulk list endpoints are disabled.
    async fn ids(&self, ctx: &Context<'_>) -> Result<Vec<String>> {
        check_list_allowed(ctx)?;
        let self_clone = self.clone();
        Ok(
            blocking_compute(move || self_clone.nn.iter_unlocked().map(|nn| nn.name()).collect())
                .await,
        )
    }

    /// Narrow the collection to nodes matching `expr`. The filter sticks to the
    /// returned view — every subsequent traversal through these nodes (their
    /// neighbours, edges, properties) continues to see the filtered scope.
    ///
    /// Useful when you want one scoping rule to apply across the whole query.
    /// E.g. restricting everything to a specific week:
    ///
    /// ```text
    /// nodes { filter(expr: {window: {start: 1234, end: 5678}}) {
    ///   list { neighbours { list { name } } }   # neighbours still windowed
    /// } }
    /// ```
    ///
    /// Contrast with `select`, which applies here and is not carried through.

    async fn filter(
        &self,
        #[graphql(desc = "Composite node filter (by name, property, type, etc.).")]
        expr: GqlNodeFilter,
    ) -> Result<Self, GraphError> {
        let self_clone = self.clone();
        blocking_compute(move || {
            let filter: CompositeNodeFilter = expr.try_into()?;
            let filtered = self_clone.nn.filter(filter)?;
            Ok(self_clone.update(filtered.into_dyn()))
        })
        .await
    }

    /// Narrow the collection to nodes matching `expr`, but only at this step —
    /// subsequent traversals out of these nodes see the unfiltered graph again.
    ///
    /// Useful when you want different scopes at different hops. E.g. nodes
    /// active on Monday, then their neighbours active on Tuesday, then *those*
    /// neighbours active on Wednesday:
    ///
    /// ```text
    /// nodes { select(expr: {window: {...monday...}}) {
    ///   list { neighbours { select(expr: {window: {...tuesday...}}) {
    ///     list { neighbours { select(expr: {window: {...wednesday...}}) {
    ///       list { name }
    ///     } } }
    ///   } } }
    /// } }
    /// ```
    ///
    /// Contrast with `filter`, which persists the scope through subsequent ops.

    async fn select(
        &self,
        #[graphql(desc = "Composite node filter (by name, property, type, etc.).")]
        expr: GqlNodeFilter,
    ) -> Result<Self, GraphError> {
        let self_clone = self.clone();
        blocking_compute(move || {
            let filter: CompositeNodeFilter = expr.try_into()?;
            let filtered = self_clone.nn.select(filter)?;
            Ok(self_clone.update(filtered.into_dyn()))
        })
        .await
    }
}
