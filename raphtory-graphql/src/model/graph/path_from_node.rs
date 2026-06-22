use crate::{
    model::graph::{
        collection::{check_list_allowed, check_page_limit},
        filtering::{GqlNodeFilter, PathFromNodeViewCollection},
        node::GqlNode,
        timeindex::{GqlEventTime, GqlTimeInput},
        windowset::GqlPathFromNodeWindowSet,
        GqlAlignmentUnit, WindowDuration,
    },
    rayon::blocking_compute,
};
use async_graphql::Context;
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use raphtory::{
    core::utils::time::TryIntoInterval,
    db::{
        api::view::{filter_ops::NodeSelect, DynamicGraph, Filter},
        graph::{path::PathFromNode, views::filter::model::CompositeNodeFilter},
    },
    errors::GraphError,
    prelude::*,
};
use raphtory_api::core::utils::time::IntoTime;

/// A collection of nodes anchored to a source node — the result of traversals
/// like `node.neighbours`, `inNeighbours`, or `outNeighbours`. Supports all
/// the usual view transforms (window, layer, filter, ...) and can be chained
/// to walk further hops.
#[derive(ResolvedObject, Clone)]
#[graphql(name = "PathFromNode")]
pub(crate) struct GqlPathFromNode {
    pub(crate) nn: PathFromNode<'static, DynamicGraph>,
}

impl GqlPathFromNode {
    fn update<N: Into<PathFromNode<'static, DynamicGraph>>>(&self, nodes: N) -> Self {
        GqlPathFromNode::new(nodes)
    }
}

impl GqlPathFromNode {
    pub(crate) fn new<N: Into<PathFromNode<'static, DynamicGraph>>>(nodes: N) -> Self {
        Self { nn: nodes.into() }
    }

    fn iter(&self) -> Box<dyn Iterator<Item = GqlNode> + '_> {
        let iter = self.nn.iter().map(GqlNode::from);
        Box::new(iter)
    }
}

#[ResolvedObjectFields]
impl GqlPathFromNode {
    ////////////////////////
    // LAYERS AND WINDOWS //
    ////////////////////////

    /// Returns a view of PathFromNode containing the specified layer, errors if the layer does not exist.

    async fn layers(
        &self,
        #[graphql(desc = "Layer names to include.")] names: Vec<String>,
    ) -> Self {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.update(self_clone.nn.valid_layers(names))).await
    }

    /// Return a view of PathFromNode containing all layers except the specified excluded layers, errors if any of the layers do not exist.

    async fn exclude_layers(
        &self,
        #[graphql(desc = "Layer names to exclude.")] names: Vec<String>,
    ) -> Self {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.update(self_clone.nn.exclude_valid_layers(names))).await
    }

    /// Return a view of PathFromNode containing the layer specified layer, errors if the layer does not exist.

    async fn layer(&self, #[graphql(desc = "Layer name to include.")] name: String) -> Self {
        self.update(self.nn.valid_layers(name))
    }

    /// Return a view of PathFromNode containing all layers except the specified excluded layers, errors if any of the layers do not exist.

    async fn exclude_layer(
        &self,
        #[graphql(desc = "Layer name to exclude.")] name: String,
    ) -> Self {
        self.update(self.nn.exclude_valid_layers(name))
    }

    /// Creates a WindowSet with the given window size and optional step using a rolling window.
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
    ) -> Result<GqlPathFromNodeWindowSet, GraphError> {
        let window = window.try_into_interval()?;
        let step = step.map(|x| x.try_into_interval()).transpose()?;
        let ws = if let Some(unit) = alignment_unit {
            self.nn.rolling_aligned(window, step, unit.into())?
        } else {
            self.nn.rolling(window, step)?
        };
        Ok(GqlPathFromNodeWindowSet::new(ws))
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
    ) -> Result<GqlPathFromNodeWindowSet, GraphError> {
        let step = step.try_into_interval()?;
        let ws = if let Some(unit) = alignment_unit {
            self.nn.expanding_aligned(step, unit.into())?
        } else {
            self.nn.expanding(step)?
        };
        Ok(GqlPathFromNodeWindowSet::new(ws))
    }

    /// Create a view of the PathFromNode including all events between a specified start (inclusive) and end (exclusive).

    async fn window(
        &self,
        #[graphql(desc = "Inclusive lower bound.")] start: GqlTimeInput,
        #[graphql(desc = "Exclusive upper bound.")] end: GqlTimeInput,
    ) -> Self {
        self.update(self.nn.window(start.into_time(), end.into_time()))
    }

    /// Create a view of the PathFromNode including all events at time.

    async fn at(
        &self,
        #[graphql(desc = "Instant to pin the view to.")] time: GqlTimeInput,
    ) -> Self {
        self.update(self.nn.at(time.into_time()))
    }

    /// Create a view of the PathFromNode including all events that are valid at the latest time.
    async fn snapshot_latest(&self) -> Self {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.update(self_clone.nn.snapshot_latest())).await
    }

    /// Create a view of the PathFromNode including all events that are valid at the specified time.

    async fn snapshot_at(
        &self,
        #[graphql(desc = "Instant at which entities must be valid.")] time: GqlTimeInput,
    ) -> Self {
        self.update(self.nn.snapshot_at(time.into_time()))
    }

    /// Create a view of the PathFromNode including all events at the latest time.
    async fn latest(&self) -> Self {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.update(self_clone.nn.latest())).await
    }

    /// Create a view of the PathFromNode including all events before the specified end (exclusive).

    async fn before(&self, #[graphql(desc = "Exclusive upper bound.")] time: GqlTimeInput) -> Self {
        self.update(self.nn.before(time.into_time()))
    }

    /// Create a view of the PathFromNode including all events after the specified start (exclusive).

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

    /// Set the start of the window to the larger of the specified start and self.start().

    async fn shrink_start(
        &self,
        #[graphql(desc = "Proposed new start (TimeInput); ignored if it would widen the window.")]
        start: GqlTimeInput,
    ) -> Self {
        self.update(self.nn.shrink_start(start.into_time()))
    }

    /// Set the end of the window to the smaller of the specified end and self.end().

    async fn shrink_end(
        &self,
        #[graphql(desc = "Proposed new end (TimeInput); ignored if it would widen the window.")]
        end: GqlTimeInput,
    ) -> Self {
        self.update(self.nn.shrink_end(end.into_time()))
    }

    /// Narrow this path to neighbours whose node type is in the given set.

    async fn type_filter(
        &self,
        #[graphql(desc = "Node types to keep.")] node_types: Vec<String>,
    ) -> Self {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.update(self_clone.nn.type_filter(&node_types))).await
    }

    ////////////////////////
    //// TIME QUERIES //////
    ////////////////////////

    /// Returns the earliest time that this PathFromNode is valid or None if the PathFromNode is valid for all times.
    async fn start(&self) -> GqlEventTime {
        self.nn.start().into()
    }

    /// Returns the latest time that this PathFromNode is valid or None if the PathFromNode is valid for all times.
    async fn end(&self) -> GqlEventTime {
        self.nn.end().into()
    }

    /////////////////
    //// List ///////
    /////////////////

    /// Number of neighbour nodes reachable from the source in this view.
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
    ) -> async_graphql::Result<Vec<GqlNode>> {
        check_page_limit(ctx, limit)?;
        let self_clone = self.clone();
        Ok(blocking_compute(move || {
            let start = page_index.unwrap_or(0) * limit + offset.unwrap_or(0);
            self_clone.iter().skip(start).take(limit).collect()
        })
        .await)
    }

    /// Materialise every neighbour node in the path. Rejected by the server when
    /// bulk list endpoints are disabled; use `page` for paginated access instead.
    async fn list(&self, ctx: &Context<'_>) -> async_graphql::Result<Vec<GqlNode>> {
        check_list_allowed(ctx)?;
        let self_clone = self.clone();
        Ok(blocking_compute(move || self_clone.iter().collect()).await)
    }

    /// Every neighbour node's id (name) as a flat list of strings. Rejected by the
    /// server when bulk list endpoints are disabled.
    async fn ids(&self, ctx: &Context<'_>) -> async_graphql::Result<Vec<String>> {
        check_list_allowed(ctx)?;
        let self_clone = self.clone();
        Ok(blocking_compute(move || self_clone.nn.name().collect()).await)
    }

    /// Takes a specified selection of views and applies them in given order.

    async fn apply_views(
        &self,
        #[graphql(
            desc = "Ordered list of view operations; each entry is a one-of variant (`window`, `layer`, `filter`, ...) applied to the running result."
        )]
        views: Vec<PathFromNodeViewCollection>,
    ) -> Result<GqlPathFromNode, GraphError> {
        let mut return_view: GqlPathFromNode = self.clone();
        for view in views {
            return_view = match view {
                PathFromNodeViewCollection::Layers(layers) => return_view.layers(layers).await,
                PathFromNodeViewCollection::ExcludeLayers(layers) => {
                    return_view.exclude_layers(layers).await
                }
                PathFromNodeViewCollection::ExcludeLayer(layer) => {
                    return_view.exclude_layer(layer).await
                }
                PathFromNodeViewCollection::Window(window) => {
                    return_view.window(window.start, window.end).await
                }
                PathFromNodeViewCollection::ShrinkWindow(window) => {
                    return_view.shrink_window(window.start, window.end).await
                }
                PathFromNodeViewCollection::ShrinkStart(time) => {
                    return_view.shrink_start(time).await
                }
                PathFromNodeViewCollection::ShrinkEnd(time) => return_view.shrink_end(time).await,
                PathFromNodeViewCollection::At(time) => return_view.at(time).await,
                PathFromNodeViewCollection::SnapshotLatest(apply) => {
                    if apply {
                        return_view.snapshot_latest().await
                    } else {
                        return_view
                    }
                }
                PathFromNodeViewCollection::SnapshotAt(time) => return_view.snapshot_at(time).await,
                PathFromNodeViewCollection::Latest(apply) => {
                    if apply {
                        return_view.latest().await
                    } else {
                        return_view
                    }
                }
                PathFromNodeViewCollection::Before(time) => return_view.before(time).await,
                PathFromNodeViewCollection::After(time) => return_view.after(time).await,
            }
        }
        Ok(return_view)
    }

    /// Narrow the neighbour set to nodes matching `expr`. The filter sticks to
    /// the returned path — every subsequent traversal (further hops, edges,
    /// properties) continues to see the filtered scope.
    ///
    /// Useful when you want one scoping rule to apply across the whole query.
    /// E.g. restricting the whole traversal to a specific week:
    ///
    /// ```text
    /// node(name: "A") { neighbours { filter(expr: {window: {...week...}}) {
    ///   list { neighbours { list { name } } }   # further hops still windowed
    /// } } }
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

    /// Narrow the neighbour set to nodes matching `expr`, but only at this hop
    /// — further traversals out of these nodes see the unfiltered graph again.
    ///
    /// Useful when each hop needs a different scope. E.g. neighbours active on
    /// Monday, then *their* neighbours active on Tuesday:
    ///
    /// ```text
    /// node(name: "A") { neighbours { select(expr: {window: {...monday...}}) {
    ///   list { neighbours { select(expr: {window: {...tuesday...}}) {
    ///     list { name }
    ///   } } }
    /// } } }
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
