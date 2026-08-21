use crate::{
    model::graph::{
        collection::{check_list_allowed, check_page_limit},
        edges::GqlEdges,
        filtering::{EdgesViewCollection, GqlFilter},
        path_from_graph::GqlPathFromGraph,
        timeindex::{GqlEventTime, GqlTimeInput},
    },
    rayon::blocking_compute,
};
use async_graphql::Context;
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use raphtory::{
    db::{
        api::view::{DynamicGraph, EdgeSelect, Filter},
        graph::{edges::NestedEdges, views::filter::model::DynFilter},
    },
    errors::GraphError,
    prelude::*,
};
use raphtory_api::core::utils::time::IntoTime;

/// A nested collection of edges anchored to a source collection — the result of
/// collection-level traversals like `nodes.edges`, `inEdges`, or `outEdges`.
/// Each source node yields its own list of incident edges, so results are
/// shaped as a list of per-source edge collections. Supports the usual view
/// transforms (window, layer, filter, ...).
#[derive(ResolvedObject, Clone)]
#[graphql(name = "NestedEdges")]
pub struct GqlNestedEdges {
    pub(crate) edges: NestedEdges<'static, DynamicGraph>,
}

impl GqlNestedEdges {
    fn update<E: Into<NestedEdges<'static, DynamicGraph>>>(&self, edges: E) -> Self {
        GqlNestedEdges::new(edges)
    }
}

impl GqlNestedEdges {
    pub(crate) fn new<E: Into<NestedEdges<'static, DynamicGraph>>>(edges: E) -> Self {
        Self {
            edges: edges.into(),
        }
    }

    /// Materialise the nested structure as one `GqlEdges` per source node. Each
    /// `Edges` carries that source's own incident-edge list, so the GraphQL
    /// type stays a single-level `[Edges!]!` — the per-source nesting is
    /// expressed via the object, not a `[[..]]` list (which the derive macro
    /// can't register).
    fn per_source(&self) -> Vec<GqlEdges> {
        self.edges.iter().map(GqlEdges::new).collect()
    }
}

#[ResolvedObjectFields]
impl GqlNestedEdges {
    ////////////////////////
    // LAYERS AND WINDOWS //
    ////////////////////////

    /// Returns a collection containing only edges in the default edge layer.
    async fn default_layer(&self) -> Self {
        self.update(self.edges.default_layer())
    }

    /// Returns a collection containing only edges belonging to the listed layers.

    async fn layers(
        &self,
        #[graphql(desc = "Layer names to include.")] names: Vec<String>,
    ) -> Self {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.update(self_clone.edges.valid_layers(names))).await
    }

    /// Returns a collection containing edges belonging to all layers except the excluded list of layers.

    async fn exclude_layers(
        &self,
        #[graphql(desc = "Layer names to exclude.")] names: Vec<String>,
    ) -> Self {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.update(self_clone.edges.exclude_valid_layers(names)))
            .await
    }

    /// Returns a collection containing edges belonging to the specified layer.

    async fn layer(&self, #[graphql(desc = "Layer name to include.")] name: String) -> Self {
        self.update(self.edges.valid_layers(name))
    }

    /// Returns a collection containing edges belonging to all layers except the excluded layer specified.

    async fn exclude_layer(
        &self,
        #[graphql(desc = "Layer name to exclude.")] name: String,
    ) -> Self {
        self.update(self.edges.exclude_valid_layers(name))
    }

    /// Creates a view of the NestedEdges including all events between the specified start (inclusive) and end (exclusive).

    async fn window(
        &self,
        #[graphql(desc = "Inclusive lower bound.")] start: GqlTimeInput,
        #[graphql(desc = "Exclusive upper bound.")] end: GqlTimeInput,
    ) -> Self {
        self.update(self.edges.window(start.into_time(), end.into_time()))
    }

    /// Creates a view of the NestedEdges including all events at a specified time.

    async fn at(
        &self,
        #[graphql(desc = "Instant to pin the view to.")] time: GqlTimeInput,
    ) -> Self {
        self.update(self.edges.at(time.into_time()))
    }

    /// View showing only the latest state of each edge (equivalent to `at(latestTime)`).
    async fn latest(&self) -> Self {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.update(self_clone.edges.latest())).await
    }

    /// Creates a view of the NestedEdges including all events that are valid at time.

    async fn snapshot_at(
        &self,
        #[graphql(desc = "Instant at which entities must be valid.")] time: GqlTimeInput,
    ) -> Self {
        self.update(self.edges.snapshot_at(time.into_time()))
    }

    /// Creates a view of the NestedEdges including all events that are valid at the latest time.
    async fn snapshot_latest(&self) -> Self {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.update(self_clone.edges.snapshot_latest())).await
    }

    /// Creates a view of the NestedEdges including all events before a specified end (exclusive).

    async fn before(&self, #[graphql(desc = "Exclusive upper bound.")] time: GqlTimeInput) -> Self {
        self.update(self.edges.before(time.into_time()))
    }

    /// Creates a view of the NestedEdges including all events after a specified start (exclusive).

    async fn after(&self, #[graphql(desc = "Exclusive lower bound.")] time: GqlTimeInput) -> Self {
        self.update(self.edges.after(time.into_time()))
    }

    /// Shrinks both the start and end of the window.

    async fn shrink_window(
        &self,
        #[graphql(desc = "Proposed new start (TimeInput); ignored if it would widen the window.")]
        start: GqlTimeInput,
        #[graphql(desc = "Proposed new end (TimeInput); ignored if it would widen the window.")]
        end: GqlTimeInput,
    ) -> Self {
        self.update(self.edges.shrink_window(start.into_time(), end.into_time()))
    }

    /// Set the start of the window.

    async fn shrink_start(
        &self,
        #[graphql(desc = "Proposed new start (TimeInput); ignored if it would widen the window.")]
        start: GqlTimeInput,
    ) -> Self {
        self.update(self.edges.shrink_start(start.into_time()))
    }

    /// Set the end of the window.

    async fn shrink_end(
        &self,
        #[graphql(desc = "Proposed new end (TimeInput); ignored if it would widen the window.")]
        end: GqlTimeInput,
    ) -> Self {
        self.update(self.edges.shrink_end(end.into_time()))
    }

    /// Takes a specified selection of views and applies them in order given.

    async fn apply_views(
        &self,
        #[graphql(
            desc = "Ordered list of view operations; each entry is a one-of variant (`window`, `layer`, `filter`, ...) applied to the running result."
        )]
        views: Vec<EdgesViewCollection>,
    ) -> Result<GqlNestedEdges, GraphError> {
        let mut return_view: GqlNestedEdges = self.update(self.edges.clone());
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
                EdgesViewCollection::EdgeFilter(filter) => {
                    return_view.filter(GqlFilter::Edges(filter)).await?
                }
            }
        }

        Ok(return_view)
    }

    ////////////////////////
    //// TIME QUERIES //////
    ////////////////////////

    /// Returns the earliest time that this NestedEdges is valid or None if valid for all times.
    async fn start(&self) -> GqlEventTime {
        self.edges.start().into()
    }

    /// Returns the latest time that this NestedEdges is valid or None if valid for all times.
    async fn end(&self) -> GqlEventTime {
        self.edges.end().into()
    }

    /// Returns the size of the window covered by this view (`end - start`), or None if the view is unbounded.
    async fn window_size(&self) -> Option<i64> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.edges.window_size().map(|s| s as i64)).await
    }

    /// Check if a layer with the given name is present in this view.
    async fn has_layer(&self, name: String) -> bool {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.edges.has_layer(name)).await
    }

    /////////////////////
    //// Traversals /////
    /////////////////////

    /// Returns the source node of each edge, grouped per source node, as a
    /// nested `PathFromGraph`.
    async fn src(&self) -> GqlPathFromGraph {
        GqlPathFromGraph::new(self.edges.src())
    }

    /// Returns the destination node of each edge, grouped per source node, as a
    /// nested `PathFromGraph`.
    async fn dst(&self) -> GqlPathFromGraph {
        GqlPathFromGraph::new(self.edges.dst())
    }

    /// Returns the node at the other end of each edge (destination for
    /// out-edges, source for in-edges), grouped per source node, as a nested
    /// `PathFromGraph`.
    async fn nbr(&self) -> GqlPathFromGraph {
        GqlPathFromGraph::new(self.edges.nbr())
    }

    /// Expand each source's edges into one edge per update — mirrors the local
    /// `NestedEdges.explode`. The per-source nesting is preserved; only the
    /// inner edge lists fan out per event.
    async fn explode(&self) -> Self {
        self.update(self.edges.explode())
    }

    /// Expand each source's edges into one edge per layer — mirrors the local
    /// `NestedEdges.explode_layers`. Each resulting edge carries only the
    /// updates from its respective layer.
    async fn explode_layers(&self) -> Self {
        self.update(self.edges.explode_layers())
    }

    /////////////////
    //// List ///////
    /////////////////

    /// Number of source edge collections in this collection (one per source node).

    /// The property keys this collection reports: the first non-empty source's first member's registry
    /// view — the graph's registered property keys for the entity kind — or an
    /// empty list when there are no members. Mirrors the local collection
    /// `properties.keys()`.
    async fn property_keys(&self) -> Vec<String> {
        let self_clone = self.clone();
        blocking_compute(move || {
            self_clone
                .edges
                .properties()
                .filter_map(|mut it| it.next())
                .next()
                .map(|p| p.keys().map(|k| k.to_string()).collect())
                .unwrap_or_default()
        })
        .await
    }

    /// The metadata keys this collection reports: the first non-empty source's first member's registry
    /// view, or an empty list when there are no members. Mirrors the local
    /// collection `metadata.keys()`.
    async fn metadata_keys(&self) -> Vec<String> {
        let self_clone = self.clone();
        blocking_compute(move || {
            self_clone
                .edges
                .metadata()
                .filter_map(|mut it| it.next())
                .next()
                .map(|p| p.keys().map(|k| k.to_string()).collect())
                .unwrap_or_default()
        })
        .await
    }

    async fn count(&self) -> usize {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.edges.len()).await
    }

    /// Fetch one page of source edge collections up to a specified limit, optionally offset by a specified amount.
    /// The page_index sets the number of pages to skip (defaults to 0).
    ///
    /// For example, if page(5, 2, 1) is called, a page with 5 source collections, offset by 11 (2 pages of 5 + 1),
    /// will be returned. Each entry is the per-source list of incident edges.

    async fn page(
        &self,
        ctx: &Context<'_>,
        #[graphql(desc = "Maximum number of source edge collections to return on this page.")]
        limit: usize,
        #[graphql(
            desc = "Extra source edge collections to skip on top of `pageIndex` paging (default 0)."
        )]
        offset: Option<usize>,
        #[graphql(
            desc = "Zero-based page number; multiplies `limit` to determine where to start (default 0)."
        )]
        page_index: Option<usize>,
    ) -> async_graphql::Result<Vec<GqlEdges>> {
        check_page_limit(ctx, limit)?;
        let self_clone = self.clone();
        Ok(blocking_compute(move || {
            let start = page_index.unwrap_or(0) * limit + offset.unwrap_or(0);
            self_clone
                .edges
                .iter()
                .map(GqlEdges::new)
                .skip(start)
                .take(limit)
                .collect()
        })
        .await)
    }

    /// Materialise every source edge collection — one `Edges` per source node,
    /// each holding that source's incident-edge list. Read
    /// `list { list { src { name } dst { name } } }` to reach the per-source
    /// edges. Rejected by the server when bulk list endpoints are disabled; use
    /// `page` for paginated access instead.
    async fn list(&self, ctx: &Context<'_>) -> async_graphql::Result<Vec<GqlEdges>> {
        check_list_allowed(ctx)?;
        let self_clone = self.clone();
        Ok(blocking_compute(move || self_clone.per_source()).await)
    }

    /// Narrow the edge set to edges matching `expr`. The filter sticks to the
    /// returned collection — every subsequent traversal continues to see the
    /// filtered scope.
    ///
    /// Contrast with `select`, which applies here and is not carried through.

    async fn filter(
        &self,
        #[graphql(
            desc = "Filter expression: node/edge predicates, graph views, or and/or/not combinations (and = intersection)."
        )]
        expr: GqlFilter,
    ) -> Result<Self, GraphError> {
        let self_clone = self.clone();
        blocking_compute(move || {
            let filter: DynFilter = expr.try_into()?;
            let filtered = self_clone.edges.filter(filter)?;
            Ok(self_clone.update(filtered.into_dyn()))
        })
        .await
    }

    /// Narrow the edge set to edges matching `expr`, but only at this hop —
    /// further traversals out of these edges see the unfiltered graph again.
    ///
    /// Contrast with `filter`, which persists the scope through subsequent ops.

    async fn select(
        &self,
        #[graphql(
            desc = "Filter expression: node/edge predicates, graph views, or and/or/not combinations (and = intersection)."
        )]
        expr: GqlFilter,
    ) -> Result<Self, GraphError> {
        let self_clone = self.clone();
        blocking_compute(move || {
            let filtered = self_clone.edges.select(expr)?;
            Ok(self_clone.update(filtered))
        })
        .await
    }
}
