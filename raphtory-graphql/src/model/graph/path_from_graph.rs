use crate::{
    model::graph::{
        collection::{check_list_allowed, check_page_limit},
        filtering::{GqlFilter, GqlNodeFilter, PathFromNodeViewCollection},
        history::GqlHistory,
        nested_edges::GqlNestedEdges,
        path_from_node::GqlPathFromNode,
        timeindex::{GqlEventTime, GqlTimeInput},
    },
    rayon::blocking_compute,
};
use async_graphql::{Context, Error, Value as GqlValue};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields, Scalar, ScalarValue};
use raphtory::{
    db::{
        api::view::{filter_ops::Select, DynamicGraph, Filter},
        graph::{
            path::PathFromGraph,
            views::filter::model::{CompositeNodeFilter, DynFilter},
        },
    },
    errors::GraphError,
    prelude::*,
};
use raphtory_api::core::utils::time::IntoTime;

/// Output-only scalars for the columnar nested terminals below. They carry a
/// `[[..]]` result (one inner list per source node) in a SINGLE field so the
/// whole thing is computed in one `blocking_compute`, instead of the
/// `list { ids }` shape which resolves one `PathFromNode` object — and its own
/// `blocking_compute` — per source. The derive macro can't register a nested
/// list type directly, hence the custom scalar.
#[derive(Clone, Debug, Scalar)]
#[graphql(name = "NestedStringList")]
pub struct NestedStringList(pub Vec<Vec<String>>);

impl ScalarValue for NestedStringList {
    fn from_value(_value: GqlValue) -> Result<Self, Error> {
        Err(Error::new("NestedStringList is an output-only scalar"))
    }

    fn to_value(&self) -> GqlValue {
        GqlValue::List(
            self.0
                .iter()
                .map(|inner| {
                    GqlValue::List(inner.iter().map(|s| GqlValue::String(s.clone())).collect())
                })
                .collect(),
        )
    }
}

/// Like [`NestedStringList`] but for integer results (`[[Int]]`) — the
/// columnar `degree`/`inDegree`/`outDegree` nested terminals.
#[derive(Clone, Debug, Scalar)]
#[graphql(name = "NestedIntList")]
pub struct NestedIntList(pub Vec<Vec<i64>>);

impl ScalarValue for NestedIntList {
    fn from_value(_value: GqlValue) -> Result<Self, Error> {
        Err(Error::new("NestedIntList is an output-only scalar"))
    }

    fn to_value(&self) -> GqlValue {
        GqlValue::List(
            self.0
                .iter()
                .map(|inner| GqlValue::List(inner.iter().map(|n| GqlValue::from(*n)).collect()))
                .collect(),
        )
    }
}

/// A nested collection of nodes anchored to a source collection — the result of
/// collection-level traversals like `nodes.neighbours`, `inNeighbours`, or
/// `outNeighbours`. Each source node yields its own list of neighbour nodes, so
/// results are shaped as a list of per-source node lists. Supports all the usual
/// view transforms (window, layer, filter, ...) and can be chained to walk
/// further hops.
#[derive(ResolvedObject, Clone)]
#[graphql(name = "PathFromGraph")]
pub struct GqlPathFromGraph {
    pub(crate) nn: PathFromGraph<'static, DynamicGraph>,
}

impl GqlPathFromGraph {
    fn update<P: Into<PathFromGraph<'static, DynamicGraph>>>(&self, path: P) -> Self {
        GqlPathFromGraph::new(path)
    }
}

impl GqlPathFromGraph {
    pub(crate) fn new<P: Into<PathFromGraph<'static, DynamicGraph>>>(path: P) -> Self {
        Self { nn: path.into() }
    }

    /// Materialise the nested structure as one `GqlPathFromNode` per source
    /// node. Each `PathFromNode` carries that source's own neighbour list,
    /// so the GraphQL type stays a single-level `[PathFromNode!]!` — the
    /// per-source nesting is expressed via the object, not a `[[..]]` list
    /// (which the derive macro can't register).
    fn per_source(&self) -> Vec<GqlPathFromNode> {
        self.nn
            .iter()
            .map(|(_src, path)| GqlPathFromNode::new(path))
            .collect()
    }
}

#[ResolvedObjectFields]
impl GqlPathFromGraph {
    ////////////////////////
    // LAYERS AND WINDOWS //
    ////////////////////////

    /// Returns a view of PathFromGraph containing the specified layers, errors if any of the layers do not exist.

    pub async fn layers(
        &self,
        #[graphql(desc = "Layer names to include.")] names: Vec<String>,
    ) -> Self {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.update(self_clone.nn.valid_layers(names))).await
    }

    /// Return a view of PathFromGraph restricted to the default layer.
    pub async fn default_layer(&self) -> Self {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.update(self_clone.nn.default_layer())).await
    }

    /// Return a view of PathFromGraph containing all layers except the specified excluded layers, errors if any of the layers do not exist.

    pub async fn exclude_layers(
        &self,
        #[graphql(desc = "Layer names to exclude.")] names: Vec<String>,
    ) -> Self {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.update(self_clone.nn.exclude_valid_layers(names))).await
    }

    /// Return a view of PathFromGraph containing the specified layer, errors if the layer does not exist.

    pub async fn layer(&self, #[graphql(desc = "Layer name to include.")] name: String) -> Self {
        self.update(self.nn.valid_layers(name))
    }

    /// Return a view of PathFromGraph containing all layers except the specified excluded layer, errors if the layer does not exist.

    pub async fn exclude_layer(
        &self,
        #[graphql(desc = "Layer name to exclude.")] name: String,
    ) -> Self {
        self.update(self.nn.exclude_valid_layers(name))
    }

    /// Create a view of the PathFromGraph including all events between a specified start (inclusive) and end (exclusive).

    pub async fn window(
        &self,
        #[graphql(desc = "Inclusive lower bound.")] start: GqlTimeInput,
        #[graphql(desc = "Exclusive upper bound.")] end: GqlTimeInput,
    ) -> Self {
        self.update(self.nn.window(start.into_time(), end.into_time()))
    }

    /// Create a view of the PathFromGraph including all events at time.

    pub async fn at(
        &self,
        #[graphql(desc = "Instant to pin the view to.")] time: GqlTimeInput,
    ) -> Self {
        self.update(self.nn.at(time.into_time()))
    }

    /// Create a view of the PathFromGraph including all events that are valid at the latest time.
    pub async fn snapshot_latest(&self) -> Self {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.update(self_clone.nn.snapshot_latest())).await
    }

    /// Create a view of the PathFromGraph including all events that are valid at the specified time.

    pub async fn snapshot_at(
        &self,
        #[graphql(desc = "Instant at which entities must be valid.")] time: GqlTimeInput,
    ) -> Self {
        self.update(self.nn.snapshot_at(time.into_time()))
    }

    /// Create a view of the PathFromGraph including all events at the latest time.
    pub async fn latest(&self) -> Self {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.update(self_clone.nn.latest())).await
    }

    /// Create a view of the PathFromGraph including all events before the specified end (exclusive).

    pub async fn before(
        &self,
        #[graphql(desc = "Exclusive upper bound.")] time: GqlTimeInput,
    ) -> Self {
        self.update(self.nn.before(time.into_time()))
    }

    /// Create a view of the PathFromGraph including all events after the specified start (exclusive).

    pub async fn after(
        &self,
        #[graphql(desc = "Exclusive lower bound.")] time: GqlTimeInput,
    ) -> Self {
        self.update(self.nn.after(time.into_time()))
    }

    /// Set the start of the window to the larger of the specified start and self.start().

    pub async fn shrink_start(
        &self,
        #[graphql(desc = "Proposed new start (TimeInput); ignored if it would widen the window.")]
        start: GqlTimeInput,
    ) -> Self {
        self.update(self.nn.shrink_start(start.into_time()))
    }

    /// Set the end of the window to the smaller of the specified end and self.end().

    pub async fn shrink_end(
        &self,
        #[graphql(desc = "Proposed new end (TimeInput); ignored if it would widen the window.")]
        end: GqlTimeInput,
    ) -> Self {
        self.update(self.nn.shrink_end(end.into_time()))
    }

    /// Narrow this path to neighbours whose node type is in the given set.

    pub async fn type_filter(
        &self,
        #[graphql(desc = "Node types to keep.")] node_types: Vec<String>,
    ) -> Self {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.update(self_clone.nn.type_filter(&node_types))).await
    }

    ////////////////////////
    //// TIME QUERIES //////
    ////////////////////////

    /// Returns the earliest time that this PathFromGraph is valid or None if the PathFromGraph is valid for all times.
    pub async fn start(&self) -> GqlEventTime {
        self.nn.start().into()
    }

    /// Returns the latest time that this PathFromGraph is valid or None if the PathFromGraph is valid for all times.
    pub async fn end(&self) -> GqlEventTime {
        self.nn.end().into()
    }

    /// Returns the size of the window covered by this view (`end - start`), or None if the view is unbounded.
    pub async fn window_size(&self) -> Option<i64> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.nn.window_size().map(|s| s as i64)).await
    }

    /// Check if a layer with the given name is present in this view.
    pub async fn has_layer(&self, name: String) -> bool {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.nn.has_layer(name)).await
    }

    /// Returns a single history object combining the time entries of all nodes in this view.
    pub async fn combined_history(&self) -> GqlHistory {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.nn.combined_history().into()).await
    }

    /////////////////
    //// List ///////
    /////////////////

    /// Number of source paths in this collection (one per source node).

    /// The property keys this collection reports: the first non-empty source's first member's registry
    /// view — the graph's registered property keys for the entity kind — or an
    /// empty list when there are no members. Mirrors the local collection
    /// `properties.keys()`.
    pub async fn property_keys(&self) -> Vec<String> {
        let self_clone = self.clone();
        blocking_compute(move || {
            self_clone
                .nn
                .properties()
                .filter_map(|(_, mut it)| it.next())
                .next()
                .map(|p| p.keys().map(|k| k.to_string()).collect())
                .unwrap_or_default()
        })
        .await
    }

    /// The metadata keys this collection reports: the first non-empty source's first member's registry
    /// view, or an empty list when there are no members. Mirrors the local
    /// collection `metadata.keys()`.
    pub async fn metadata_keys(&self) -> Vec<String> {
        let self_clone = self.clone();
        blocking_compute(move || {
            self_clone
                .nn
                .metadata()
                .filter_map(|(_, mut it)| it.next())
                .next()
                .map(|p| p.keys().map(|k| k.to_string()).collect())
                .unwrap_or_default()
        })
        .await
    }

    pub async fn count(&self) -> usize {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.nn.len()).await
    }

    /// Fetch one page of source paths up to a specified limit, optionally offset by a specified amount.
    /// The page_index sets the number of pages to skip (defaults to 0).
    ///
    /// For example, if page(5, 2, 1) is called, a page with 5 source paths, offset by 11 (2 pages of 5 + 1),
    /// will be returned. Each entry is the per-source list of neighbour nodes.

    pub async fn page(
        &self,
        ctx: &Context<'_>,
        #[graphql(desc = "Maximum number of source paths to return on this page.")] limit: usize,
        #[graphql(desc = "Extra source paths to skip on top of `pageIndex` paging (default 0).")]
        offset: Option<usize>,
        #[graphql(
            desc = "Zero-based page number; multiplies `limit` to determine where to start (default 0)."
        )]
        page_index: Option<usize>,
    ) -> async_graphql::Result<Vec<GqlPathFromNode>> {
        check_page_limit(ctx, limit)?;
        let self_clone = self.clone();
        Ok(blocking_compute(move || {
            let start = page_index.unwrap_or(0) * limit + offset.unwrap_or(0);
            self_clone
                .nn
                .iter()
                .map(|(_src, path)| GqlPathFromNode::new(path))
                .skip(start)
                .take(limit)
                .collect()
        })
        .await)
    }

    /// Materialise every source path — one `PathFromNode` per source node,
    /// each holding that source's neighbour list. Read `list { ids }` /
    /// `list { list { name } }` to reach the per-source neighbours. Rejected
    /// by the server when bulk list endpoints are disabled; use `page` for
    /// paginated access instead.
    pub async fn list(&self, ctx: &Context<'_>) -> async_graphql::Result<Vec<GqlPathFromNode>> {
        check_list_allowed(ctx)?;
        let self_clone = self.clone();
        Ok(blocking_compute(move || self_clone.per_source()).await)
    }

    /// Columnar `ids`: every source node's neighbour ids as `[[String]]`,
    /// computed in ONE `blocking_compute`. Fast-path equivalent of
    /// `list { ids }`, which resolves one `PathFromNode` object — and its own
    /// `blocking_compute` — per source.
    pub async fn ids(&self, ctx: &Context<'_>) -> async_graphql::Result<NestedStringList> {
        check_list_allowed(ctx)?;
        let self_clone = self.clone();
        Ok(blocking_compute(move || {
            NestedStringList(
                self_clone
                    .nn
                    .iter()
                    .map(|(_src, path)| path.name().collect())
                    .collect(),
            )
        })
        .await)
    }

    /// Columnar `sourceIds`: the id of the source node each path hangs off, in
    /// the same order as `ids` / `list` — one entry per source path, so entry
    /// `i` of `sourceIds` and entry `i` of `ids` describe the same pair. Lets a
    /// client reconstruct the `(source, path)` pairing in ONE request instead of
    /// one request per source. Computed in ONE `blocking_compute`, like `ids`.
    pub async fn source_ids(&self, ctx: &Context<'_>) -> async_graphql::Result<Vec<String>> {
        check_list_allowed(ctx)?;
        let self_clone = self.clone();
        Ok(
            blocking_compute(move || self_clone.nn.iter().map(|(src, _)| src.name()).collect())
                .await,
        )
    }

    /// Columnar `degree`: each source node's per-neighbour degrees as `[[Int]]`,
    /// computed in ONE `blocking_compute`. Fast-path for `list { degree }`.
    pub async fn degree(&self, ctx: &Context<'_>) -> async_graphql::Result<NestedIntList> {
        check_list_allowed(ctx)?;
        let self_clone = self.clone();
        Ok(blocking_compute(move || {
            NestedIntList(
                self_clone
                    .nn
                    .iter()
                    .map(|(_src, path)| path.degree().map(|d| d as i64).collect())
                    .collect(),
            )
        })
        .await)
    }

    /// Columnar `inDegree`. Fast-path for `list { inDegree }`.
    pub async fn in_degree(&self, ctx: &Context<'_>) -> async_graphql::Result<NestedIntList> {
        check_list_allowed(ctx)?;
        let self_clone = self.clone();
        Ok(blocking_compute(move || {
            NestedIntList(
                self_clone
                    .nn
                    .iter()
                    .map(|(_src, path)| path.in_degree().map(|d| d as i64).collect())
                    .collect(),
            )
        })
        .await)
    }

    /// Columnar `outDegree`. Fast-path for `list { outDegree }`.
    pub async fn out_degree(&self, ctx: &Context<'_>) -> async_graphql::Result<NestedIntList> {
        check_list_allowed(ctx)?;
        let self_clone = self.clone();
        Ok(blocking_compute(move || {
            NestedIntList(
                self_clone
                    .nn
                    .iter()
                    .map(|(_src, path)| path.out_degree().map(|d| d as i64).collect())
                    .collect(),
            )
        })
        .await)
    }

    /// Takes a specified selection of views and applies them in given order.

    pub async fn apply_views(
        &self,
        #[graphql(
            desc = "Ordered list of view operations; each entry is a one-of variant (`window`, `layer`, `filter`, ...) applied to the running result."
        )]
        views: Vec<PathFromNodeViewCollection>,
    ) -> Result<GqlPathFromGraph, GraphError> {
        let mut return_view: GqlPathFromGraph = self.clone();
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
    /// Contrast with `select`, which applies here and is not carried through.

    pub async fn filter(
        &self,
        #[graphql(
            desc = "Filter expression: node/edge predicates, graph views, or and/or/not combinations (and = intersection)."
        )]
        expr: GqlFilter,
    ) -> Result<Self, GraphError> {
        let self_clone = self.clone();
        blocking_compute(move || {
            let filter: DynFilter = expr.try_into()?;
            let filtered = self_clone.nn.filter(filter)?;
            Ok(self_clone.update(filtered.into_dyn()))
        })
        .await
    }

    /// Narrow the neighbour set to nodes matching `expr`, but only at this hop
    /// — further traversals out of these nodes see the unfiltered graph again.
    ///
    /// Contrast with `filter`, which persists the scope through subsequent ops.

    pub async fn select(
        &self,
        #[graphql(
            desc = "Filter expression: node predicates, graph views, or and/or/not combinations (and = intersection). Expressions that test edges are rejected."
        )]
        expr: GqlFilter,
    ) -> Result<Self, GraphError> {
        let self_clone = self.clone();
        blocking_compute(move || {
            let filtered = self_clone.nn.select(expr)?;
            Ok(self_clone.update(filtered.into_dyn()))
        })
        .await
    }

    /////////////////////
    //// Traversals /////
    /////////////////////

    /// Returns the neighbouring nodes reachable one further hop from each source
    /// path (both directions), as a nested `PathFromGraph`.
    pub async fn neighbours(&self, select: Option<GqlNodeFilter>) -> Result<Self, GraphError> {
        let base = self.nn.neighbours();
        if let Some(expr) = select {
            let nf: CompositeNodeFilter = expr.try_into()?;
            let narrowed = blocking_compute(move || base.select(nf)).await?;
            return Ok(GqlPathFromGraph::new(narrowed));
        }
        Ok(GqlPathFromGraph::new(base))
    }

    /// Returns the in-neighbours reachable one further hop from each source
    /// path, as a nested `PathFromGraph`.
    pub async fn in_neighbours(&self, select: Option<GqlNodeFilter>) -> Result<Self, GraphError> {
        let base = self.nn.in_neighbours();
        if let Some(expr) = select {
            let nf: CompositeNodeFilter = expr.try_into()?;
            let narrowed = blocking_compute(move || base.select(nf)).await?;
            return Ok(GqlPathFromGraph::new(narrowed));
        }
        Ok(GqlPathFromGraph::new(base))
    }

    /// Returns the out-neighbours reachable one further hop from each source
    /// path, as a nested `PathFromGraph`.
    pub async fn out_neighbours(&self, select: Option<GqlNodeFilter>) -> Result<Self, GraphError> {
        let base = self.nn.out_neighbours();
        if let Some(expr) = select {
            let nf: CompositeNodeFilter = expr.try_into()?;
            let narrowed = blocking_compute(move || base.select(nf)).await?;
            return Ok(GqlPathFromGraph::new(narrowed));
        }
        Ok(GqlPathFromGraph::new(base))
    }

    /// Returns the incident edges (both directions) of each source path, as a
    /// nested `NestedEdges` collection.
    pub async fn edges(&self) -> GqlNestedEdges {
        GqlNestedEdges::new(self.nn.edges())
    }

    /// Returns the incoming edges of each source path, as a nested `NestedEdges`
    /// collection.
    pub async fn in_edges(&self) -> GqlNestedEdges {
        GqlNestedEdges::new(self.nn.in_edges())
    }

    /// Returns the outgoing edges of each source path, as a nested `NestedEdges`
    /// collection.
    pub async fn out_edges(&self) -> GqlNestedEdges {
        GqlNestedEdges::new(self.nn.out_edges())
    }
}
