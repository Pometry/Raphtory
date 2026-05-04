use crate::{
    data::Data,
    graph::GraphWithVectors,
    model::{
        graph::{
            edge::GqlEdge,
            edges::GqlEdges,
            filtering::{GqlEdgeFilter, GqlGraphFilter, GqlNodeFilter, GraphViewCollection},
            index::GqlIndexSpec,
            node::GqlNode,
            node_id::GqlNodeId,
            nodes::GqlNodes,
            property::{GqlMetadata, GqlProperties},
            timeindex::{GqlEventTime, GqlTimeInput},
            windowset::GqlGraphWindowSet,
            GqlAlignmentUnit, WindowDuration,
        },
        plugins::graph_algorithm_plugin::GraphAlgorithmPlugin,
        schema::graph_schema::GraphSchema,
    },
    paths::{ExistingGraphFolder, PathValidationError, ValidGraphPaths},
    rayon::blocking_compute,
    GQLError,
};
use async_graphql::Context;
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields, Result};
use itertools::Itertools;
#[cfg(feature = "search")]
use raphtory::db::api::view::SearchableGraphOps;
use raphtory::{
    core::{
        entities::nodes::node_ref::{AsNodeRef, NodeRef},
        utils::time::TryIntoInterval,
    },
    db::{
        api::view::{
            filter_ops::NodeSelect, DynamicGraph, EdgeSelect, Filter, IntoDynamic, NodeViewOps,
            StaticGraphViewOps, TimeOps,
        },
        graph::{
            node::NodeView,
            views::filter::model::{
                edge_filter::CompositeEdgeFilter, graph_filter::GraphFilter,
                node_filter::CompositeNodeFilter, DynView,
            },
        },
    },
    errors::GraphError,
    prelude::*,
};
use raphtory_api::core::{storage::timeindex::AsTime, utils::time::IntoTime};
use std::{
    collections::HashSet,
    convert::{Into, TryInto},
    sync::Arc,
};

/// A view of a Raphtory graph. Every field here returns either data from the
/// view or a derived view (`window`, `layer`, `at`, `filter`, ...) that you can
/// keep chaining. Views are cheap — they don't copy the underlying data.
#[derive(ResolvedObject, Clone)]
#[graphql(name = "Graph")]
pub(crate) struct GqlGraph {
    path: ExistingGraphFolder,
    graph: DynamicGraph,
}

impl From<GraphWithVectors> for GqlGraph {
    fn from(value: GraphWithVectors) -> Self {
        GqlGraph::new(value.folder, value.graph)
    }
}

impl GqlGraph {
    pub fn new<G: StaticGraphViewOps + IntoDynamic>(path: ExistingGraphFolder, graph: G) -> Self {
        Self {
            path,
            graph: graph.into_dynamic(),
        }
    }

    fn apply<F, G>(&self, graph_operation: F) -> Self
    where
        F: Fn(&DynamicGraph) -> G,
        G: StaticGraphViewOps + IntoDynamic,
    {
        Self {
            path: self.path.clone(),
            graph: graph_operation(&self.graph).into_dynamic(),
        }
    }
}

#[ResolvedObjectFields]
impl GqlGraph {
    ////////////////////////
    // LAYERS AND WINDOWS //
    ////////////////////////

    /// Returns the names of all layers in the graphview.
    /// Distinct layer names observed in the current view — any layer that has at
    /// least one edge event visible here. Excludes layers that exist elsewhere in
    /// the graph but whose edges have been filtered out.
    async fn unique_layers(&self) -> Vec<String> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.graph.unique_layers().map_into().collect()).await
    }

    /// View restricted to the default layer — where nodes and edges end up
    /// when `addNode` / `addEdge` is called without a `layer` argument.
    /// Useful for separating "unlayered" base-graph events from named-layer
    /// ones.
    async fn default_layer(&self) -> GqlGraph {
        self.apply(|g| g.default_layer())
    }

    /// View restricted to the named layers. Updates on any other layer are hidden;
    /// if that leaves a node or edge with no updates left, it disappears from the
    /// view.

    async fn layers(
        &self,
        #[graphql(desc = "Layer names to include.")] names: Vec<String>,
    ) -> GqlGraph {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.apply(|g| g.valid_layers(names.clone()))).await
    }

    /// View with the named layers hidden. Updates on those layers are removed; if
    /// that leaves a node or edge with no updates left, it disappears from the
    /// view.

    async fn exclude_layers(
        &self,
        #[graphql(desc = "Layer names to exclude.")] names: Vec<String>,
    ) -> GqlGraph {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.apply(|g| g.exclude_valid_layers(names.clone()))).await
    }

    /// View restricted to a single layer. Convenience form of
    /// `layers(names: [name])` — updates on any other layer are hidden, and
    /// entities with nothing left disappear.

    async fn layer(&self, #[graphql(desc = "Layer name to include.")] name: String) -> GqlGraph {
        self.apply(|g| g.valid_layers(name.clone()))
    }

    /// View with one layer hidden. Convenience form of
    /// `excludeLayers(names: [name])` — updates on that layer are removed, and
    /// entities with nothing left disappear.

    async fn exclude_layer(
        &self,
        #[graphql(desc = "Layer name to exclude.")] name: String,
    ) -> GqlGraph {
        self.apply(|g| g.exclude_valid_layers(name.clone()))
    }

    /// View restricted to a chosen set of nodes and the edges between them. Edges
    /// connecting a selected node to a non-selected node are hidden.

    async fn subgraph(
        &self,
        #[graphql(desc = "Node ids to keep.")] nodes: Vec<GqlNodeId>,
    ) -> GqlGraph {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.apply(|g| g.subgraph(nodes.clone()))).await
    }

    /// View containing only valid edges — for persistent graphs this drops edges
    /// whose most recent event is a deletion at the latest time of the current
    /// view (a later re-addition would keep them). On event graphs this is a
    /// no-op.
    async fn valid(&self) -> GqlGraph {
        self.apply(|g| g.valid())
    }

    /// View restricted to nodes with the given node types.

    async fn subgraph_node_types(
        &self,
        #[graphql(desc = "Node types to include.")] node_types: Vec<String>,
    ) -> GqlGraph {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.apply(|g| g.subgraph_node_types(node_types.clone())))
            .await
    }

    /// View with a set of nodes removed (along with any edges touching them).

    async fn exclude_nodes(
        &self,
        #[graphql(desc = "Node ids to exclude.")] nodes: Vec<GqlNodeId>,
    ) -> GqlGraph {
        let self_clone = self.clone();
        blocking_compute(move || {
            let nodes: Vec<NodeRef> = nodes.iter().map(|v| v.as_node_ref()).collect();
            self_clone.apply(|g| g.exclude_nodes(nodes.clone()))
        })
        .await
    }

    /// Creates a rolling window with the specified window size and an optional step.
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
    ) -> Result<GqlGraphWindowSet, GraphError> {
        let window = window.try_into_interval()?;
        let step = step.map(|x| x.try_into_interval()).transpose()?;
        let ws = if let Some(unit) = alignment_unit {
            self.graph.rolling_aligned(window, step, unit.into())?
        } else {
            self.graph.rolling(window, step)?
        };
        Ok(GqlGraphWindowSet::new(ws, self.path.clone()))
    }

    /// Creates an expanding window with the specified step size.
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
    ) -> Result<GqlGraphWindowSet, GraphError> {
        let step = step.try_into_interval()?;
        let ws = if let Some(unit) = alignment_unit {
            self.graph.expanding_aligned(step, unit.into())?
        } else {
            self.graph.expanding(step)?
        };
        Ok(GqlGraphWindowSet::new(ws, self.path.clone()))
    }

    /// Return a graph containing only the activity between start and end, by default raphtory stores times in milliseconds from the unix epoch.

    async fn window(
        &self,
        #[graphql(desc = "Inclusive lower bound.")] start: GqlTimeInput,
        #[graphql(desc = "Exclusive upper bound.")] end: GqlTimeInput,
    ) -> GqlGraph {
        let start = start.into_time();
        let end = end.into_time();
        self.apply(|g| g.window(start, end))
    }

    /// Creates a view including all events at a specified time.

    async fn at(
        &self,
        #[graphql(desc = "Instant to pin the view to.")] time: GqlTimeInput,
    ) -> GqlGraph {
        let time = time.into_time();
        self.apply(|g| g.at(time))
    }

    /// Creates a view including all events at the latest time.
    async fn latest(&self) -> GqlGraph {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.apply(|g| g.latest())).await
    }

    /// Create a view including all events that are valid at the specified time.

    async fn snapshot_at(
        &self,
        #[graphql(desc = "Instant at which entities must be valid.")] time: GqlTimeInput,
    ) -> GqlGraph {
        let time = time.into_time();
        self.apply(|g| g.snapshot_at(time))
    }

    /// Create a view including all events that are valid at the latest time.
    async fn snapshot_latest(&self) -> GqlGraph {
        self.apply(|g| g.snapshot_latest())
    }

    /// Create a view including all events before a specified end (exclusive).

    async fn before(
        &self,
        #[graphql(desc = "Exclusive upper bound.")] time: GqlTimeInput,
    ) -> GqlGraph {
        let time = time.into_time();
        self.apply(|g| g.before(time))
    }

    /// Create a view including all events after a specified start (exclusive).

    async fn after(
        &self,
        #[graphql(desc = "Exclusive lower bound.")] time: GqlTimeInput,
    ) -> GqlGraph {
        let time = time.into_time();
        self.apply(|g| g.after(time))
    }

    /// Shrink both the start and end of the window. The new bounds are taken as the
    /// intersection with the current window; this never widens the view.

    async fn shrink_window(
        &self,
        #[graphql(desc = "Proposed new start (TimeInput); ignored if before the current start.")]
        start: GqlTimeInput,
        #[graphql(desc = "Proposed new end (TimeInput); ignored if after the current end.")]
        end: GqlTimeInput,
    ) -> Self {
        let start = start.into_time();
        let end = end.into_time();
        self.apply(|g| g.shrink_window(start, end))
    }

    /// Set the start of the window to the larger of the specified value or current start.

    async fn shrink_start(
        &self,
        #[graphql(
            desc = "Proposed new start (TimeInput); has no effect if it would widen the window."
        )]
        start: GqlTimeInput,
    ) -> Self {
        let start = start.into_time();
        self.apply(|g| g.shrink_start(start))
    }

    /// Set the end of the window to the smaller of the specified value or current end.

    async fn shrink_end(
        &self,
        #[graphql(
            desc = "Proposed new end (TimeInput); has no effect if it would widen the window."
        )]
        end: GqlTimeInput,
    ) -> Self {
        let end = end.into_time();
        self.apply(|g| g.shrink_end(end))
    }

    ////////////////////////
    //// TIME QUERIES //////
    ////////////////////////

    /// Filesystem creation timestamp (epoch millis) of the graph's on-disk folder
    /// — i.e. when this graph was first saved to the server, not when its earliest
    /// event occurred. Use `earliestTime` for the latter.
    async fn created(&self) -> Result<i64> {
        Ok(self.path.created_async().await?)
    }

    /// Returns the graph's last opened timestamp according to system time.
    async fn last_opened(&self) -> Result<i64> {
        Ok(self.path.last_opened_async().await?)
    }

    /// Returns the graph's last updated timestamp.
    async fn last_updated(&self) -> Result<i64> {
        Ok(self.path.last_updated_async().await?)
    }

    /// Returns the time entry of the earliest activity in the graph.
    async fn earliest_time(&self) -> Result<GqlEventTime> {
        let self_clone = self.clone();
        Ok(blocking_compute(move || self_clone.graph.earliest_time().into()).await)
    }

    /// Returns the time entry of the latest activity in the graph.
    async fn latest_time(&self) -> Result<GqlEventTime> {
        let self_clone = self.clone();
        Ok(blocking_compute(move || self_clone.graph.latest_time().into()).await)
    }

    /// Returns the start time of the window. Errors if there is no window.
    async fn start(&self) -> Result<GqlEventTime> {
        Ok(self.graph.start().into())
    }

    /// Returns the end time of the window. Errors if there is no window.
    async fn end(&self) -> Result<GqlEventTime> {
        Ok(self.graph.end().into())
    }

    /// The earliest time at which any edge in this graph is valid.
    ///
    /// * `includeNegative` — if false, edge events with a timestamp `< 0` are
    ///   skipped when computing the minimum. Defaults to true.
    async fn earliest_edge_time(
        &self,
        #[graphql(
            desc = "If false, edge events with a timestamp `< 0` are skipped when computing the minimum. Defaults to true."
        )]
        include_negative: Option<bool>,
    ) -> Result<GqlEventTime> {
        let self_clone = self.clone();
        Ok(blocking_compute(move || {
            let include_negative = include_negative.unwrap_or(true);
            self_clone
                .graph
                .edges()
                .earliest_time()
                .into_iter()
                .filter_map(|edge_time| edge_time.filter(|&time| include_negative || time.t() >= 0))
                .min()
                .into()
        })
        .await)
    }

    /// The latest time at which any edge in this graph is valid.

    async fn latest_edge_time(
        &self,
        #[graphql(
            desc = "If false, edge events with a timestamp `< 0` are skipped when computing the maximum. Defaults to true."
        )]
        include_negative: Option<bool>,
    ) -> Result<GqlEventTime> {
        let self_clone = self.clone();
        Ok(blocking_compute(move || {
            let include_negative = include_negative.unwrap_or(true);
            self_clone
                .graph
                .edges()
                .latest_time()
                .into_iter()
                .filter_map(|edge_time| edge_time.filter(|&time| include_negative || time.t() >= 0))
                .max()
                .into()
        })
        .await)
    }

    ////////////////////////
    //////// COUNTERS //////
    ////////////////////////

    /// Returns the number of edges in the graph.
    ///
    /// Returns:
    ///     int:
    async fn count_edges(&self) -> usize {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.graph.count_edges()).await
    }

    /// Returns the number of temporal edges in the graph.
    async fn count_temporal_edges(&self) -> usize {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.graph.count_temporal_edges()).await
    }

    /// Returns the number of nodes in the graph.
    ///
    /// Optionally takes a list of node ids to return a subset.
    async fn count_nodes(&self) -> usize {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.graph.count_nodes()).await
    }

    ////////////////////////
    //// EXISTS CHECKERS ///
    ////////////////////////

    /// Returns true if a node with the given id exists in this view.

    async fn has_node(
        &self,
        #[graphql(desc = "Node id to look up.")] name: GqlNodeId,
    ) -> Result<bool> {
        Ok(self.graph.has_node(name))
    }

    /// Returns true if an edge exists between `src` and `dst` in this view, optionally
    /// restricted to a single layer.

    async fn has_edge(
        &self,
        #[graphql(desc = "Source node id.")] src: GqlNodeId,
        #[graphql(desc = "Destination node id.")] dst: GqlNodeId,
        #[graphql(
            desc = "Optional; if provided, only checks whether the edge exists on this layer. If null or omitted, any layer counts."
        )]
        layer: Option<String>,
    ) -> Result<bool> {
        Ok(match layer {
            Some(name) => self
                .graph
                .layers(name)
                .map(|l| l.has_edge(src, dst))
                .unwrap_or(false),
            None => self.graph.has_edge(src, dst),
        })
    }

    ////////////////////////
    //////// GETTERS ///////
    ////////////////////////

    /// Look up a single node by id. Returns null if the node doesn't exist in this
    /// view.

    async fn node(&self, #[graphql(desc = "Node id.")] name: GqlNodeId) -> Result<Option<GqlNode>> {
        Ok(self.graph.node(name).map(|node| node.into()))
    }

    /// All nodes in this view, optionally narrowed by a filter.

    async fn nodes(
        &self,
        #[graphql(
            desc = "Optional node filter (by name, property, type, etc.). If omitted, every node in the view is returned."
        )]
        select: Option<GqlNodeFilter>,
    ) -> Result<GqlNodes> {
        let nn = self.graph.nodes();

        if let Some(sel) = select {
            let nf: CompositeNodeFilter = sel.try_into()?;
            let narrowed = blocking_compute({
                let nn_clone = nn.clone();
                move || nn_clone.select(nf)
            })
            .await?;
            return Ok(GqlNodes::new(narrowed));
        }

        Ok(GqlNodes::new(nn))
    }

    /// Look up a single edge by its endpoint ids. Returns null if no edge exists
    /// between `src` and `dst` in this view.

    async fn edge(
        &self,
        #[graphql(desc = "Source node id.")] src: GqlNodeId,
        #[graphql(desc = "Destination node id.")] dst: GqlNodeId,
    ) -> Result<Option<GqlEdge>> {
        Ok(self.graph.edge(src, dst).map(|e| e.into()))
    }

    /// All edges in this view, optionally narrowed by a filter.

    async fn edges<'a>(
        &self,
        #[graphql(
            desc = "Optional edge filter (by property, layer, src/dst, etc.). If omitted, every edge in the view is returned."
        )]
        select: Option<GqlEdgeFilter>,
    ) -> Result<GqlEdges> {
        let base = self.graph.edges_unlocked();

        if let Some(sel) = select {
            let ef: CompositeEdgeFilter = sel.try_into()?;
            let narrowed = blocking_compute(move || base.select(ef)).await?;
            return Ok(GqlEdges::new(narrowed));
        }

        Ok(GqlEdges::new(base))
    }

    ////////////////////////
    /////// PROPERTIES /////
    ////////////////////////

    /// Returns the properties of the graph.
    async fn properties(&self) -> Result<GqlProperties> {
        Ok(self.graph.properties().into())
    }

    /// Returns the metadata of the graph.
    async fn metadata(&self) -> Result<GqlMetadata> {
        Ok(self.graph.metadata().into())
    }

    ////////////////////////
    // GRAPHQL SPECIFIC ////
    ////////////////////////

    //These name/path functions basically can only fail
    //if someone write non-utf characters as a filename

    /// Returns the graph name.
    async fn name(&self) -> Result<String, PathValidationError> {
        self.path.get_graph_name()
    }

    /// Returns path of graph.
    async fn path(&self) -> String {
        self.path.local_path().into()
    }

    /// Returns namespace of graph.
    async fn namespace(&self) -> String {
        self.path
            .local_path()
            .rsplit_once("/")
            .map_or("", |(prefix, _)| prefix)
            .to_string()
    }

    /// Returns the graph schema.
    async fn schema(&self) -> Result<GraphSchema> {
        let self_clone = self.clone();
        Ok(blocking_compute(move || GraphSchema::new(&self_clone.graph)).await)
    }

    /// Access registered graph algorithms (PageRank, shortest path, etc.) for this
    /// graph view. The set of available algorithms is defined by the plugin registry
    /// loaded at server startup.
    async fn algorithms(&self) -> GraphAlgorithmPlugin {
        self.graph.clone().into()
    }

    /// Nodes that are neighbours of every node in `selectedNodes`. Returns the
    /// intersection of each selected node's neighbour set (undirected).

    async fn shared_neighbours(
        &self,
        #[graphql(
            desc = "Node ids whose common neighbours you want. Returns an empty list if `selectedNodes` is empty or any id does not exist."
        )]
        selected_nodes: Vec<GqlNodeId>,
    ) -> Result<Vec<GqlNode>> {
        let self_clone = self.clone();
        Ok(blocking_compute(move || {
            if selected_nodes.is_empty() {
                return vec![];
            }

            let neighbours: Vec<HashSet<NodeView<DynamicGraph>>> = selected_nodes
                .iter()
                .filter_map(|n| self_clone.graph.node(n))
                .map(|n| {
                    n.neighbours()
                        .collect()
                        .iter()
                        .map(|vv| vv.clone())
                        .collect::<HashSet<NodeView<DynamicGraph>>>()
                })
                .collect();

            let intersection = neighbours.iter().fold(None, |acc, n| match acc {
                None => Some(n.clone()),
                Some(acc) => Some(acc.intersection(n).map(|vv| vv.clone()).collect()),
            });
            match intersection {
                Some(intersection) => intersection.into_iter().map(|vv| vv.into()).collect(),
                None => vec![],
            }
        })
        .await)
    }

    /// Copy all nodes and edges of the current graph view into another already-
    /// existing graph stored on the server. The destination graph is preserved
    /// — this only adds; it does not replace.

    async fn export_to<'a>(
        &self,
        ctx: &Context<'a>,
        #[graphql(desc = "Destination graph path relative to the root namespace.")] path: String,
    ) -> Result<bool> {
        let data = ctx.data_unchecked::<Data>();
        let other_g = data.get_graph(path.as_ref()).await?.graph;
        let g = self.graph.clone();
        blocking_compute(move || {
            other_g.import_nodes(g.nodes(), true)?;
            other_g.import_edges(g.edges(), true)?;
            Ok(true)
        })
        .await
    }

    /// Returns a filtered view of the graph. Applies a mixed node/edge filter
    /// expression and narrows nodes, edges, and their properties to what matches.

    async fn filter(
        &self,
        #[graphql(
            desc = "Optional composite filter combining node, edge, property, and metadata conditions. If omitted, applies the identity filter (equivalent to no filtering)."
        )]
        expr: Option<GqlGraphFilter>,
    ) -> Result<Self, GraphError> {
        let self_clone = self.clone();
        blocking_compute(move || {
            let filter: DynView = match expr {
                Some(f) => f.try_into()?,
                None => Arc::new(GraphFilter),
            };
            let filtered_graph = self_clone.graph.filter(filter)?;
            Ok(GqlGraph::new(
                self_clone.path.clone(),
                filtered_graph.into_dynamic(),
            ))
        })
        .await
    }

    /// Returns a graph view restricted to nodes that match the given filter; edges
    /// are kept only if both endpoints survive.

    async fn filter_nodes(
        &self,
        #[graphql(desc = "Composite node filter (by name, property, type, etc.).")]
        expr: GqlNodeFilter,
    ) -> Result<Self, GraphError> {
        let self_clone = self.clone();
        blocking_compute(move || {
            let filter: CompositeNodeFilter = expr.try_into()?;
            let filtered_graph = self_clone.graph.filter(filter)?;
            Ok(GqlGraph::new(
                self_clone.path.clone(),
                filtered_graph.into_dynamic(),
            ))
        })
        .await
    }

    /// Returns a graph view restricted to edges that match the given filter. Nodes
    /// remain in the view even if all their edges are filtered out.

    async fn filter_edges(
        &self,
        #[graphql(desc = "Composite edge filter (by property, layer, src/dst, etc.).")]
        expr: GqlEdgeFilter,
    ) -> Result<Self, GraphError> {
        let self_clone = self.clone();
        blocking_compute(move || {
            let filter: CompositeEdgeFilter = expr.try_into()?;
            let filtered_graph = self_clone.graph.filter(filter)?;
            Ok(GqlGraph::new(
                self_clone.path.clone(),
                filtered_graph.into_dynamic(),
            ))
        })
        .await
    }

    ////////////////////////
    // INDEX SEARCH     ////
    ////////////////////////

    /// (Experimental) Get index specification.
    async fn get_index_spec(&self) -> Result<GqlIndexSpec, GraphError> {
        #[cfg(feature = "search")]
        {
            let index_spec = self.graph.get_index_spec()?;
            let props = index_spec.props(&self.graph);

            Ok(GqlIndexSpec {
                node_metadata: props.node_metadata,
                node_properties: props.node_properties,
                edge_metadata: props.edge_metadata,
                edge_properties: props.edge_properties,
            })
        }
        #[cfg(not(feature = "search"))]
        {
            Err(GraphError::IndexingNotSupported.into())
        }
    }

    /// (Experimental) Searches for nodes which match the given filter
    /// expression. Uses Tantivy's exact search; requires the graph to have
    /// been indexed.

    async fn search_nodes(
        &self,
        #[graphql(desc = "Composite node filter (by name, property, type, etc.).")]
        filter: GqlNodeFilter,
        #[graphql(desc = "Maximum number of nodes to return.")] limit: usize,
        #[graphql(desc = "Number of matches to skip before returning results.")] offset: usize,
    ) -> Result<Vec<GqlNode>> {
        #[cfg(feature = "search")]
        {
            let self_clone = self.clone();
            blocking_compute(move || {
                let f: CompositeNodeFilter = filter.try_into()?;
                let nodes = self_clone.graph.search_nodes(f, limit, offset)?;
                let result = nodes.into_iter().map(|vv| vv.into()).collect();
                Ok(result)
            })
            .await
        }
        #[cfg(not(feature = "search"))]
        {
            Err(GraphError::IndexingNotSupported.into())
        }
    }

    /// (Experimental) Searches the index for edges which match the given
    /// filter expression. Uses Tantivy's exact search; requires the graph to
    /// have been indexed.

    async fn search_edges(
        &self,
        #[graphql(desc = "Composite edge filter (by property, layer, src/dst, etc.).")]
        filter: GqlEdgeFilter,
        #[graphql(desc = "Maximum number of edges to return.")] limit: usize,
        #[graphql(desc = "Number of matches to skip before returning results.")] offset: usize,
    ) -> Result<Vec<GqlEdge>> {
        #[cfg(feature = "search")]
        {
            let self_clone = self.clone();
            blocking_compute(move || {
                let f: CompositeEdgeFilter = filter.try_into()?;
                let edges = self_clone.graph.search_edges(f, limit, offset)?;
                let result = edges.into_iter().map(|vv| vv.into()).collect();
                Ok(result)
            })
            .await
        }
        #[cfg(not(feature = "search"))]
        {
            Err(GraphError::IndexingNotSupported.into())
        }
    }

    /// Apply a list of view operations in the given order and return the
    /// resulting graph view. Lets callers compose multiple view transforms
    /// (window, layer, filter, snapshot, ...) in a single call.

    async fn apply_views(
        &self,
        #[graphql(
            desc = "Ordered list of view operations; each entry is a one-of variant applied to the running result."
        )]
        views: Vec<GraphViewCollection>,
    ) -> Result<GqlGraph, GraphError> {
        let mut return_view: GqlGraph = GqlGraph::new(self.path.clone(), self.graph.clone());
        for view in views {
            return_view = match view {
                GraphViewCollection::DefaultLayer(apply) => {
                    if apply {
                        return_view.default_layer().await
                    } else {
                        return_view
                    }
                }
                GraphViewCollection::Layers(layers) => return_view.layers(layers).await,
                GraphViewCollection::ExcludeLayers(layers) => {
                    return_view.exclude_layers(layers).await
                }
                GraphViewCollection::ExcludeLayer(layer) => return_view.exclude_layer(layer).await,
                GraphViewCollection::Subgraph(nodes) => return_view.subgraph(nodes).await,
                GraphViewCollection::SubgraphNodeTypes(node_types) => {
                    return_view.subgraph_node_types(node_types).await
                }
                GraphViewCollection::ExcludeNodes(nodes) => return_view.exclude_nodes(nodes).await,
                GraphViewCollection::Valid(apply) => {
                    if apply {
                        return_view.valid().await
                    } else {
                        return_view
                    }
                }
                GraphViewCollection::Window(window) => {
                    return_view.window(window.start, window.end).await
                }
                GraphViewCollection::At(at) => return_view.at(at).await,
                GraphViewCollection::Latest(apply) => {
                    if apply {
                        return_view.latest().await
                    } else {
                        return_view
                    }
                }
                GraphViewCollection::SnapshotAt(at) => return_view.snapshot_at(at).await,
                GraphViewCollection::SnapshotLatest(apply) => {
                    if apply {
                        return_view.snapshot_latest().await
                    } else {
                        return_view
                    }
                }
                GraphViewCollection::Before(before) => return_view.before(before).await,
                GraphViewCollection::After(after) => return_view.after(after).await,
                GraphViewCollection::ShrinkWindow(window) => {
                    return_view.shrink_window(window.start, window.end).await
                }
                GraphViewCollection::ShrinkStart(start) => return_view.shrink_start(start).await,
                GraphViewCollection::ShrinkEnd(end) => return_view.shrink_end(end).await,
                GraphViewCollection::NodeFilter(filter) => return_view.filter_nodes(filter).await?,
                GraphViewCollection::EdgeFilter(filter) => return_view.filter_edges(filter).await?,
            };
        }
        Ok(return_view)
    }
}
