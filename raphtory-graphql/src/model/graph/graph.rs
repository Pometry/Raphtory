use crate::{
    data::Data,
    model::{
        graph::{
            edge::GqlEdge,
            edges::GqlEdges,
            filtering::{EdgeFilter, GraphViewCollection, NodeFilter},
            index::GqlIndexSpec,
            node::GqlNode,
            nodes::GqlNodes,
            property::{GqlMetadata, GqlProperties},
            windowset::GqlGraphWindowSet,
            WindowDuration,
            WindowDuration::{Duration, Epoch},
        },
        plugins::graph_algorithm_plugin::GraphAlgorithmPlugin,
        schema::graph_schema::GraphSchema,
    },
    paths::ExistingGraphFolder,
    rayon::blocking_compute,
};
use async_graphql::Context;
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use itertools::Itertools;
use raphtory::{
    core::entities::nodes::node_ref::{AsNodeRef, NodeRef},
    db::{
        api::{
            properties::dyn_props::DynProperties,
            view::{
                DynamicGraph, IntoDynamic, NodeViewOps, SearchableGraphOps, StaticGraphViewOps,
                TimeOps,
            },
        },
        graph::{
            node::NodeView,
            views::filter::model::{
                edge_filter::CompositeEdgeFilter, node_filter::CompositeNodeFilter,
            },
        },
    },
    errors::{GraphError, InvalidPathReason},
    prelude::*,
};
use std::{
    collections::HashSet,
    convert::{Into, TryInto},
    sync::Arc,
};

#[derive(ResolvedObject, Clone)]
#[graphql(name = "Graph")]
pub(crate) struct GqlGraph {
    path: ExistingGraphFolder,
    graph: DynamicGraph,
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
    async fn unique_layers(&self) -> Vec<String> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.graph.unique_layers().map_into().collect()).await
    }

    /// Returns a view containing only the default layer.
    async fn default_layer(&self) -> GqlGraph {
        self.apply(|g| g.default_layer())
    }

    /// Returns a view containing all the specified layers.
    async fn layers(&self, names: Vec<String>) -> GqlGraph {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.apply(|g| g.valid_layers(names.clone()))).await
    }

    /// Returns a view containing all layers except the specified excluded layers.
    async fn exclude_layers(&self, names: Vec<String>) -> GqlGraph {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.apply(|g| g.exclude_valid_layers(names.clone()))).await
    }

    /// Returns a view containing the layer specified.
    async fn layer(&self, name: String) -> GqlGraph {
        self.apply(|g| g.valid_layers(name.clone()))
    }

    /// Returns a view containing all layers except the specified excluded layer.
    async fn exclude_layer(&self, name: String) -> GqlGraph {
        self.apply(|g| g.exclude_valid_layers(name.clone()))
    }

    /// Returns a subgraph of a specified set of nodes which contains only the edges that connect nodes of the subgraph to each other.
    async fn subgraph(&self, nodes: Vec<String>) -> GqlGraph {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.apply(|g| g.subgraph(nodes.clone()))).await
    }

    /// Returns a view of the graph that only includes valid edges.
    async fn valid(&self) -> GqlGraph {
        self.apply(|g| g.valid())
    }

    /// Returns a subgraph filtered by the specified node types.
    async fn subgraph_node_types(&self, node_types: Vec<String>) -> GqlGraph {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.apply(|g| g.subgraph_node_types(node_types.clone())))
            .await
    }

    /// Returns a subgraph containing all nodes except the specified excluded nodes.
    async fn exclude_nodes(&self, nodes: Vec<String>) -> GqlGraph {
        let self_clone = self.clone();
        blocking_compute(move || {
            let nodes: Vec<NodeRef> = nodes.iter().map(|v| v.as_node_ref()).collect();
            self_clone.apply(|g| g.exclude_nodes(nodes.clone()))
        })
        .await
    }

    /// Creates a rolling window with the specified window size and an optional step.
    async fn rolling(
        &self,
        window: WindowDuration,
        step: Option<WindowDuration>,
    ) -> Result<GqlGraphWindowSet, GraphError> {
        match window {
            Duration(window_duration) => match step {
                Some(step) => match step {
                    Duration(step_duration) => Ok(GqlGraphWindowSet::new(
                        self.graph.rolling(window_duration, Some(step_duration))?,
                        self.path.clone(),
                    )),
                    Epoch(_) => Err(GraphError::MismatchedIntervalTypes),
                },
                None => Ok(GqlGraphWindowSet::new(
                    self.graph.rolling(window_duration, None)?,
                    self.path.clone(),
                )),
            },
            Epoch(window_duration) => match step {
                Some(step) => match step {
                    Duration(_) => Err(GraphError::MismatchedIntervalTypes),
                    Epoch(step_duration) => Ok(GqlGraphWindowSet::new(
                        self.graph.rolling(window_duration, Some(step_duration))?,
                        self.path.clone(),
                    )),
                },
                None => Ok(GqlGraphWindowSet::new(
                    self.graph.rolling(window_duration, None)?,
                    self.path.clone(),
                )),
            },
        }
    }

    /// Creates a expanding window with the specified step size.
    async fn expanding(&self, step: WindowDuration) -> Result<GqlGraphWindowSet, GraphError> {
        match step {
            Duration(step) => Ok(GqlGraphWindowSet::new(
                self.graph.expanding(step)?,
                self.path.clone(),
            )),
            Epoch(step) => Ok(GqlGraphWindowSet::new(
                self.graph.expanding(step)?,
                self.path.clone(),
            )),
        }
    }

    /// Return a graph containing only the activity between start and end, by default raphtory stores times in milliseconds from the unix epoch.
    async fn window(&self, start: i64, end: i64) -> GqlGraph {
        self.apply(|g| g.window(start, end))
    }

    /// Creates a view including all events at a specified time.
    async fn at(&self, time: i64) -> GqlGraph {
        self.apply(|g| g.at(time))
    }

    /// Creates a view including all events at the latest time.
    async fn latest(&self) -> GqlGraph {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.apply(|g| g.latest())).await
    }

    /// Create a view including all events that are valid at the specified time.
    async fn snapshot_at(&self, time: i64) -> GqlGraph {
        self.apply(|g| g.snapshot_at(time))
    }

    /// Create a view including all events that are valid at the latest time.
    async fn snapshot_latest(&self) -> GqlGraph {
        self.apply(|g| g.snapshot_latest())
    }

    /// Create a view including all events before a specified end (exclusive).
    async fn before(&self, time: i64) -> GqlGraph {
        self.apply(|g| g.before(time))
    }

    /// Create a view including all events after a specified start (exclusive).
    async fn after(&self, time: i64) -> GqlGraph {
        self.apply(|g| g.after(time))
    }

    /// Shrink both the start and end of the window.
    async fn shrink_window(&self, start: i64, end: i64) -> Self {
        self.apply(|g| g.shrink_window(start, end))
    }

    /// Set the start of the window to the larger of the specified value or current start.
    async fn shrink_start(&self, start: i64) -> Self {
        self.apply(|g| g.shrink_start(start))
    }

    /// Set the end of the window to the smaller of the specified value or current end.
    async fn shrink_end(&self, end: i64) -> Self {
        self.apply(|g| g.shrink_end(end))
    }

    ////////////////////////
    //// TIME QUERIES //////
    ////////////////////////

    /// Returns the timestamp for the creation of the graph.
    async fn created(&self) -> Result<i64, GraphError> {
        self.path.created_async().await
    }

    /// Returns the graph's last opened timestamp according to system time.
    async fn last_opened(&self) -> Result<i64, GraphError> {
        self.path.last_opened_async().await
    }

    /// Returns the graph's last updated timestamp.
    async fn last_updated(&self) -> Result<i64, GraphError> {
        self.path.last_updated_async().await
    }

    /// Returns the timestamp of the earliest activity in the graph.
    async fn earliest_time(&self) -> Option<i64> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.graph.earliest_time()).await
    }

    /// Returns the timestamp of the latest activity in the graph.
    async fn latest_time(&self) -> Option<i64> {
        let self_clone = self.clone();
        blocking_compute(move || self_clone.graph.latest_time()).await
    }

    /// Returns the start time of the window. Errors if there is no window.
    async fn start(&self) -> Option<i64> {
        self.graph.start()
    }

    /// Returns the end time of the window. Errors if there is no window.
    async fn end(&self) -> Option<i64> {
        self.graph.end()
    }

    /// Returns the earliest time that any edge in this graph is valid.
    async fn earliest_edge_time(&self, include_negative: Option<bool>) -> Option<i64> {
        let self_clone = self.clone();
        blocking_compute(move || {
            let include_negative = include_negative.unwrap_or(true);
            let all_edges = self_clone
                .graph
                .edges()
                .earliest_time()
                .into_iter()
                .filter_map(|edge_time| edge_time.filter(|&time| include_negative || time >= 0))
                .min();
            all_edges
        })
        .await
    }

    /// /// Returns the latest time that any edge in this graph is valid.
    async fn latest_edge_time(&self, include_negative: Option<bool>) -> Option<i64> {
        let self_clone = self.clone();
        blocking_compute(move || {
            let include_negative = include_negative.unwrap_or(true);
            let all_edges = self_clone
                .graph
                .edges()
                .latest_time()
                .into_iter()
                .filter_map(|edge_time| edge_time.filter(|&time| include_negative || time >= 0))
                .max();

            all_edges
        })
        .await
    }

    ////////////////////////
    //////// COUNTERS //////
    ////////////////////////

    /// Returns the number of edges in the graph.
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

    /// Returns true if the graph contains the specified node.
    async fn has_node(&self, name: String) -> bool {
        self.graph.has_node(name)
    }

    /// Returns true if the graph contains the specified edge. Edges are specified by providing a source and destination node id. You can restrict the search to a specified layer.
    async fn has_edge(&self, src: String, dst: String, layer: Option<String>) -> bool {
        match layer {
            Some(name) => self
                .graph
                .layers(name)
                .map(|l| l.has_edge(src, dst))
                .unwrap_or(false),
            None => self.graph.has_edge(src, dst),
        }
    }

    ////////////////////////
    //////// GETTERS ///////
    ////////////////////////

    /// Gets the node with the specified id.
    async fn node(&self, name: String) -> Option<GqlNode> {
        self.graph.node(name).map(|node| node.into())
    }

    /// Gets (optionally a subset of) the nodes in the graph.
    async fn nodes(&self, ids: Option<Vec<String>>) -> GqlNodes {
        let nodes = self.graph.nodes();
        match ids {
            None => GqlNodes::new(nodes),
            Some(ids) => GqlNodes::new(blocking_compute(move || nodes.id_filter(ids)).await),
        }
    }

    /// Gets the edge with the specified source and destination nodes.
    async fn edge(&self, src: String, dst: String) -> Option<GqlEdge> {
        self.graph.edge(src, dst).map(|e| e.into())
    }

    /// Gets the edges in the graph.
    async fn edges<'a>(&self) -> GqlEdges {
        GqlEdges::new(self.graph.edges())
    }

    ////////////////////////
    /////// PROPERTIES /////
    ////////////////////////

    /// Returns the properties of the graph.
    async fn properties(&self) -> GqlProperties {
        Into::<DynProperties>::into(self.graph.properties()).into()
    }

    /// Returns the metadata of the graph.
    async fn metadata(&self) -> GqlMetadata {
        self.graph.metadata().into()
    }

    ////////////////////////
    // GRAPHQL SPECIFIC ////
    ////////////////////////

    //These name/path functions basically can only fail
    //if someone write non-utf characters as a filename

    /// Returns the graph name.
    async fn name(&self) -> Result<String, GraphError> {
        self.path.get_graph_name()
    }

    /// Returns path of graph.
    async fn path(&self) -> Result<String, GraphError> {
        Ok(self
            .path
            .get_original_path()
            .to_str()
            .ok_or(InvalidPathReason::PathNotParsable(
                self.path.to_error_path(),
            ))?
            .to_owned())
    }

    /// Returns namespace of graph.
    async fn namespace(&self) -> Result<String, GraphError> {
        Ok(self
            .path
            .get_original_path()
            .parent()
            .and_then(|p| p.to_str().map(|s| s.to_string()))
            .ok_or(InvalidPathReason::PathNotParsable(
                self.path.to_error_path(),
            ))?
            .to_owned())
    }

    /// Returns the graph schema.
    async fn schema(&self) -> GraphSchema {
        let self_clone = self.clone();
        blocking_compute(move || GraphSchema::new(&self_clone.graph)).await
    }

    async fn algorithms(&self) -> GraphAlgorithmPlugin {
        self.graph.clone().into()
    }

    async fn shared_neighbours(&self, selected_nodes: Vec<String>) -> Vec<GqlNode> {
        let self_clone = self.clone();
        blocking_compute(move || {
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
        .await
    }

    /// Export all nodes and edges from this graph view to another existing graph
    async fn export_to<'a>(
        &self,
        ctx: &Context<'a>,
        path: String,
    ) -> Result<bool, Arc<GraphError>> {
        let data = ctx.data_unchecked::<Data>();
        let other_g = data.get_graph(path.as_ref()).await?.0;
        let g = self.graph.clone();
        blocking_compute(move || {
            other_g.import_nodes(g.nodes(), true)?;
            other_g.import_edges(g.edges(), true)?;
            other_g.write_updates()?;
            Ok(true)
        })
        .await
    }

    async fn node_filter(&self, filter: NodeFilter) -> Result<Self, GraphError> {
        let self_clone = self.clone();
        blocking_compute(move || {
            let filter: CompositeNodeFilter = filter.try_into()?;
            let filtered_graph = self_clone.graph.filter_nodes(filter)?;
            Ok(GqlGraph::new(
                self_clone.path.clone(),
                filtered_graph.into_dynamic(),
            ))
        })
        .await
    }

    async fn edge_filter(&self, filter: EdgeFilter) -> Result<Self, GraphError> {
        let self_clone = self.clone();
        blocking_compute(move || {
            let filter: CompositeEdgeFilter = filter.try_into()?;
            let filtered_graph = self_clone.graph.filter_edges(filter)?;
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

    /// (Experimental) Searches for nodes which match the given filter expression.
    ///
    /// Uses Tantivy's exact search.
    async fn search_nodes(
        &self,
        filter: NodeFilter,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<GqlNode>, GraphError> {
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

    /// (Experimental) Searches the index for edges which match the given filter expression.
    ///
    /// Uses Tantivy's exact search.
    async fn search_edges(
        &self,
        filter: EdgeFilter,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<GqlEdge>, GraphError> {
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

    /// Returns the specified graph view or if none is specified returns the default view.
    /// This allows you to specify multiple operations together.
    async fn apply_views(&self, views: Vec<GraphViewCollection>) -> Result<GqlGraph, GraphError> {
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
                GraphViewCollection::Layer(layer) => return_view.layer(layer).await,
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
                GraphViewCollection::NodeFilter(filter) => return_view.node_filter(filter).await?,
                GraphViewCollection::EdgeFilter(filter) => return_view.edge_filter(filter).await?,
            };
        }
        Ok(return_view)
    }
}
