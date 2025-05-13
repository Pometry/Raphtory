use crate::{
    data::Data,
    model::{
        graph::{
            edge::GqlEdge,
            edges::GqlEdges,
            filtering::{EdgeFilter, GraphViewCollection, NodeFilter},
            node::GqlNode,
            nodes::GqlNodes,
            property::GqlProperties,
            windowset::GqlGraphWindowSet,
        },
        plugins::graph_algorithm_plugin::GraphAlgorithmPlugin,
        schema::graph_schema::GraphSchema,
    },
    paths::ExistingGraphFolder,
};
use async_graphql::Context;
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use itertools::Itertools;
use raphtory::{
    core::{
        entities::nodes::node_ref::{AsNodeRef, NodeRef},
        utils::errors::{
            GraphError,
            GraphError::{MismatchedIntervalTypes, NoIntervalProvided, WrongNumOfArgs},
            InvalidPathReason::PathNotParsable,
        },
    },
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
    prelude::*,
};
use std::{
    collections::HashSet,
    convert::{Into, TryInto},
    sync::Arc,
};
use tokio::task::spawn_blocking;

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

    async fn execute_search<F, R>(&self, search_fn: F) -> Result<R, GraphError>
    where
        F: FnOnce() -> Result<R, GraphError>,
    {
        if self.graph.is_indexed() {
            search_fn()
        } else {
            Err(GraphError::IndexMissing)
        }
    }
}

#[ResolvedObjectFields]
impl GqlGraph {
    ////////////////////////
    // LAYERS AND WINDOWS //
    ////////////////////////

    async fn unique_layers(&self) -> Vec<String> {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.graph.unique_layers().map_into().collect())
            .await
            .unwrap()
    }

    async fn default_layer(&self) -> GqlGraph {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.apply(|g| g.default_layer()))
            .await
            .unwrap()
    }

    async fn layers(&self, names: Vec<String>) -> GqlGraph {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.apply(|g| g.valid_layers(names.clone())))
            .await
            .unwrap()
    }

    async fn exclude_layers(&self, names: Vec<String>) -> GqlGraph {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.apply(|g| g.exclude_valid_layers(names.clone())))
            .await
            .unwrap()
    }

    async fn layer(&self, name: String) -> GqlGraph {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.apply(|g| g.valid_layers(name.clone())))
            .await
            .unwrap()
    }

    async fn exclude_layer(&self, name: String) -> GqlGraph {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.apply(|g| g.exclude_valid_layers(name.clone())))
            .await
            .unwrap()
    }

    async fn subgraph(&self, nodes: Vec<String>) -> GqlGraph {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.apply(|g| g.subgraph(nodes.clone())))
            .await
            .unwrap()
    }

    async fn subgraph_node_types(&self, node_types: Vec<String>) -> GqlGraph {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.apply(|g| g.subgraph_node_types(node_types.clone())))
            .await
            .unwrap()
    }

    async fn exclude_nodes(&self, nodes: Vec<String>) -> GqlGraph {
        let self_clone = self.clone();
        spawn_blocking(move || {
            let nodes: Vec<NodeRef> = nodes.iter().map(|v| v.as_node_ref()).collect();
            self_clone.apply(|g| g.exclude_nodes(nodes.clone()))
        })
        .await
        .unwrap()
    }

    async fn rolling(
        &self,
        window_str: Option<String>,
        window_int: Option<i64>,
        step_str: Option<String>,
        step_int: Option<i64>,
    ) -> Result<GqlGraphWindowSet, GraphError> {
        let self_clone = self.clone();
        spawn_blocking(move || match (window_str, window_int) {
            (Some(_), Some(_)) => Err(WrongNumOfArgs(
                "window_str".to_string(),
                "window_int".to_string(),
            )),
            (None, Some(window_int)) => {
                if step_str.is_some() {
                    return Err(MismatchedIntervalTypes);
                }
                Ok(GqlGraphWindowSet::new(
                    self_clone.graph.rolling(window_int, step_int)?,
                    self_clone.path.clone(),
                ))
            }
            (Some(window_str), None) => {
                if step_int.is_some() {
                    return Err(MismatchedIntervalTypes);
                }
                Ok(GqlGraphWindowSet::new(
                    self_clone.graph.rolling(window_str, step_str)?,
                    self_clone.path.clone(),
                ))
            }
            (None, None) => return Err(NoIntervalProvided),
        })
        .await
        .unwrap()
    }

    async fn expanding(
        &self,
        step_str: Option<String>,
        step_int: Option<i64>,
    ) -> Result<GqlGraphWindowSet, GraphError> {
        let self_clone = self.clone();
        spawn_blocking(move || match (step_str, step_int) {
            (Some(_), Some(_)) => Err(WrongNumOfArgs(
                "step_str".to_string(),
                "step_int".to_string(),
            )),
            (None, Some(step_int)) => Ok(GqlGraphWindowSet::new(
                self_clone.graph.expanding(step_int)?,
                self_clone.path.clone(),
            )),
            (Some(step_str), None) => Ok(GqlGraphWindowSet::new(
                self_clone.graph.expanding(step_str)?,
                self_clone.path.clone(),
            )),
            (None, None) => return Err(NoIntervalProvided),
        })
        .await
        .unwrap()
    }

    /// Return a graph containing only the activity between `start` and `end` measured as milliseconds from epoch
    async fn window(&self, start: i64, end: i64) -> GqlGraph {
        self.apply(|g| g.window(start, end))
    }

    async fn at(&self, time: i64) -> GqlGraph {
        self.apply(|g| g.at(time))
    }

    async fn latest(&self) -> GqlGraph {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.apply(|g| g.latest()))
            .await
            .unwrap()
    }

    async fn snapshot_at(&self, time: i64) -> GqlGraph {
        self.apply(|g| g.snapshot_at(time))
    }

    async fn snapshot_latest(&self) -> GqlGraph {
        self.apply(|g| g.snapshot_latest())
    }

    async fn before(&self, time: i64) -> GqlGraph {
        self.apply(|g| g.before(time))
    }

    async fn after(&self, time: i64) -> GqlGraph {
        self.apply(|g| g.after(time))
    }

    async fn shrink_window(&self, start: i64, end: i64) -> Self {
        self.apply(|g| g.shrink_window(start, end))
    }

    async fn shrink_start(&self, start: i64) -> Self {
        self.apply(|g| g.shrink_start(start))
    }

    async fn shrink_end(&self, end: i64) -> Self {
        self.apply(|g| g.shrink_end(end))
    }

    ////////////////////////
    //// TIME QUERIES //////
    ////////////////////////

    async fn created(&self) -> Result<i64, GraphError> {
        self.path.created()
    }

    async fn last_opened(&self) -> Result<i64, GraphError> {
        self.path.last_opened()
    }

    async fn last_updated(&self) -> Result<i64, GraphError> {
        self.path.last_updated()
    }

    async fn earliest_time(&self) -> Option<i64> {
        self.graph.earliest_time()
    }

    async fn latest_time(&self) -> Option<i64> {
        self.graph.latest_time()
    }

    async fn start(&self) -> Option<i64> {
        self.graph.start()
    }

    async fn end(&self) -> Option<i64> {
        self.graph.end()
    }

    async fn earliest_edge_time(&self, include_negative: Option<bool>) -> Option<i64> {
        let self_clone = self.clone();
        spawn_blocking(move || {
            let include_negative = include_negative.unwrap_or(true);
            let all_edges = self_clone
                .graph
                .edges()
                .earliest_time()
                .into_iter()
                .filter_map(|edge_time| edge_time.filter(|&time| (include_negative || time >= 0)))
                .min();
            all_edges
        })
        .await
        .unwrap()
    }

    async fn latest_edge_time(&self, include_negative: Option<bool>) -> Option<i64> {
        let self_clone = self.clone();
        spawn_blocking(move || {
            let include_negative = include_negative.unwrap_or(true);
            let all_edges = self_clone
                .graph
                .edges()
                .latest_time()
                .into_iter()
                .filter_map(|edge_time| edge_time.filter(|&time| (include_negative || time >= 0)))
                .max();

            all_edges
        })
        .await
        .unwrap()
    }

    ////////////////////////
    //////// COUNTERS //////
    ////////////////////////

    async fn count_edges(&self) -> usize {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.graph.count_edges())
            .await
            .unwrap()
    }

    async fn count_temporal_edges(&self) -> usize {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.graph.count_temporal_edges())
            .await
            .unwrap()
    }

    async fn count_nodes(&self) -> usize {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.graph.count_nodes())
            .await
            .unwrap()
    }

    ////////////////////////
    //// EXISTS CHECKERS ///
    ////////////////////////

    async fn has_node(&self, name: String) -> bool {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.graph.has_node(name))
            .await
            .unwrap()
    }

    async fn has_edge(&self, src: String, dst: String, layer: Option<String>) -> bool {
        let self_clone = self.clone();
        spawn_blocking(move || match layer {
            Some(name) => self_clone
                .graph
                .layers(name)
                .map(|l| l.has_edge(src, dst))
                .unwrap_or(false),
            None => self_clone.graph.has_edge(src, dst),
        })
        .await
        .unwrap()
    }

    ////////////////////////
    //////// GETTERS ///////
    ////////////////////////

    async fn node(&self, name: String) -> Option<GqlNode> {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.graph.node(name).map(|v| v.into()))
            .await
            .unwrap()
    }

    /// query (optionally a subset of) the nodes in the graph
    async fn nodes(&self, ids: Option<Vec<String>>) -> GqlNodes {
        match ids {
            None => GqlNodes::new(self.graph.nodes()),
            Some(ids) => GqlNodes::new(self.graph.nodes().id_filter(ids)),
        }
    }

    pub fn edge(&self, src: String, dst: String) -> Option<GqlEdge> {
        self.graph.edge(src, dst).map(|e| e.into())
    }

    async fn edges<'a>(&self) -> GqlEdges {
        GqlEdges::new(self.graph.edges())
    }

    ////////////////////////
    /////// PROPERTIES /////
    ////////////////////////

    async fn properties(&self) -> GqlProperties {
        Into::<DynProperties>::into(self.graph.properties()).into()
    }

    ////////////////////////
    // GRAPHQL SPECIFIC ////
    ////////////////////////

    //These name/path functions basically can only fail
    //if someone write non-utf characters as a filename

    async fn name(&self) -> Result<String, GraphError> {
        self.path.get_graph_name()
    }

    async fn path(&self) -> Result<String, GraphError> {
        Ok(self
            .path
            .get_original_path()
            .to_str()
            .ok_or(PathNotParsable(self.path.to_error_path()))?
            .to_owned())
    }

    async fn namespace(&self) -> Result<String, GraphError> {
        Ok(self
            .path
            .get_original_path()
            .parent()
            .and_then(|p| p.to_str().map(|s| s.to_string()))
            .ok_or(PathNotParsable(self.path.to_error_path()))?
            .to_owned())
    }

    async fn schema(&self) -> GraphSchema {
        GraphSchema::new(&self.graph)
    }

    async fn algorithms(&self) -> GraphAlgorithmPlugin {
        self.graph.clone().into()
    }

    async fn shared_neighbours(&self, selected_nodes: Vec<String>) -> Vec<GqlNode> {
        if selected_nodes.is_empty() {
            return vec![];
        }

        let neighbours: Vec<HashSet<NodeView<DynamicGraph>>> = selected_nodes
            .iter()
            .filter_map(|n| self.graph.node(n))
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
    }

    /// Export all nodes and edges from this graph view to another existing graph
    async fn export_to<'a>(
        &'a self,
        ctx: &Context<'a>,
        path: String,
    ) -> Result<bool, Arc<GraphError>> {
        let data = ctx.data_unchecked::<Data>();
        let other_g = data.get_graph(path.as_ref())?.0;
        other_g.import_nodes(self.graph.nodes(), true)?;
        other_g.import_edges(self.graph.edges(), true)?;
        other_g.write_updates()?;
        Ok(true)
    }

    async fn node_filter(&self, filter: NodeFilter) -> Result<Self, GraphError> {
        filter.validate()?;
        let filter: CompositeNodeFilter = filter.try_into()?;
        let filtered_graph = self.graph.filter_nodes(filter)?;
        Ok(GqlGraph::new(
            self.path.clone(),
            filtered_graph.into_dynamic(),
        ))
    }

    async fn edge_filter(&self, filter: EdgeFilter) -> Result<Self, GraphError> {
        filter.validate()?;
        let filter: CompositeEdgeFilter = filter.try_into()?;
        let filtered_graph = self.graph.filter_edges(filter)?;
        Ok(GqlGraph::new(
            self.path.clone(),
            filtered_graph.into_dynamic(),
        ))
    }

    ////////////////////////
    // INDEX SEARCH     ////
    ////////////////////////
    async fn search_nodes(
        &self,
        filter: NodeFilter,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<GqlNode>, GraphError> {
        filter.validate()?;
        let f: CompositeNodeFilter = filter.try_into()?;
        self.execute_search(|| {
            Ok(self
                .graph
                .search_nodes(f, limit, offset)
                .into_iter()
                .flatten()
                .map(|vv| vv.into())
                .collect())
        })
        .await
    }

    async fn search_edges(
        &self,
        filter: EdgeFilter,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<GqlEdge>, GraphError> {
        filter.validate()?;
        let f: CompositeEdgeFilter = filter.try_into()?;
        self.execute_search(|| {
            Ok(self
                .graph
                .search_edges(f, limit, offset)
                .into_iter()
                .flatten()
                .map(|vv| vv.into())
                .collect())
        })
        .await
    }

    async fn apply_views(&self, views: Vec<GraphViewCollection>) -> Result<GqlGraph, GraphError> {
        let mut return_view: GqlGraph = GqlGraph::new(self.path.clone(), self.graph.clone());

        for view in views {
            let mut count = 0;
            if let Some(_) = view.default_layer {
                count += 1;
                return_view = return_view.default_layer().await;
            }
            if let Some(layers) = view.layers {
                count += 1;
                return_view = return_view.layers(layers).await;
            }
            if let Some(layers) = view.exclude_layers {
                count += 1;
                return_view = return_view.exclude_layers(layers).await;
            }
            if let Some(layer) = view.layer {
                count += 1;
                return_view = return_view.layer(layer).await;
            }
            if let Some(layer) = view.exclude_layer {
                count += 1;
                return_view = return_view.exclude_layer(layer).await;
            }
            if let Some(nodes) = view.subgraph {
                count += 1;
                return_view = return_view.subgraph(nodes).await;
            }
            if let Some(types) = view.subgraph_node_types {
                count += 1;
                return_view = return_view.subgraph_node_types(types).await;
            }
            if let Some(nodes) = view.exclude_nodes {
                count += 1;
                return_view = return_view.exclude_nodes(nodes).await;
            }
            if let Some(window) = view.window {
                count += 1;
                return_view = return_view.window(window.start, window.end).await;
            }
            if let Some(time) = view.at {
                count += 1;
                return_view = return_view.at(time).await;
            }
            if let Some(_) = view.latest {
                count += 1;
                return_view = return_view.latest().await;
            }
            if let Some(time) = view.snapshot_at {
                count += 1;
                return_view = return_view.snapshot_at(time).await;
            }
            if let Some(_) = view.snapshot_latest {
                count += 1;
                return_view = return_view.snapshot_latest().await;
            }
            if let Some(time) = view.before {
                count += 1;
                return_view = return_view.before(time).await;
            }
            if let Some(time) = view.after {
                count += 1;
                return_view = return_view.after(time).await;
            }
            if let Some(window) = view.shrink_window {
                count += 1;
                return_view = return_view.shrink_window(window.start, window.end).await;
            }
            if let Some(time) = view.shrink_start {
                count += 1;
                return_view = return_view.shrink_start(time).await;
            }
            if let Some(time) = view.shrink_end {
                count += 1;
                return_view = return_view.shrink_end(time).await;
            }
            if let Some(node_filter) = view.node_filter {
                count += 1;
                return_view = return_view.node_filter(node_filter).await?;
            }
            if let Some(edge_filter) = view.edge_filter {
                count += 1;
                return_view = return_view.edge_filter(edge_filter).await?;
            }

            if count > 1 {
                return Err(GraphError::TooManyViewsSet);
            }
        }

        Ok(return_view)
    }
}
