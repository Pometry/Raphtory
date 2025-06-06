use crate::{
    data::Data,
    model::{
        graph::{
            edge::GqlEdge,
            edges::GqlEdges,
            filtering::{EdgeFilter, GraphViewCollection, NodeFilter},
            index::IndexSpec,
            node::GqlNode,
            nodes::GqlNodes,
            property::GqlProperties,
            windowset::GqlGraphWindowSet,
            WindowDuration,
            WindowDuration::{Duration, Epoch},
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
use tokio::{spawn, task::spawn_blocking};

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
        self.apply(|g| g.default_layer())
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
        self.apply(|g| g.valid_layers(name.clone()))
    }

    async fn exclude_layer(&self, name: String) -> GqlGraph {
        self.apply(|g| g.exclude_valid_layers(name.clone()))
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
        window: WindowDuration,
        step: Option<WindowDuration>,
    ) -> Result<GqlGraphWindowSet, GraphError> {
        let self_clone = self.clone();
        spawn_blocking(move || match window {
            Duration(window_duration) => match step {
                Some(step) => match step {
                    Duration(step_duration) => Ok(GqlGraphWindowSet::new(
                        self_clone
                            .graph
                            .rolling(window_duration, Some(step_duration))?,
                        self_clone.path.clone(),
                    )),
                    Epoch(_) => Err(GraphError::MismatchedIntervalTypes),
                },
                None => Ok(GqlGraphWindowSet::new(
                    self_clone.graph.rolling(window_duration, None)?,
                    self_clone.path.clone(),
                )),
            },
            Epoch(window_duration) => match step {
                Some(step) => match step {
                    Duration(_) => Err(GraphError::MismatchedIntervalTypes),
                    Epoch(step_duration) => Ok(GqlGraphWindowSet::new(
                        self_clone
                            .graph
                            .rolling(window_duration, Some(step_duration))?,
                        self_clone.path.clone(),
                    )),
                },
                None => Ok(GqlGraphWindowSet::new(
                    self_clone.graph.rolling(window_duration, None)?,
                    self_clone.path.clone(),
                )),
            },
        })
        .await
        .unwrap()
    }

    async fn expanding(&self, step: WindowDuration) -> Result<GqlGraphWindowSet, GraphError> {
        let self_clone = self.clone();
        spawn_blocking(move || match step {
            Duration(step) => Ok(GqlGraphWindowSet::new(
                self_clone.graph.expanding(step)?,
                self_clone.path.clone(),
            )),
            Epoch(step) => Ok(GqlGraphWindowSet::new(
                self_clone.graph.expanding(step)?,
                self_clone.path.clone(),
            )),
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
        self.path.created_async().await
    }

    async fn last_opened(&self) -> Result<i64, GraphError> {
        self.path.last_opened_async().await
    }

    async fn last_updated(&self) -> Result<i64, GraphError> {
        self.path.last_updated_async().await
    }

    async fn earliest_time(&self) -> Option<i64> {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.graph.earliest_time())
            .await
            .unwrap()
    }

    async fn latest_time(&self) -> Option<i64> {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.graph.latest_time())
            .await
            .unwrap()
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
                .filter_map(|edge_time| edge_time.filter(|&time| include_negative || time >= 0))
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
                .filter_map(|edge_time| edge_time.filter(|&time| include_negative || time >= 0))
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
        let self_clone = self.clone();
        spawn_blocking(move || match ids {
            None => GqlNodes::new(self_clone.graph.nodes()),
            Some(ids) => GqlNodes::new(self_clone.graph.nodes().id_filter(ids)),
        })
        .await
        .unwrap()
    }

    async fn edge(&self, src: String, dst: String) -> Option<GqlEdge> {
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.graph.edge(src, dst).map(|e| e.into()))
            .await
            .unwrap()
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
        let self_clone = self.clone();
        spawn_blocking(move || self_clone.path.get_graph_name())
            .await
            .unwrap()
    }

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

    async fn namespace(&self) -> Result<String, GraphError> {
        let self_clone = self.clone();
        spawn_blocking(move || {
            Ok(self_clone
                .path
                .get_original_path()
                .parent()
                .and_then(|p| p.to_str().map(|s| s.to_string()))
                .ok_or(InvalidPathReason::PathNotParsable(
                    self_clone.path.to_error_path(),
                ))?
                .to_owned())
        })
        .await
        .unwrap()
    }

    async fn schema(&self) -> GraphSchema {
        let self_clone = self.clone();
        spawn_blocking(move || GraphSchema::new(&self_clone.graph))
            .await
            .unwrap()
    }

    async fn algorithms(&self) -> GraphAlgorithmPlugin {
        self.graph.clone().into()
    }

    async fn shared_neighbours(&self, selected_nodes: Vec<String>) -> Vec<GqlNode> {
        let self_clone = self.clone();
        spawn_blocking(move || {
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
        .unwrap()
    }

    /// Export all nodes and edges from this graph view to another existing graph
    async fn export_to<'a>(
        &self,
        ctx: &Context<'a>,
        path: String,
    ) -> Result<bool, Arc<GraphError>> {
        let data = ctx.data_unchecked::<Data>();
        let other_g = data.get_graph_async(path.as_ref()).await?.0;
        let g = self.graph.clone();
        spawn_blocking(move || {
            other_g.import_nodes(g.nodes(), true)?;
            other_g.import_edges(g.edges(), true)?;
            other_g.write_updates()?;
            Ok(true)
        })
        .await
        .unwrap()
    }

    async fn node_filter(&self, filter: NodeFilter) -> Result<Self, GraphError> {
        let self_clone = self.clone();
        spawn_blocking(move || {
            filter.validate()?;
            let filter: CompositeNodeFilter = filter.try_into()?;
            let filtered_graph = self_clone.graph.filter_nodes(filter)?;
            Ok(GqlGraph::new(
                self_clone.path.clone(),
                filtered_graph.into_dynamic(),
            ))
        })
        .await
        .unwrap()
    }

    async fn edge_filter(&self, filter: EdgeFilter) -> Result<Self, GraphError> {
        let self_clone = self.clone();
        spawn_blocking(move || {
            filter.validate()?;
            let filter: CompositeEdgeFilter = filter.try_into()?;
            let filtered_graph = self_clone.graph.filter_edges(filter)?;
            Ok(GqlGraph::new(
                self_clone.path.clone(),
                filtered_graph.into_dynamic(),
            ))
        })
        .await
        .unwrap()
    }

    ////////////////////////
    // INDEX SEARCH     ////
    ////////////////////////
    async fn get_index_spec(&self) -> Result<IndexSpec, GraphError> {
        #[cfg(feature = "search")]
        {
            let index_spec = self.graph.get_index_spec()?;
            let props = index_spec.props(&self.graph.0);

            Ok(IndexSpec {
                node_const_props: props.get(0).cloned().unwrap_or_default(),
                node_temp_props: props.get(1).cloned().unwrap_or_default(),
                edge_const_props: props.get(2).cloned().unwrap_or_default(),
                edge_temp_props: props.get(3).cloned().unwrap_or_default(),
            })
        }
        #[cfg(not(feature = "search"))]
        {
            Err(GraphError::IndexingNotSupported.into())
        }
    }

    async fn search_nodes(
        &self,
        filter: NodeFilter,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<GqlNode>, GraphError> {
        let self_clone = self.clone();
        spawn(async move {
            filter.validate()?;
            let f: CompositeNodeFilter = filter.try_into()?;
            self_clone
                .execute_search(|| {
                    Ok(self_clone
                        .graph
                        .search_nodes(f, limit, offset)
                        .into_iter()
                        .flatten()
                        .map(|vv| vv.into())
                        .collect())
                })
                .await
        })
        .await
        .unwrap()
    }

    async fn search_edges(
        &self,
        filter: EdgeFilter,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<GqlEdge>, GraphError> {
        let self_clone = self.clone();
        spawn(async move {
            filter.validate()?;
            let f: CompositeEdgeFilter = filter.try_into()?;
            self_clone
                .execute_search(|| {
                    Ok(self_clone
                        .graph
                        .search_edges(f, limit, offset)
                        .into_iter()
                        .flatten()
                        .map(|vv| vv.into())
                        .collect())
                })
                .await
        })
        .await
        .unwrap()
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
