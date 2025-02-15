use crate::{
    data::Data,
    model::{
        graph::{
            edge::Edge,
            edges::GqlEdges,
            filtering::{FilterCondition, FilterProperty, FilterWindow, Operator},
            node::Node,
            nodes::GqlNodes,
            property::GqlProperties,
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
        utils::errors::{GraphError, InvalidPathReason::PathNotParsable},
    },
    db::{
        api::{
            properties::dyn_props::DynProperties,
            view::{DynamicGraph, IntoDynamic, NodeViewOps, StaticGraphViewOps, TimeOps},
        },
        graph::node::NodeView,
    },
    prelude::*,
    search::{into_indexed::DynamicIndexedGraph, IndexedGraph},
};
use std::{collections::HashSet, convert::Into, sync::Arc};

#[derive(ResolvedObject)]
pub(crate) struct GqlGraph {
    path: ExistingGraphFolder,
    graph: DynamicGraph,
    index: Option<IndexedGraph<DynamicGraph>>,
}

impl GqlGraph {
    pub fn new<G: StaticGraphViewOps + IntoDynamic, I: DynamicIndexedGraph>(
        path: ExistingGraphFolder,
        graph: G,
        index: Option<I>,
    ) -> Self {
        Self {
            path,
            graph: graph.into_dynamic(),
            index: index.map(|index| index.into_dynamic_indexed()),
        }
    }

    fn apply<F, G, I, Y>(&self, graph_operation: F, index_operation: I) -> Self
    where
        F: Fn(&DynamicGraph) -> G,
        G: StaticGraphViewOps + IntoDynamic,
        I: Fn(&IndexedGraph<DynamicGraph>) -> Y,
        Y: DynamicIndexedGraph,
    {
        Self {
            path: self.path.clone(),
            graph: graph_operation(&self.graph).into_dynamic(),
            index: self
                .index
                .as_ref()
                .map(|index| index_operation(index).into_dynamic_indexed()),
        }
    }

    fn get_index(&self) -> Result<&IndexedGraph<DynamicGraph>, GraphError> {
        match self.index.as_ref() {
            Some(index) => Ok(index),
            None => Err(GraphError::IndexMissing),
        }
    }
}

#[ResolvedObjectFields]
impl GqlGraph {
    ////////////////////////
    // LAYERS AND WINDOWS //
    ////////////////////////

    async fn unique_layers(&self) -> Vec<String> {
        self.graph.unique_layers().map_into().collect()
    }

    async fn default_layer(&self) -> GqlGraph {
        self.apply(|g| g.default_layer(), |g| g.default_layer())
    }

    async fn layers(&self, names: Vec<String>) -> GqlGraph {
        self.apply(
            |g| g.valid_layers(names.clone()),
            |g| g.valid_layers(names.clone()),
        )
    }

    async fn exclude_layers(&self, names: Vec<String>) -> GqlGraph {
        self.apply(
            |g| g.exclude_valid_layers(names.clone()),
            |g| g.exclude_valid_layers(names.clone()),
        )
    }

    async fn layer(&self, name: String) -> GqlGraph {
        self.apply(
            |g| g.valid_layers(name.clone()),
            |g| g.valid_layers(name.clone()),
        )
    }

    async fn exclude_layer(&self, name: String) -> GqlGraph {
        self.apply(
            |g| g.exclude_valid_layers(name.clone()),
            |g| g.exclude_valid_layers(name.clone()),
        )
    }

    async fn subgraph(&self, nodes: Vec<String>) -> GqlGraph {
        self.apply(|g| g.subgraph(nodes.clone()), |g| g.subgraph(nodes.clone()))
    }

    async fn subgraph_id(&self, nodes: Vec<u64>) -> GqlGraph {
        let nodes: Vec<NodeRef> = nodes.iter().map(|v| v.as_node_ref()).collect();
        self.apply(|g| g.subgraph(nodes.clone()), |g| g.subgraph(nodes.clone()))
    }

    async fn subgraph_node_types(&self, node_types: Vec<String>) -> GqlGraph {
        self.apply(
            |g| g.subgraph_node_types(node_types.clone()),
            |g| g.subgraph_node_types(node_types.clone()),
        )
    }

    async fn exclude_nodes(&self, nodes: Vec<String>) -> GqlGraph {
        let nodes: Vec<NodeRef> = nodes.iter().map(|v| v.as_node_ref()).collect();
        self.apply(
            |g| g.exclude_nodes(nodes.clone()),
            |g| g.exclude_nodes(nodes.clone()),
        )
    }

    async fn exclude_nodes_id(&self, nodes: Vec<u64>) -> GqlGraph {
        let nodes: Vec<NodeRef> = nodes.iter().map(|v| v.as_node_ref()).collect();
        self.apply(
            |g| g.exclude_nodes(nodes.clone()),
            |g| g.exclude_nodes(nodes.clone()),
        )
    }

    /// Return a graph containing only the activity between `start` and `end` measured as milliseconds from epoch

    async fn window(&self, start: i64, end: i64) -> GqlGraph {
        self.apply(|g| g.window(start, end), |g| g.window(start, end))
    }

    async fn at(&self, time: i64) -> GqlGraph {
        self.apply(|g| g.at(time), |g| g.at(time))
    }

    async fn latest(&self) -> GqlGraph {
        self.apply(|g| g.latest(), |g| g.latest())
    }

    async fn snapshot_at(&self, time: i64) -> GqlGraph {
        self.apply(|g| g.snapshot_at(time), |g| g.snapshot_at(time))
    }

    async fn snapshot_latest(&self) -> GqlGraph {
        self.apply(|g| g.snapshot_latest(), |g| g.snapshot_latest())
    }

    async fn before(&self, time: i64) -> GqlGraph {
        self.apply(|g| g.before(time), |g| g.before(time))
    }

    async fn after(&self, time: i64) -> GqlGraph {
        self.apply(|g| g.after(time), |g| g.after(time))
    }

    async fn shrink_window(&self, start: i64, end: i64) -> Self {
        self.apply(
            |g| g.shrink_window(start, end),
            |g| g.shrink_window(start, end),
        )
    }

    async fn shrink_start(&self, start: i64) -> Self {
        self.apply(|g| g.shrink_start(start), |g| g.shrink_start(start))
    }

    async fn shrink_end(&self, end: i64) -> Self {
        self.apply(|g| g.shrink_end(end), |g| g.shrink_end(end))
    }

    async fn node_filter(
        &self,
        property: String,
        condition: FilterCondition,
    ) -> Result<Self, GraphError> {
        match condition.operator {
            Operator::Equal => {
                if let Some(value) = condition.value {
                    let filtered_graph = self
                        .graph
                        .filter_nodes(PropertyFilter::eq(property, value.0))?;
                    Ok(GqlGraph::new(
                        self.path.clone(),
                        filtered_graph.into_dynamic(),
                        self.index.clone(),
                    ))
                } else {
                    Err(GraphError::ExpectedValueForOperator(
                        "value".into(),
                        "Equal".into(),
                    ))
                }
            }
            Operator::NotEqual => {
                if let Some(value) = condition.value {
                    let filtered_graph = self
                        .graph
                        .filter_nodes(PropertyFilter::ne(property, value.0))?;
                    Ok(GqlGraph::new(
                        self.path.clone(),
                        filtered_graph.into_dynamic(),
                        self.index.clone(),
                    ))
                } else {
                    Err(GraphError::ExpectedValueForOperator(
                        "value".into(),
                        "NotEqual".into(),
                    ))
                }
            }
            Operator::GreaterThanOrEqual => {
                if let Some(value) = condition.value {
                    let filtered_graph = self
                        .graph
                        .filter_nodes(PropertyFilter::ge(property, value.0))?;
                    Ok(GqlGraph::new(
                        self.path.clone(),
                        filtered_graph.into_dynamic(),
                        self.index.clone(),
                    ))
                } else {
                    Err(GraphError::ExpectedValueForOperator(
                        "value".into(),
                        "GreaterThanOrEqual".into(),
                    ))
                }
            }
            Operator::LessThanOrEqual => {
                if let Some(value) = condition.value {
                    let filtered_graph = self
                        .graph
                        .filter_nodes(PropertyFilter::le(property, value.0))?;
                    Ok(GqlGraph::new(
                        self.path.clone(),
                        filtered_graph.into_dynamic(),
                        self.index.clone(),
                    ))
                } else {
                    Err(GraphError::ExpectedValueForOperator(
                        "value".into(),
                        "LessThanOrEqual".into(),
                    ))
                }
            }
            Operator::GreaterThan => {
                if let Some(value) = condition.value {
                    let filtered_graph = self
                        .graph
                        .filter_nodes(PropertyFilter::gt(property, value.0))?;
                    Ok(GqlGraph::new(
                        self.path.clone(),
                        filtered_graph.into_dynamic(),
                        self.index.clone(),
                    ))
                } else {
                    Err(GraphError::ExpectedValueForOperator(
                        "value".into(),
                        "GreaterThan".into(),
                    ))
                }
            }
            Operator::LessThan => {
                if let Some(value) = condition.value {
                    let filtered_graph = self
                        .graph
                        .filter_nodes(PropertyFilter::lt(property, value.0))?;
                    Ok(GqlGraph::new(
                        self.path.clone(),
                        filtered_graph.into_dynamic(),
                        self.index.clone(),
                    ))
                } else {
                    Err(GraphError::ExpectedValueForOperator(
                        "value".into(),
                        "LessThan".into(),
                    ))
                }
            }
            Operator::IsNone => {
                let filtered_graph = self.graph.filter_nodes(PropertyFilter::is_none(property))?;
                Ok(GqlGraph::new(
                    self.path.clone(),
                    filtered_graph.into_dynamic(),
                    self.index.clone(),
                ))
            }
            Operator::IsSome => {
                let filtered_graph = self.graph.filter_nodes(PropertyFilter::is_some(property))?;
                Ok(GqlGraph::new(
                    self.path.clone(),
                    filtered_graph.into_dynamic(),
                    self.index.clone(),
                ))
            }
            Operator::Any => {
                if let Some(Prop::List(list)) = condition.value.map(|v| v.0) {
                    let prop_values: Vec<Prop> = list.iter().cloned().collect();
                    let filtered_graph = self
                        .graph
                        .filter_nodes(PropertyFilter::any(property, prop_values))?;
                    Ok(GqlGraph::new(
                        self.path.clone(),
                        filtered_graph.into_dynamic(),
                        self.index.clone(),
                    ))
                } else {
                    Err(GraphError::ExpectedValueForOperator(
                        "list".into(),
                        "Any".into(),
                    ))
                }
            }
            Operator::NotAny => {
                if let Some(Prop::List(list)) = condition.value.map(|v| v.0) {
                    let prop_values: Vec<Prop> = list.iter().cloned().collect();
                    let filtered_graph = self
                        .graph
                        .filter_nodes(PropertyFilter::not_any(property, prop_values))?;
                    Ok(GqlGraph::new(
                        self.path.clone(),
                        filtered_graph.into_dynamic(),
                        self.index.clone(),
                    ))
                } else {
                    Err(GraphError::ExpectedValueForOperator(
                        "list".into(),
                        "NotAny".into(),
                    ))
                }
            }
        }
    }

    async fn edge_filter(
        &self,
        property: String,
        condition: FilterCondition,
    ) -> Result<Self, GraphError> {
        match condition.operator {
            Operator::Equal => {
                if let Some(value) = condition.value {
                    let filtered_graph = self
                        .graph
                        .filter_edges(PropertyFilter::eq(property, value.0))?;
                    Ok(GqlGraph::new(
                        self.path.clone(),
                        filtered_graph.into_dynamic(),
                        self.index.clone(),
                    ))
                } else {
                    Err(GraphError::ExpectedValueForOperator(
                        "value".into(),
                        "Equal".into(),
                    ))
                }
            }
            Operator::NotEqual => {
                if let Some(value) = condition.value {
                    let filtered_graph = self
                        .graph
                        .filter_edges(PropertyFilter::ne(property, value.0))?;
                    Ok(GqlGraph::new(
                        self.path.clone(),
                        filtered_graph.into_dynamic(),
                        self.index.clone(),
                    ))
                } else {
                    Err(GraphError::ExpectedValueForOperator(
                        "value".into(),
                        "NotEqual".into(),
                    ))
                }
            }
            Operator::GreaterThanOrEqual => {
                if let Some(value) = condition.value {
                    let filtered_graph = self
                        .graph
                        .filter_edges(PropertyFilter::ge(property, value.0))?;
                    Ok(GqlGraph::new(
                        self.path.clone(),
                        filtered_graph.into_dynamic(),
                        self.index.clone(),
                    ))
                } else {
                    Err(GraphError::ExpectedValueForOperator(
                        "value".into(),
                        "GreaterThanOrEqual".into(),
                    ))
                }
            }
            Operator::LessThanOrEqual => {
                if let Some(value) = condition.value {
                    let filtered_graph = self
                        .graph
                        .filter_edges(PropertyFilter::le(property, value.0))?;
                    Ok(GqlGraph::new(
                        self.path.clone(),
                        filtered_graph.into_dynamic(),
                        self.index.clone(),
                    ))
                } else {
                    Err(GraphError::ExpectedValueForOperator(
                        "value".into(),
                        "LessThanOrEqual".into(),
                    ))
                }
            }
            Operator::GreaterThan => {
                if let Some(value) = condition.value {
                    let filtered_graph = self
                        .graph
                        .filter_edges(PropertyFilter::gt(property, value.0))?;
                    Ok(GqlGraph::new(
                        self.path.clone(),
                        filtered_graph.into_dynamic(),
                        self.index.clone(),
                    ))
                } else {
                    Err(GraphError::ExpectedValueForOperator(
                        "value".into(),
                        "GreaterThan".into(),
                    ))
                }
            }
            Operator::LessThan => {
                if let Some(value) = condition.value {
                    let filtered_graph = self
                        .graph
                        .filter_edges(PropertyFilter::lt(property, value.0))?;
                    Ok(GqlGraph::new(
                        self.path.clone(),
                        filtered_graph.into_dynamic(),
                        self.index.clone(),
                    ))
                } else {
                    Err(GraphError::ExpectedValueForOperator(
                        "value".into(),
                        "LessThan".into(),
                    ))
                }
            }
            Operator::IsNone => {
                let filtered_graph = self.graph.filter_edges(PropertyFilter::is_none(property))?;
                Ok(GqlGraph::new(
                    self.path.clone(),
                    filtered_graph.into_dynamic(),
                    self.index.clone(),
                ))
            }
            Operator::IsSome => {
                let filtered_graph = self.graph.filter_edges(PropertyFilter::is_some(property))?;
                Ok(GqlGraph::new(
                    self.path.clone(),
                    filtered_graph.into_dynamic(),
                    self.index.clone(),
                ))
            }
            Operator::Any => {
                if let Some(Prop::List(list)) = condition.value.map(|v| v.0) {
                    let prop_values: Vec<Prop> = list.iter().cloned().collect();
                    let filtered_graph = self
                        .graph
                        .filter_edges(PropertyFilter::any(property, prop_values))?;
                    Ok(GqlGraph::new(
                        self.path.clone(),
                        filtered_graph.into_dynamic(),
                        self.index.clone(),
                    ))
                } else {
                    Err(GraphError::ExpectedValueForOperator(
                        "list".into(),
                        "Any".into(),
                    ))
                }
            }
            Operator::NotAny => {
                if let Some(Prop::List(list)) = condition.value.map(|v| v.0) {
                    let prop_values: Vec<Prop> = list.iter().cloned().collect();
                    let filtered_graph = self
                        .graph
                        .filter_edges(PropertyFilter::not_any(property, prop_values))?;
                    Ok(GqlGraph::new(
                        self.path.clone(),
                        filtered_graph.into_dynamic(),
                        self.index.clone(),
                    ))
                } else {
                    Err(GraphError::ExpectedValueForOperator(
                        "list".into(),
                        "NotAny".into(),
                    ))
                }
            }
        }
    }

    async fn filter(
        &self,
        default_layer: Option<bool>,
        layers: Option<Vec<String>>,
        exclude_layers: Option<Vec<String>>,
        layer: Option<String>,
        exclude_layer: Option<String>,
        subgraph: Option<Vec<String>>,
        subgraph_id: Option<Vec<u64>>,
        subgraph_node_types: Option<Vec<String>>,
        exclude_nodes: Option<Vec<String>>,
        exclude_nodes_id: Option<Vec<u64>>,
        window: Option<FilterWindow>,
        at: Option<i64>,
        latest: Option<bool>,
        snapshot_latest: Option<bool>,
        before: Option<i64>,
        after: Option<i64>,
        shrink_window: Option<FilterWindow>,
        shrink_start: Option<i64>,
        shrink_end: Option<i64>,
        node_filter: Option<FilterProperty>,
        edge_filter: Option<FilterProperty>,
    ) -> Result<GqlGraph, GraphError> {
        let mut return_view =
            GqlGraph::new(self.path.clone(), self.graph.clone(), self.index.clone());

        // Apply default layer if specified
        if let Some(true) = default_layer {
            return_view = return_view.default_layer().await;
        }

        // Apply layers inclusion filter
        if let Some(layers) = layers {
            return_view = return_view.layers(layers).await;
        }

        // Apply layer exclusion filter
        if let Some(exclude_layers) = exclude_layers {
            return_view = return_view.exclude_layers(exclude_layers).await;
        }

        // Apply single layer filter
        if let Some(layer) = layer {
            return_view = return_view.layer(layer).await;
        }

        // Apply single layer exclusion
        if let Some(exclude_layer) = exclude_layer {
            return_view = return_view.exclude_layer(exclude_layer).await;
        }

        // Apply subgraph filters
        if let Some(subgraph) = subgraph {
            return_view = return_view.subgraph(subgraph).await;
        }
        if let Some(subgraph_id) = subgraph_id {
            return_view = return_view.subgraph_id(subgraph_id).await;
        }
        if let Some(subgraph_node_types) = subgraph_node_types {
            return_view = return_view.subgraph_node_types(subgraph_node_types).await;
        }

        // Apply node exclusions
        if let Some(exclude_nodes) = exclude_nodes {
            return_view = return_view.exclude_nodes(exclude_nodes).await;
        }
        if let Some(exclude_nodes_id) = exclude_nodes_id {
            return_view = return_view.exclude_nodes_id(exclude_nodes_id).await;
        }

        // Apply time filters
        if let Some(window) = window {
            return_view = return_view.window(window.start, window.end).await;
        }
        if let Some(at) = at {
            return_view = return_view.at(at).await;
        }
        if let Some(true) = latest {
            return_view = return_view.latest().await;
        }
        if let Some(_) = snapshot_latest {
            return_view = return_view.snapshot_latest().await;
        }
        if let Some(before) = before {
            return_view = return_view.before(before).await;
        }
        if let Some(after) = after {
            return_view = return_view.after(after).await;
        }

        // Apply shrinking window filters
        if let Some(shrink_window) = shrink_window {
            return_view = return_view
                .shrink_window(shrink_window.start, shrink_window.end)
                .await;
        }
        if let Some(shrink_start) = shrink_start {
            return_view = return_view.shrink_start(shrink_start).await;
        }
        if let Some(shrink_end) = shrink_end {
            return_view = return_view.shrink_end(shrink_end).await;
        }

        // Apply node and edge property filters
        if let Some(node_filter) = node_filter {
            return_view = return_view
                .node_filter(node_filter.property, node_filter.condition)
                .await?;
        }
        if let Some(edge_filter) = edge_filter {
            return_view = return_view
                .edge_filter(edge_filter.property, edge_filter.condition)
                .await?;
        }

        Ok(return_view)
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
        let include_negative = include_negative.unwrap_or(true);
        let all_edges = self
            .graph
            .edges()
            .earliest_time()
            .into_iter()
            .filter_map(|edge_time| edge_time.filter(|&time| (include_negative || time >= 0)))
            .min();
        return all_edges;
    }

    async fn latest_edge_time(&self, include_negative: Option<bool>) -> Option<i64> {
        let include_negative = include_negative.unwrap_or(true);
        let all_edges = self
            .graph
            .edges()
            .latest_time()
            .into_iter()
            .filter_map(|edge_time| edge_time.filter(|&time| (include_negative || time >= 0)))
            .max();

        return all_edges;
    }

    ////////////////////////
    //////// COUNTERS //////
    ////////////////////////

    async fn count_edges(&self) -> usize {
        self.graph.count_edges()
    }

    async fn count_temporal_edges(&self) -> usize {
        self.graph.count_temporal_edges()
    }

    async fn search_edge_count(&self, query: String) -> Result<usize, GraphError> {
        Ok(self.get_index()?.search_edge_count(&query).unwrap_or(0))
    }

    async fn count_nodes(&self) -> usize {
        self.graph.count_nodes()
    }

    async fn search_node_count(&self, query: String) -> Result<usize, GraphError> {
        Ok(self.get_index()?.search_node_count(&query).unwrap_or(0))
    }

    ////////////////////////
    //// EXISTS CHECKERS ///
    ////////////////////////

    async fn has_node(&self, name: String) -> bool {
        self.graph.has_node(name)
    }

    async fn has_node_id(&self, id: u64) -> bool {
        self.graph.has_node(id)
    }

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

    async fn has_edge_id(&self, src: u64, dst: u64, layer: Option<String>) -> bool {
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

    async fn node(&self, name: String) -> Option<Node> {
        self.graph.node(name).map(|v| v.into())
    }

    async fn node_id(&self, id: u64) -> Option<Node> {
        self.graph.node(id).map(|v| v.into())
    }

    async fn nodes(&self) -> GqlNodes {
        GqlNodes::new(self.graph.nodes())
    }

    async fn search_nodes(
        &self,
        query: String,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<Node>, GraphError> {
        Ok(self
            .get_index()?
            .search_nodes(&query, limit, offset)
            .into_iter()
            .flatten()
            .map(|vv| vv.into())
            .collect())
    }

    async fn fuzzy_search_nodes(
        &self,
        query: String,
        limit: usize,
        offset: usize,
        prefix: bool,
        levenshtein_distance: u8,
    ) -> Result<Vec<Node>, GraphError> {
        Ok(self
            .get_index()?
            .fuzzy_search_nodes(&query, limit, offset, prefix, levenshtein_distance)
            .into_iter()
            .flatten()
            .map(|vv| vv.into())
            .collect())
    }

    pub fn edge(&self, src: String, dst: String) -> Option<Edge> {
        self.graph.edge(src, dst).map(|e| e.into())
    }

    pub fn edge_id(&self, src: u64, dst: u64) -> Option<Edge> {
        self.graph.edge(src, dst).map(|e| e.into())
    }

    async fn edges<'a>(&self) -> GqlEdges {
        GqlEdges::new(self.graph.edges())
    }

    async fn search_edges(
        &self,
        query: String,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<Edge>, GraphError> {
        Ok(self
            .get_index()?
            .search_edges(&query, limit, offset)
            .into_iter()
            .flatten()
            .map(|vv| vv.into())
            .collect())
    }

    async fn fuzzy_search_edges(
        &self,
        query: String,
        limit: usize,
        offset: usize,
        prefix: bool,
        levenshtein_distance: u8,
    ) -> Result<Vec<Edge>, GraphError> {
        Ok(self
            .get_index()?
            .fuzzy_search_edges(&query, limit, offset, prefix, levenshtein_distance)
            .into_iter()
            .flatten()
            .map(|vv| vv.into())
            .collect())
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

    async fn shared_neighbours(&self, selected_nodes: Vec<String>) -> Vec<Node> {
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
}
