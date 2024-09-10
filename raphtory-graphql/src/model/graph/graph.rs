use crate::{
    data::{get_graph_name, Data},
    model::{
        algorithms::graph_algorithms::GraphAlgorithms,
        graph::{
            edge::Edge, edges::GqlEdges, node::Node, nodes::GqlNodes, property::GqlProperties,
        },
        schema::graph_schema::GraphSchema,
    },
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
            view::{DynamicGraph, NodeViewOps, TimeOps},
        },
        graph::node::NodeView,
    },
    prelude::*,
    search::{into_indexed::DynamicIndexedGraph, IndexedGraph},
};
use std::{collections::HashSet, convert::Into, path::PathBuf, sync::Arc};
use tracing::instrument;

#[derive(ResolvedObject)]
pub(crate) struct GqlGraph {
    path: PathBuf,
    graph: IndexedGraph<DynamicGraph>,
}

impl GqlGraph {
    pub fn new<G: DynamicIndexedGraph>(path: PathBuf, graph: G) -> Self {
        Self {
            path,
            graph: graph.into_dynamic_indexed(),
        }
    }
}

#[ResolvedObjectFields]
impl GqlGraph {
    ////////////////////////
    // LAYERS AND WINDOWS //
    ////////////////////////
    #[instrument(skip(self))]
    async fn unique_layers(&self) -> Vec<String> {
        self.graph.unique_layers().map_into().collect()
    }
    #[instrument(skip(self))]
    async fn default_layer(&self) -> GqlGraph {
        GqlGraph::new(self.path.clone(), self.graph.default_layer())
    }
    #[instrument(skip(self))]
    async fn layers(&self, names: Vec<String>) -> GqlGraph {
        GqlGraph::new(self.path.clone(), self.graph.valid_layers(names))
    }
    #[instrument(skip(self))]
    async fn exclude_layers(&self, names: Vec<String>) -> GqlGraph {
        GqlGraph::new(self.path.clone(), self.graph.exclude_valid_layers(names))
    }
    #[instrument(skip(self))]
    async fn layer(&self, name: String) -> GqlGraph {
        GqlGraph::new(self.path.clone(), self.graph.valid_layers(name))
    }
    #[instrument(skip(self))]
    async fn exclude_layer(&self, name: String) -> GqlGraph {
        GqlGraph::new(self.path.clone(), self.graph.exclude_valid_layers(name))
    }
    #[instrument(skip(self))]
    async fn subgraph(&self, nodes: Vec<String>) -> GqlGraph {
        GqlGraph::new(self.path.clone(), self.graph.subgraph(nodes))
    }
    #[instrument(skip(self))]
    async fn subgraph_id(&self, nodes: Vec<u64>) -> GqlGraph {
        let nodes: Vec<NodeRef> = nodes.iter().map(|v| v.as_node_ref()).collect();
        GqlGraph::new(self.path.clone(), self.graph.subgraph(nodes))
    }
    #[instrument(skip(self))]
    async fn subgraph_node_types(&self, node_types: Vec<String>) -> GqlGraph {
        GqlGraph::new(
            self.path.clone(),
            self.graph.subgraph_node_types(node_types),
        )
    }
    #[instrument(skip(self))]
    async fn exclude_nodes(&self, nodes: Vec<String>) -> GqlGraph {
        let nodes: Vec<NodeRef> = nodes.iter().map(|v| v.as_node_ref()).collect();
        GqlGraph::new(self.path.clone(), self.graph.exclude_nodes(nodes))
    }
    #[instrument(skip(self))]
    async fn exclude_nodes_id(&self, nodes: Vec<u64>) -> GqlGraph {
        let nodes: Vec<NodeRef> = nodes.iter().map(|v| v.as_node_ref()).collect();
        GqlGraph::new(self.path.clone(), self.graph.exclude_nodes(nodes))
    }

    /// Return a graph containing only the activity between `start` and `end` measured as milliseconds from epoch
    #[instrument(skip(self))]
    async fn window(&self, start: i64, end: i64) -> GqlGraph {
        GqlGraph::new(self.path.clone(), self.graph.window(start, end))
    }
    #[instrument(skip(self))]
    async fn at(&self, time: i64) -> GqlGraph {
        GqlGraph::new(self.path.clone(), self.graph.at(time))
    }
    #[instrument(skip(self))]
    async fn before(&self, time: i64) -> GqlGraph {
        GqlGraph::new(self.path.clone(), self.graph.before(time))
    }
    #[instrument(skip(self))]
    async fn after(&self, time: i64) -> GqlGraph {
        GqlGraph::new(self.path.clone(), self.graph.after(time))
    }
    #[instrument(skip(self))]
    async fn shrink_window(&self, start: i64, end: i64) -> Self {
        GqlGraph::new(self.path.clone(), self.graph.shrink_window(start, end))
    }
    #[instrument(skip(self))]
    async fn shrink_start(&self, start: i64) -> Self {
        GqlGraph::new(self.path.clone(), self.graph.shrink_start(start))
    }
    #[instrument(skip(self))]
    async fn shrink_end(&self, end: i64) -> Self {
        GqlGraph::new(self.path.clone(), self.graph.shrink_end(end))
    }

    ////////////////////////
    //// TIME QUERIES //////
    ////////////////////////

    #[instrument(skip(self))]
    async fn earliest_time(&self) -> Option<i64> {
        self.graph.earliest_time()
    }
    #[instrument(skip(self))]
    async fn latest_time(&self) -> Option<i64> {
        self.graph.latest_time()
    }
    #[instrument(skip(self))]
    async fn start(&self) -> Option<i64> {
        self.graph.start()
    }

    #[instrument(skip(self))]
    async fn end(&self) -> Option<i64> {
        self.graph.end()
    }

    #[instrument(skip(self))]
    async fn earliest_edge_time(&self, include_negative: Option<bool>) -> Option<i64> {
        let include_negative = include_negative.unwrap_or(true);
        let all_edges = self
            .graph
            .edges()
            .earliest_time()
            .into_iter()
            .filter_map(|edge_time| edge_time.filter(|&time| include_negative || time >= 0))
            .min();
        return all_edges;
    }

    #[instrument(skip(self))]
    async fn latest_edge_time(&self, include_negative: Option<bool>) -> Option<i64> {
        let include_negative = include_negative.unwrap_or(true);
        let all_edges = self
            .graph
            .edges()
            .latest_time()
            .into_iter()
            .filter_map(|edge_time| edge_time.filter(|&time| include_negative || time >= 0))
            .max();

        return all_edges;
    }

    ////////////////////////
    //////// COUNTERS //////
    ////////////////////////

    #[instrument(skip(self))]
    async fn count_edges(&self) -> usize {
        self.graph.count_edges()
    }

    #[instrument(skip(self))]
    async fn count_temporal_edges(&self) -> usize {
        self.graph.count_temporal_edges()
    }

    #[instrument(skip(self))]
    async fn search_edge_count(&self, query: String) -> usize {
        self.graph.search_edge_count(&query).unwrap_or(0)
    }

    #[instrument(skip(self))]
    async fn count_nodes(&self) -> usize {
        self.graph.count_nodes()
    }
    #[instrument(skip(self))]
    async fn search_node_count(&self, query: String) -> usize {
        self.graph.search_node_count(&query).unwrap_or(0)
    }

    ////////////////////////
    //// EXISTS CHECKERS ///
    ////////////////////////

    #[instrument(skip(self))]
    async fn has_node(&self, name: String) -> bool {
        self.graph.has_node(name)
    }
    #[instrument(skip(self))]
    async fn has_node_id(&self, id: u64) -> bool {
        self.graph.has_node(id)
    }
    #[instrument(skip(self))]
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

    #[instrument(skip(self))]
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

    #[instrument(skip(self))]
    async fn node(&self, name: String) -> Option<Node> {
        self.graph.node(name).map(|v| v.into())
    }

    #[instrument(skip(self))]
    async fn node_id(&self, id: u64) -> Option<Node> {
        self.graph.node(id).map(|v| v.into())
    }

    #[instrument(skip(self))]
    async fn nodes(&self) -> GqlNodes {
        GqlNodes::new(self.graph.nodes())
    }

    #[instrument(skip(self))]
    async fn search_nodes(&self, query: String, limit: usize, offset: usize) -> Vec<Node> {
        self.graph
            .search_nodes(&query, limit, offset)
            .into_iter()
            .flatten()
            .map(|vv| vv.into())
            .collect()
    }

    #[instrument(skip(self))]
    async fn fuzzy_search_nodes(
        &self,
        query: String,
        limit: usize,
        offset: usize,
        prefix: bool,
        levenshtein_distance: u8,
    ) -> Vec<Node> {
        self.graph
            .fuzzy_search_nodes(&query, limit, offset, prefix, levenshtein_distance)
            .into_iter()
            .flatten()
            .map(|vv| vv.into())
            .collect()
    }

    #[instrument(skip(self))]
    pub fn edge(&self, src: String, dst: String) -> Option<Edge> {
        self.graph.edge(src, dst).map(|e| e.into())
    }

    #[instrument(skip(self))]
    pub fn edge_id(&self, src: u64, dst: u64) -> Option<Edge> {
        self.graph.edge(src, dst).map(|e| e.into())
    }

    #[instrument(skip(self))]
    async fn edges<'a>(&self) -> GqlEdges {
        GqlEdges::new(self.graph.edges())
    }

    #[instrument(skip(self))]
    async fn search_edges(&self, query: String, limit: usize, offset: usize) -> Vec<Edge> {
        self.graph
            .search_edges(&query, limit, offset)
            .into_iter()
            .flatten()
            .map(|vv| vv.into())
            .collect()
    }

    #[instrument(skip(self))]
    async fn fuzzy_search_edges(
        &self,
        query: String,
        limit: usize,
        offset: usize,
        prefix: bool,
        levenshtein_distance: u8,
    ) -> Vec<Edge> {
        self.graph
            .fuzzy_search_edges(&query, limit, offset, prefix, levenshtein_distance)
            .into_iter()
            .flatten()
            .map(|vv| vv.into())
            .collect()
    }

    ////////////////////////
    /////// PROPERTIES /////
    ////////////////////////

    #[instrument(skip(self))]
    async fn properties(&self) -> GqlProperties {
        Into::<DynProperties>::into(self.graph.properties()).into()
    }

    ////////////////////////
    // GRAPHQL SPECIFIC ////
    ////////////////////////

    //These name/path functions basically can only fail
    //if someone write non-utf characters as a filename
    #[instrument(skip(self))]
    async fn name(&self) -> Result<String, GraphError> {
        get_graph_name(&self.path)
    }
    #[instrument(skip(self))]
    async fn path(&self) -> Result<String, GraphError> {
        self.path
            .to_str()
            .map(|s| s.to_string())
            .ok_or(PathNotParsable(self.path.to_path_buf()).into())
    }

    #[instrument(skip(self))]
    async fn schema(&self) -> GraphSchema {
        GraphSchema::new(self.graph.graph())
    }

    #[instrument(skip(self))]
    async fn algorithms(&self) -> GraphAlgorithms {
        self.graph.graph().clone().into()
    }

    #[instrument(skip(self))]
    async fn shared_neighbours(&self, selected_nodes: Vec<String>) -> Vec<Node> {
        if selected_nodes.is_empty() {
            return vec![];
        }

        let neighbours: Vec<HashSet<NodeView<IndexedGraph<DynamicGraph>>>> = selected_nodes
            .iter()
            .filter_map(|n| self.graph.node(n))
            .map(|n| {
                n.neighbours()
                    .collect()
                    .iter()
                    .map(|vv| vv.clone())
                    .collect::<HashSet<NodeView<IndexedGraph<DynamicGraph>>>>()
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
    #[instrument(skip(self, ctx))]
    async fn export_to<'a>(
        &'a self,
        ctx: &Context<'a>,
        path: String,
    ) -> Result<bool, Arc<GraphError>> {
        let data = ctx.data_unchecked::<Data>();
        let other_g = data.get_graph(path.as_ref())?;
        other_g.import_nodes(self.graph.nodes(), true)?;
        other_g.import_edges(self.graph.edges(), true)?;
        other_g.write_updates()?;
        Ok(true)
    }
}
