use crate::model::{
    algorithms::graph_algorithms::GraphAlgorithms,
    filters::{edge_filter::EdgeFilter, node_filter::NodeFilter},
    graph::{
        edge::Edge, edges::GqlEdges, get_expanded_edges, node::Node, nodes::GqlNodes,
        property::GqlProperties,
    },
    schema::graph_schema::GraphSchema,
};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use itertools::Itertools;
use raphtory::{
    core::{entities::nodes::node_ref::NodeRef, utils::errors::GraphError},
    db::{
        api::{
            properties::dyn_props::DynProperties,
            view::{DynamicGraph, NodeViewOps, TimeOps},
        },
        graph::edge::EdgeView,
    },
    prelude::*,
    search::{into_indexed::DynamicIndexedGraph, IndexedGraph},
};
use std::{
    collections::{HashMap, HashSet},
    convert::Into,
    ops::Deref,
};

#[derive(ResolvedObject)]
pub(crate) struct GqlGraph {
    name: String,
    graph: IndexedGraph<DynamicGraph>,
}

impl GqlGraph {
    pub fn new<G: DynamicIndexedGraph>(name: String, graph: G) -> Self {
        Self {
            name,
            graph: graph.into_dynamic_indexed(),
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
        GqlGraph::new(self.name.clone(), self.graph.default_layer())
    }

    async fn layers(&self, names: Vec<String>) -> GqlGraph {
        let name = self.name.clone();
        GqlGraph::new(name, self.graph.valid_layers(names))
    }
    async fn layer(&self, name: String) -> GqlGraph {
        GqlGraph::new(self.name.clone(), self.graph.valid_layers(name))
    }

    async fn subgraph(&self, nodes: Vec<String>) -> GqlGraph {
        let nodes: Vec<NodeRef> = nodes.iter().map(|v| v.as_str().into()).collect();
        GqlGraph::new(self.name.clone(), self.graph.subgraph(nodes))
    }

    async fn subgraph_id(&self, nodes: Vec<u64>) -> GqlGraph {
        let nodes: Vec<NodeRef> = nodes
            .iter()
            .map(|v| {
                let v = *v;
                let v: NodeRef = v.into();
                v
            })
            .collect();
        GqlGraph::new(self.name.clone(), self.graph.subgraph(nodes))
    }

    /// Return a graph containing only the activity between `start` and `end` measured as milliseconds from epoch
    async fn window(&self, start: i64, end: i64) -> GqlGraph {
        GqlGraph::new(self.name.clone(), self.graph.window(start, end))
    }
    async fn at(&self, time: i64) -> GqlGraph {
        GqlGraph::new(self.name.clone(), self.graph.at(time))
    }

    async fn before(&self, time: i64) -> GqlGraph {
        GqlGraph::new(self.name.clone(), self.graph.before(time))
    }

    async fn after(&self, time: i64) -> GqlGraph {
        GqlGraph::new(self.name.clone(), self.graph.after(time))
    }

    async fn shrink_window(&self, start: i64, end: i64) -> Self {
        GqlGraph::new(self.name.clone(), self.graph.shrink_window(start, end))
    }

    async fn shrink_start(&self, start: i64) -> Self {
        GqlGraph::new(self.name.clone(), self.graph.shrink_start(start))
    }

    async fn shrink_end(&self, end: i64) -> Self {
        GqlGraph::new(self.name.clone(), self.graph.shrink_end(end))
    }

    ////////////////////////
    //// TIME QUERIES //////
    ////////////////////////
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
            .filter_map(|edge_time| edge_time.filter(|&time| include_negative || time >= 0))
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
            .filter_map(|edge_time| edge_time.filter(|&time| include_negative || time >= 0))
            .max();

        return all_edges;
    }

    ////////////////////////
    //////// COUNTERS //////
    ////////////////////////

    async fn count_edges(&self, filter: Option<EdgeFilter>) -> usize {
        if let Some(filter) = filter {
            self.graph
                .edges()
                .into_iter()
                .map(|ev| ev.into())
                .filter(|ev| filter.matches(ev))
                .count()
        } else {
            self.graph.count_edges()
        }
    }

    async fn count_temporal_edges(&self, filter: Option<EdgeFilter>) -> usize {
        if let Some(filter) = filter {
            self.graph
                .edges()
                .explode()
                .into_iter()
                .map(|ev| ev.into())
                .filter(|ev| filter.matches(ev))
                .count()
        } else {
            self.graph.count_temporal_edges()
        }
    }

    async fn search_edge_count(&self, query: String) -> usize {
        self.graph.search_edge_count(&query).unwrap_or(0)
    }

    async fn count_nodes(&self, filter: Option<NodeFilter>) -> usize {
        if let Some(filter) = filter {
            self.graph
                .nodes()
                .iter()
                .map(|vv| vv.into())
                .filter(|n| filter.matches(n))
                .count()
        } else {
            self.graph.count_nodes()
        }
    }
    async fn search_node_count(&self, query: String) -> usize {
        self.graph.search_node_count(&query).unwrap_or(0)
    }

    ////////////////////////
    //// EXISTS CHECKERS ///
    ////////////////////////

    async fn has_node(&self, name: String) -> bool {
        let v_ref: NodeRef = name.into();
        self.graph.has_node(v_ref)
    }
    async fn has_node_id(&self, id: u64) -> bool {
        let v_ref: NodeRef = id.into();
        self.graph.has_node(v_ref)
    }
    async fn has_edge(&self, src: String, dst: String, layer: Option<String>) -> bool {
        let src: NodeRef = src.into();
        let dst: NodeRef = dst.into();
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
        let src: NodeRef = src.into();
        let dst: NodeRef = dst.into();
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
        let v_ref: NodeRef = name.into();
        self.graph.node(v_ref).map(|v| v.into())
    }
    async fn node_id(&self, id: u64) -> Option<Node> {
        let v_ref: NodeRef = id.into();
        self.graph.node(v_ref).map(|v| v.into())
    }

    async fn nodes(&self, filter: Option<NodeFilter>) -> GqlNodes {
        GqlNodes::new(self.graph.nodes(), filter)
    }

    async fn search_nodes(&self, query: String, limit: usize, offset: usize) -> Vec<Node> {
        self.graph
            .search_nodes(&query, limit, offset)
            .into_iter()
            .flatten()
            .map(|vv| vv.into())
            .collect()
    }
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

    pub fn edge(&self, src: String, dst: String) -> Option<Edge> {
        let src: NodeRef = src.into();
        let dst: NodeRef = dst.into();
        self.graph.edge(src, dst).map(|e| e.into())
    }

    pub fn edge_id(&self, src: u64, dst: u64) -> Option<Edge> {
        let src: NodeRef = src.into();
        let dst: NodeRef = dst.into();
        self.graph.edge(src, dst).map(|e| e.into())
    }

    async fn edges<'a>(&self, filter: Option<EdgeFilter>) -> GqlEdges {
        GqlEdges::new(self.graph.edges(), filter)
    }

    async fn search_edges(&self, query: String, limit: usize, offset: usize) -> Vec<Edge> {
        self.graph
            .search_edges(&query, limit, offset)
            .into_iter()
            .flatten()
            .map(|vv| vv.into())
            .collect()
    }
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
    async fn properties(&self) -> GqlProperties {
        Into::<DynProperties>::into(self.graph.properties()).into()
    }
    ////////////////////////
    // GRAPHQL SPECIFIC ////
    ////////////////////////

    async fn name(&self) -> String {
        self.name.clone()
    }
    async fn schema(&self) -> GraphSchema {
        GraphSchema::new(&self.graph)
    }
    async fn algorithms(&self) -> GraphAlgorithms {
        self.graph.deref().clone().into()
    }

    async fn node_names(&self) -> Vec<String> {
        self.graph
            .nodes()
            .into_iter()
            .map(|v| v.name())
            .collect_vec()
    }
    async fn expanded_edges(
        &self,
        nodes_to_expand: Vec<String>,
        graph_nodes: Vec<String>,
        filter: Option<EdgeFilter>,
    ) -> Vec<Edge> {
        if nodes_to_expand.is_empty() {
            return vec![];
        }

        let nodes: Vec<Node> = self
            .graph
            .nodes()
            .iter()
            .map(|vv| vv.into())
            .filter(|n| NodeFilter::new(nodes_to_expand.clone()).matches(n))
            .collect();

        let mut all_graph_nodes: HashSet<String> = graph_nodes.into_iter().collect();
        let mut all_expanded_edges: HashMap<String, EdgeView<DynamicGraph>> = HashMap::new();

        let mut maybe_layers: Option<Vec<String>> = None;
        if filter.is_some() {
            maybe_layers = filter.clone().unwrap().layer_names.map(|l| l.contains);
        }

        for node in nodes {
            let expanded_edges =
                get_expanded_edges(all_graph_nodes.clone(), node.vv, maybe_layers.clone());
            expanded_edges.clone().into_iter().for_each(|e| {
                let src = e.src().name();
                let dst = e.dst().name();
                all_expanded_edges.insert(src.to_owned() + &dst, e);
                all_graph_nodes.insert(src);
                all_graph_nodes.insert(dst);
            });
        }

        let fetched_edges = all_expanded_edges
            .values()
            .map(|ee| ee.clone().into())
            .collect_vec();

        match filter {
            Some(filter) => fetched_edges
                .into_iter()
                .filter(|ev| filter.matches(ev))
                .collect(),
            None => fetched_edges,
        }
    }
}
