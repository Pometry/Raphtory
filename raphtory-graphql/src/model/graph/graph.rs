use crate::model::{
    algorithms::graph_algorithms::GraphAlgorithms,
    filters::{edge_filter::EdgeFilter, node_filter::NodeFilter},
    graph::{edge::Edge, get_expanded_edges, node::Node, property::Property},
    schema::graph_schema::GraphSchema,
};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use itertools::Itertools;
use raphtory::{
    core::entities::vertices::vertex_ref::VertexRef,
    db::{
        api::view::{DynamicGraph, IntoDynamic, StaticGraphViewOps, TimeOps, VertexViewOps},
        graph::edge::EdgeView,
    },
    prelude::*,
    search::IndexedGraph,
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
    pub fn new(name: String, graph: IndexedGraph<DynamicGraph>) -> Self {
        Self { name, graph }
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
        GqlGraph::new(
            self.name().await,
            self.graph.default_layer().into_dynamic_indexed(),
        )
    }

    async fn layers(&self, names: Vec<String>) -> Option<GqlGraph> {
        let name = self.name().await;
        self.graph
            .layer(names)
            .map(move |g| GqlGraph::new(name, g.into_dynamic_indexed()))
    }
    async fn layer(&self, name: String) -> Option<GqlGraph> {
        let name = self.name().await;
        self.graph
            .layer(name.clone())
            .map(|g| GqlGraph::new(name, g.into_dynamic_indexed()))
    }

    async fn subgraph(&self, nodes: Vec<String>) -> GqlGraph {
        let nodes: Vec<VertexRef> = nodes.iter().map(|v| v.clone().into()).collect();
        GqlGraph::new(
            self.name().await,
            self.graph.subgraph(nodes).into_dynamic_indexed(),
        )
    }

    async fn subgraph_id(&self, nodes: Vec<u64>) -> GqlGraph {
        let nodes: Vec<VertexRef> = nodes
            .iter()
            .map(|v| {
                let v = *v;
                let v: VertexRef = v.into();
                v
            })
            .collect();
        GqlGraph::new(
            self.name().await,
            self.graph.subgraph(nodes).into_dynamic_indexed(),
        )
    }

    /// Return a graph containing only the activity between `start` and `end` measured as milliseconds from epoch
    async fn window(&self, start: i64, end: i64) -> GqlGraph {
        GqlGraph::new(
            self.name().await,
            self.graph.window(start, end).into_dynamic_indexed(),
        )
    }
    async fn at(&self, time: i64) -> GqlGraph {
        GqlGraph::new(
            self.name().await,
            self.graph.at(time).into_dynamic_indexed(),
        )
    }

    async fn before(&self, time: i64) -> GqlGraph {
        GqlGraph::new(
            self.name().await,
            self.graph.before(time).into_dynamic_indexed(),
        )
    }

    async fn after(&self, time: i64) -> GqlGraph {
        GqlGraph::new(
            self.name().await,
            self.graph.after(time).into_dynamic_indexed(),
        )
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

    async fn edge_count(&self, filter: Option<EdgeFilter>) -> usize {
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

    async fn temporal_edge_count(&self, filter: Option<EdgeFilter>) -> usize {
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

    async fn node_count(&self, filter: Option<NodeFilter>) -> usize {
        if let Some(filter) = filter {
            self.graph
                .vertices()
                .iter()
                .map(|vv| vv.into())
                .filter(|n| filter.matches(n))
                .count()
        } else {
            self.graph.count_vertices()
        }
    }
    async fn search_node_count(&self, query: String) -> usize {
        self.graph.search_vertex_count(&query).unwrap_or(0)
    }

    ////////////////////////
    //// EXISTS CHECKERS ///
    ////////////////////////

    async fn has_node(&self, name: String) -> bool {
        let v_ref: VertexRef = name.into();
        self.graph.has_vertex(v_ref)
    }
    async fn has_node_id(&self, id: u64) -> bool {
        let v_ref: VertexRef = id.into();
        self.graph.has_vertex(v_ref)
    }
    async fn has_edge(&self, src: String, dst: String, layer: Option<String>) -> bool {
        let src: VertexRef = src.into();
        let dst: VertexRef = dst.into();
        self.graph.has_edge(src, dst, layer)
    }

    async fn has_edge_id(&self, src: u64, dst: u64, layer: Option<String>) -> bool {
        let src: VertexRef = src.into();
        let dst: VertexRef = dst.into();
        self.graph.has_edge(src, dst, layer)
    }

    ////////////////////////
    //////// GETTERS ///////
    ////////////////////////
    async fn node(&self, name: String) -> Option<Node> {
        let v_ref: VertexRef = name.into();
        self.graph.vertex(v_ref).map(|v| v.into())
    }
    async fn node_id(&self, id: u64) -> Option<Node> {
        let v_ref: VertexRef = id.into();
        self.graph.vertex(v_ref).map(|v| v.into())
    }

    async fn nodes(&self, filter: Option<NodeFilter>) -> Vec<Node> {
        match filter {
            Some(filter) => self
                .graph
                .vertices()
                .iter()
                .map(|vv| vv.into())
                .filter(|n| filter.matches(n))
                .collect(),
            None => self.graph.vertices().iter().map(|vv| vv.into()).collect(),
        }
    }

    async fn search_nodes(&self, query: String, limit: usize, offset: usize) -> Vec<Node> {
        self.graph
            .search_nodes(&query, limit, offset)
            .into_iter()
            .flat_map(|vv| vv)
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
            .flat_map(|vv| vv)
            .map(|vv| vv.into())
            .collect()
    }

    pub fn edge(&self, src: String, dst: String) -> Option<Edge> {
        let src: VertexRef = src.into();
        let dst: VertexRef = dst.into();
        self.graph.edge(src, dst).map(|e| e.into())
    }

    pub fn edge_id(&self, src: u64, dst: u64) -> Option<Edge> {
        let src: VertexRef = src.into();
        let dst: VertexRef = dst.into();
        self.graph.edge(src, dst).map(|e| e.into())
    }

    async fn edges<'a>(
        &self,
        filter: Option<EdgeFilter>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Vec<Edge> {
        match (filter, limit, offset) {
            (None, None, None) => {
                return self.graph.edges().into_iter().map(|ev| ev.into()).collect()
            }
            (Some(filter), None, None) => return get_edges_with_filter(&self.graph, filter),
            (None, Some(limit), Some(offset)) => {
                return get_edges_with_limit(&self.graph, limit, offset)
            }
            (Some(filter), Some(limit), Some(offset)) => {
                return get_edges_with_filter_limit(&self.graph, filter, limit, offset);
            }
            (Some(filter), None, Some(offset)) => {
                return get_edges_with_filter_limit(&self.graph, filter, 10, offset)
            }
            (Some(filter), Some(limit), None) => {
                return get_edges_with_filter_limit(&self.graph, filter, limit, 0)
            }
            (None, None, Some(offset)) => return get_edges_with_limit(&self.graph, 10, offset),
            (None, Some(limit), None) => return get_edges_with_limit(&self.graph, limit, 0),
        }

        fn get_edges_with_filter_limit(
            graph: &IndexedGraph<DynamicGraph>,
            filter: EdgeFilter,
            limit: usize,
            offset: usize,
        ) -> Vec<Edge> {
            let start_position = limit * offset;
            let end_position = start_position + limit;
            graph
                .edges()
                .into_iter()
                .enumerate()
                .map(|(i, ev)| (i, std::convert::Into::<Edge>::into(ev)))
                .filter_map(|(i, ev)| {
                    if filter.matches(&ev) && i >= start_position && i < end_position {
                        Some(ev)
                    } else {
                        None
                    }
                })
                .collect()
        }

        fn get_edges_with_filter(
            graph: &IndexedGraph<DynamicGraph>,
            filter: EdgeFilter,
        ) -> Vec<Edge> {
            graph
                .edges()
                .into_iter()
                .map(|ev| ev.into())
                .filter(|ev| filter.matches(ev))
                .collect()
        }

        fn get_edges_with_limit(
            graph: &IndexedGraph<DynamicGraph>,
            limit: usize,
            offset: usize,
        ) -> Vec<Edge> {
            let start_position = limit * offset;
            let end_position = start_position + limit;
            graph
                .edges()
                .into_iter()
                .enumerate()
                .map(|(i, ev)| (i, std::convert::Into::<Edge>::into(ev)))
                .filter_map(|(i, ev)| {
                    if i >= start_position && i < end_position {
                        Some(ev)
                    } else {
                        None
                    }
                })
                .collect()
        }
    }

    async fn search_edges(&self, query: String, limit: usize, offset: usize) -> Vec<Edge> {
        self.graph
            .search_edges(&query, limit, offset)
            .into_iter()
            .flat_map(|vv| vv)
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
            .flat_map(|vv| vv)
            .map(|vv| vv.into())
            .collect()
    }

    ////////////////////////
    /////// PROPERTIES /////
    ////////////////////////
    async fn properties(&self) -> Vec<Property> {
        self.graph
            .properties()
            .into_iter()
            .map(|(k, v)| Property::new(k.into(), v))
            .collect()
    }

    async fn constant_properties(&self) -> Vec<Property> {
        self.graph
            .properties()
            .constant()
            .into_iter()
            .map(|(k, v)| Property::new(k.into(), v))
            .collect()
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
            .vertices()
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
            .vertices()
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
