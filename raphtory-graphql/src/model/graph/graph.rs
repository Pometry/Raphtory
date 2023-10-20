use std::{
    collections::{HashMap, HashSet},
    ops::Deref,
};

use crate::model::{
    algorithm::Algorithms,
    filters::{edge_filter::EdgeFilter, node_filter::NodeFilter},
    graph::{edge::Edge, get_expanded_edges, node::Node, property::Property},
    schema::graph_schema::GraphSchema,
};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use itertools::Itertools;
use raphtory::{
    db::{
        api::view::{
            internal::{DynamicGraph, IntoDynamic},
            GraphViewOps, TimeOps, VertexViewOps,
        },
        graph::edge::EdgeView,
    },
    prelude::EdgeViewOps,
    search::IndexedGraph,
};

#[derive(ResolvedObject)]
pub(crate) struct GraphMeta {
    name: String,
    graph: DynamicGraph,
}

impl GraphMeta {
    pub fn new(name: String, graph: DynamicGraph) -> Self {
        Self { name, graph }
    }
}

#[ResolvedObjectFields]
impl GraphMeta {
    async fn name(&self) -> String {
        self.name.clone()
    }

    async fn static_properties(&self) -> Vec<Property> {
        self.graph
            .properties()
            .constant()
            .into_iter()
            .map(|(k, v)| Property::new(k.into(), v))
            .collect()
    }

    async fn node_names(&self) -> Vec<String> {
        self.graph
            .vertices()
            .into_iter()
            .map(|v| v.name())
            .collect_vec()
    }
}

#[derive(ResolvedObject)]
pub(crate) struct GqlGraph {
    graph: IndexedGraph<DynamicGraph>,
}

impl<G: GraphViewOps + IntoDynamic> From<IndexedGraph<G>> for GqlGraph {
    fn from(value: IndexedGraph<G>) -> Self {
        Self {
            graph: value.into_dynamic_indexed(),
        }
    }
}

impl GqlGraph {
    pub(crate) fn new(graph: IndexedGraph<DynamicGraph>) -> Self {
        Self { graph }
    }
}

#[ResolvedObjectFields]
impl GqlGraph {
    /// Return a graph containing only the activity between `start` and `end` measured as milliseconds from epoch
    async fn window(&self, start: i64, end: i64) -> GqlGraph {
        let w = self.graph.window(start, end);
        w.into_dynamic_indexed().into()
    }

    async fn layer_names(&self) -> Vec<String> {
        self.graph.unique_layers().map_into().collect()
    }

    async fn static_properties(&self) -> Vec<Property> {
        self.graph
            .properties()
            .constant()
            .into_iter()
            .map(|(k, v)| Property::new(k.into(), v))
            .collect()
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

    /// Returns the schema of this graph
    async fn schema(&self) -> GraphSchema {
        GraphSchema::new(&self.graph)
    }

    async fn search(&self, query: String, limit: usize, offset: usize) -> Vec<Node> {
        self.graph
            .search(&query, limit, offset)
            .into_iter()
            .flat_map(|vv| vv)
            .map(|vv| vv.into())
            .collect()
    }

    async fn search_edges(&self, query: String, limit: usize, offset: usize) -> Vec<Edge> {
        self.graph
            .search_edges(&query, limit, offset)
            .into_iter()
            .flat_map(|vv| vv)
            .map(|vv| vv.into())
            .collect()
    }

    async fn search_count(&self, query: String) -> usize {
        self.graph
            .search(&query, 999999, 0)
            .into_iter()
            .flatten()
            .count()
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
            (Some(filter), Some(limit), Some(offset)) => {
                return get_edges_with_filter_limit(&self.graph, filter, limit, offset);
            }
            (Some(filter), None, Some(offset)) => {
                return get_edges_with_filter_limit(&self.graph, filter, 10, offset)
            }
            (Some(filter), Some(limit), None) => {
                return get_edges_with_filter_limit(&self.graph, filter, limit, 0)
            }
            (Some(filter), None, None) => return get_edges_with_filter(&self.graph, filter),
            (None, Some(limit), Some(offset)) => {
                return get_edges_with_limit(&self.graph, limit, offset)
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
            let start_time = limit * offset;
            let end_time = start_time + limit;
            graph
                .edges()
                .into_iter()
                .enumerate()
                .map(|(i, ev)| (i, std::convert::Into::<Edge>::into(ev)))
                .filter_map(|(i, ev)| {
                    if filter.matches(&ev) && i >= start_time && i < end_time {
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
            let start_time = limit * offset;
            let end_time = start_time + limit;
            graph
                .edges()
                .into_iter()
                .enumerate()
                .map(|(i, ev)| (i, std::convert::Into::<Edge>::into(ev)))
                .filter_map(|(i, ev)| {
                    if i >= start_time && i < end_time {
                        Some(ev)
                    } else {
                        None
                    }
                })
                .collect()
        }
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

    async fn node(&self, name: String) -> Option<Node> {
        self.graph
            .vertices()
            .iter()
            .find(|vv| &vv.name() == &name)
            .map(|vv| vv.into())
    }

    async fn node_id(&self, id: u64) -> Option<Node> {
        self.graph
            .vertices()
            .iter()
            .find(|vv| vv.id() == id)
            .map(|vv| vv.into())
    }

    async fn algorithms(&self) -> Algorithms {
        self.graph.deref().clone().into()
    }
}
