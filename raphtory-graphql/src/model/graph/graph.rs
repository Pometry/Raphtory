use std::{
    collections::{HashMap, HashSet},
    ops::Deref,
};

use crate::model::{
    algorithm::Algorithms,
    filters::{edgefilter::EdgeFilter, nodefilter::NodeFilter},
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
            .map(|(k, v)| Property::new(k, v))
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

impl<G: GraphViewOps + IntoDynamic> From<G> for GqlGraph {
    fn from(value: G) -> Self {
        Self {
            graph: value.into_dynamic().into(),
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
    async fn window(&self, t_start: i64, t_end: i64) -> GqlGraph {
        let w = self.graph.window(t_start, t_end);
        w.into()
    }

    async fn layer_names(&self) -> Vec<String> {
        self.graph.get_unique_layers()
    }

    async fn static_properties(&self) -> Vec<Property> {
        self.graph
            .properties()
            .constant()
            .into_iter()
            .map(|(k, v)| Property::new(k, v))
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

    async fn edges<'a>(&self, filter: Option<EdgeFilter>) -> Vec<Edge> {
        match filter {
            Some(filter) => self
                .graph
                .edges()
                .into_iter()
                .map(|ev| ev.into())
                .filter(|ev| filter.matches(ev))
                .collect(),
            None => self.graph.edges().into_iter().map(|ev| ev.into()).collect(),
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
