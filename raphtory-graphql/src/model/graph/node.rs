use crate::model::{
    filters::edge_filter::EdgeFilter,
    graph::{edge::Edge, get_expanded_edges, property::GqlProperties},
};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use itertools::Itertools;
use raphtory::{
    core::utils::errors::GraphError,
    db::{
        api::{properties::dyn_props::DynProperties, view::*},
        graph::node::NodeView,
    },
};
use std::collections::HashSet;

#[derive(ResolvedObject)]
pub(crate) struct Node {
    pub(crate) vv: NodeView<DynamicGraph>,
}

impl<G: StaticGraphViewOps + IntoDynamic, GH: StaticGraphViewOps + IntoDynamic>
    From<NodeView<G, GH>> for Node
{
    fn from(value: NodeView<G, GH>) -> Self {
        Self {
            vv: NodeView {
                base_graph: value.base_graph.into_dynamic(),
                graph: value.graph.into_dynamic(),
                node: value.node,
            },
        }
    }
}

#[ResolvedObjectFields]
impl Node {
    async fn id(&self) -> String {
        self.vv.id().to_string()
    }

    pub async fn name(&self) -> String {
        self.vv.name()
    }

    ////////////////////////
    // LAYERS AND WINDOWS //
    ////////////////////////

    async fn layers(&self, names: Vec<String>) -> Node {
        self.vv.valid_layers(names).into()
    }
    async fn layer(&self, name: String) -> Node {
        self.vv.valid_layers(name).into()
    }
    async fn window(&self, start: i64, end: i64) -> Node {
        self.vv.window(start, end).into()
    }
    async fn at(&self, time: i64) -> Node {
        self.vv.at(time).into()
    }

    async fn before(&self, time: i64) -> Node {
        self.vv.before(time).into()
    }

    async fn after(&self, time: i64) -> Node {
        self.vv.after(time).into()
    }

    async fn shrink_window(&self, start: i64, end: i64) -> Self {
        self.vv.shrink_window(start, end).into()
    }

    async fn shrink_start(&self, start: i64) -> Self {
        self.vv.shrink_start(start).into()
    }

    async fn shrink_end(&self, end: i64) -> Self {
        self.vv.shrink_end(end).into()
    }

    ////////////////////////
    //// TIME QUERIES //////
    ////////////////////////

    async fn earliest_time(&self) -> Option<i64> {
        self.vv.earliest_time()
    }

    async fn first_update(&self) -> Option<i64> {
        self.vv.history().first().cloned()
    }

    async fn latest_time(&self) -> Option<i64> {
        self.vv.latest_time()
    }

    async fn last_update(&self) -> Option<i64> {
        self.vv.history().last().cloned()
    }

    async fn start(&self) -> Option<i64> {
        self.vv.start()
    }

    async fn end(&self) -> Option<i64> {
        self.vv.end()
    }

    async fn history(&self) -> Vec<i64> {
        self.vv.history()
    }
    ////////////////////////
    /////// PROPERTIES /////
    ////////////////////////
    pub async fn node_type(&self) -> String {
        self.vv
            .properties()
            .get("type")
            .map(|p| p.to_string())
            .unwrap_or("NONE".to_string())
    }

    async fn properties(&self) -> GqlProperties {
        Into::<DynProperties>::into(self.vv.properties()).into()
    }
    ////////////////////////
    //// EDGE GETTERS //////
    ////////////////////////
    /// Returns the number of edges connected to this node
    async fn degree(&self) -> usize {
        self.vv.degree()
    }

    /// Returns the number edges with this node as the source
    async fn out_degree(&self) -> usize {
        self.vv.out_degree()
    }

    /// Returns the number edges with this node as the destination
    async fn in_degree(&self) -> usize {
        self.vv.in_degree()
    }

    async fn edges(&self, filter: Option<EdgeFilter>) -> Vec<Edge> {
        match filter {
            Some(filter) => self
                .vv
                .edges()
                .iter()
                .map(|ev| ev.into())
                .filter(|ev| filter.matches(ev))
                .collect(),
            None => self.vv.edges().iter().map(|ee| ee.into()).collect(),
        }
    }
    async fn out_edges(&self, filter: Option<EdgeFilter>) -> Vec<Edge> {
        match filter {
            Some(filter) => self
                .vv
                .out_edges()
                .iter()
                .map(|ev| ev.into())
                .filter(|ev| filter.matches(ev))
                .collect(),
            None => self.vv.out_edges().iter().map(|ee| ee.into()).collect(),
        }
    }

    async fn in_edges(&self, filter: Option<EdgeFilter>) -> Vec<Edge> {
        match filter {
            Some(filter) => self
                .vv
                .in_edges()
                .iter()
                .map(|ev| ev.into())
                .filter(|ev| filter.matches(ev))
                .collect(),
            None => self.vv.in_edges().iter().map(|ee| ee.into()).collect(),
        }
    }

    async fn neighbours<'a>(&self) -> Vec<Node> {
        self.vv.neighbours().iter().map(|vv| vv.into()).collect()
    }

    async fn in_neighbours<'a>(&self) -> Vec<Node> {
        self.vv.in_neighbours().iter().map(|vv| vv.into()).collect()
    }

    async fn out_neighbours(&self) -> Vec<Node> {
        self.vv
            .out_neighbours()
            .iter()
            .map(|vv| vv.into())
            .collect()
    }

    ////////////////////////
    // GRAPHQL SPECIFIC ////
    ////////////////////////

    async fn expanded_edges(
        &self,
        graph_nodes: Vec<String>,
        filter: Option<EdgeFilter>,
    ) -> Vec<Edge> {
        let all_graph_nodes: HashSet<String> = graph_nodes.into_iter().collect();

        match filter {
            Some(edge_filter) => {
                let maybe_layers = edge_filter.clone().layer_names.map(|l| l.contains);
                let fetched_edges =
                    get_expanded_edges(all_graph_nodes, self.vv.clone(), maybe_layers)
                        .iter()
                        .map(|ee| ee.clone().into())
                        .collect_vec();
                fetched_edges
                    .into_iter()
                    .filter(|ev| edge_filter.matches(ev))
                    .collect()
            }
            None => get_expanded_edges(all_graph_nodes, self.vv.clone(), None)
                .iter()
                .map(|ee| ee.clone().into())
                .collect_vec(),
        }
    }
}
