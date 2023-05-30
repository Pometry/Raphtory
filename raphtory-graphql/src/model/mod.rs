use crate::model::algorithm::Algorithms;
use crate::Data;
use async_graphql::{Context, Json};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields, Scalar, SimpleObject};
use dynamic_graphql::{ScalarValue, Value};
use itertools::Itertools;
use raphtory::core::Prop;
use raphtory::db::edge::EdgeView;
use raphtory::db::graph::Graph;
use raphtory::db::graph_window::WindowedGraph;
use raphtory::db::vertex::VertexView;
use raphtory::db::view_api::internal::GraphViewInternalOps;
use raphtory::db::view_api::EdgeListOps;
use raphtory::db::view_api::EdgeViewOps;
use raphtory::db::view_api::{GraphViewOps, TimeOps, VertexViewOps};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;

mod algorithm;

#[derive(ResolvedObject)]
#[graphql(root)]
pub(crate) struct QueryRoot;

#[ResolvedObjectFields]
impl QueryRoot {
    async fn hello() -> &'static str {
        "Hello world from raphtory-graphql"
    }

    /// Returns a view including all events between `t_start` (inclusive) and `t_end` (exclusive)
    async fn graph<'a>(ctx: &Context<'a>, name: &str) -> Option<GqlGraph> {
        let data = ctx.data_unchecked::<Data>();
        let g = data.graphs.get(name)?;
        Some(g.clone().into())
    }
}

#[derive(ResolvedObject)]
pub(crate) struct GqlGraph {
    graph: Arc<Graph>,
}

impl From<Graph> for GqlGraph {
    fn from(value: Graph) -> Self {
        let graph = Arc::new(value);
        Self { graph }
    }
}

#[ResolvedObjectFields]
impl GqlGraph {
    async fn window<'a>(&self, t_start: i64, t_end: i64) -> GqlWindowGraph<Graph> {
        let w = self.graph.window(t_start, t_end);
        w.into()
    }
}

#[derive(ResolvedObject)]
pub(crate) struct GqlWindowGraph<G: GraphViewOps> {
    graph: Arc<WindowedGraph<G>>,
}

impl<G: GraphViewOps> From<WindowedGraph<G>> for GqlWindowGraph<G> {
    fn from(value: WindowedGraph<G>) -> Self {
        let graph = Arc::new(value);
        Self { graph }
    }
}

#[ResolvedObjectFields]
impl<G: GraphViewOps> GqlWindowGraph<G> {
    async fn nodes<'a>(&self, _ctx: &Context<'a>) -> Vec<Node<WindowedGraph<G>>> {
        self.graph
            .vertices()
            .iter()
            .map(|vv| Node::new(vv))
            .collect()
    }

    async fn edges<'a>(&self, _ctx: &Context<'a>) -> Vec<Edge<WindowedGraph<G>>> {
        self.graph
            .edges()
            .into_iter()
            .map(|ev| Edge::new(ev))
            .collect()
    }

    async fn node<'a>(&self, _ctx: &Context<'a>, name: String) -> Option<Node<WindowedGraph<G>>> {
        self.graph
            .vertices()
            .iter()
            .find(|vv| &vv.name() == &name)
            .map(|vv| Node::new(vv))
    }

    async fn node_id<'a>(&self, _ctx: &Context<'a>, id: u64) -> Option<Node<WindowedGraph<G>>> {
        self.graph
            .vertices()
            .iter()
            .find(|vv| vv.id() == id)
            .map(|vv| Node::new(vv))
    }

    async fn algorithms<'a>(&self, _ctx: &Context<'a>) -> Algorithms<WindowedGraph<G>> {
        Algorithms::new(&self.graph)
    }
}

#[derive(ResolvedObject)]
pub(crate) struct Property {
    key: String,
    value: Prop,
}

impl Property {
    fn new(key: String, value: Prop) -> Self {
        Self { key, value }
    }
}

#[ResolvedObjectFields]
impl Property {
    async fn key(&self, _ctx: &Context<'_>) -> String {
        self.key.to_string()
    }

    async fn value(&self, _ctx: &Context<'_>) -> String {
        self.value.to_string()
    }
}

#[derive(ResolvedObject)]
pub(crate) struct Node<G: GraphViewOps> {
    vv: VertexView<G>,
}

impl<G: GraphViewOps> Node<G> {
    pub fn new(vv: VertexView<G>) -> Self {
        Self { vv }
    }
}

#[ResolvedObjectFields]
impl<G: GraphViewOps> Node<G> {
    async fn id(&self, _ctx: &Context<'_>) -> u64 {
        self.vv.id()
    }

    async fn name(&self, _ctx: &Context<'_>) -> String {
        self.vv.name()
    }

    async fn property_names<'a>(&self, _ctx: &Context<'a>) -> Vec<String> {
        self.vv.property_names(true)
    }

    async fn properties(&self) -> Option<Vec<Property>> {
        Some(
            self.vv
                .properties(true)
                .into_iter()
                .map(|(k, v)| Property::new(k, v))
                .collect_vec(),
        )
    }

    async fn property(&self, name: String) -> Option<Property> {
        let prop = self.vv.property(name.clone(), true)?;
        Some(Property::new(name, prop))
    }

    async fn out_neighbours<'a>(&self, _ctx: &Context<'a>) -> Vec<Node<G>> {
        self.vv
            .out_neighbours()
            .iter()
            .map(|vv| Node::new(vv.clone()))
            .collect()
    }

    async fn degree(&self) -> usize {
        self.vv.degree()
    }

    async fn out_degree(&self) -> usize {
        self.vv.out_degree()
    }

    async fn in_degree(&self) -> usize {
        self.vv.in_degree()
    }

    async fn out_edges(&self, _ctx: &Context<'_>) -> Vec<Edge<G>> {
        self.vv
            .out_edges()
            .map(|ee| Edge::new(ee.clone()))
            .collect()
    }

    async fn in_edges(&self, _ctx: &Context<'_>) -> Vec<Edge<G>> {
        self.vv.in_edges().map(|ee| Edge::new(ee.clone())).collect()
    }

    async fn exploded_edges(&self, _ctx: &Context<'_>) -> Vec<Edge<G>> {
        self.vv
            .out_edges()
            .explode()
            .map(|ee| Edge::new(ee.clone()))
            .collect()
    }

    async fn start_date(&self, _ctx: &Context<'_>) -> Option<i64> {
        self.vv.earliest_time()
    }

    async fn end_date(&self, _ctx: &Context<'_>) -> Option<i64> {
        self.vv.latest_time()
    }
}

#[derive(ResolvedObject)]
pub(crate) struct Edge<G: GraphViewOps> {
    ee: EdgeView<G>,
}

impl<G: GraphViewOps> Edge<G> {
    pub fn new(ee: EdgeView<G>) -> Self {
        Self { ee }
    }
}

#[ResolvedObjectFields]
impl<G: GraphViewOps> Edge<G> {
    async fn earliest_time(&self, _ctx: &Context<'_>) -> Option<i64> {
        self.ee.earliest_time()
    }

    async fn latest_time(&self, _ctx: &Context<'_>) -> Option<i64> {
        self.ee.latest_time()
    }

    async fn src(&self, _ctx: &Context<'_>) -> Node<G> {
        Node::new(self.ee.src())
    }

    async fn dst(&self, _ctx: &Context<'_>) -> Node<G> {
        Node::new(self.ee.dst())
    }

    async fn property(&self, name: String) -> Option<Property> {
        let prop = self.ee.property(name.clone(), true)?;
        Some(Property::new(name, prop))
    }

    async fn history(&self, _ctx: &Context<'_>) -> Vec<i64> {
        self.ee.history()
    }
}
