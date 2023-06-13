use crate::data::Data;
use async_graphql::Context;
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use itertools::Itertools;
use raphtory::core::Prop;
use raphtory::db::edge::EdgeView;
use raphtory::db::vertex::VertexView;
use raphtory::db::view_api::internal::{GraphViewInternalOps, WrappedGraph};
use raphtory::db::view_api::EdgeListOps;
use raphtory::db::view_api::EdgeViewOps;
use raphtory::db::view_api::{GraphViewOps, TimeOps, VertexViewOps};
use std::sync::Arc;

use crate::model::algorithm::Algorithms;

pub(crate) mod algorithm;

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

#[derive(Clone)]
pub struct DynamicGraph(Arc<dyn GraphViewInternalOps + Send + Sync + 'static>);

impl WrappedGraph for DynamicGraph {
    type Internal = dyn GraphViewInternalOps + Send + Sync + 'static;
    fn graph(&self) -> &(dyn GraphViewInternalOps + Send + Sync + 'static) {
        &*self.0
    }
}

#[derive(ResolvedObject)]
pub(crate) struct GqlGraph {
    graph: DynamicGraph,
}

impl<G: GraphViewOps> From<G> for GqlGraph {
    fn from(value: G) -> Self {
        let graph = DynamicGraph(Arc::new(value));
        Self { graph }
    }
}

#[ResolvedObjectFields]
impl GqlGraph {
    async fn window(&self, t_start: i64, t_end: i64) -> GqlGraph {
        let w = self.graph.window(t_start, t_end);
        w.into()
    }

    async fn nodes(&self) -> Vec<Node> {
        self.graph.vertices().iter().map(|vv| vv.into()).collect()
    }

    async fn edges<'a>(&self) -> Vec<Edge> {
        self.graph.edges().into_iter().map(|ev| ev.into()).collect()
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
        self.graph.clone().into()
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
pub(crate) struct Node {
    vv: VertexView<DynamicGraph>,
}

impl From<VertexView<DynamicGraph>> for Node {
    fn from(vv: VertexView<DynamicGraph>) -> Self {
        Self { vv }
    }
}

#[ResolvedObjectFields]
impl Node {
    async fn id(&self) -> u64 {
        self.vv.id()
    }

    async fn name(&self) -> String {
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

    async fn in_neighbours<'a>(&self, _ctx: &Context<'a>) -> Vec<Node> {
        self.vv.in_neighbours().iter().map(|vv| vv.into()).collect()
    }

    async fn out_neighbours(&self) -> Vec<Node> {
        self.vv
            .out_neighbours()
            .iter()
            .map(|vv| vv.into())
            .collect()
    }

    async fn neighbours<'a>(&self, _ctx: &Context<'a>) -> Vec<Node> {
        self.vv.neighbours().iter().map(|vv| vv.into()).collect()
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

    async fn out_edges(&self) -> Vec<Edge> {
        self.vv.out_edges().map(|ee| ee.clone().into()).collect()
    }

    async fn in_edges(&self) -> Vec<Edge> {
        self.vv.in_edges().map(|ee| ee.into()).collect()
    }

    async fn exploded_edges(&self) -> Vec<Edge> {
        self.vv.out_edges().explode().map(|ee| ee.into()).collect()
    }

    async fn start_date(&self) -> Option<i64> {
        self.vv.earliest_time()
    }

    async fn end_date(&self) -> Option<i64> {
        self.vv.latest_time()
    }
}

#[derive(ResolvedObject)]
pub(crate) struct Edge {
    ee: EdgeView<DynamicGraph>,
}

impl From<EdgeView<DynamicGraph>> for Edge {
    fn from(ee: EdgeView<DynamicGraph>) -> Self {
        Self { ee }
    }
}

#[ResolvedObjectFields]
impl Edge {
    async fn earliest_time(&self) -> Option<i64> {
        self.ee.earliest_time()
    }

    async fn latest_time(&self) -> Option<i64> {
        self.ee.latest_time()
    }

    async fn src(&self) -> Node {
        self.ee.src().into()
    }

    async fn dst(&self) -> Node {
        self.ee.dst().into()
    }

    async fn property(&self, name: String) -> Option<Property> {
        let prop = self.ee.property(name.clone(), true)?;
        Some(Property::new(name, prop))
    }

    async fn history(&self) -> Vec<i64> {
        self.ee.history()
    }
}
