use crate::data::Data;
use async_graphql::Context;
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields, SimpleObject};
use raphtory::db::edge::EdgeView;
use raphtory::db::graph::Graph;
use raphtory::db::graph_window::WindowedGraph;
use raphtory::db::vertex::VertexView;
use raphtory::db::view_api::internal::{GraphViewInternalOps, WrappedGraph};
use raphtory::db::view_api::EdgeListOps;
use raphtory::db::view_api::EdgeViewOps;
use raphtory::db::view_api::{GraphViewOps, TimeOps, VertexViewOps};
use std::any::Any;
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
    fn as_graph(&self) -> &(dyn GraphViewInternalOps + Send + Sync + 'static) {
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

    async fn out_neighbours(&self) -> Vec<Node> {
        self.vv
            .out_neighbours()
            .iter()
            .map(|vv| vv.into())
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

    async fn history(&self) -> Vec<i64> {
        self.ee.history()
    }
}
