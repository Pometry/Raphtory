use async_graphql::Context;
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields, SimpleObject};
use raphtory::db::graph::Graph;
use raphtory::db::graph_window::WindowedGraph;
use raphtory::db::vertex::VertexView;
use raphtory::db::view_api::{GraphViewOps, TimeOps, VertexViewOps};

use crate::data::Metadata;
use crate::model::algorithm::Algorithms;

mod algorithm;

#[derive(ResolvedObject)]
#[graphql(root)]
pub(crate) struct QueryRoot;

#[ResolvedObjectFields]
impl QueryRoot {
    async fn hello() -> &'static str {
        "Hello world"
    }

    async fn window<'a>(ctx: &Context<'a>, t_start: i64, t_end: i64) -> GqlWindowGraph<Graph> {
        let meta = ctx.data_unchecked::<Metadata<Graph>>();
        let g = meta.graph().window(t_start, t_end);
        GqlWindowGraph::new(g)
    }
}

#[derive(ResolvedObject)]
pub(crate) struct GqlWindowGraph<G: GraphViewOps> {
    graph: WindowedGraph<G>,
}

impl<G: GraphViewOps> GqlWindowGraph<G> {
    pub fn new(graph: WindowedGraph<G>) -> Self {
        Self { graph: graph }
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
}
