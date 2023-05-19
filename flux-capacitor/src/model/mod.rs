use async_graphql::dynamic::{
    Field, FieldFuture, FieldValue, InputValue, Object, ResolverContext, TypeRef,
};
use async_graphql::Context;
use dynamic_graphql::internal::{OutputTypeName, Register, Registry, ResolveOwned, TypeName};
use dynamic_graphql::{
    App, ExpandObject, ExpandObjectFields, ResolvedObject, ResolvedObjectFields, SimpleObject,
};
use itertools::Itertools;
use raphtory::algorithms::pagerank::unweighted_page_rank;
use raphtory::db::graph::Graph;
use raphtory::db::graph_window::WindowedGraph;
use raphtory::db::vertex::VertexView;
use raphtory::db::view_api::internal::GraphViewInternalOps;
use raphtory::db::view_api::{GraphViewOps, TimeOps, VertexViewOps};
use std::any::Any;
use std::borrow::Cow;
use std::sync::Arc;

use crate::data::Metadata;

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

    async fn algorithms<'a>(&self, _ctx: &Context<'a>) -> Algorithms<G> {
        Algorithms {
            graph: self.graph.clone(),
        }
    }
}

struct Algorithms<G: GraphViewOps> {
    graph: WindowedGraph<G>,
}

impl<G: GraphViewOps> TypeName for Algorithms<G> {
    fn get_type_name() -> Cow<'static, str> {
        "Algorithms".into()
    }
}

impl<G: GraphViewOps> OutputTypeName for Algorithms<G> {}
impl<G: GraphViewOps> Register for Algorithms<G> {
    fn register(registry: Registry) -> Registry {
        let object = Object::new("Algorithms");

        let object = object.field(Field::new(
            "numVerticesAlgo",
            TypeRef::named_nn(TypeRef::INT),
            |ctx| {
                let algos: &Algorithms<G> = ctx.parent_value.downcast_ref().unwrap();
                let graph = &algos.graph;
                FieldFuture::new(async move { Ok(Some(FieldValue::value(graph.num_vertices()))) })
            },
        ));

        let (registry, object) = PageRank::register_algo::<G>(registry, object);
        // let (registry, object) = Algo2::register_algo(registry, object);
        //
        // let (registry, object) = unsafe {
        //     let mut container: Container<ExternalAlgo> = Container::load("libalgo3.dylib").unwrap();
        //     container.register_algo(registry, object)
        // };

        registry.register_type(object).set_root("Query")
    }
}

impl<'a, G: GraphViewOps> ResolveOwned<'a> for Algorithms<G> {
    fn resolve_owned(self, _ctx: &Context) -> dynamic_graphql::Result<Option<FieldValue<'a>>> {
        Ok(Some(FieldValue::owned_any(self)))
    }
}

trait Algo: Register + 'static {
    fn algo_name<'a>() -> &'a str;
    fn output_type() -> TypeRef;
    fn args<'a>() -> Vec<(&'a str, TypeRef)>;
    fn apply_algo<'a, G: GraphViewOps>(graph: &G, ctx: ResolverContext) -> FieldValue<'a>;
    fn register_algo<G: GraphViewOps>(registry: Registry, parent: Object) -> (Registry, Object) {
        let registry = registry.register::<Self>();
        let mut field = Field::new(Self::algo_name(), Self::output_type(), |ctx| {
            FieldFuture::new(async move {
                let algos: &Algorithms<G> = ctx.parent_value.downcast_ref().unwrap();
                let graph = &algos.graph;
                Ok(Some(Self::apply_algo(graph, ctx)))
            })
        });
        for (name, type_ref) in Self::args() {
            field = field.argument(InputValue::new(name, type_ref));
        }
        let parent = parent.field(field);
        (registry, parent)
    }
}

#[derive(SimpleObject)]
struct PageRank {
    name: String,
    rank: f32,
}

impl Algo for PageRank {
    fn algo_name<'a>() -> &'a str {
        "pageRank"
    }
    fn output_type() -> TypeRef {
        TypeRef::named_nn_list(Self::get_type_name())
    }
    fn args<'a>() -> Vec<(&'a str, TypeRef)> {
        vec![
            ("iterCount", TypeRef::named_nn(TypeRef::INT)),
            ("threads", TypeRef::named_nn(TypeRef::INT)),
            ("tol", TypeRef::named_nn(TypeRef::FLOAT)),
        ]
    }
    fn apply_algo<'a, G: GraphViewOps>(graph: &G, ctx: ResolverContext) -> FieldValue<'a> {
        let iter_count = ctx.args.try_get("iterCount").unwrap().u64().unwrap();
        let threads = ctx.args.try_get("threads").unwrap().u64().unwrap();
        let tol = ctx.args.try_get("tol").unwrap().f32().unwrap();

        let result = unweighted_page_rank(
            graph,
            iter_count as usize,
            Some(threads as usize),
            Some(tol),
        )
        .into_iter()
        .map(|(key, value)| {
            FieldValue::owned_any(PageRank {
                name: key,
                rank: value,
            })
        });

        FieldValue::list(result)
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
