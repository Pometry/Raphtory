use async_graphql::dynamic::{
    Field, FieldFuture, FieldValue, InputValue, Object, ResolverContext, TypeRef,
};
use async_graphql::{Context, FieldResult};
use dynamic_graphql::internal::{OutputTypeName, Register, Registry, ResolveOwned, TypeName};
use dynamic_graphql::SimpleObject;
use raphtory::algorithms::pagerank::unweighted_page_rank;
use raphtory::db::view_api::GraphViewOps;
use std::borrow::Cow;
use std::collections::HashMap;

pub(crate) struct Algorithms<G: GraphViewOps> {
    graph: G,
}

impl<G: GraphViewOps> Algorithms<G> {
    pub(crate) fn new(graph: &G) -> Self {
        Self {
            graph: graph.clone(),
        }
    }
}

impl<G: GraphViewOps> Register for Algorithms<G> {
    fn register(registry: Registry) -> Registry {
        let mut registry = registry;
        let mut object = Object::new("Algorithms");

        object = object.field(Field::new(
            "numVerticesAlgo",
            TypeRef::named_nn(TypeRef::INT),
            |ctx| {
                let algos: &Algorithms<G> = ctx.parent_value.downcast_ref().unwrap();
                let graph = &algos.graph;
                FieldFuture::new(async move { Ok(Some(FieldValue::value(graph.num_vertices()))) })
            },
        ));

        let algos = HashMap::from([("pageRank", PageRank::register_algo::<G>)]);

        for (name, register_algo) in algos {
            (registry, object) = register_algo(name, registry, object);
        }

        // TODO: uncomment this for plugins
        // let (registry, object) = unsafe {
        //     let mut container: Container<ExternalAlgo> = Container::load("libalgo3.dylib").unwrap();
        //     container.register_algo(registry, object)
        // };

        registry.register_type(object)
    }
}

impl<G: GraphViewOps> TypeName for Algorithms<G> {
    fn get_type_name() -> Cow<'static, str> {
        "Algorithms".into()
    }
}

impl<G: GraphViewOps> OutputTypeName for Algorithms<G> {}

impl<'a, G: GraphViewOps> ResolveOwned<'a> for Algorithms<G> {
    fn resolve_owned(self, _ctx: &Context) -> dynamic_graphql::Result<Option<FieldValue<'a>>> {
        Ok(Some(FieldValue::owned_any(self)))
    }
}

trait Algo: Register + 'static {
    fn output_type() -> TypeRef;
    fn args<'a>() -> Vec<(&'a str, TypeRef)>;
    fn apply_algo<'a, G: GraphViewOps>(
        graph: &G,
        ctx: ResolverContext,
    ) -> FieldResult<Option<FieldValue<'a>>>;
    fn register_algo<G: GraphViewOps>(
        name: &str,
        registry: Registry,
        parent: Object,
    ) -> (Registry, Object) {
        let registry = registry.register::<Self>();
        let mut field = Field::new(name, Self::output_type(), |ctx| {
            FieldFuture::new(async move {
                let algos: &Algorithms<G> = ctx.parent_value.downcast_ref().unwrap();
                let graph = &algos.graph;
                Self::apply_algo(graph, ctx)
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

impl From<(String, f32)> for PageRank {
    fn from((name, rank): (String, f32)) -> Self {
        Self { name, rank }
    }
}

impl Algo for PageRank {
    fn output_type() -> TypeRef {
        // first _nn means that the list is never nul, second _nn means no element is null
        TypeRef::named_nn_list_nn(Self::get_type_name()) //
    }
    fn args<'a>() -> Vec<(&'a str, TypeRef)> {
        vec![
            ("iterCount", TypeRef::named_nn(TypeRef::INT)), // _nn stands for not null
            ("threads", TypeRef::named(TypeRef::INT)),      // this one though might be null
            ("tol", TypeRef::named(TypeRef::FLOAT)),
        ]
    }
    fn apply_algo<'a, G: GraphViewOps>(
        graph: &G,
        ctx: ResolverContext,
    ) -> FieldResult<Option<FieldValue<'a>>> {
        let iter_count = ctx.args.try_get("iterCount")?.u64()? as usize;
        let threads = ctx.args.get("threads").map(|v| v.u64()).transpose()?;
        let threads = threads.map(|v| v as usize);
        let tol = ctx.args.get("tol").map(|v| v.f32()).transpose()?;
        let result = unweighted_page_rank(graph, iter_count, threads, tol)
            .into_iter()
            .map(|pair| FieldValue::owned_any(PageRank::from(pair)));
        Ok(Some(FieldValue::list(result)))
    }
}
