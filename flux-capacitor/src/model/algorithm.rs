use async_graphql::dynamic::{
    Field, FieldFuture, FieldValue, InputValue, Object, ResolverContext, TypeRef,
};
use async_graphql::Context;
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

        registry.register_type(object) // .set_root("Query")
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
    fn apply_algo<'a, G: GraphViewOps>(graph: &G, ctx: ResolverContext) -> FieldValue<'a>;
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
