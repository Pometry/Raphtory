use async_graphql::dynamic::{
    Field, FieldFuture, FieldValue, InputValue, Object, ResolverContext, TypeRef,
};
use async_graphql::{Context, FieldResult};
use dynamic_graphql::internal::{OutputTypeName, Register, Registry, ResolveOwned, TypeName};
use dynamic_graphql::SimpleObject;
use once_cell::sync::Lazy;
use raphtory::algorithms::pagerank::unweighted_page_rank;
use raphtory::db::view_api::GraphViewOps;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Mutex;
use raphtory::db::dynamic::DynamicGraph;

type RegisterFunction = fn(&str, Registry, Object) -> (Registry, Object);

pub(crate) static PLUGIN_ALGOS: Lazy<Mutex<HashMap<String, RegisterFunction>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

pub(crate) struct Algorithms {
    graph: DynamicGraph,
}

impl From<DynamicGraph> for Algorithms {
    fn from(graph: DynamicGraph) -> Self {
        Self { graph }
    }
}

impl Register for Algorithms {
    fn register(registry: Registry) -> Registry {
        let mut registry = registry;
        let mut object = Object::new("Algorithms");

        let algos = HashMap::from([("pagerank", Pagerank::register_algo)]);
        for (name, register_algo) in algos {
            (registry, object) = register_algo(name, registry, object);
        }

        for (name, register_algo) in PLUGIN_ALGOS.lock().unwrap().iter() {
            (registry, object) = register_algo(name, registry, object);
        }

        registry.register_type(object)
    }
}

impl TypeName for Algorithms {
    fn get_type_name() -> Cow<'static, str> {
        "Algorithms".into()
    }
}

impl OutputTypeName for Algorithms {}

impl<'a> ResolveOwned<'a> for Algorithms {
    fn resolve_owned(self, _ctx: &Context) -> dynamic_graphql::Result<Option<FieldValue<'a>>> {
        Ok(Some(FieldValue::owned_any(self)))
    }
}

pub trait Algorithm: Register + 'static {
    fn output_type() -> TypeRef;
    fn args<'a>() -> Vec<(&'a str, TypeRef)>;
    fn apply_algo<'a, G: GraphViewOps>(
        graph: &G,
        ctx: ResolverContext,
    ) -> FieldResult<Option<FieldValue<'a>>>;
    fn register_algo(name: &str, registry: Registry, parent: Object) -> (Registry, Object) {
        let registry = registry.register::<Self>();
        let mut field = Field::new(name, Self::output_type(), |ctx| {
            FieldFuture::new(async move {
                let algos: &Algorithms = ctx.parent_value.downcast_ref().unwrap();
                Self::apply_algo(&algos.graph, ctx)
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
struct Pagerank {
    name: String,
    rank: f64,
}

impl From<(String, f64)> for Pagerank {
    fn from((name, rank): (String, f64)) -> Self {
        Self { name, rank }
    }
}

impl Algorithm for Pagerank {
    fn output_type() -> TypeRef {
        // first _nn means that the list is never null, second _nn means no element is null
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
        let tol = ctx.args.get("tol").map(|v| v.f64()).transpose()?;
        let result = unweighted_page_rank(graph, iter_count, threads, tol, true)
            .into_iter()
            .map(|pair| FieldValue::owned_any(Pagerank::from(pair)));
        Ok(Some(FieldValue::list(result)))
    }
}
