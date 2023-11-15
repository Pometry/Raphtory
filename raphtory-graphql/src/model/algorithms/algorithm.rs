use crate::model::algorithms::{
    algorithm_entry_point::AlgorithmEntryPoint, graph_algorithms::GraphAlgorithms,
};
use async_graphql::{
    dynamic::{Field, FieldFuture, FieldValue, InputValue, Object, ResolverContext, TypeRef},
    FieldResult,
};
use dynamic_graphql::{
    internal::{Register, Registry, TypeName},
    SimpleObject,
};
use futures_util::future::BoxFuture;
use itertools::Itertools;
use ordered_float::OrderedFloat;
use raphtory::algorithms::centrality::pagerank::unweighted_page_rank;

pub trait Algorithm<'a, A: AlgorithmEntryPoint<'a> + 'static> {
    type OutputType: Register + 'static;

    fn output_type() -> TypeRef;
    fn args<'b>() -> Vec<(&'b str, TypeRef)>;
    fn apply_algo<'b>(
        entry_point: &A,
        ctx: ResolverContext,
    ) -> BoxFuture<'b, FieldResult<Option<FieldValue<'b>>>>;
    fn register_algo(name: &str, registry: Registry, parent: Object) -> (Registry, Object) {
        let registry = registry.register::<Self::OutputType>();
        let mut field = Field::new(name, Self::output_type(), |ctx| {
            FieldFuture::new(async move {
                let algos: &A = ctx.parent_value.downcast_ref().unwrap();
                Self::apply_algo(&algos, ctx).await
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
pub(crate) struct PagerankOutput {
    name: String,
    rank: f64,
}

impl From<(String, f64)> for PagerankOutput {
    fn from((name, rank): (String, f64)) -> Self {
        Self { name, rank }
    }
}

impl From<(String, Option<f64>)> for PagerankOutput {
    fn from((name, rank): (String, Option<f64>)) -> Self {
        Self {
            name,
            rank: rank.unwrap_or_default(), // use 0.0 if rank is None
        }
    }
}

impl From<(String, OrderedFloat<f64>)> for PagerankOutput {
    fn from((name, rank): (String, OrderedFloat<f64>)) -> Self {
        let rank = rank.into_inner();
        Self { name, rank }
    }
}

impl From<(&String, &OrderedFloat<f64>)> for PagerankOutput {
    fn from((name, rank): (&String, &OrderedFloat<f64>)) -> Self {
        Self {
            name: name.to_string(),
            rank: rank.into_inner(),
        }
    }
}

pub(crate) struct Pagerank;

impl<'a> Algorithm<'a, GraphAlgorithms> for Pagerank {
    type OutputType = PagerankOutput;

    fn output_type() -> TypeRef {
        // first _nn means that the list is never null, second _nn means no element is null
        TypeRef::named_nn_list_nn(PagerankOutput::get_type_name()) //
    }
    fn args<'b>() -> Vec<(&'b str, TypeRef)> {
        vec![
            ("iterCount", TypeRef::named_nn(TypeRef::INT)), // _nn stands for not null
            ("threads", TypeRef::named(TypeRef::INT)),      // this one though might be null
            ("tol", TypeRef::named(TypeRef::FLOAT)),
        ]
    }
    fn apply_algo<'b>(
        entry_point: &GraphAlgorithms,
        ctx: ResolverContext,
    ) -> BoxFuture<'b, FieldResult<Option<FieldValue<'b>>>> {
        let result = apply_pagerank(entry_point, ctx);
        Box::pin(async move { result })
    }
}

fn apply_pagerank<'b>(
    entry_point: &GraphAlgorithms,
    ctx: ResolverContext,
) -> FieldResult<Option<FieldValue<'b>>> {
    let iter_count = ctx.args.try_get("iterCount")?.u64()? as usize;
    let threads = ctx.args.get("threads").map(|v| v.u64()).transpose()?;
    let threads = threads.map(|v| v as usize);
    let tol = ctx.args.get("tol").map(|v| v.f64()).transpose()?;
    let binding = unweighted_page_rank(&entry_point.graph, iter_count, threads, tol, true);
    let result = binding
        .get_all_with_names()
        .into_iter()
        .map(|pair| FieldValue::owned_any(PagerankOutput::from(pair)))
        .collect_vec();
    Ok(Some(FieldValue::list(result)))
}
