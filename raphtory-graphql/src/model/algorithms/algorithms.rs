use crate::model::plugins::{graph_algorithm_plugin::GraphAlgorithmPlugin, operation::Operation};
use async_graphql::{
    dynamic::{FieldValue, ResolverContext, TypeRef},
    FieldResult,
};
use dynamic_graphql::{internal::TypeName, SimpleObject};
use futures_util::future::BoxFuture;
use itertools::Itertools;
use ordered_float::OrderedFloat;
use raphtory::algorithms::{
    centrality::pagerank::unweighted_page_rank,
    pathing::dijkstra::dijkstra_single_source_shortest_paths,
};
use raphtory_api::core::Direction;
use std::collections::HashMap;

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

impl<'a> Operation<'a, GraphAlgorithmPlugin> for Pagerank {
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

    fn apply<'b>(
        entry_point: &GraphAlgorithmPlugin,
        ctx: ResolverContext,
    ) -> BoxFuture<'b, FieldResult<Option<FieldValue<'b>>>> {
        let result = apply_pagerank(entry_point, ctx);
        Box::pin(async move { result })
    }
}

fn apply_pagerank<'b>(
    entry_point: &GraphAlgorithmPlugin,
    ctx: ResolverContext,
) -> FieldResult<Option<FieldValue<'b>>> {
    let iter_count = ctx.args.try_get("iterCount")?.u64()? as usize;
    let threads = ctx.args.get("threads").map(|v| v.u64()).transpose()?;
    let threads = threads.map(|v| v as usize);
    let tol = ctx.args.get("tol").map(|v| v.f64()).transpose()?;
    let damping_factor = ctx
        .args
        .get("damping_factor")
        .map(|v| v.f64())
        .transpose()?;
    let binding = unweighted_page_rank(
        &entry_point.graph,
        Some(iter_count),
        threads,
        tol,
        true,
        damping_factor,
    );
    let result = binding
        .get_all_with_names()
        .into_iter()
        .map(|pair| FieldValue::owned_any(PagerankOutput::from(pair)))
        .collect_vec();
    Ok(Some(FieldValue::list(result)))
}

pub(crate) struct ShortestPath;

#[derive(SimpleObject)]
pub(crate) struct ShortestPathOutput {
    target: String,
    nodes: Vec<String>,
}

impl From<(String, Vec<String>)> for ShortestPathOutput {
    fn from((target, nodes): (String, Vec<String>)) -> Self {
        Self { target, nodes }
    }
}

impl<'a> Operation<'a, GraphAlgorithmPlugin> for ShortestPath {
    type OutputType = ShortestPathOutput;

    fn output_type() -> TypeRef {
        TypeRef::named_nn_list_nn(ShortestPathOutput::get_type_name())
    }

    fn args<'b>() -> Vec<(&'b str, TypeRef)> {
        vec![
            ("source", TypeRef::named_nn(TypeRef::STRING)), // _nn stands for not null
            ("targets", TypeRef::named_nn_list_nn(TypeRef::STRING)),
            ("direction", TypeRef::named(TypeRef::STRING)),
        ]
    }

    fn apply<'b>(
        entry_point: &GraphAlgorithmPlugin,
        ctx: ResolverContext,
    ) -> BoxFuture<'b, FieldResult<Option<FieldValue<'b>>>> {
        let result = apply_shortest_path(entry_point, ctx);
        Box::pin(async move { result })
    }
}

fn apply_shortest_path<'b>(
    entry_point: &GraphAlgorithmPlugin,
    ctx: ResolverContext,
) -> FieldResult<Option<FieldValue<'b>>> {
    let source = ctx.args.try_get("source")?.string()?;
    let targets = ctx.args.try_get("targets")?.list()?;
    let direction = match ctx.args.try_get("direction")?.string()? {
        "out" => Direction::OUT,
        "in" => Direction::IN,
        "both" => Direction::BOTH,
        _ => return Err("Invalid direction".into()),
    };
    let targets = targets
        .iter()
        .map(|v| v.string())
        .collect::<Result<Vec<&str>, _>>()?;
    let binding: Result<HashMap<String, (f64, Vec<String>)>, &str> =
        Ok(dijkstra_single_source_shortest_paths(
            &entry_point.graph,
            source,
            targets,
            None,
            direction,
        )?
        .get_all_with_names());
    let result: Vec<FieldValue> = binding
        .into_iter()
        .flat_map(|pair| {
            pair.into_iter()
                .map(|(key, value)| ShortestPathOutput::from((key.to_string(), value.1)))
        })
        .map(FieldValue::owned_any)
        .collect();

    Ok(Some(FieldValue::list(result)))
}
