use std::ops::Deref;

use crate::{
    data::Data,
    model::graph::graph::{GqlGraph, GraphMeta},
};
use async_graphql::Context;
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use itertools::Itertools;
use raphtory::db::api::view::internal::IntoDynamic;

pub(crate) mod algorithm;
pub(crate) mod filters;
pub(crate) mod graph;

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
        let graph = g.deref();
        Some(graph.clone().into())
    }

    async fn graphs<'a>(ctx: &Context<'a>) -> Vec<GraphMeta> {
        let data = ctx.data_unchecked::<Data>();
        data.graphs
            .iter()
            .map(|(name, g)| GraphMeta::new(name.clone(), g.deref().clone().into_dynamic()))
            .collect_vec()
    }
}
