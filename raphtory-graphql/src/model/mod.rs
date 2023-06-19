use crate::data::Data;
use crate::model::graph::graph::GqlGraph;
use async_graphql::Context;
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};

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
        Some(g.clone().into())
    }
}
