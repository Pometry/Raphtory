use crate::data::Data;
use async_graphql::Context;
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use itertools::Itertools;
use raphtory::core::Prop;
use raphtory::db::edge::EdgeView;
use raphtory::db::vertex::VertexView;
use raphtory::db::view_api::internal::{GraphViewInternalOps, WrappedGraph};
use raphtory::db::view_api::EdgeListOps;
use raphtory::db::view_api::EdgeViewOps;
use raphtory::db::view_api::{GraphViewOps, TimeOps, VertexViewOps};
use std::sync::Arc;
use crate::model::algorithm::Algorithms;
use crate::model::graph::graph::GqlGraph;

pub(crate) mod algorithm;
pub(crate) mod graph;
pub(crate) mod filters;

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




