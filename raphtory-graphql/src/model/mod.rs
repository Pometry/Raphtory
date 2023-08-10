use std::ops::Deref;

use crate::{
    data::Data,
    model::graph::graph::{GqlGraph, GraphMeta},
};
use async_graphql::Context;
use dynamic_graphql::{
    Mutation, MutationFields, MutationRoot, ResolvedObject, ResolvedObjectFields,
};
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
        let g = data.graphs.read().get(name).cloned()?;
        Some(GqlGraph::new(g))
    }

    async fn graphs<'a>(ctx: &Context<'a>) -> Vec<GraphMeta> {
        let data = ctx.data_unchecked::<Data>();
        data.graphs
            .read()
            .iter()
            .map(|(name, g)| GraphMeta::new(name.clone(), g.deref().clone().into_dynamic()))
            .collect_vec()
    }
}

#[derive(MutationRoot)]
pub(crate) struct MutRoot;

#[derive(Mutation)]
pub(crate) struct Mut(MutRoot);

#[MutationFields]
impl Mut {
    /// Load new graphs from a directory of bincode files (existing graphs with the same name are overwritten)
    async fn load_graphs_from_path<'a>(ctx: &Context<'a>, path: String) -> Vec<String> {
        let new_graphs = Data::load_from_file(&path);
        let keys: Vec<_> = new_graphs.keys().cloned().collect();
        let mut data = ctx.data_unchecked::<Data>().graphs.write();
        data.extend(new_graphs);
        keys
    }
}
