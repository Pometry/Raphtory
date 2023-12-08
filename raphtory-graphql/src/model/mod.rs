use crate::{
    data::Data,
    model::graph::{graph::GqlGraph, vectorised_graph::GqlVectorisedGraph},
};
use async_graphql::Context;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use chrono::Utc;
use dynamic_graphql::{
    App, Mutation, MutationFields, MutationRoot, ResolvedObject, ResolvedObjectFields, Result,
    Upload,
};
use itertools::Itertools;
use raphtory::{
    core::{utils::errors::GraphError, ArcStr, Prop},
    db::api::view::MaterializedGraph,
    prelude::{GraphViewOps, NodeViewOps, PropertyAdditionOps},
    search::IndexedGraph,
};
use std::{
    collections::HashMap,
    error::Error,
    fmt::{Display, Formatter},
    io::BufReader,
};
use utils::path_prefix;
use uuid::Uuid;

pub mod algorithms;
pub(crate) mod filters;
pub(crate) mod graph;
pub(crate) mod schema;
pub(crate) mod utils;

#[derive(Debug)]
pub struct MissingGraph;

impl Display for MissingGraph {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Graph does not exist")
    }
}

impl Error for MissingGraph {}

#[derive(ResolvedObject)]
#[graphql(root)]
pub(crate) struct QueryRoot;

#[ResolvedObjectFields]
impl QueryRoot {
    async fn hello() -> &'static str {
        "Hello world from raphtory-graphql"
    }

    /// Returns a graph
    async fn graph<'a>(ctx: &Context<'a>, name: &str) -> Option<GqlGraph> {
        let data = ctx.data_unchecked::<Data>();
        let g = data.graphs.read().get(name).cloned()?;
        Some(GqlGraph::new(name.to_string(), g))
    }

    async fn vectorised_graph<'a>(ctx: &Context<'a>, name: &str) -> Option<GqlVectorisedGraph> {
        let data = ctx.data_unchecked::<Data>();
        let g = data.vector_stores.read().get(name).cloned()?;
        Some(g.into())
    }

    async fn graphs<'a>(ctx: &Context<'a>) -> Vec<GqlGraph> {
        let data = ctx.data_unchecked::<Data>();
        data.graphs
            .read()
            .iter()
            .map(|(name, g)| GqlGraph::new(name.clone(), g.clone()))
            .collect_vec()
    }

    async fn receive_graph<'a>(ctx: &Context<'a>, name: &str) -> Result<String> {
        let data = ctx.data_unchecked::<Data>();
        let g = data
            .graphs
            .read()
            .get(name)
            .cloned()
            .ok_or(MissingGraph)?
            .materialize()?;
        let bincode = bincode::serialize(&g)?;
        Ok(URL_SAFE_NO_PAD.encode(bincode))
    }
}

#[derive(MutationRoot)]
pub(crate) struct MutRoot;

#[derive(Mutation)]
pub(crate) struct Mut(MutRoot);

#[MutationFields]
impl Mut {
    /// Load graphs from a directory of bincode files (existing graphs with the same name are overwritten)
    ///
    /// Returns::
    ///   list of names for newly added graphs
    async fn load_graphs_from_path<'a>(ctx: &Context<'a>, path: String) -> Vec<String> {
        let new_graphs = Data::load_from_file(&path);
        let keys: Vec<_> = new_graphs.keys().cloned().collect();
        let mut data = ctx.data_unchecked::<Data>().graphs.write();
        data.extend(new_graphs);
        keys
    }

    async fn rename_graph<'a>(
        ctx: &Context<'a>,
        parent_graph_name: String,
        graph_name: String,
        new_graph_name: String,
    ) -> Result<bool> {
        let data = ctx.data_unchecked::<Data>();
        if data.graphs.read().contains_key(&new_graph_name) {
            return Err((GraphError::GraphNameAlreadyExists {
                name: new_graph_name,
            })
            .into());
        }

        if new_graph_name.ne(&graph_name) && parent_graph_name.ne(&graph_name) {
            let mut data = ctx.data_unchecked::<Data>().graphs.write();

            let subgraph = data.get(&graph_name).ok_or("Graph not found")?;
            let path = subgraph
                .properties()
                .constant()
                .get("path")
                .ok_or("Path is missing")?
                .to_string();

            let parent_graph = data.get(&parent_graph_name).ok_or("Graph not found")?;
            let new_subgraph = parent_graph
                .subgraph(subgraph.nodes().iter().map(|v| v.name()).collect_vec())
                .materialize()?;

            let static_props_without_name: Vec<(ArcStr, Prop)> = subgraph
                .properties()
                .into_iter()
                .filter(|(a, _)| a != "name")
                .collect_vec();

            new_subgraph.update_constant_properties(static_props_without_name)?;

            new_subgraph
                .update_constant_properties([("name", Prop::Str(new_graph_name.clone().into()))])?;

            let dt = Utc::now();
            let timestamp: i64 = dt.timestamp();
            new_subgraph
                .update_constant_properties([("lastUpdated", Prop::I64(timestamp * 1000))])?;

            new_subgraph.save_to_file(path)?;

            let gi: IndexedGraph<MaterializedGraph> = new_subgraph.into();

            data.insert(new_graph_name, gi);
            data.remove(&graph_name);
        }

        Ok(true)
    }

    async fn save_graph<'a>(
        ctx: &Context<'a>,
        parent_graph_name: String,
        graph_name: String,
        new_graph_name: String,
        props: String,
        is_archive: u8,
        graph_nodes: Vec<String>,
    ) -> Result<bool> {
        let mut data = ctx.data_unchecked::<Data>().graphs.write();

        let subgraph = data.get(&graph_name).ok_or("Graph not found")?;
        let mut path = subgraph
            .properties()
            .constant()
            .get("path")
            .ok_or("Path is missing")?
            .to_string();

        if new_graph_name.ne(&graph_name) {
            path = path_prefix(path)? + "/" + &Uuid::new_v4().hyphenated().to_string();
        }

        let parent_graph = data.get(&parent_graph_name).ok_or("Graph not found")?;

        let new_subgraph = parent_graph.subgraph(graph_nodes).materialize()?;

        new_subgraph.update_constant_properties([("name", Prop::str(new_graph_name.clone()))])?;

        // parent_graph_name == graph_name, means its a graph created from UI
        if parent_graph_name.ne(&graph_name) {
            // graph_name == new_graph_name, means its a "save" and not "save as" action
            if graph_name.ne(&new_graph_name) {
                let static_props: Vec<(ArcStr, Prop)> = subgraph
                    .properties()
                    .into_iter()
                    .filter(|(a, _)| a != "name" && a != "creationTime" && a != "uiProps")
                    .collect_vec();
                new_subgraph.update_constant_properties(static_props)?;
            } else {
                let static_props: Vec<(ArcStr, Prop)> = subgraph
                    .properties()
                    .into_iter()
                    .filter(|(a, _)| a != "name" && a != "lastUpdated" && a != "uiProps")
                    .collect_vec();
                new_subgraph.update_constant_properties(static_props)?;
            }
        }

        let dt = Utc::now();
        let timestamp: i64 = dt.timestamp();

        if parent_graph_name.eq(&graph_name) || graph_name.ne(&new_graph_name) {
            new_subgraph
                .update_constant_properties([("creationTime", Prop::I64(timestamp * 1000))])?;
        }

        new_subgraph.update_constant_properties([("lastUpdated", Prop::I64(timestamp * 1000))])?;
        new_subgraph.update_constant_properties([("uiProps", Prop::Str(props.into()))])?;
        new_subgraph.update_constant_properties([("path", Prop::Str(path.clone().into()))])?;
        new_subgraph.update_constant_properties([("isArchive", Prop::U8(is_archive))])?;

        new_subgraph.save_to_file(path)?;

        let gi: IndexedGraph<MaterializedGraph> = new_subgraph.into();

        data.insert(new_graph_name, gi);

        Ok(true)
    }

    /// Load new graphs from a directory of bincode files (existing graphs will not been overwritten)
    ///
    /// Returns::
    ///   list of names for newly added graphs
    async fn load_new_graphs_from_path<'a>(ctx: &Context<'a>, path: String) -> Vec<String> {
        let mut data = ctx.data_unchecked::<Data>().graphs.write();
        let new_graphs: HashMap<_, _> = Data::load_from_file(&path)
            .into_iter()
            .filter(|(key, _)| !data.contains_key(key))
            .collect();
        let keys: Vec<_> = new_graphs.keys().cloned().collect();
        data.extend(new_graphs);
        keys
    }

    /// Use GQL multipart upload to send new graphs to server
    ///
    /// Returns::
    ///    name of the new graph
    async fn upload_graph<'a>(ctx: &Context<'a>, name: String, graph: Upload) -> Result<String> {
        let g: MaterializedGraph =
            bincode::deserialize_from(BufReader::new(graph.value(ctx)?.content))?;
        let gi: IndexedGraph<MaterializedGraph> = g.into();
        let mut data = ctx.data_unchecked::<Data>().graphs.write();
        data.insert(name.clone(), gi);
        Ok(name)
    }

    /// Send graph bincode as base64 encoded string
    ///
    /// Returns::
    ///    name of the new graph
    async fn send_graph<'a>(ctx: &Context<'a>, name: String, graph: String) -> Result<String> {
        let g: MaterializedGraph = bincode::deserialize(&URL_SAFE_NO_PAD.decode(graph)?)?;
        let mut data = ctx.data_unchecked::<Data>().graphs.write();
        data.insert(name.clone(), g.into());
        Ok(name)
    }

    async fn archive_graph<'a>(
        ctx: &Context<'a>,
        graph_name: String,
        parent_graph_name: String,
        is_archive: u8,
    ) -> Result<bool> {
        let mut data = ctx.data_unchecked::<Data>().graphs.write();

        let subgraph = data.get(&graph_name).ok_or("Graph not found")?;

        let path = subgraph
            .properties()
            .constant()
            .get("path")
            .ok_or("Path is missing")?
            .to_string();

        let parent_graph = data.get(&parent_graph_name).ok_or("Graph not found")?;
        let new_subgraph = parent_graph
            .subgraph(subgraph.nodes().iter().map(|v| v.name()).collect_vec())
            .materialize()?;

        let static_props_without_isactive: Vec<(ArcStr, Prop)> = subgraph
            .properties()
            .into_iter()
            .filter(|(a, _)| a != "isArchive")
            .collect_vec();
        new_subgraph.update_constant_properties(static_props_without_isactive)?;
        new_subgraph.update_constant_properties([("isArchive", Prop::U8(is_archive))])?;
        new_subgraph.save_to_file(path)?;

        let gi: IndexedGraph<MaterializedGraph> = new_subgraph.into();

        data.insert(graph_name, gi);

        Ok(true)
    }
}

#[derive(App)]
pub struct App(QueryRoot, MutRoot, Mut);
