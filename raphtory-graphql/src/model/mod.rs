use crate::{
    data::Data,
    model::{
        algorithms::global_plugins::GlobalPlugins,
        graph::{graph::GqlGraph, vectorised_graph::GqlVectorisedGraph},
    },
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
use serde_json::Value;
use std::{
    collections::HashMap,
    error::Error,
    fmt::{Display, Formatter},
    io::Read,
    path::Path,
};
use uuid::Uuid;

pub mod algorithms;
pub(crate) mod filters;
pub(crate) mod graph;
pub(crate) mod schema;

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

    async fn plugins<'a>(ctx: &Context<'a>) -> GlobalPlugins {
        let data = ctx.data_unchecked::<Data>();
        GlobalPlugins {
            graphs: data.graphs.clone(),
            vectorised_graphs: data.vector_stores.clone(),
        }
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
            new_subgraph
                .update_constant_properties([("lastOpened", Prop::I64(timestamp * 1000))])?;
            new_subgraph.save_to_file(path)?;

            let gi: IndexedGraph<MaterializedGraph> = new_subgraph.into();

            data.insert(new_graph_name, gi);
            data.remove(&graph_name);
        }

        Ok(true)
    }

    async fn update_graph_last_opened<'a>(ctx: &Context<'a>, graph_name: String) -> Result<bool> {
        let data = ctx.data_unchecked::<Data>().graphs.write();

        let subgraph = data.get(&graph_name).ok_or("Graph not found")?;

        let dt = Utc::now();
        let timestamp: i64 = dt.timestamp();

        subgraph.update_constant_properties([("lastOpened", Prop::I64(timestamp * 1000))])?;

        let path = subgraph
            .properties()
            .constant()
            .get("path")
            .ok_or("Path is missing")?
            .to_string();

        subgraph.save_to_file(path)?;

        Ok(true)
    }

    async fn save_graph<'a>(
        ctx: &Context<'a>,
        parent_graph_name: String,
        graph_name: String,
        new_graph_name: String,
        props: String,
        is_archive: u8,
        graph_nodes: String,
    ) -> Result<bool> {
        let mut data = ctx.data_unchecked::<Data>().graphs.write();

        let subgraph = data.get(&graph_name).ok_or("Graph not found")?;

        let path = match data.get(&new_graph_name) {
            Some(new_graph) => new_graph
                .properties()
                .constant()
                .get("path")
                .ok_or("Path is missing")?
                .to_string(),
            None => {
                let base_path = subgraph
                    .properties()
                    .constant()
                    .get("path")
                    .ok_or("Path is missing")?
                    .to_string();
                let path: &Path = Path::new(base_path.as_str());
                path.with_file_name(Uuid::new_v4().hyphenated().to_string())
                    .to_str()
                    .ok_or("Invalid path")?
                    .to_string()
            }
        };
        println!("Saving graph to path {path}");

        let parent_graph = data.get(&parent_graph_name).ok_or("Graph not found")?;

        let deserialized_node_map: Value = serde_json::from_str(graph_nodes.as_str())?;
        let node_map = deserialized_node_map
            .as_object()
            .ok_or("graph_nodes not object")?;
        let node_ids = node_map.keys().map(|key| key.as_str());
        let new_subgraph = parent_graph.subgraph(node_ids).materialize()?;

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
        new_subgraph.update_constant_properties([("lastOpened", Prop::I64(timestamp * 1000))])?;
        new_subgraph.update_constant_properties([("uiProps", Prop::Str(props.into()))])?;
        new_subgraph.update_constant_properties([("path", Prop::Str(path.clone().into()))])?;
        new_subgraph.update_constant_properties([("isArchive", Prop::U8(is_archive))])?;

        // Temporary hack to allow adding of arbitrary maps of data to nodes
        for node in new_subgraph.nodes() {
            if !node_map.contains_key(&node.name().to_string()) {
                println!("Could not find key! {} {:?}", node.name(), node_map);
                panic!()
            }
            let node_props = node_map
                .get(node.name().to_string().as_str())
                .ok_or(format!(
                    "Could not find node {} in provided map",
                    node.name()
                ))?;
            let node_props_as_obj = node_props
                .as_object()
                .ok_or("node_props must be an object")?
                .to_owned();
            let values = node_props_as_obj
                .into_iter()
                .map(|(key, value)| Ok((key, value.try_into()?)))
                .collect::<Result<Vec<(String, Prop)>>>()?;
            node.update_constant_properties(values)?;
        }

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
        let mut buffer = Vec::new();
        let mut buff_read = graph.value(ctx)?.content;
        buff_read.read_to_end(&mut buffer)?;
        let g: MaterializedGraph = MaterializedGraph::from_bincode(&buffer)?;
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
        _parent_graph_name: String,
        is_archive: u8,
    ) -> Result<bool> {
        let data = ctx.data_unchecked::<Data>().graphs.write();
        let subgraph = data.get(&graph_name).ok_or("Graph not found")?;
        subgraph.update_constant_properties([("isArchive", Prop::U8(is_archive))])?;

        let path = subgraph
            .properties()
            .constant()
            .get("path")
            .ok_or("Path is missing")?
            .to_string();
        subgraph.save_to_file(path)?;

        Ok(true)
    }
}

#[derive(App)]
pub struct App(QueryRoot, MutRoot, Mut);
