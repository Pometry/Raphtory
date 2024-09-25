use crate::{
    data::Data,
    model::{
        algorithms::global_plugins::GlobalPlugins,
        graph::{
            graph::GqlGraph, graphs::GqlGraphs, mutable_graph::GqlMutableGraph,
            vectorised_graph::GqlVectorisedGraph,
        },
    },
    paths::ExistingGraphFolder,
    url_encode::{url_decode_graph, url_encode_graph},
};
use async_graphql::Context;
use chrono::Utc;
use dynamic_graphql::{
    App, Enum, Mutation, MutationFields, MutationRoot, ResolvedObject, ResolvedObjectFields,
    Result, Upload,
};
use itertools::Itertools;
#[cfg(feature = "storage")]
use raphtory::db::api::{storage::graph::storage_ops::GraphStorage, view::internal::CoreGraphOps};
use raphtory::{
    core::{utils::errors::GraphError, Prop},
    db::{api::view::MaterializedGraph, graph::views::deletion_graph::PersistentGraph},
    prelude::*,
};
use raphtory_api::core::storage::arc_str::ArcStr;
use std::{
    error::Error,
    fmt::{Display, Formatter},
    fs::{self, File},
    io::{copy, Read},
    path::Path,
    sync::Arc,
};
use zip::ZipArchive;

pub mod algorithms;
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

#[derive(thiserror::Error, Debug)]
pub enum GqlGraphError {
    #[error("Disk Graph is immutable")]
    ImmutableDiskGraph,
    #[error("Graph does exists at path {0}")]
    GraphDoesNotExists(String),
    #[error("Failed to load graph")]
    FailedToLoadGraph,
    #[error("Invalid namespace: {0}")]
    InvalidNamespace(String),
    #[error("Failed to create dir {0}")]
    FailedToCreateDir(String),
}

#[derive(Enum)]
pub enum GqlGraphType {
    Persistent,
    Event,
}

#[derive(ResolvedObject)]
#[graphql(root)]
pub(crate) struct QueryRoot;

#[ResolvedObjectFields]
impl QueryRoot {
    async fn hello() -> &'static str {
        "Hello world from raphtory-graphql"
    }

    /// Returns a graph
    async fn graph<'a>(ctx: &Context<'a>, path: &str) -> Result<GqlGraph> {
        let data = ctx.data_unchecked::<Data>();
        Ok(data
            .get_graph(path)
            .map(|(g, folder)| GqlGraph::new(folder, g.graph))?)
    }

    async fn update_graph<'a>(ctx: &Context<'a>, path: String) -> Result<GqlMutableGraph> {
        let data = ctx.data_unchecked::<Data>();
        let graph = data
            .get_graph(path.as_ref())
            .map(|(g, folder)| GqlMutableGraph::new(folder, g))?;
        Ok(graph)
    }

    async fn vectorised_graph<'a>(ctx: &Context<'a>, path: &str) -> Option<GqlVectorisedGraph> {
        let data = ctx.data_unchecked::<Data>();
        let g = data.get_graph(path).ok()?.0.vectors?;
        Some(g.into())
    }

    async fn graphs<'a>(ctx: &Context<'a>) -> Result<GqlGraphs> {
        let data = ctx.data_unchecked::<Data>();
        let paths = data.get_all_graph_folders();
        let work_dir = data.work_dir.clone();
        Ok(GqlGraphs::new(work_dir, paths))
    }

    async fn plugins<'a>(ctx: &Context<'a>) -> GlobalPlugins {
        let data = ctx.data_unchecked::<Data>();
        data.get_global_plugins()
    }

    async fn receive_graph<'a>(ctx: &Context<'a>, path: String) -> Result<String, Arc<GraphError>> {
        let path = path.as_ref();
        let data = ctx.data_unchecked::<Data>();
        let g = data.get_graph(path)?.0.graph.graph.clone();
        let res = url_encode_graph(g)?;
        Ok(res)
    }
}

#[derive(MutationRoot)]
pub(crate) struct MutRoot;

#[derive(Mutation)]
pub(crate) struct Mut(MutRoot);

#[MutationFields]
impl Mut {
    // If namespace is not provided, it will be set to the current working directory.
    async fn delete_graph<'a>(ctx: &Context<'a>, path: String) -> Result<bool> {
        let data = ctx.data_unchecked::<Data>();
        data.delete_graph(&path)?;
        Ok(true)
    }

    async fn new_graph<'a>(
        ctx: &Context<'a>,
        path: String,
        graph_type: GqlGraphType,
    ) -> Result<bool> {
        let data = ctx.data_unchecked::<Data>();
        let graph = match graph_type {
            GqlGraphType::Persistent => PersistentGraph::new().materialize()?,
            GqlGraphType::Event => Graph::new().materialize()?,
        };
        data.insert_graph(&path, graph).await?;
        Ok(true)
    }

    // If namespace is not provided, it will be set to the current working directory.
    // This applies to both the graph namespace and new graph namespace.
    async fn move_graph<'a>(ctx: &Context<'a>, path: &str, new_path: &str) -> Result<bool> {
        Self::copy_graph(ctx, path, new_path).await?;
        let data = ctx.data_unchecked::<Data>();
        data.delete_graph(path)?;
        Ok(true)
    }

    // If namespace is not provided, it will be set to the current working directory.
    // This applies to both the graph namespace and new graph namespace.
    async fn copy_graph<'a>(ctx: &Context<'a>, path: &str, new_path: &str) -> Result<bool> {
        let data = ctx.data_unchecked::<Data>();
        let graph = data.get_graph(path)?.0.graph.materialize()?;

        #[cfg(feature = "storage")]
        if let GraphStorage::Disk(_) = graph.core_graph() {
            return Err(GqlGraphError::ImmutableDiskGraph.into());
        }
        // TODO: review, can't we do here as well:
        // if graph.graph.storage().is_immutable() {
        //     return Err(GqlGraphError::ImmutableDiskGraph.into());
        // }
        let timestamp: i64 = Utc::now().timestamp();
        graph.update_constant_properties([("lastUpdated", Prop::I64(timestamp * 1000))])?;
        graph.update_constant_properties([("lastOpened", Prop::I64(timestamp * 1000))])?;
        // TODO: make sure that further calls to graph.write_updates() will target the new path!!!!
        data.insert_graph(new_path, graph).await?;

        Ok(true)
    }

    async fn create_graph<'a>(
        ctx: &Context<'a>,
        parent_graph_path: &str,
        new_graph_path: &str,
        props: String,
        is_archive: u8,
        graph_nodes: Vec<String>,
    ) -> Result<bool> {
        let data = ctx.data_unchecked::<Data>();

        // Creating a new graph (owner is user) from UI
        // Graph is created from the parent graph. This means the new graph retains the character of the parent graph i.e.,
        // the new graph is an event or persistent graph depending on if the parent graph is event or persistent graph, respectively.
        let parent_graph = data.get_graph(parent_graph_path)?.0.graph;
        let new_subgraph = parent_graph.subgraph(graph_nodes).materialize()?;

        let now: Prop = Prop::I64(Utc::now().timestamp_millis());
        new_subgraph.update_constant_properties([("creationTime", now.clone())])?;
        new_subgraph.update_constant_properties([("lastUpdated", now.clone())])?;
        new_subgraph.update_constant_properties([("lastOpened", now)])?;
        new_subgraph.update_constant_properties([("uiProps", Prop::Str(props.into()))])?;
        new_subgraph.update_constant_properties([("isArchive", Prop::U8(is_archive))])?;

        data.insert_graph(new_graph_path, new_subgraph).await?;

        Ok(true)
    }

    // /// Load graph from path
    // ///
    // /// Returns::
    // ///   list of names for newly added graphs
    // async fn load_graph_from_path<'a>(
    //     ctx: &Context<'a>,
    //     path_on_server: String,
    //     namespace: &Option<String>,
    //     overwrite: bool,
    // ) -> Result<String> {
    //     let path_on_server = Path::new(&path_on_server);
    //     let data = ctx.data_unchecked::<Data>();
    //     let new_path = load_graph_from_path(&data.work_dir, path_on_server, namespace, overwrite)?;
    //     data.graphs.remove(&new_path.to_path_buf());
    //     Ok(new_path.display().to_string())
    // }

    /// Use GQL multipart upload to send new graphs to server
    ///
    /// Returns::
    ///    name of the new graph
    async fn upload_graph<'a>(
        ctx: &Context<'a>,
        path: String,
        graph: Upload,
        overwrite: bool,
    ) -> Result<String> {
        let data = ctx.data_unchecked::<Data>();
        let graph = {
            let in_file = graph.value(ctx)?.content;
            let mut archive = ZipArchive::new(in_file)?;
            let mut entry = archive.by_name("graph")?;
            let mut buf = vec![];
            entry.read_to_end(&mut buf)?;
            MaterializedGraph::decode_from_bytes(&buf)?
        };
        if overwrite {
            let _ignored = data.delete_graph(&path);
        }
        data.insert_graph(&path, graph).await?;
        Ok(path)
    }

    /// Send graph bincode as base64 encoded string
    ///
    /// Returns::
    ///    path of the new graph
    async fn send_graph<'a>(
        ctx: &Context<'a>,
        path: &str,
        graph: String,
        overwrite: bool,
    ) -> Result<String> {
        let data = ctx.data_unchecked::<Data>();
        let g: MaterializedGraph = url_decode_graph(graph)?;
        if overwrite {
            let _ignored = data.delete_graph(path);
        }
        data.insert_graph(path, g).await?;
        Ok(path.to_owned()) // TODO: review if this is ok?
    }
}

// TODO: what is this for?
pub(crate) fn create_dirs_if_not_present(path: &Path) -> Result<(), GraphError> {
    if let Some(parent) = path.parent() {
        if !parent.exists() {
            fs::create_dir_all(parent)?;
        }
    }
    Ok(())
}

#[derive(App)]
pub struct App(QueryRoot, MutRoot, Mut);

fn delete_graph(path: &Path) -> Result<()> {
    if path.is_file() {
        fs::remove_file(path)?;
    } else if path.is_dir() {
        fs::remove_dir_all(path)?;
    } else {
        return Err(GqlGraphError::GraphDoesNotExists(path.display().to_string()).into());
    }
    Ok(())
}
