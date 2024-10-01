use crate::{
    data::Data,
    model::{
        graph::{
            graph::GqlGraph, graphs::GqlGraphs, mutable_graph::GqlMutableGraph,
            vectorised_graph::GqlVectorisedGraph,
        },
        plugins::{mutation_plugin::MutationPlugin, query_plugin::QueryPlugin},
    },
    url_encode::{url_decode_graph, url_encode_graph},
};
use async_graphql::Context;
use chrono::Utc;
use dynamic_graphql::{
    App, Enum, Mutation, MutationFields, MutationRoot, ResolvedObject, ResolvedObjectFields,
    Result, Upload,
};
#[cfg(feature = "storage")]
use raphtory::db::api::{storage::graph::storage_ops::GraphStorage, view::internal::CoreGraphOps};
use raphtory::{
    core::{utils::errors::GraphError, Prop},
    db::api::view::MaterializedGraph,
    prelude::*,
};
use std::{
    error::Error,
    fmt::{Display, Formatter},
    fs,
    fs::File,
    io::copy,
    path::Path,
    sync::Arc,
};

pub mod algorithms;
pub(crate) mod graph;
pub mod plugins;
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
    async fn graph<'a>(ctx: &Context<'a>, path: String) -> Result<GqlGraph> {
        let path = Path::new(&path);
        let data = ctx.data_unchecked::<Data>();
        let work_dir = data.work_dir.clone();

        Ok(data
            .get_graph(path)
            .map(|g| GqlGraph::new(work_dir, path.to_path_buf(), g))?)
    }

    async fn update_graph<'a>(ctx: &Context<'a>, path: String) -> Result<GqlMutableGraph> {
        let data = ctx.data_unchecked::<Data>();
        let work_dir = data.work_dir.clone();
        let graph = data
            .get_graph(path.as_ref())
            .map(|g| GqlMutableGraph::new(work_dir, path, g))?;
        Ok(graph)
    }

    async fn vectorised_graph<'a>(ctx: &Context<'a>, path: String) -> Option<GqlVectorisedGraph> {
        let data = ctx.data_unchecked::<Data>();
        let g = data
            .global_plugin
            .vectorised_graphs
            .read()
            .get(&path)
            .cloned()?;
        Some(g.into())
    }

    async fn graphs<'a>(ctx: &Context<'a>) -> Result<GqlGraphs> {
        let data = ctx.data_unchecked::<Data>();
        let paths = data.get_graph_names_paths()?;
        let work_dir = data.work_dir.clone();
        Ok(GqlGraphs::new(work_dir, paths))
    }

    async fn plugins<'a>(ctx: &Context<'a>) -> QueryPlugin {
        let data = ctx.data_unchecked::<Data>();
        data.global_plugin.clone()
    }

    async fn receive_graph<'a>(ctx: &Context<'a>, path: String) -> Result<String, Arc<GraphError>> {
        let path = path.as_ref();
        let data = ctx.data_unchecked::<Data>();
        let g = data.get_graph(path)?.graph.clone();
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
    async fn plugins<'a>(ctx: &Context<'a>) -> MutationPlugin {
        MutationPlugin::default()
    }

    // If namespace is not provided, it will be set to the current working directory.
    async fn delete_graph<'a>(ctx: &Context<'a>, path: String) -> Result<bool> {
        let path = Path::new(&path);
        let data = ctx.data_unchecked::<Data>();

        let full_path = data.construct_graph_full_path(path)?;
        if !full_path.exists() {
            return Err(GraphError::GraphNotFound(path.to_path_buf()).into());
        }

        delete_graph(&full_path)?;
        data.graphs.remove(&path.to_path_buf());
        Ok(true)
    }

    async fn new_graph<'a>(
        ctx: &Context<'a>,
        path: String,
        graph_type: GqlGraphType,
    ) -> Result<bool> {
        let data = ctx.data_unchecked::<Data>();
        data.new_graph(path.as_ref(), graph_type)?;
        Ok(true)
    }

    // If namespace is not provided, it will be set to the current working directory.
    // This applies to both the graph namespace and new graph namespace.
    async fn move_graph<'a>(ctx: &Context<'a>, path: String, new_path: String) -> Result<bool> {
        let path = Path::new(&path);
        let new_path = Path::new(&new_path);
        let data = ctx.data_unchecked::<Data>();

        let full_path = data.construct_graph_full_path(path)?;
        if !full_path.exists() {
            return Err(GraphError::GraphNotFound(path.to_path_buf()).into());
        }
        let new_full_path = data.construct_graph_full_path(new_path)?;
        if new_full_path.exists() {
            return Err(GraphError::GraphNameAlreadyExists(new_path.to_path_buf()).into());
        }

        let graph = data.get_graph(&path)?;

        #[cfg(feature = "storage")]
        if let GraphStorage::Disk(_) = graph.core_graph() {
            return Err(GqlGraphError::ImmutableDiskGraph.into());
        }

        if new_full_path.ne(&full_path) {
            let timestamp: i64 = Utc::now().timestamp();

            graph.update_constant_properties([("lastUpdated", Prop::I64(timestamp * 1000))])?;
            graph.update_constant_properties([("lastOpened", Prop::I64(timestamp * 1000))])?;
            create_dirs_if_not_present(&new_full_path)?;
            graph.graph.encode(&new_full_path)?;

            delete_graph(&full_path)?;
            data.graphs.remove(&path.to_path_buf());
        }

        Ok(true)
    }

    // If namespace is not provided, it will be set to the current working directory.
    // This applies to both the graph namespace and new graph namespace.
    async fn copy_graph<'a>(ctx: &Context<'a>, path: String, new_path: String) -> Result<bool> {
        let path = Path::new(&path);
        let new_path = Path::new(&new_path);
        let data = ctx.data_unchecked::<Data>();

        let full_path = data.construct_graph_full_path(path)?;
        if !full_path.exists() {
            return Err(GraphError::GraphNotFound(path.to_path_buf()).into());
        }
        let new_full_path = data.construct_graph_full_path(new_path)?;
        if new_full_path.exists() {
            return Err(GraphError::GraphNameAlreadyExists(new_path.to_path_buf()).into());
        }

        let graph = data.get_graph(path)?;

        #[cfg(feature = "storage")]
        if let GraphStorage::Disk(_) = graph.core_graph() {
            return Err(GqlGraphError::ImmutableDiskGraph.into());
        }

        if new_full_path.ne(&full_path) {
            let timestamp: i64 = Utc::now().timestamp();
            let new_graph = graph.materialize()?;
            new_graph.update_constant_properties([("lastOpened", Prop::I64(timestamp * 1000))])?;
            create_dirs_if_not_present(&new_full_path)?;
            new_graph.encode(&new_full_path)?;
        }

        Ok(true)
    }

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
        let path = Path::new(&path);
        let data = ctx.data_unchecked::<Data>();

        let full_path = data.construct_graph_full_path(path)?;
        if full_path.exists() && !overwrite {
            return Err(GraphError::GraphNameAlreadyExists(path.to_path_buf()).into());
        }

        let mut in_file = graph.value(ctx)?.content;
        create_dirs_if_not_present(&full_path)?;
        let mut out_file = File::create(&full_path)?;
        copy(&mut in_file, &mut out_file)?;
        let g = MaterializedGraph::load_cached(&full_path)?;
        data.graphs.insert(path.to_path_buf(), g.into());
        Ok(path.display().to_string())
    }

    /// Send graph bincode as base64 encoded string
    ///
    /// Returns::
    ///    path of the new graph
    async fn send_graph<'a>(
        ctx: &Context<'a>,
        path: String,
        graph: String,
        overwrite: bool,
    ) -> Result<String> {
        let path = Path::new(&path);
        let data = ctx.data_unchecked::<Data>();
        let full_path = data.construct_graph_full_path(path)?;
        if full_path.exists() && !overwrite {
            return Err(GraphError::GraphNameAlreadyExists(path.to_path_buf()).into());
        }
        let g: MaterializedGraph = url_decode_graph(graph)?;
        create_dirs_if_not_present(&full_path)?;
        g.cache(&full_path)?;
        data.graphs.insert(path.to_path_buf(), g.into());
        Ok(path.display().to_string())
    }
}

pub fn create_dirs_if_not_present(path: &Path) -> Result<(), GraphError> {
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
