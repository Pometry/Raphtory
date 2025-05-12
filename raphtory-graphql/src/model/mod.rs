use crate::{
    auth::ContextValidation,
    data::Data,
    model::{
        graph::{
            graph::GqlGraph, graphs::GqlGraphs, mutable_graph::GqlMutableGraph,
            namespace::Namespace, vectorised_graph::GqlVectorisedGraph,
        },
        plugins::{mutation_plugin::MutationPlugin, query_plugin::QueryPlugin},
    },
    paths::valid_path,
    url_encode::{url_decode_graph, url_encode_graph},
};
use async_graphql::Context;
use dynamic_graphql::{
    App, Enum, Mutation, MutationFields, MutationRoot, ResolvedObject, ResolvedObjectFields,
    Result, Upload,
};
use std::time::{Duration, Instant};

#[cfg(feature = "storage")]
use raphtory::db::api::{storage::graph::storage_ops::GraphStorage, view::internal::CoreGraphOps};
use raphtory::{
    core::utils::errors::{GraphError, InvalidPathReason},
    db::{api::view::MaterializedGraph, graph::views::deletion_graph::PersistentGraph},
    prelude::*,
};
use std::{
    error::Error,
    fmt::{Display, Formatter},
    io::Read,
    sync::Arc,
};
use zip::ZipArchive;

pub mod algorithms;
pub(crate) mod graph;
pub mod plugins;
pub(crate) mod schema;
pub(crate) mod sorting;

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

    //Server unresponsive for 20 seconds
    async fn busy_loop_test() -> i64 {
        let wait_time = Duration::from_secs(20);
        let start = Instant::now();

        while Instant::now() - start < wait_time {}
        1
    }

    //Server runs fine and eventually returns 1
    async fn busy_loop_spawn_blocking_test() -> i64 {
        tokio::task::spawn_blocking(|| {
            let start = Instant::now();
            while Instant::now() - start < Duration::from_secs(20) {
                // real busy loop
            }
        })
        .await
        .unwrap();
        1
    }

    /// Returns a graph
    async fn graph<'a>(ctx: &Context<'a>, path: &str) -> Result<GqlGraph> {
        let data = ctx.data_unchecked::<Data>();
        Ok(data
            .get_graph(path)
            .map(|(g, folder)| GqlGraph::new(folder, g.graph))?)
    }

    async fn update_graph<'a>(ctx: &Context<'a>, path: String) -> Result<GqlMutableGraph> {
        ctx.require_write_access()?;
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

    async fn namespaces<'a>(ctx: &Context<'a>) -> Vec<Namespace> {
        let data = ctx.data_unchecked::<Data>();
        let root = Namespace::new(data.work_dir.clone(), data.work_dir.clone());
        root.get_all_children()
    }
    async fn namespace<'a>(
        ctx: &Context<'a>,
        path: String,
    ) -> Result<Namespace, InvalidPathReason> {
        let data = ctx.data_unchecked::<Data>();
        let current_dir = valid_path(data.work_dir.clone(), path.as_str(), true)?;

        if current_dir.exists() {
            Ok(Namespace::new(data.work_dir.clone(), current_dir))
        } else {
            Err(InvalidPathReason::NamespaceDoesNotExist(path))
        }
    }
    async fn root<'a>(ctx: &Context<'a>) -> Namespace {
        let data = ctx.data_unchecked::<Data>();
        Namespace::new(data.work_dir.clone(), data.work_dir.clone())
    }

    //To deprecate I think
    async fn graphs<'a>(ctx: &Context<'a>) -> Result<GqlGraphs> {
        let data = ctx.data_unchecked::<Data>();
        let paths = data.get_all_graph_folders();
        Ok(GqlGraphs::new(paths))
    }

    async fn plugins<'a>(ctx: &Context<'a>) -> QueryPlugin {
        let data = ctx.data_unchecked::<Data>();
        data.get_global_plugins()
    }

    async fn receive_graph<'a>(ctx: &Context<'a>, path: String) -> Result<String, Arc<GraphError>> {
        let path = path.as_ref();
        let data = ctx.data_unchecked::<Data>();
        let g = data.get_graph(path)?.0.graph.clone();
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
    async fn plugins<'a>(_ctx: &Context<'a>) -> MutationPlugin {
        MutationPlugin::default()
    }

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
        // doing this in a more efficient way is not trivial, this at least is correct
        // there are questions like, maybe the new vectorised graph have different rules
        // for the templates or if it needs to be vectorised at all
        let data = ctx.data_unchecked::<Data>();
        let graph = data.get_graph(path)?.0.graph.materialize()?;

        #[cfg(feature = "storage")]
        if let GraphStorage::Disk(_) = graph.core_graph() {
            return Err(GqlGraphError::ImmutableDiskGraph.into());
        }
        data.insert_graph(new_path, graph).await?;

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
        Ok(path.to_owned())
    }

    /// Create a subgraph out of some existing graph in the server
    ///
    /// Returns::
    ///    name of the new graph
    async fn create_subgraph<'a>(
        ctx: &Context<'a>,
        parent_path: &str,
        nodes: Vec<String>,
        new_path: String,
        overwrite: bool,
    ) -> Result<String> {
        let data = ctx.data_unchecked::<Data>();
        let parent_graph = data.get_graph(parent_path)?.0.graph;
        let new_subgraph = parent_graph.subgraph(nodes).materialize()?;
        if overwrite {
            let _ignored = data.delete_graph(&new_path);
        }
        data.insert_graph(&new_path, new_subgraph).await?;
        Ok(new_path)
    }
}

#[derive(App)]
pub struct App(QueryRoot, MutRoot, Mut);
