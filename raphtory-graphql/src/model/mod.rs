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
use dynamic_graphql::{
    App, Enum, Mutation, MutationFields, MutationRoot, ResolvedObject, ResolvedObjectFields,
    Result, Upload,
};
#[cfg(feature = "storage")]
use raphtory::db::api::{storage::graph::storage_ops::GraphStorage, view::internal::CoreGraphOps};
use raphtory::{
    core::utils::errors::GraphError,
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

    /// Returns a graph
    async fn graph<'a>(ctx: &Context<'a>, path: &str) -> Result<GqlGraph> {
        let data = ctx.data_unchecked::<Data>();
        Ok(data
            .get_graph(path)
            .map(|(g, folder)| GqlGraph::new(folder, g.graph, g.index))?)
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
}

#[derive(App)]
pub struct App(QueryRoot, MutRoot, Mut);
