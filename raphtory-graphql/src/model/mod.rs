use crate::{
    auth::ContextValidation,
    data::Data,
    model::{
        graph::{
            collection::GqlCollection, graph::GqlGraph, index::IndexSpecInput,
            mutable_graph::GqlMutableGraph, namespace::Namespace,
            vectorised_graph::GqlVectorisedGraph,
        },
        plugins::{mutation_plugin::MutationPlugin, query_plugin::QueryPlugin},
    },
    paths::valid_path,
    rayon::blocking_compute,
    url_encode::{url_decode_graph, url_encode_graph},
};
use async_graphql::Context;
use dynamic_graphql::{
    App, Enum, Mutation, MutationFields, MutationRoot, ResolvedObject, ResolvedObjectFields,
    Result, Upload,
};
use raphtory::{
    db::{api::view::{internal::InternalStorageOps, MaterializedGraph}, graph::views::deletion_graph::PersistentGraph},
    errors::{GraphError, InvalidPathReason},
    prelude::*,
    serialise::*,
    version,
};
use std::{
    error::Error,
    fmt::{Display, Formatter},
    path::PathBuf,
    sync::Arc,
};

pub(crate) mod graph;
pub mod plugins;
pub(crate) mod schema;
pub(crate) mod sorting;

/// a thin wrapper around spawn_blocking that unwraps the join handle
pub(crate) async fn blocking_io<F, R>(f: F) -> R
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    tokio::task::spawn_blocking(f).await.unwrap()
}

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
#[graphql(name = "GraphType")]
pub enum GqlGraphType {
    /// Persistent.
    Persistent,
    /// Event.
    Event,
}

#[derive(ResolvedObject)]
#[graphql(root)]
pub(crate) struct QueryRoot;

#[ResolvedObjectFields]
impl QueryRoot {
    /// Hello world demo
    async fn hello() -> &'static str {
        "Hello world from raphtory-graphql"
    }

    /// Returns a graph
    async fn graph<'a>(ctx: &Context<'a>, path: &str) -> Result<GqlGraph> {
        let data = ctx.data_unchecked::<Data>();
        Ok(data
            .get_graph(path)
            .await
            .map(|(g, folder)| GqlGraph::new(folder, g.graph))?)
    }
    /// Update graph query, has side effects to update graph state
    ///
    /// Returns:: GqlMutableGraph
    async fn update_graph<'a>(ctx: &Context<'a>, path: String) -> Result<GqlMutableGraph> {
        ctx.require_write_access()?;
        let data = ctx.data_unchecked::<Data>();

        let graph = data
            .get_graph(path.as_ref())
            .await
            .map(|(g, folder)| GqlMutableGraph::new(folder, g))?;
        Ok(graph)
    }

    /// Create vectorised graph in the format used for queries
    ///
    /// Returns:: GqlVectorisedGraph
    async fn vectorised_graph<'a>(ctx: &Context<'a>, path: &str) -> Option<GqlVectorisedGraph> {
        let data = ctx.data_unchecked::<Data>();
        let g = data.get_graph(path).await.ok()?.0.vectors?;
        Some(g.into())
    }
    /// Returns all namespaces using recursive search
    ///
    /// Returns::  List of namespaces on root
    async fn namespaces<'a>(ctx: &Context<'a>) -> GqlCollection<Namespace> {
        let data = ctx.data_unchecked::<Data>();
        let root = Namespace::new(data.work_dir.clone(), data.work_dir.clone());
        GqlCollection::new(root.get_all_namespaces().into())
    }

    /// Returns a specific namespace at a given path
    ///
    /// Returns:: Namespace or error if no namespace found
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
    /// Returns root namespace
    ///
    /// Returns::  Root namespace
    async fn root<'a>(ctx: &Context<'a>) -> Namespace {
        let data = ctx.data_unchecked::<Data>();
        Namespace::new(data.work_dir.clone(), data.work_dir.clone())
    }
    /// Returns a plugin.
    async fn plugins<'a>() -> QueryPlugin {
        QueryPlugin::default()
    }
    /// Encodes graph and returns as string
    ///
    /// Returns:: Base64 url safe encoded string
    async fn receive_graph<'a>(ctx: &Context<'a>, path: String) -> Result<String, Arc<GraphError>> {
        let path = path.as_ref();
        let data = ctx.data_unchecked::<Data>();
        let g = data.get_graph(path).await?.0.graph.clone();
        let res = url_encode_graph(g)?;
        Ok(res)
    }

    async fn version<'a>(_ctx: &Context<'a>) -> String {
        String::from(version())
    }
}

#[derive(MutationRoot)]
pub(crate) struct MutRoot;

#[derive(Mutation)]
pub(crate) struct Mut(MutRoot);

#[MutationFields]
impl Mut {
    /// Returns a collection of mutation plugins.
    async fn plugins<'a>(_ctx: &Context<'a>) -> MutationPlugin {
        MutationPlugin::default()
    }

    /// Delete graph from a path on the server.
    // If namespace is not provided, it will be set to the current working directory.
    async fn delete_graph<'a>(ctx: &Context<'a>, path: String) -> Result<bool> {
        let data = ctx.data_unchecked::<Data>();
        data.delete_graph(&path).await?;
        Ok(true)
    }

    /// Creates a new graph.
    async fn new_graph<'a>(
        ctx: &Context<'a>,
        path: String,
        graph_type: GqlGraphType,
    ) -> Result<bool> {
        let data = ctx.data_unchecked::<Data>();
        let overwrite = false;
        let folder = data.validate_path_for_insert(&path, overwrite)?;
        let path = folder.get_graph_path();
        let graph = match graph_type {
            GqlGraphType::Persistent => PersistentGraph::new_at_path(path).materialize()?,
            GqlGraphType::Event => Graph::new_at_path(path).materialize()?,
        };

        data.insert_graph(folder, graph).await?;

        Ok(true)
    }

    /// Move graph from a path path on the server to a new_path on the server.
    ///
    /// If namespace is not provided, it will be set to the current working directory.
    /// This applies to both the graph namespace and new graph namespace.
    async fn move_graph<'a>(ctx: &Context<'a>, path: &str, new_path: &str) -> Result<bool> {
        Self::copy_graph(ctx, path, new_path).await?;
        let data = ctx.data_unchecked::<Data>();
        data.delete_graph(path).await?;
        Ok(true)
    }

    /// Copy graph from a path path on the server to a new_path on the server.
    ///
    /// If namespace is not provided, it will be set to the current working directory.
    /// This applies to both the graph namespace and new graph namespace.
    async fn copy_graph<'a>(ctx: &Context<'a>, path: &str, new_path: &str) -> Result<bool> {
        // doing this in a more efficient way is not trivial, this at least is correct
        // there are questions like, maybe the new vectorised graph have different rules
        // for the templates or if it needs to be vectorised at all
        let data = ctx.data_unchecked::<Data>();
        let overwrite = false;
        let folder = data.validate_path_for_insert(new_path, overwrite)?;
        let graph = data.get_graph(path).await?.0.graph;
        data.insert_graph(folder, graph).await?;

        Ok(true)
    }

    /// Upload a graph file from a path on the client using GQL multipart uploading.
    ///
    /// Returns::
    /// name of the new graph
    async fn upload_graph<'a>(
        ctx: &Context<'a>,
        path: String,
        graph: Upload,
        overwrite: bool,
    ) -> Result<String> {
        let data = ctx.data_unchecked::<Data>();
        let in_file = graph.value(ctx)?.content;
        let folder = data.validate_path_for_insert(&path, overwrite)?;

        if overwrite {
            let _ignored = data.delete_graph(&path).await;
        }

        data.insert_graph_as_bytes(folder, in_file).await?;

        Ok(path)
    }

    /// Send graph bincode as base64 encoded string.
    ///
    /// Returns::
    /// path of the new graph
    async fn send_graph<'a>(
        ctx: &Context<'a>,
        path: &str,
        graph: String,
        overwrite: bool,
    ) -> Result<String> {
        let data = ctx.data_unchecked::<Data>();
        let folder = data.validate_path_for_insert(path, overwrite)?;
        let path_for_decoded_graph = Some(folder.get_graph_path());
        let g: MaterializedGraph = url_decode_graph(graph, path_for_decoded_graph)?;

        if overwrite {
            let _ignored = data.delete_graph(path).await;
        }

        data.insert_graph(folder, g).await?;
        Ok(path.to_owned())
    }

    /// Returns a subgraph given a set of nodes from an existing graph in the server.
    ///
    /// Returns::
    /// name of the new graph
    async fn create_subgraph<'a>(
        ctx: &Context<'a>,
        parent_path: &str,
        nodes: Vec<String>,
        new_path: String,
        overwrite: bool,
    ) -> Result<String> {
        let data = ctx.data_unchecked::<Data>();
        let parent_graph = data.get_graph(parent_path).await?.0.graph;
        let new_subgraph =
            blocking_compute(move || parent_graph.subgraph(nodes).materialize()).await?;
        let folder = data.validate_path_for_insert(&new_path, overwrite)?;

        if overwrite {
            let _ignored = data.delete_graph(&new_path).await;
        }

        data.insert_graph(folder, new_subgraph).await?;
        Ok(new_path)
    }

    /// (Experimental) Creates search index.
    async fn create_index<'a>(
        ctx: &Context<'a>,
        path: &str,
        index_spec: Option<IndexSpecInput>,
        in_ram: bool,
    ) -> Result<bool> {
        #[cfg(feature = "search")]
        {
            let data = ctx.data_unchecked::<Data>();
            let graph = data.get_graph(path).await?.0.graph;
            match index_spec {
                Some(index_spec) => {
                    let index_spec = index_spec.to_index_spec(graph.clone())?;
                    if in_ram {
                        graph.create_index_in_ram_with_spec(index_spec)
                    } else {
                        graph.create_index_with_spec(index_spec)
                    }
                }
                None => {
                    if in_ram {
                        graph.create_index_in_ram()
                    } else {
                        graph.create_index()
                    }
                }
            }?;

            Ok(true)
        }
        #[cfg(not(feature = "search"))]
        {
            Err(GraphError::IndexingNotSupported.into())
        }
    }
}

#[derive(App)]
pub struct App(QueryRoot, MutRoot, Mut);
