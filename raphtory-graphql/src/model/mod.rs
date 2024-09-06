use crate::{
    data::Data,
    model::{
        algorithms::global_plugins::GlobalPlugins,
        graph::{
            graph::GqlGraph, graphs::GqlGraphs, mutable_graph::GqlMutableGraph,
            vectorised_graph::GqlVectorisedGraph,
        },
    },
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
    db::api::view::MaterializedGraph,
    prelude::*,
};
use raphtory_api::core::storage::arc_str::ArcStr;
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
            .global_plugins
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

    async fn plugins<'a>(ctx: &Context<'a>) -> GlobalPlugins {
        let data = ctx.data_unchecked::<Data>();
        data.global_plugins.clone()
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

    async fn update_graph_last_opened<'a>(ctx: &Context<'a>, path: String) -> Result<bool> {
        let path = Path::new(&path);
        let data = ctx.data_unchecked::<Data>();
        let graph = data.get_graph(path)?;

        if graph.graph.storage().is_immutable() {
            return Err(GqlGraphError::ImmutableDiskGraph.into());
        }

        let dt = Utc::now();
        let timestamp: i64 = dt.timestamp();

        graph.update_constant_properties([("lastOpened", Prop::I64(timestamp * 1000))])?;

        graph.write_updates()?;

        Ok(true)
    }

    async fn create_graph<'a>(
        ctx: &Context<'a>,
        parent_graph_path: String,
        new_graph_path: String,
        props: String,
        is_archive: u8,
        graph_nodes: Vec<String>,
    ) -> Result<bool> {
        let parent_graph_path = Path::new(&parent_graph_path);
        let new_graph_path = Path::new(&new_graph_path);
        let data = ctx.data_unchecked::<Data>();

        let parent_graph_full_path = data.construct_graph_full_path(parent_graph_path)?;
        if !parent_graph_full_path.exists() {
            return Err(GraphError::GraphNotFound(parent_graph_path.to_path_buf()).into());
        }
        let new_graph_full_path = data.construct_graph_full_path(new_graph_path)?;
        if new_graph_full_path.exists() {
            return Err(GraphError::GraphNameAlreadyExists(new_graph_path.to_path_buf()).into());
        }

        let timestamp: i64 = Utc::now().timestamp();
        let node_ids = graph_nodes.iter().map(|key| key.as_str()).collect_vec();

        // Creating a new graph (owner is user) from UI
        // Graph is created from the parent graph. This means the new graph retains the character of the parent graph i.e.,
        // the new graph is an event or persistent graph depending on if the parent graph is event or persistent graph, respectively.
        let parent_graph = data.get_graph(&parent_graph_path.to_path_buf())?;
        let new_subgraph = parent_graph.subgraph(node_ids.clone()).materialize()?;

        new_subgraph.update_constant_properties([("creationTime", Prop::I64(timestamp * 1000))])?;
        new_subgraph.update_constant_properties([("lastUpdated", Prop::I64(timestamp * 1000))])?;
        new_subgraph.update_constant_properties([("lastOpened", Prop::I64(timestamp * 1000))])?;
        new_subgraph.update_constant_properties([("uiProps", Prop::Str(props.into()))])?;
        new_subgraph.update_constant_properties([("isArchive", Prop::U8(is_archive))])?;

        create_dirs_if_not_present(&new_graph_full_path)?;
        new_subgraph.cache(new_graph_full_path)?;

        data.graphs
            .insert(new_graph_path.to_path_buf(), new_subgraph.into());

        Ok(true)
    }

    async fn update_graph<'a>(
        ctx: &Context<'a>,
        parent_graph_path: String,
        graph_path: String,
        new_graph_path: String,
        props: String,
        is_archive: u8,
        graph_nodes: Vec<String>,
    ) -> Result<bool> {
        let parent_graph_path = Path::new(&parent_graph_path);
        let graph_path = Path::new(&graph_path);
        let new_graph_path = Path::new(&new_graph_path);
        let data = ctx.data_unchecked::<Data>();

        let parent_graph_full_path = data.construct_graph_full_path(parent_graph_path)?;
        if !parent_graph_full_path.exists() {
            return Err(GraphError::GraphNotFound(parent_graph_path.to_path_buf()).into());
        }

        // Saving an existing graph
        let graph_full_path = data.construct_graph_full_path(graph_path)?;
        if !graph_full_path.exists() {
            return Err(GraphError::GraphNotFound(graph_path.to_path_buf()).into());
        }

        let new_graph_full_path = data.construct_graph_full_path(new_graph_path)?;
        if graph_path != new_graph_path {
            // Save as
            if new_graph_full_path.exists() {
                return Err(
                    GraphError::GraphNameAlreadyExists(new_graph_path.to_path_buf()).into(),
                );
            }
        }

        let current_graph = data.get_graph(graph_path)?;
        #[cfg(feature = "storage")]
        if current_graph.graph.storage().is_immutable() {
            return Err(GqlGraphError::ImmutableDiskGraph.into());
        }

        let timestamp: i64 = Utc::now().timestamp();
        let node_ids = graph_nodes.iter().map(|key| key.as_str()).collect_vec();

        // Creating a new graph from the current graph instead of the parent graph preserves the character of the new graph
        // i.e., the new graph is an event or persistent graph depending on if the current graph is event or persistent graph, respectively.
        let new_subgraph = current_graph.subgraph(node_ids.clone()).materialize()?;

        let parent_graph = data.get_graph(parent_graph_path)?;
        let new_node_ids = node_ids
            .iter()
            .filter(|x| current_graph.graph.node(x).is_none())
            .collect_vec();
        let parent_subgraph = parent_graph.subgraph(new_node_ids);

        let nodes = parent_subgraph.nodes();
        new_subgraph.import_nodes(nodes, true)?;
        let edges = parent_subgraph.edges();
        new_subgraph.import_edges(edges, true)?;

        if graph_path == new_graph_path {
            // Save
            let static_props: Vec<(ArcStr, Prop)> = current_graph
                .properties()
                .into_iter()
                .filter(|(a, _)| a != "name" && a != "lastUpdated" && a != "uiProps")
                .collect_vec();
            new_subgraph.update_constant_properties(static_props)?;
        } else {
            // Save as
            let static_props: Vec<(ArcStr, Prop)> = current_graph
                .properties()
                .into_iter()
                .filter(|(a, _)| a != "name" && a != "creationTime" && a != "uiProps")
                .collect_vec();
            new_subgraph.update_constant_properties(static_props)?;
            new_subgraph
                .update_constant_properties([("creationTime", Prop::I64(timestamp * 1000))])?;
        }

        new_subgraph.update_constant_properties([("lastUpdated", Prop::I64(timestamp * 1000))])?;
        new_subgraph.update_constant_properties([("lastOpened", Prop::I64(timestamp * 1000))])?;
        new_subgraph.update_constant_properties([("uiProps", Prop::Str(props.into()))])?;
        new_subgraph.update_constant_properties([("isArchive", Prop::U8(is_archive))])?;

        new_subgraph.cache(new_graph_full_path)?;

        data.graphs.remove(&graph_path.to_path_buf());
        data.graphs
            .insert(new_graph_path.to_path_buf(), new_subgraph.into());

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

    async fn archive_graph<'a>(ctx: &Context<'a>, path: String, is_archive: u8) -> Result<bool> {
        let path = Path::new(&path);
        let data = ctx.data_unchecked::<Data>();
        let graph = data.get_graph(path)?;

        if graph.graph.storage().is_immutable() {
            return Err(GqlGraphError::ImmutableDiskGraph.into());
        }

        graph.update_constant_properties([("isArchive", Prop::U8(is_archive))])?;
        graph.write_updates()?;
        Ok(true)
    }
}

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
