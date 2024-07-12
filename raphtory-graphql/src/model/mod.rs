use crate::{
    data::{load_graph_from_path, load_graphs_from_path, Data},
    model::{
        algorithms::global_plugins::GlobalPlugins,
        graph::{graph::GqlGraph, graphs::GqlGraphs, vectorised_graph::GqlVectorisedGraph},
    },
    url_encode::url_decode_graph,
};
use async_graphql::Context;
use base64::{engine::general_purpose::STANDARD, Engine};
use chrono::Utc;
use dynamic_graphql::{
    App, Mutation, MutationFields, MutationRoot, ResolvedObject, ResolvedObjectFields, Result,
    Upload,
};
use itertools::Itertools;
use raphtory::{
    core::{utils::errors::GraphError, ArcStr, Prop},
    db::api::view::MaterializedGraph,
    prelude::{GraphViewOps, ImportOps, NodeViewOps, PropertyAdditionOps},
};
use serde_json::Value;
use std::{
    error::Error,
    fmt::{Display, Formatter},
    fs,
    io::Read,
    path::{Path, PathBuf},
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

#[derive(ResolvedObject)]
#[graphql(root)]
pub(crate) struct QueryRoot;

#[ResolvedObjectFields]
impl QueryRoot {
    async fn hello() -> &'static str {
        "Hello world from raphtory-graphql"
    }

    /// Returns a graph
    async fn graph<'a>(
        ctx: &Context<'a>,
        name: &str,
        namespace: &Option<String>,
    ) -> Result<GqlGraph> {
        let data = ctx.data_unchecked::<Data>();
        Ok(data
            .get_graph(name, namespace)
            .map(|g| GqlGraph::new(name.to_string(), namespace.clone(), g))?)
    }

    async fn vectorised_graph<'a>(
        ctx: &Context<'a>,
        name: &str,
        namespace: &Option<String>, // TODO: Need to fix this
    ) -> Option<GqlVectorisedGraph> {
        let data = ctx.data_unchecked::<Data>();
        let g = data
            .global_plugins
            .vectorised_graphs
            .read()
            .get(name)
            .cloned()?;
        Some(g.into())
    }

    async fn graphs<'a>(ctx: &Context<'a>) -> Result<Option<GqlGraphs>> {
        let data = ctx.data_unchecked::<Data>();
        let (names, namespaces) = data.get_graph_names_namespaces()?;
        Ok(Some(GqlGraphs::new(names, namespaces)))
    }

    async fn plugins<'a>(ctx: &Context<'a>) -> GlobalPlugins {
        let data = ctx.data_unchecked::<Data>();
        data.global_plugins.clone()
    }

    async fn receive_graph<'a>(
        ctx: &Context<'a>,
        name: &str,
        namespace: &Option<String>,
    ) -> Result<String> {
        let data = ctx.data_unchecked::<Data>();
        let g = data.get_graph(name, namespace)?.materialize()?;
        Ok(STANDARD.encode(g.bincode()?))
    }
}

#[derive(MutationRoot)]
pub(crate) struct MutRoot;

#[derive(Mutation)]
pub(crate) struct Mut(MutRoot);

#[MutationFields]
impl Mut {
    // If namespace is not provided, it will be set to the current working directory.
    async fn delete_graph<'a>(
        ctx: &Context<'a>,
        graph_name: String,
        graph_namespace: &Option<String>,
    ) -> Result<bool> {
        let data = ctx.data_unchecked::<Data>();
        let current_graph_path =
            construct_graph_path(&data.work_dir, &graph_name, graph_namespace)?;
        if !current_graph_path.exists() {
            return Err(GraphError::GraphNotFound(construct_graph_name(
                &graph_name,
                graph_namespace,
            ))
            .into());
        }

        delete_graph(&current_graph_path)?;
        data.graphs.remove(&graph_name);

        Ok(true)
    }

    // If namespace is not provided, it will be set to the current working directory.
    // This applies to both the graph namespace and new graph namespace.
    async fn move_graph<'a>(
        ctx: &Context<'a>,
        graph_name: String,
        graph_namespace: &Option<String>,
        new_graph_name: String,
        new_graph_namespace: &Option<String>,
    ) -> Result<bool> {
        let data = ctx.data_unchecked::<Data>();
        let current_graph_path =
            construct_graph_path(&data.work_dir, &graph_name, graph_namespace)?;
        if !current_graph_path.exists() {
            return Err(GraphError::GraphNotFound(construct_graph_name(
                &graph_name,
                graph_namespace,
            ))
            .into());
        }
        let new_graph_path =
            construct_graph_path(&data.work_dir, &new_graph_name, new_graph_namespace)?;
        if new_graph_path.exists() {
            return Err(GraphError::GraphNameAlreadyExists {
                name: construct_graph_name(&new_graph_name, new_graph_namespace),
            }
            .into());
        }

        let current_graph = data.get_graph(&graph_name, graph_namespace)?;

        #[cfg(feature = "storage")]
        if current_graph.clone().graph.into_disk_graph().is_some() {
            return Err(GqlGraphError::ImmutableDiskGraph.into());
        }

        if new_graph_path.ne(&current_graph_path) {
            let timestamp: i64 = Utc::now().timestamp();

            current_graph
                .update_constant_properties([("name", Prop::Str(new_graph_name.clone().into()))])?;
            current_graph
                .update_constant_properties([("lastUpdated", Prop::I64(timestamp * 1000))])?;
            current_graph
                .update_constant_properties([("lastOpened", Prop::I64(timestamp * 1000))])?;
            current_graph.save_to_file(&new_graph_path)?;

            delete_graph(&current_graph_path)?;
            data.graphs.remove(&graph_name);
        }

        Ok(true)
    }

    // If namespace is not provided, it will be set to the current working directory.
    // This applies to both the graph namespace and new graph namespace.
    async fn copy_graph<'a>(
        ctx: &Context<'a>,
        graph_name: String,
        graph_namespace: &Option<String>,
        new_graph_name: String,
        new_graph_namespace: &Option<String>,
    ) -> Result<bool> {
        let data = ctx.data_unchecked::<Data>();
        let current_graph_path =
            construct_graph_path(&data.work_dir, &graph_name, graph_namespace)?;
        if !current_graph_path.exists() {
            return Err(GraphError::GraphNotFound(construct_graph_name(
                &graph_name,
                graph_namespace,
            ))
            .into());
        }
        let new_graph_path =
            construct_graph_path(&data.work_dir, &new_graph_name, new_graph_namespace)?;
        if new_graph_path.exists() {
            return Err(GraphError::GraphNameAlreadyExists {
                name: construct_graph_name(&new_graph_name, new_graph_namespace),
            }
            .into());
        }

        let current_graph = data.get_graph(&graph_name, graph_namespace)?;

        #[cfg(feature = "storage")]
        if current_graph.clone().graph.into_disk_graph().is_some() {
            return Err(GqlGraphError::ImmutableDiskGraph.into());
        }

        if new_graph_path.ne(&current_graph_path) {
            let timestamp: i64 = Utc::now().timestamp();

            let new_graph = current_graph
                .subgraph(current_graph.nodes().name().collect_vec())
                .materialize()?;

            new_graph
                .update_constant_properties([("name", Prop::Str(new_graph_name.clone().into()))])?;
            new_graph.update_constant_properties([("lastUpdated", Prop::I64(timestamp * 1000))])?;
            new_graph.update_constant_properties([("lastOpened", Prop::I64(timestamp * 1000))])?;

            new_graph.save_to_file(&new_graph_path)?;
        }

        Ok(true)
    }

    async fn update_graph_last_opened<'a>(
        ctx: &Context<'a>,
        graph_name: String,
        namespace: &Option<String>,
    ) -> Result<bool> {
        let data = ctx.data_unchecked::<Data>();
        let graph = data.get_graph(&graph_name, namespace)?;

        #[cfg(feature = "storage")]
        if graph.clone().graph.into_disk_graph().is_some() {
            return Err(GqlGraphError::ImmutableDiskGraph.into());
        }

        let dt = Utc::now();
        let timestamp: i64 = dt.timestamp();

        graph.update_constant_properties([("lastOpened", Prop::I64(timestamp * 1000))])?;

        let path = construct_graph_path(&data.work_dir, &graph_name, namespace)?;
        graph.save_to_file(path)?;
        data.graphs.insert(graph_name, graph);

        Ok(true)
    }

    async fn create_graph<'a>(
        ctx: &Context<'a>,
        parent_graph_name: String,
        parent_graph_namespace: &Option<String>,
        new_graph_namespace: &Option<String>,
        new_graph_name: String,
        props: String,
        is_archive: u8,
        graph_nodes: String,
    ) -> Result<bool> {
        let data = ctx.data_unchecked::<Data>();
        let parent_graph_path =
            construct_graph_path(&data.work_dir, &parent_graph_name, parent_graph_namespace)?;
        if !parent_graph_path.exists() {
            return Err(GraphError::GraphNotFound(construct_graph_name(
                &parent_graph_name,
                parent_graph_namespace,
            ))
            .into());
        }

        let new_graph_path =
            construct_graph_path(&data.work_dir, &new_graph_name, new_graph_namespace)?;
        if new_graph_path.exists() {
            return Err(GraphError::GraphNameAlreadyExists {
                name: construct_graph_name(&new_graph_name, new_graph_namespace),
            }
            .into());
        }

        let timestamp: i64 = Utc::now().timestamp();
        let deserialized_node_map: Value = serde_json::from_str(graph_nodes.as_str())?;
        let node_map = deserialized_node_map
            .as_object()
            .ok_or("graph_nodes not object")?;
        let node_ids = node_map.keys().map(|key| key.as_str()).collect_vec();

        // Creating a new graph (owner is user) from UI
        // Graph is created from the parent graph. This means the new graph retains the character of the parent graph i.e.,
        // the new graph is an event or persistent graph depending on if the parent graph is event or persistent graph, respectively.
        let parent_graph = data.get_graph(&parent_graph_name, parent_graph_namespace)?;
        let new_subgraph = parent_graph.subgraph(node_ids.clone()).materialize()?;

        new_subgraph.update_constant_properties([("creationTime", Prop::I64(timestamp * 1000))])?;
        new_subgraph.update_constant_properties([("name", Prop::str(new_graph_name.clone()))])?;
        new_subgraph.update_constant_properties([("lastUpdated", Prop::I64(timestamp * 1000))])?;
        new_subgraph.update_constant_properties([("lastOpened", Prop::I64(timestamp * 1000))])?;
        new_subgraph.update_constant_properties([("uiProps", Prop::Str(props.into()))])?;
        new_subgraph.update_constant_properties([("isArchive", Prop::U8(is_archive))])?;

        new_subgraph.save_to_file(new_graph_path)?;

        data.graphs
            .insert(new_graph_name.clone(), new_subgraph.into());

        Ok(true)
    }

    async fn update_graph<'a>(
        ctx: &Context<'a>,
        parent_graph_name: String,
        parent_graph_namespace: &Option<String>,
        graph_name: String,
        graph_namespace: &Option<String>,
        new_graph_name: String,
        props: String,
        is_archive: u8,
        graph_nodes: Vec<String>,
    ) -> Result<bool> {
        let data = ctx.data_unchecked::<Data>();
        let parent_graph_path =
            construct_graph_path(&data.work_dir, &parent_graph_name, parent_graph_namespace)?;
        if !parent_graph_path.exists() {
            return Err(GraphError::GraphNotFound(construct_graph_name(
                &parent_graph_name,
                parent_graph_namespace,
            ))
            .into());
        }

        // Saving an existing graph
        let current_graph_path =
            construct_graph_path(&data.work_dir, &graph_name, graph_namespace)?;
        if !current_graph_path.exists() {
            return Err(GraphError::GraphNotFound(construct_graph_name(
                &graph_name,
                graph_namespace,
            ))
            .into());
        }

        let new_graph_path =
            construct_graph_path(&data.work_dir, &new_graph_name, graph_namespace)?;
        if graph_name != new_graph_name {
            // Save as
            if new_graph_path.exists() {
                return Err(GraphError::GraphNameAlreadyExists {
                    name: construct_graph_name(&new_graph_name, graph_namespace),
                }
                .into());
            }
        }

        let current_graph = data.get_graph(&graph_name, graph_namespace)?;
        #[cfg(feature = "storage")]
        if current_graph.clone().graph.into_disk_graph().is_some() {
            return Err(GqlGraphError::ImmutableDiskGraph.into());
        }

        let timestamp: i64 = Utc::now().timestamp();
        let node_ids = graph_nodes.iter().map(|key| key.as_str()).collect_vec();

        // Creating a new graph from the current graph instead of the parent graph preserves the character of the new graph
        // i.e., the new graph is an event or persistent graph depending on if the current graph is event or persistent graph, respectively.
        let new_subgraph = current_graph.subgraph(node_ids.clone()).materialize()?;

        let parent_graph = data.get_graph(&parent_graph_name, parent_graph_namespace)?;
        let new_node_ids = node_ids
            .iter()
            .filter(|x| current_graph.graph.node(x).is_none())
            .collect_vec();
        let parent_subgraph = parent_graph.subgraph(new_node_ids);

        let nodes = parent_subgraph.nodes();
        new_subgraph.import_nodes(nodes, true)?;
        let edges = parent_subgraph.edges();
        new_subgraph.import_edges(edges, true)?;

        if graph_name == new_graph_name {
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

        new_subgraph.update_constant_properties([("name", Prop::str(new_graph_name.clone()))])?;
        new_subgraph.update_constant_properties([("lastUpdated", Prop::I64(timestamp * 1000))])?;
        new_subgraph.update_constant_properties([("lastOpened", Prop::I64(timestamp * 1000))])?;
        new_subgraph.update_constant_properties([("uiProps", Prop::Str(props.into()))])?;
        new_subgraph.update_constant_properties([("isArchive", Prop::U8(is_archive))])?;

        new_subgraph.save_to_file(new_graph_path)?;

        data.graphs.remove(graph_name.as_str());
        data.graphs
            .insert(new_graph_name.clone(), new_subgraph.into());

        Ok(true)
    }

    /// Load graph from path
    ///
    /// Returns::
    ///   list of names for newly added graphs
    async fn load_graph_from_path<'a>(
        ctx: &Context<'a>,
        file_path: String,
        namespace: &Option<String>,
        overwrite: bool,
    ) -> Result<String> {
        let data = ctx.data_unchecked::<Data>();
        let name = load_graph_from_path(
            data.work_dir.as_ref(),
            (&file_path).as_ref(),
            namespace,
            overwrite,
        )?;
        data.graphs.invalidate(&name);
        Ok(name)
    }

    /// Use GQL multipart upload to send new graphs to server
    ///
    /// Returns::
    ///    name of the new graph
    async fn upload_graph<'a>(
        ctx: &Context<'a>,
        name: String,
        graph: Upload,
        namespace: &Option<String>,
        overwrite: bool,
    ) -> Result<String> {
        let data = ctx.data_unchecked::<Data>();
        let path = construct_graph_path(data.work_dir.as_str(), name.as_str(), namespace)?;
        if path.exists() && !overwrite {
            return Err(GraphError::GraphNameAlreadyExists { name }.into());
        }
        let mut buffer = Vec::new();
        let mut buff_read = graph.value(ctx)?.content;
        buff_read.read_to_end(&mut buffer)?;
        let g: MaterializedGraph = MaterializedGraph::from_bincode(&buffer)?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(GraphError::from)?;
        }
        g.save_to_file(&path)?;
        data.graphs.insert(name.clone(), g.into());
        Ok(name)
    }

    /// Send graph bincode as base64 encoded string
    ///
    /// Returns::
    ///    name of the new graph
    async fn send_graph<'a>(
        ctx: &Context<'a>,
        name: String,
        graph: String,
        namespace: &Option<String>,
        overwrite: bool,
    ) -> Result<String> {
        let data = ctx.data_unchecked::<Data>();
        let path = construct_graph_path(&data.work_dir, &name, namespace)?;
        if path.exists() && !overwrite {
            return Err(GraphError::GraphNameAlreadyExists { name }.into());
        }
        let g: MaterializedGraph = url_decode_graph(graph)?;
        g.save_to_file(&path)?;
        data.graphs.insert(name.clone(), g.into());
        Ok(name)
    }

    async fn archive_graph<'a>(
        ctx: &Context<'a>,
        graph_name: String,
        namespace: &Option<String>,
        is_archive: u8,
    ) -> Result<bool> {
        let data = ctx.data_unchecked::<Data>();
        let subgraph = data.get_graph(&graph_name, namespace)?;

        #[cfg(feature = "storage")]
        if subgraph.clone().graph.into_disk_graph().is_some() {
            return Err(GqlGraphError::ImmutableDiskGraph.into());
        }

        subgraph.update_constant_properties([("isArchive", Prop::U8(is_archive))])?;

        let path = construct_graph_path(&data.work_dir, &graph_name, namespace)?;
        subgraph.save_to_file(path)?;

        data.graphs.insert(graph_name, subgraph);

        Ok(true)
    }

    // This function does not serve use case as yet. Need to decide on the semantics for multi-graphs.
    async fn load_graphs_from_path<'a>(
        ctx: &Context<'a>,
        path: String,
        namespace: &Option<String>,
        overwrite: bool,
    ) -> Result<Vec<String>> {
        let data = ctx.data_unchecked::<Data>();
        let names = load_graphs_from_path(
            data.work_dir.as_ref(),
            (&path).as_ref(),
            namespace,
            overwrite,
        )?;
        names.iter().for_each(|name| data.graphs.invalidate(name));
        Ok(names)
    }
}

pub(crate) fn construct_graph_name(name: &String, namespace: &Option<String>) -> String {
    match namespace {
        Some(namespace) if !namespace.is_empty() => format!("{}/{}", namespace, name),
        _ => name.clone(),
    }
}

pub(crate) fn construct_graph_path(
    work_dir: &str,
    name: &str,
    namespace: &Option<String>,
) -> Result<PathBuf, GqlGraphError> {
    let mut path = PathBuf::from(work_dir);
    if let Some(ns) = namespace {
        if ns.contains("//") {
            return Err(GqlGraphError::InvalidNamespace(ns.to_string()));
        }

        let ns_path = Path::new(&ns);
        for comp in ns_path.components() {
            if matches!(comp, std::path::Component::ParentDir) {
                return Err(GqlGraphError::InvalidNamespace(ns.to_string()));
            }
        }
        path = path.join(ns_path);
    }
    path = path.join(name);

    // Check if the path exists, if not create it
    if let Some(parent) = path.parent() {
        if !parent.exists() {
            fs::create_dir_all(parent)
                .map_err(|e| GqlGraphError::FailedToCreateDir(e.to_string()))?;
        }
    }

    Ok(path)
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
