use crate::{
    data::{load_graph_from_path, load_graphs_from_path, Data},
    model::{
        algorithms::global_plugins::GlobalPlugins,
        graph::{graph::GqlGraph, graphs::GqlGraphs, vectorised_graph::GqlVectorisedGraph},
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
    prelude::{GraphViewOps, ImportOps, NodeViewOps, PropertyAdditionOps},
    search::{into_indexed::DynamicIndexedGraph, IndexedGraph},
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
    ) -> Result<Option<GqlGraph>> {
        let data = ctx.data_unchecked::<Data>();
        Ok(data
            .get_graph(name, namespace)?
            .map(|g| GqlGraph::new(name.to_string(), namespace.clone(), g)))
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
        let g = data
            .get_graph(name, namespace)?
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
    async fn rename_graph<'a>(
        ctx: &Context<'a>,
        parent_graph_name: String,
        graph_name: String,
        new_graph_name: String,
        namespace: &Option<String>,
    ) -> Result<bool> {
        let data = ctx.data_unchecked::<Data>();
        if data.graphs.contains_key(&new_graph_name) {
            return Err(GraphError::GraphNameAlreadyExists {
                name: new_graph_name,
            }
            .into());
        }

        let data = ctx.data_unchecked::<Data>();
        let subgraph = data
            .get_graph(&graph_name, namespace)?
            .ok_or("Graph not found")?;

        #[cfg(feature = "storage")]
        if subgraph.clone().graph.into_disk_graph().is_some() {
            return Err(GqlGraphError::ImmutableDiskGraph.into());
        }

        if new_graph_name.ne(&graph_name) && parent_graph_name.ne(&graph_name) {
            let old_path = subgraph
                .properties()
                .constant()
                .get("path")
                .ok_or("Path is missing")?
                .to_string();
            let old_path = Path::new(&old_path);
            let path = old_path
                .parent()
                .map(|p| p.to_path_buf())
                .ok_or("Path is missing")?;
            let path = path.join(&new_graph_name);

            let parent_graph = data
                .get_graph(&parent_graph_name, namespace)?
                .ok_or("Graph not found")?;
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
            new_subgraph.update_constant_properties([(
                "path",
                Prop::Str(path.display().to_string().into()),
            )])?;

            let dt = Utc::now();
            let timestamp: i64 = dt.timestamp();
            new_subgraph
                .update_constant_properties([("lastUpdated", Prop::I64(timestamp * 1000))])?;
            new_subgraph
                .update_constant_properties([("lastOpened", Prop::I64(timestamp * 1000))])?;
            new_subgraph.save_to_file(&path)?;

            let gi: IndexedGraph<MaterializedGraph> = new_subgraph.into();

            data.graphs.insert(new_graph_name, gi);
            data.graphs.remove(&graph_name);
            delete_graph(&old_path)?;
        }

        Ok(true)
    }

    async fn update_graph_last_opened<'a>(
        ctx: &Context<'a>,
        graph_name: String,
        namespace: &Option<String>,
    ) -> Result<bool> {
        let data = ctx.data_unchecked::<Data>();
        let subgraph = data
            .get_graph(&graph_name, namespace)?
            .ok_or("Graph not found")?;

        #[cfg(feature = "storage")]
        if subgraph.clone().graph.into_disk_graph().is_some() {
            return Err(GqlGraphError::ImmutableDiskGraph.into());
        }

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
        data.graphs.insert(graph_name, subgraph);

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
        namespace: &Option<String>,
    ) -> Result<bool> {
        let data = ctx.data_unchecked::<Data>();

        // If graph_name == new_graph_name, it is a "save" action otherwise it is "save as" action.
        // Overwriting the same graph is permitted, not otherwise
        if graph_name.ne(&new_graph_name) {
            let new_graph_path = Path::new(&data.work_dir).join(&new_graph_name);
            if new_graph_path.exists() {
                return Err(GraphError::GraphNameAlreadyExists {
                    name: new_graph_name,
                }
                .into());
            }
        }

        let parent_graph = data
            .get_graph(&parent_graph_name, namespace)?
            .ok_or("Graph not found")?;
        let subgraph = data
            .get_graph(&graph_name, namespace)?
            .ok_or("Graph not found")?;

        #[cfg(feature = "storage")]
        if subgraph.clone().graph.into_disk_graph().is_some() {
            return Err(GqlGraphError::ImmutableDiskGraph.into());
        }

        let path = match data.get_graph(&new_graph_name, namespace)? {
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
                path.with_file_name(&new_graph_name)
                    .to_str()
                    .ok_or("Invalid path")?
                    .to_string()
            }
        };
        println!("Saving graph to path {path}");

        let deserialized_node_map: Value = serde_json::from_str(graph_nodes.as_str())?;
        let node_map = deserialized_node_map
            .as_object()
            .ok_or("graph_nodes not object")?;
        let node_ids = node_map.keys().map(|key| key.as_str()).collect_vec();

        let _new_subgraph = parent_graph.subgraph(node_ids.clone()).materialize()?;
        _new_subgraph.update_constant_properties([("name", Prop::str(new_graph_name.clone()))])?;

        let new_subgraph = &_new_subgraph.clone().into_persistent().unwrap();
        let new_subgraph_data = subgraph.subgraph(node_ids).materialize()?;

        // Copy nodes over
        let new_subgraph_nodes: Vec<_> = new_subgraph_data
            .clone()
            .into_persistent()
            .unwrap()
            .nodes()
            .collect();
        let nodeviews = new_subgraph_nodes.iter().map(|node| node).collect();
        new_subgraph.import_nodes(nodeviews, true)?;

        // Copy edges over
        let new_subgraph_edges: Vec<_> = new_subgraph_data
            .into_persistent()
            .unwrap()
            .edges()
            .collect();
        let edgeviews = new_subgraph_edges.iter().map(|edge| edge).collect();
        new_subgraph.import_edges(edgeviews, true)?;

        // If parent_graph_name == graph_name, it means that the graph is being created from UI
        if parent_graph_name.ne(&graph_name) {
            // If graph_name == new_graph_name, it is a "save" action otherwise it is "save as" action
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

        new_subgraph.save_to_file(path)?;

        let m_g = new_subgraph.materialize()?;
        let gi: IndexedGraph<MaterializedGraph> = m_g.into();

        data.graphs.insert(new_graph_name.clone(), gi);

        Ok(true)
    }

    /// Load graphs from a directory of bincode files (existing graphs with the same name are overwritten)
    ///
    /// Returns::
    ///   list of names for newly added graphs
    async fn load_graphs_from_path<'a>(
        ctx: &Context<'a>,
        path: String,
        namespace: &Option<String>,
        overwrite: bool,
    ) -> Result<Vec<String>> {
        let data = ctx.data_unchecked::<Data>();
        let names = load_graphs_from_path(data.work_dir.as_ref(), (&path).as_ref(), overwrite)?;
        names.iter().for_each(|name| data.graphs.invalidate(name));
        Ok(names)
    }

    /// Load graph from path
    ///
    /// Returns::
    ///   list of names for newly added graphs
    async fn load_graph_from_path<'a>(
        ctx: &Context<'a>,
        path: String,
        namespace: &Option<String>,
        overwrite: bool,
    ) -> Result<String> {
        let data = ctx.data_unchecked::<Data>();
        let name = load_graph_from_path(data.work_dir.as_ref(), (&path).as_ref(), overwrite)?;
        match name {
            Some(name) => {
                data.graphs.invalidate(&name);
                Ok(name)
            }
            None => Err(GqlGraphError::FailedToLoadGraph.into()),
        }
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
        let path = Path::new(data.work_dir.as_str()).join(name.as_str());
        if path.exists() && !overwrite {
            return Err(GraphError::GraphNameAlreadyExists { name }.into());
        }
        let mut buffer = Vec::new();
        let mut buff_read = graph.value(ctx)?.content;
        buff_read.read_to_end(&mut buffer)?;
        let g: MaterializedGraph = MaterializedGraph::from_bincode(&buffer)?;
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
        let g: MaterializedGraph = bincode::deserialize(&URL_SAFE_NO_PAD.decode(graph)?)?;
        g.save_to_file(&path)?;
        data.graphs.insert(name.clone(), g.into());
        Ok(name)
    }

    async fn archive_graph<'a>(
        ctx: &Context<'a>,
        graph_name: String,
        _parent_graph_name: String,
        namespace: &Option<String>,
        is_archive: u8,
    ) -> Result<bool> {
        let data = ctx.data_unchecked::<Data>();
        let subgraph = data
            .get_graph(&graph_name, namespace)?
            .ok_or("Graph not found")?;

        #[cfg(feature = "storage")]
        if subgraph.clone().graph.into_disk_graph().is_some() {
            return Err(GqlGraphError::ImmutableDiskGraph.into());
        }

        subgraph.update_constant_properties([("isArchive", Prop::U8(is_archive))])?;

        let path = subgraph
            .properties()
            .constant()
            .get("path")
            .ok_or("Path is missing")?
            .to_string();
        subgraph.save_to_file(path)?;

        data.graphs.insert(graph_name, subgraph);

        Ok(true)
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
