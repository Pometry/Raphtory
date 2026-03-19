use crate::{
    auth::{Access, AuthError, ContextValidation},
    auth_policy::GraphPermission,
    data::Data,
    model::{
        graph::{
            collection::GqlCollection,
            filtering::{GqlEdgeFilter, GqlGraphFilter, GqlNodeFilter, GraphAccessFilter},
            graph::GqlGraph,
            index::IndexSpecInput,
            meta_graph::MetaGraph,
            mutable_graph::GqlMutableGraph,
            namespace::Namespace,
            namespaced_item::NamespacedItem,
            vectorised_graph::GqlVectorisedGraph,
        },
        plugins::{
            mutation_plugin::MutationPlugin,
            permissions_plugin::{PermissionsPlugin, PermissionsQueryPlugin},
            query_plugin::QueryPlugin,
        },
    },
    paths::{ExistingGraphFolder, ValidGraphPaths, ValidWriteableGraphFolder},
    rayon::blocking_compute,
    url_encode::{url_decode_graph_at, url_encode_graph},
};
use async_graphql::Context;
use dynamic_graphql::{
    App, Enum, Mutation, MutationFields, MutationRoot, ResolvedObject, ResolvedObjectFields,
    Result, Upload,
};
use itertools::Itertools;
use raphtory::{
    db::{
        api::{
            storage::storage::{Extension, PersistenceStrategy},
            view::{DynamicGraph, Filter, IntoDynamic, MaterializedGraph},
        },
        graph::views::{deletion_graph::PersistentGraph, filter::model::NodeViewFilterOps},
    },
    errors::GraphError,
    prelude::*,
    version,
};
use std::{
    error::Error,
    fmt::{Display, Formatter},
};
use tracing::warn;

pub mod graph;
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

/// Applies a stored data filter (serialised as `serde_json::Value` with optional `node`, `edge`,
/// `graph` keys) to a `DynamicGraph`, returning a new filtered view.
async fn apply_graph_filter(
    mut graph: DynamicGraph,
    filter: GraphAccessFilter,
) -> async_graphql::Result<DynamicGraph> {
    use raphtory::db::graph::views::filter::model::{
        edge_filter::CompositeEdgeFilter, node_filter::CompositeNodeFilter, DynView,
    };

    if let Some(gql_filter) = filter.node {
        let raphtory_filter = CompositeNodeFilter::try_from(gql_filter)
            .map_err(|e| async_graphql::Error::new(format!("node filter conversion: {e}")))?;
        graph = blocking_compute({
            let g = graph.clone();
            move || g.filter(raphtory_filter)
        })
        .await
        .map_err(|e| async_graphql::Error::new(format!("node filter apply: {e}")))?
        .into_dynamic();
    }

    if let Some(gql_filter) = filter.edge {
        let raphtory_filter = CompositeEdgeFilter::try_from(gql_filter)
            .map_err(|e| async_graphql::Error::new(format!("edge filter conversion: {e}")))?;
        graph = blocking_compute({
            let g = graph.clone();
            move || g.filter(raphtory_filter)
        })
        .await
        .map_err(|e| async_graphql::Error::new(format!("edge filter apply: {e}")))?
        .into_dynamic();
    }

    if let Some(gql_filter) = filter.graph {
        let dyn_view = DynView::try_from(gql_filter)
            .map_err(|e| async_graphql::Error::new(format!("graph filter conversion: {e}")))?;
        graph = blocking_compute({
            let g = graph.clone();
            move || g.filter(dyn_view)
        })
        .await
        .map_err(|e| async_graphql::Error::new(format!("graph filter apply: {e}")))?
        .into_dynamic();
    }

    Ok(graph)
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
        let role = ctx.data::<Option<String>>().ok().and_then(|r| r.as_deref());
        let is_admin = ctx.data::<Access>().is_ok_and(|a| a == &Access::Rw);

        let perms = if let Some(policy) = &data.auth_policy {
            policy
                .graph_permissions(is_admin, role, path)
                .map_err(|msg| {
                    warn!(
                        role = role.unwrap_or("<no role>"),
                        graph = path,
                        "Access denied by auth policy"
                    );
                    async_graphql::Error::new(msg)
                })?
        } else {
            GraphPermission::Write // no policy: unrestricted
        };

        if matches!(perms, GraphPermission::Introspect) {
            return Err(async_graphql::Error::new(format!(
                "Access denied: role '{}' has introspect-only access to graph '{path}' — \
                 use namespace listings for graph metadata",
                role.unwrap_or("<no role>")
            )));
        }

        let graph_with_vecs = data.get_graph(path).await?;
        let graph: DynamicGraph = graph_with_vecs.graph.into_dynamic();

        let graph = if let GraphPermission::Read {
            filter: Some(ref f),
        } = perms
        {
            apply_graph_filter(graph, f.clone()).await?
        } else {
            graph
        };

        Ok(GqlGraph::new_with_permissions(
            graph_with_vecs.folder,
            graph,
            perms,
        ))
    }

    /// Returns lightweight metadata for a graph (node/edge counts, timestamps) without loading it.
    /// Requires at least INTROSPECT permission.
    async fn graph_metadata<'a>(ctx: &Context<'a>, path: String) -> Result<MetaGraph> {
        let data = ctx.data_unchecked::<Data>();
        let role = ctx.data::<Option<String>>().ok().and_then(|r| r.as_deref());
        let is_admin = ctx.data::<Access>().is_ok_and(|a| a == &Access::Rw);

        if let Some(policy) = &data.auth_policy {
            policy
                .graph_permissions(is_admin, role, &path)
                .map_err(|msg| {
                    warn!(
                        role = role.unwrap_or("<no role>"),
                        graph = path.as_str(),
                        "Access denied by auth policy"
                    );
                    async_graphql::Error::new(msg)
                })?;
        }

        let folder = ExistingGraphFolder::try_from(data.work_dir.clone(), &path)
            .map_err(|e| async_graphql::Error::new(e.to_string()))?;
        Ok(MetaGraph::new(folder))
    }

    /// Update graph query, has side effects to update graph state
    ///
    /// Returns:: GqlMutableGraph
    async fn update_graph<'a>(ctx: &Context<'a>, path: String) -> Result<GqlMutableGraph> {
        let data = ctx.data_unchecked::<Data>();
        let is_admin = ctx.data::<Access>().is_ok_and(|a| a == &Access::Rw);
        let role = ctx.data::<Option<String>>().ok().and_then(|r| r.as_deref());
        if !is_admin {
            match &data.auth_policy {
                None => ctx.require_write_access()?,
                Some(policy) => {
                    let perms = policy
                        .graph_permissions(false, role, &path)
                        .map_err(|msg| async_graphql::Error::new(msg))?;
                    if !matches!(perms, GraphPermission::Write) {
                        // role=Some: policy is active and grants less than Write.
                        // role=None: store is empty (no policy yet), fall back to JWT check.
                        return if let Some(role) = role {
                            Err(async_graphql::Error::new(format!(
                                "Access denied: role '{role}' does not have write permission for graph '{path}'"
                            )))
                        } else {
                            Err(AuthError::RequireWrite.into())
                        };
                    }
                }
            }
        }

        let graph = data.get_graph(path.as_ref()).await?.into();

        Ok(graph)
    }

    /// Create vectorised graph in the format used for queries
    ///
    /// Returns:: GqlVectorisedGraph
    async fn vectorised_graph<'a>(ctx: &Context<'a>, path: &str) -> Option<GqlVectorisedGraph> {
        let data = ctx.data_unchecked::<Data>();
        let g = data.get_graph(path).await.ok()?.vectors?;
        Some(g.into())
    }

    /// Returns all namespaces using recursive search
    ///
    /// Returns::  List of namespaces on root
    async fn namespaces<'a>(ctx: &Context<'a>) -> GqlCollection<Namespace> {
        let data = ctx.data_unchecked::<Data>();
        let root = Namespace::root(data.work_dir.clone());
        let list = blocking_compute(move || {
            root.get_all_children()
                .filter_map(|child| match child {
                    NamespacedItem::Namespace(item) => Some(item),
                    NamespacedItem::MetaGraph(_) => None,
                })
                .sorted()
                .collect()
        })
        .await;
        GqlCollection::new(list)
    }

    /// Returns a specific namespace at a given path
    ///
    /// Returns:: Namespace or error if no namespace found
    async fn namespace<'a>(ctx: &Context<'a>, path: String) -> Result<Namespace> {
        let data = ctx.data_unchecked::<Data>();
        Ok(Namespace::try_new(data.work_dir.clone(), path)?)
    }

    /// Returns root namespace
    ///
    /// Returns::  Root namespace
    async fn root<'a>(ctx: &Context<'a>) -> Namespace {
        let data = ctx.data_unchecked::<Data>();
        Namespace::root(data.work_dir.clone())
    }

    /// Returns a plugin.
    async fn plugins<'a>() -> QueryPlugin {
        QueryPlugin::default()
    }

    /// Encodes graph and returns as string
    ///
    /// Returns:: Base64 url safe encoded string
    async fn receive_graph<'a>(ctx: &Context<'a>, path: String) -> Result<String> {
        let path = path.as_ref();
        let data = ctx.data_unchecked::<Data>();
        let g = data.get_graph(path).await?.graph.clone();
        let res = url_encode_graph(g)?;
        Ok(res)
    }

    async fn version<'a>(_ctx: &Context<'a>) -> String {
        String::from(version())
    }

    /// Returns the permissions namespace for inspecting roles and access policies (admin only).
    async fn permissions<'a>(_ctx: &Context<'a>) -> PermissionsQueryPlugin {
        PermissionsQueryPlugin::default()
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

    /// Returns the permissions namespace for managing roles and access policies.
    async fn permissions<'a>(_ctx: &Context<'a>) -> PermissionsPlugin {
        PermissionsPlugin::default()
    }

    /// Delete graph from a path on the server.
    // If namespace is not provided, it will be set to the current working directory.
    async fn delete_graph<'a>(ctx: &Context<'a>, path: String) -> Result<bool> {
        ctx.require_write_access()?;
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
        let is_admin = ctx.data::<Access>().is_ok_and(|a| a == &Access::Rw);
        let role = ctx.data::<Option<String>>().ok().and_then(|r| r.as_deref());
        if !is_admin {
            match &data.auth_policy {
                None => ctx.require_write_access()?,
                Some(policy) => {
                    let perms = policy
                        .graph_permissions(false, role, &path)
                        .map_err(|msg| async_graphql::Error::new(msg))?;
                    if !matches!(perms, GraphPermission::Write) {
                        // role=Some: policy is active and grants less than Write.
                        // role=None: store is empty (no policy yet), fall back to JWT check.
                        return if let Some(role) = role {
                            Err(async_graphql::Error::new(format!(
                                "Access denied: role '{role}' does not have write permission for graph '{path}'"
                            )))
                        } else {
                            Err(AuthError::RequireWrite.into())
                        };
                    }
                }
            }
        }
        let overwrite = false;
        let folder = data.validate_path_for_insert(&path, overwrite)?;
        let graph_path = folder.graph_folder();
        let graph: MaterializedGraph = if Extension::disk_storage_enabled() {
            match graph_type {
                GqlGraphType::Persistent => PersistentGraph::new_at_path(graph_path)?.into(),
                GqlGraphType::Event => Graph::new_at_path(graph_path)?.into(),
            }
        } else {
            match graph_type {
                GqlGraphType::Persistent => PersistentGraph::new().into(),
                GqlGraphType::Event => Graph::new().into(),
            }
        };

        data.insert_graph(folder, graph).await?;

        Ok(true)
    }

    /// Move graph from a path on the server to a new_path on the server.
    async fn move_graph<'a>(
        ctx: &Context<'a>,
        path: &str,
        new_path: &str,
        overwrite: Option<bool>,
    ) -> Result<bool> {
        ctx.require_write_access()?;
        Self::copy_graph(ctx, path, new_path, overwrite).await?;
        let data = ctx.data_unchecked::<Data>();
        data.delete_graph(path).await?;
        Ok(true)
    }

    /// Copy graph from a path on the server to a new_path on the server.
    async fn copy_graph<'a>(
        ctx: &Context<'a>,
        path: &str,
        new_path: &str,
        overwrite: Option<bool>,
    ) -> Result<bool> {
        ctx.require_write_access()?;
        // doing this in a more efficient way is not trivial, this at least is correct
        // there are questions like, maybe the new vectorised graph have different rules
        // for the templates or if it needs to be vectorised at all
        let overwrite = overwrite.unwrap_or(false);
        let data = ctx.data_unchecked::<Data>();
        let graph = data.get_graph(path).await?.graph;
        let folder = data.validate_path_for_insert(new_path, overwrite)?;
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
        ctx.require_write_access()?;
        let data = ctx.data_unchecked::<Data>();
        let in_file = graph.value(ctx)?.content;
        let folder = data.validate_path_for_insert(&path, overwrite)?;
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
        ctx.require_write_access()?;
        let data = ctx.data_unchecked::<Data>();
        let folder = if overwrite {
            ValidWriteableGraphFolder::try_existing_or_new(data.work_dir.clone(), path)?
        } else {
            ValidWriteableGraphFolder::try_new(data.work_dir.clone(), path)?
        };
        let config = data.graph_conf.clone();
        let folder_clone = folder.clone();
        let g: MaterializedGraph = blocking_compute(move || {
            url_decode_graph_at(graph, folder_clone.graph_folder(), config)
        })
        .await?;
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
        ctx.require_write_access()?;
        let data = ctx.data_unchecked::<Data>();
        let folder = data.validate_path_for_insert(&new_path, overwrite)?;
        let parent_graph = data.get_graph(parent_path).await?.graph;
        let folder_clone = folder.clone();
        let new_subgraph = blocking_compute(move || {
            let subgraph = parent_graph.subgraph(nodes);
            if Extension::disk_storage_enabled() {
                subgraph.materialize_at(folder_clone.graph_folder())
            } else {
                subgraph.materialize()
            }
        })
        .await?;

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
        ctx.require_write_access()?;
        #[cfg(feature = "search")]
        {
            let data = ctx.data_unchecked::<Data>();
            let graph = data.get_graph(path).await?.graph;
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
