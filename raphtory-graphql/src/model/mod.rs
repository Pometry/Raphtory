use crate::{
    auth::{ContextValidation, Roles},
    auth_policy::{AuthorizationPolicy, NamespacePermission},
    data::{
        gql_error_with_code, parent_namespace, require_graph_write, Data, GqlGraphType,
        PermissionError, CODE_ACCESS_DENIED,
    },
    model::{
        graph::{
            collection::GqlCollection,
            graph::GqlGraph,
            meta_graph::MetaGraph,
            mutable_graph::GqlMutableGraph,
            namespace::{is_namespace_visible, Namespace},
            namespaced_item::NamespacedItem,
            node_id::GqlNodeId,
        },
        plugins::{
            mutation_plugin::MutationPlugin, query_plugin::QueryPlugin, PermissionsEntrypointMut,
            PermissionsEntrypointQuery,
        },
    },
    paths::{ExistingGraphFolder, ValidGraphPaths, ValidWriteableGraphFolder},
    rayon::{blocking_compute, blocking_write},
    url_encode::{url_decode_graph_at, url_encode_graph},
};
use async_graphql::Context;
use dynamic_graphql::{
    App, Mutation, MutationFields, MutationRoot, OneOfInput, ResolvedObject, ResolvedObjectFields,
    Result, Upload,
};
use itertools::Itertools;
use raphtory::{
    arrow_loader::df_loaders::edges::ColumnNames,
    db::{
        api::{
            storage::storage::{Extension, PersistenceStrategy},
            view::MaterializedGraph,
        },
        graph::views::deletion_graph::PersistentGraph,
    },
    errors::GraphError,
    io::parquet_loaders::{load_edges_from_parquet, load_nodes_from_parquet},
    prelude::*,
    version,
};
use raphtory_api::core::entities::properties::prop::PropType;
use std::{collections::HashMap, path::PathBuf, sync::Arc};
use tracing::{error, warn};

#[cfg(feature = "vectors")]
use crate::model::graph::vectorised_graph::VectorQuery;

pub(crate) mod algorithms;
pub mod graph;
pub mod plugins;
pub(crate) mod schema;
pub(crate) mod sorting;

pub(crate) fn parse_json_schema(
    json: Option<&str>,
) -> Result<Option<HashMap<String, PropType>>, GraphError> {
    let json = match json {
        None | Some("") => return Ok(None),
        Some(s) => s,
    };
    let map: HashMap<String, String> =
        serde_json::from_str(json).map_err(|e| GraphError::InvalidProperty {
            reason: format!("Invalid JSON schema: {e}"),
        })?;
    map.into_iter()
        .map(|(col, type_str)| {
            let prop_type =
                type_str
                    .parse::<PropType>()
                    .map_err(|e| GraphError::InvalidProperty {
                        reason: format!("Column '{col}': {e}"),
                    })?;
            Ok((col, prop_type))
        })
        .collect::<Result<HashMap<_, _>, _>>()
        .map(Some)
}

/// a thin wrapper around spawn_blocking that unwraps the join handle
pub(crate) async fn blocking_io<F, R>(f: F) -> R
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    tokio::task::spawn_blocking(f).await.unwrap()
}

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
    #[error("{0}")]
    LoadError(String),
}

/// Auto-grants Write on `path` for the creator's role after a graph is created.
/// Returns an error if the grant fails so the caller can roll back the graph.
/// No-op when there is no active auth policy; identity checks are delegated to the policy.
fn auto_grant_on_create(
    ctx: &Context<'_>,
    policy: &Option<Arc<dyn AuthorizationPolicy>>,
    path: &str,
) -> Result<()> {
    if let Some(policy) = policy {
        policy.on_graph_created(ctx, path)?;
    }
    Ok(())
}

fn require_namespace_write(
    ctx: &Context<'_>,
    policy: &Option<Arc<dyn AuthorizationPolicy>>,
    ns_path: &str,
    new_path: &str,
    operation: &str,
) -> Result<()> {
    match policy {
        None => ctx
            .require_jwt_write_access()
            .map_err(|e| gql_error_with_code(e.to_string(), CODE_ACCESS_DENIED)),
        Some(p) => {
            let ns_perm = p.namespace_permissions(ctx, ns_path).map_err(|e| {
                error!(
                    namespace = ns_path,
                    error = %e,
                    "Authorization policy could not resolve namespace permissions"
                );
                gql_error_with_code(e.to_string(), CODE_ACCESS_DENIED)
            })?;
            if ns_perm < Some(NamespacePermission::Write) {
                return Err(PermissionError::NamespaceWriteRequired {
                    namespace: ns_path.to_string(),
                    graph: new_path.to_string(),
                    operation: operation.to_string(),
                }
                .into_gql_error());
            }
            Ok(())
        }
    }
}
#[derive(ResolvedObject)]
#[graphql(root)]
pub struct QueryRoot;

#[derive(OneOfInput, Clone, Debug)]
pub enum Template {
    /// The default template.
    Enabled(bool),
    /// A custom template.
    Custom(String),
}

#[cfg_attr(not(feature = "vectors"), allow(dead_code))]
fn resolve(template: Option<Template>, default: &str) -> Option<String> {
    match template? {
        Template::Enabled(false) => None,
        Template::Enabled(true) => Some(default.to_owned()),
        Template::Custom(template) => Some(template),
    }
}

#[ResolvedObjectFields]
impl QueryRoot {
    /// Hello world demo
    pub async fn hello() -> &'static str {
        "Hello world from raphtory-graphql"
    }

    /// Returns a graph
    pub async fn graph<'a>(
        ctx: &Context<'a>,
        #[graphql(
            desc = "Graph path relative to the root namespace (e.g. `\"master\"` or `\"team/project/graph\"`)."
        )]
        path: &str,
        #[graphql(
            desc = "Optional override for graph semantics — `EVENT` treats every update as a point-in-time event, `PERSISTENT` carries values forward until overwritten or deleted. Defaults to the stored graph's native type."
        )]
        graph_type: Option<GqlGraphType>,
    ) -> Result<Option<GqlGraph>> {
        let data = ctx.data_unchecked::<Data>();
        // Ok(None) = permission denied (hides existence/access level); Err = load failed.
        let Some((folder, graph)) = data
            .get_graph_with_read_permission(ctx, path, graph_type)
            .await?
        else {
            return Ok(None);
        };
        Ok(Some(GqlGraph::new(folder, graph)))
    }

    /// Returns lightweight metadata for a graph (node/edge counts, timestamps) without loading it.
    /// Requires at least INTROSPECT permission.
    pub async fn graph_metadata<'a>(
        ctx: &Context<'a>,
        #[graphql(desc = "Graph path relative to the root namespace.")] path: String,
    ) -> Result<Option<MetaGraph>> {
        let data = ctx.data_unchecked::<Data>();

        if let Some(policy) = &data.auth_policy {
            match policy.graph_permissions(ctx, &path) {
                Ok(Some(_)) => {}
                Ok(None) => {
                    // Logged as `None` when the claims are absent rather than as an empty
                    // list, which would read as "denied, caller had no roles".
                    warn!(
                        roles = ?Roles::from_context(ctx).ok().map(Roles::as_slice),
                        graph = path.as_str(),
                        "Access denied by auth policy"
                    );
                    return Ok(None);
                }
                Err(e) => {
                    error!(
                        graph = path.as_str(),
                        error = %e,
                        "Authorization policy could not resolve graph permissions"
                    );
                    return Ok(None);
                }
            }
        }

        let work_dir = data.work_dir_read().await;
        let folder = ExistingGraphFolder::try_from(work_dir, &path)
            .map_err(|e| async_graphql::Error::new(e.to_string()))?;
        Ok(Some(MetaGraph::new(folder)))
    }

    /// Update graph query, has side effects to update graph state
    ///
    /// Returns:: GqlMutableGraph
    pub async fn update_graph<'a>(
        ctx: &Context<'a>,
        #[graphql(desc = "Graph path relative to the root namespace.")] path: String,
    ) -> Result<GqlMutableGraph> {
        let data = ctx.data_unchecked::<Data>();
        let graph = data
            .get_graph_with_write_permission(ctx, &path)
            .await?
            .into();

        Ok(graph)
    }

    /// Returns all namespaces using recursive search
    ///
    /// Returns::  List of namespaces on root
    pub async fn namespaces<'a>(ctx: &Context<'a>) -> Result<GqlCollection<Namespace>> {
        let data = ctx.data_unchecked::<Data>();
        let root = Namespace::root(data.work_dir_read().await);
        let all: Vec<Namespace> = blocking_compute(move || {
            root.self_and_all_children()
                .filter_map(|child| match child {
                    NamespacedItem::Namespace(item) => Some(item),
                    NamespacedItem::MetaGraph(_) => None,
                })
                .sorted()
                .collect()
        })
        .await;
        // Filter to namespaces the caller may see. A policy that cannot answer fails the
        // listing rather than shortening it: an omitted entry is indistinguishable from one
        // the caller simply has no grant on.
        let mut visible = Vec::new();
        for n in all {
            if is_namespace_visible(ctx, &data.auth_policy, &n)? {
                visible.push(n);
            }
        }
        Ok(GqlCollection::new(visible.into()))
    }

    /// Returns a specific namespace at a given path
    ///
    /// Returns:: Namespace or error if no namespace found
    pub async fn namespace<'a>(ctx: &Context<'a>, path: String) -> Result<Namespace> {
        let data = ctx.data_unchecked::<Data>();
        Ok(Namespace::try_new(data.work_dir_read().await, path)?)
    }

    /// Returns root namespace
    ///
    /// Returns::  Root namespace
    pub async fn root<'a>(ctx: &Context<'a>) -> Namespace {
        let data = ctx.data_unchecked::<Data>();
        Namespace::root(data.work_dir_read().await)
    }

    /// Returns a plugin.
    pub async fn plugins<'a>() -> QueryPlugin {
        QueryPlugin
    }

    /// Encodes graph and returns as string.
    ///
    /// Returns:: Base64 url safe encoded string
    pub async fn receive_graph<'a>(
        ctx: &Context<'a>,
        #[graphql(desc = "Graph path relative to the root namespace.")] path: String,
    ) -> Result<String> {
        let data = ctx.data_unchecked::<Data>();
        let (_, graph) = data.get_graph_requiring_read(ctx, &path, None).await?;
        let materialized = blocking_compute(move || graph.materialize())
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?;
        Ok(url_encode_graph(materialized)?)
    }

    /// Version string of the running `raphtory-graphql` server build.
    pub async fn version<'a>(_ctx: &Context<'a>) -> String {
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
    pub async fn plugins<'a>(_ctx: &Context<'a>) -> MutationPlugin {
        MutationPlugin
    }

    /// Delete graph from a path on the server.
    pub async fn delete_graph<'a>(
        ctx: &Context<'a>,
        #[graphql(desc = "Graph path relative to the root namespace.")] path: String,
    ) -> Result<bool> {
        let data = ctx.data_unchecked::<Data>();
        require_graph_write(ctx, &data.auth_policy, &path)?;
        let src_ns = parent_namespace(&path);
        require_namespace_write(ctx, &data.auth_policy, src_ns, &path, "delete")?;
        data.delete_graph(&path).await?;
        Ok(true)
    }

    /// Load nodes
    pub async fn load_nodes<'a>(
        ctx: &Context<'a>,
        #[graphql(desc = "Graph path relative to the root namespace.")] graph_path: String,
        #[graphql(desc = "Path to the parquet directory.")] data_path: String,
        #[graphql(desc = "The column name for the timestamps.")] time: String,
        #[graphql(desc = "The column name for the node IDs.")] id: String,
        #[graphql(
            desc = "A value to use as the node type for all nodes. Cannot be used in combination with node_type_col."
        )]
        node_type: Option<String>,
        #[graphql(
            desc = "The node type column name in a dataframe. Cannot be used in combination with node_type."
        )]
        node_type_col: Option<String>,
        #[graphql(desc = "List of node property column names.")] properties: Option<Vec<String>>,
        #[graphql(desc = "List of node metadata column names.")] metadata: Option<Vec<String>>,
        #[graphql(
            desc = "A JSON-formatted dict of {'column_name': column_type} to cast columns to. Defaults to None."
        )]
        schema: Option<String>,
        #[graphql(desc = "The column name for the secondary index.")] event_id: Option<String>,
        #[graphql(
            desc = "A value to use as the layer for all nodes. Cannot be used in combination with layer_col."
        )]
        layer: Option<String>,
        #[graphql(
            desc = "The node layer column name in a dataframe. Cannot be used in combination with layer."
        )]
        layer_col: Option<String>,
    ) -> Result<bool> {
        let data = ctx.data_unchecked::<Data>();
        // src: require WRITE on graph
        // require_graph_write(ctx, &data.auth_policy, graph_path)?;
        let graph = data
            .get_graph_with_write_permission(ctx, &graph_path)
            .await?
            .graph()
            .clone();
        // NOTE: skipping shared metadata for now until we figure out parsing of types
        let properties_owned = properties.unwrap_or_default();
        let properties: Vec<&str> = properties_owned.iter().map(String::as_str).collect();

        let metadata_owned = metadata.unwrap_or_default();
        let metadata: Vec<&str> = metadata_owned.iter().map(String::as_str).collect();

        let schema = parse_json_schema(schema.as_deref())?;

        // extracting PathBuf handles Strings too
        let data_path = PathBuf::from(data_path);

        data.is_parquet_path_allowed(&data_path)
            .await
            .map_err(|e| GqlGraphError::LoadError(e.to_string()))?;

        // wrap in Arc to avoid cloning the entire schema for inner loops
        let arced_schema = schema.map(Arc::new);

        load_nodes_from_parquet(
            &graph,
            &data_path,
            &time,
            event_id.as_deref(),
            &id,
            node_type.as_deref(),
            node_type_col.as_deref(),
            properties.as_slice(),
            metadata.as_slice(),
            None,
            layer.as_deref(),
            layer_col.as_deref(),
            None,
            None,
            true,
            arced_schema.clone(),
        )?;
        Ok(true)
    }

    /// Load edges
    pub async fn load_edges<'a>(
        ctx: &Context<'a>,
        #[graphql(desc = "Graph path relative to the root namespace.")] graph_path: String,
        #[graphql(desc = "Path to the parquet directory.")] data_path: String,
        #[graphql(desc = "The column name for the update timestamps.")] time: String,
        #[graphql(desc = "The column name for the source node IDs.")] src: String,
        #[graphql(desc = "The column name for the destination node IDs.")] dst: String,
        #[graphql(desc = "List of edge property column names. Defaults to None.")]
        properties: Option<Vec<String>>,
        #[graphql(desc = "List of edge metadata column names. Defaults to None.")] metadata: Option<
            Vec<String>,
        >,
        #[graphql(
            desc = "A JSON-formatted dict of {'column_name': column_type} to cast columns to. Defaults to None."
        )]
        schema: Option<String>,
        #[graphql(desc = "The column name for the secondary index.")] event_id: Option<String>,
        #[graphql(
            desc = "A value to use as the layer for all edges. Cannot be used in combination with layer_col. Defaults to None."
        )]
        layer: Option<String>,
        #[graphql(
            desc = "The edge layer column name in a dataframe. Cannot be used in combination with layer. Defaults to None."
        )]
        layer_col: Option<String>,
    ) -> Result<bool> {
        let data = ctx.data_unchecked::<Data>();
        // src: require WRITE on graph
        // require_graph_write(ctx, &data.auth_policy, graph_path)?;
        let graph = data
            .get_graph_with_write_permission(ctx, &graph_path)
            .await?
            .graph()
            .clone();
        // NOTE: skipping shared metadata for now until we figure out parsing of types
        let properties_owned = properties.unwrap_or_default();
        let properties: Vec<&str> = properties_owned.iter().map(String::as_str).collect();

        let metadata_owned = metadata.unwrap_or_default();
        let metadata: Vec<&str> = metadata_owned.iter().map(String::as_str).collect();

        let schema = parse_json_schema(schema.as_deref())?;

        // extracting PathBuf handles Strings too
        let data_path = PathBuf::from(data_path);

        data.is_parquet_path_allowed(&data_path)
            .await
            .map_err(|e| GqlGraphError::LoadError(e.to_string()))?;

        // wrap in Arc to avoid cloning the entire schema for inner loops
        let arced_schema = schema.map(Arc::new);

        load_edges_from_parquet(
            &graph,
            &data_path,
            ColumnNames::new(
                time.as_str(),
                event_id.as_deref(),
                src.as_str(),
                dst.as_str(),
                layer_col.as_deref(),
            ),
            true,
            properties.as_slice(),
            metadata.as_slice(),
            None,
            layer.as_deref(),
            None,
            arced_schema.clone(),
        )?;
        Ok(true)
    }

    /// Creates a new graph.
    pub async fn new_graph<'a>(
        ctx: &Context<'a>,
        #[graphql(desc = "Destination path relative to the root namespace.")] path: String,
        graph_type: GqlGraphType,
    ) -> Result<bool> {
        let data = ctx.data_unchecked::<Data>();
        let ns = parent_namespace(&path);
        require_namespace_write(ctx, &data.auth_policy, ns, &path, "create")?;
        let overwrite = false;
        let folder = data
            .work_dir_write()
            .await
            .validate_path_for_insert(&path, overwrite)?;
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
        if let Err(e) = auto_grant_on_create(ctx, &data.auth_policy, &path) {
            let _ = data.delete_graph(&path).await;
            return Err(e);
        }

        Ok(true)
    }

    /// Move graph from a path on the server to a new_path on the server.
    pub async fn move_graph<'a>(
        ctx: &Context<'a>,
        #[graphql(desc = "Current graph path relative to the root namespace.")] path: &str,
        #[graphql(desc = "Destination path relative to the root namespace.")] new_path: &str,
        #[graphql(
            desc = "If true, allow replacing an existing graph at `newPath`; defaults to false."
        )]
        overwrite: Option<bool>,
    ) -> Result<bool> {
        let data = ctx.data_unchecked::<Data>();
        // src: require WRITE on graph (moving = deleting source)
        require_graph_write(ctx, &data.auth_policy, path)?;
        // src: require WRITE on parent namespace (removing graph from namespace)
        let src_ns = parent_namespace(path);
        require_namespace_write(ctx, &data.auth_policy, src_ns, path, "move")?;
        // copy_graph handles dst namespace WRITE check (and src READ, which WRITE implies)
        if path != new_path {
            // moving with the same path should be a no-op, not delete the graph
            Self::copy_graph(ctx, path, new_path, overwrite).await?;
            data.delete_graph(path).await?;
        }
        Ok(true)
    }

    /// Copy graph from a path on the server to a new_path on the server.
    pub async fn copy_graph<'a>(
        ctx: &Context<'a>,
        #[graphql(desc = "Source graph path relative to the root namespace.")] path: &str,
        #[graphql(desc = "Destination path relative to the root namespace.")] new_path: &str,
        #[graphql(
            desc = "If true, allow replacing an existing graph at `newPath`; defaults to false."
        )]
        overwrite: Option<bool>,
    ) -> Result<bool> {
        let data = ctx.data_unchecked::<Data>();
        let dst_ns = parent_namespace(new_path);
        require_namespace_write(ctx, &data.auth_policy, dst_ns, new_path, "create")?;
        // doing this in a more efficient way is not trivial, this at least is correct
        // there are questions like, maybe the new vectorised graph have different rules
        // for the templates or if it needs to be vectorised at all
        let overwrite = overwrite.unwrap_or(false);
        let src = data.get_raw_graph_with_read_permission(ctx, path).await?;
        let graph = src.graph().clone();
        drop(src);
        let folder = data
            .work_dir_write()
            .await
            .validate_path_for_insert(new_path, overwrite)?;
        data.insert_graph(folder, graph).await?;
        if let Err(e) = auto_grant_on_create(ctx, &data.auth_policy, new_path) {
            let _ = data.delete_graph(new_path).await;
            return Err(e);
        }

        Ok(true)
    }

    /// Upload a graph file from a path on the client using GQL multipart uploading.
    ///
    /// Returns::
    /// name of the new graph
    pub async fn upload_graph<'a>(
        ctx: &Context<'a>,
        #[graphql(desc = "Destination path relative to the root namespace.")] path: String,
        #[graphql(desc = "Multipart upload of the serialised graph file.")] graph: Upload,
        #[graphql(desc = "If true, replace any graph already at `path`.")] overwrite: bool,
    ) -> Result<String> {
        let data = ctx.data_unchecked::<Data>();
        let dst_ns = parent_namespace(&path);
        require_namespace_write(ctx, &data.auth_policy, dst_ns, &path, "upload")?;
        let in_file = graph.value(ctx)?.content;
        let folder = data
            .work_dir_write()
            .await
            .validate_path_for_insert(&path, overwrite)?;
        data.insert_graph_as_bytes(folder, in_file).await?;
        if let Err(e) = auto_grant_on_create(ctx, &data.auth_policy, &path) {
            let _ = data.delete_graph(&path).await;
            return Err(e);
        }

        Ok(path)
    }

    /// Send graph bincode as base64 encoded string.
    ///
    /// Returns::
    /// path of the new graph
    pub async fn send_graph<'a>(
        ctx: &Context<'a>,
        #[graphql(desc = "Destination path relative to the root namespace.")] path: &str,
        #[graphql(desc = "Base64-encoded bincode of the serialised graph.")] graph: String,
        #[graphql(desc = "If true, replace any graph already at `path`.")] overwrite: bool,
    ) -> Result<String> {
        let data = ctx.data_unchecked::<Data>();
        let dst_ns = parent_namespace(path);
        require_namespace_write(ctx, &data.auth_policy, dst_ns, path, "send")?;
        let work_dir = data.work_dir_write().await;
        let folder = if overwrite {
            ValidWriteableGraphFolder::try_existing_or_new(work_dir, path)?
        } else {
            ValidWriteableGraphFolder::try_new(work_dir, path)?
        };
        let config = data.graph_conf.clone();
        let folder_clone = folder.clone();
        let g: MaterializedGraph = blocking_compute(move || {
            url_decode_graph_at(graph, folder_clone.graph_folder(), config)
        })
        .await?;
        data.insert_graph(folder, g).await?;
        if let Err(e) = auto_grant_on_create(ctx, &data.auth_policy, path) {
            let _ = data.delete_graph(path).await;
            return Err(e);
        }
        Ok(path.to_owned())
    }

    /// Create an empty namespace at `path`.
    ///
    /// Creates any missing parent namespaces along the way. Requires WRITE
    /// permission on the parent namespace. Rejects paths that already host a
    /// graph or an existing namespace, and paths that fail validation.
    ///
    /// Returns:: the path of the created namespace
    pub async fn create_namespace<'a>(
        ctx: &Context<'a>,
        #[graphql(desc = "Destination path relative to the root namespace.")] path: &str,
    ) -> Result<String> {
        let data = ctx.data_unchecked::<Data>();
        let ns = parent_namespace(path);
        require_namespace_write(ctx, &data.auth_policy, ns, path, "create")?;
        data.create_namespace(path).await?;
        Ok(path.to_string())
    }

    /// Delete a namespace and all of its descendants (graphs and sub-namespaces).
    ///
    /// Requires WRITE permission on the parent namespace, on the namespace
    /// itself, and on every descendant graph and sub-namespace. Cached graphs
    /// at any deleted path are invalidated. Rejects empty and non-existent
    /// paths.
    ///
    /// Returns:: true on success
    pub async fn delete_namespace<'a>(
        ctx: &Context<'a>,
        #[graphql(desc = "Path to delete relative to the root namespace.")] path: &str,
    ) -> Result<bool> {
        let data = ctx.data_unchecked::<Data>();
        let parent_ns = parent_namespace(path);
        require_namespace_write(ctx, &data.auth_policy, parent_ns, path, "delete")?;
        require_namespace_write(ctx, &data.auth_policy, path, path, "delete")?;

        let namespace = Namespace::try_new(data.work_dir_write().await.into(), path.to_string())?;
        let ns_clone = namespace.clone();
        let descendants: Vec<NamespacedItem> =
            blocking_compute(move || ns_clone.get_all_children().collect()).await;
        for item in &descendants {
            match item {
                NamespacedItem::Namespace(n) => {
                    require_namespace_write(
                        ctx,
                        &data.auth_policy,
                        n.relative_path(),
                        path,
                        "delete",
                    )?;
                }
                NamespacedItem::MetaGraph(g) => {
                    require_graph_write(ctx, &data.auth_policy, g.local_path())?;
                }
            }
        }

        data.delete_namespace(namespace, &descendants).await?;
        Ok(true)
    }

    /// Returns a subgraph given a set of nodes from an existing graph in the server.
    ///
    /// Returns::
    /// name of the new graph
    pub async fn create_subgraph<'a>(
        ctx: &Context<'a>,
        #[graphql(desc = "Source graph path relative to the root namespace.")] parent_path: &str,
        #[graphql(desc = "Node ids to include in the subgraph.")] nodes: Vec<GqlNodeId>,
        #[graphql(desc = "Destination path relative to the root namespace.")] new_path: String,
        #[graphql(desc = "If true, replace any graph already at `newPath`.")] overwrite: bool,
    ) -> Result<String> {
        let data = ctx.data_unchecked::<Data>();
        let dst_ns = parent_namespace(&new_path);
        require_namespace_write(ctx, &data.auth_policy, dst_ns, &new_path, "create")?;
        let folder = data
            .work_dir_write()
            .await
            .validate_path_for_insert(&new_path, overwrite)?;
        let (_, parent_graph) = data
            .get_graph_requiring_read(ctx, parent_path, None)
            .await?;
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
        new_subgraph.flush()?;

        data.insert_graph(folder, new_subgraph).await?;
        if let Err(e) = auto_grant_on_create(ctx, &data.auth_policy, &new_path) {
            let _ = data.delete_graph(&new_path).await;
            return Err(e);
        }
        Ok(new_path)
    }

    /// Flush any pending writes for the graph at `graphPath` to disk.
    pub async fn flush<'a>(
        ctx: &Context<'a>,
        #[graphql(desc = "Graph path relative to the root namespace.")] graph_path: String,
    ) -> Result<bool> {
        let data = ctx.data_unchecked::<Data>();
        let graph = data
            .get_graph_with_write_permission(ctx, &graph_path)
            .await?;
        blocking_write(move || {
            let res = graph.persist();
            if res.is_err() {
                graph.set_dirty(true);
            }
            res
        })
        .await?;
        Ok(true)
    }
}

#[derive(App)]
pub struct App(
    QueryRoot,
    MutRoot,
    #[cfg(feature = "vectors")] VectorQuery<'static>,
    Mut,
    PermissionsEntrypointMut,
    PermissionsEntrypointQuery,
);
