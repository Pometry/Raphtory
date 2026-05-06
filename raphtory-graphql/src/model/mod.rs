use crate::{
    auth::{AuthError, ContextValidation},
    auth_policy::{AuthPolicyError, AuthorizationPolicy, GraphPermission, NamespacePermission},
    data::Data,
    model::{
        graph::{
            collection::GqlCollection,
            filtering::{GqlEdgeFilter, GqlNodeFilter, GraphAccessFilter},
            graph::GqlGraph,
            index::IndexSpecInput,
            meta_graph::MetaGraph,
            mutable_graph::GqlMutableGraph,
            namespace::Namespace,
            namespaced_item::NamespacedItem,
            node_id::GqlNodeId,
            vectorised_graph::GqlVectorisedGraph,
        },
        plugins::{
            mutation_plugin::MutationPlugin, query_plugin::QueryPlugin, PermissionsEntrypointMut,
            PermissionsEntrypointQuery,
        },
    },
    paths::{ExistingGraphFolder, ValidGraphPaths, ValidWriteableGraphFolder},
    rayon::blocking_compute,
    url_encode::{url_decode_graph_at, url_encode_graph},
};
use async_graphql::Context;
use dynamic_graphql::{
    App, Enum, InputObject, Mutation, MutationFields, MutationRoot, OneOfInput, ResolvedObject,
    ResolvedObjectFields, Result, Upload,
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
    errors::{GraphError, GraphResult},
    prelude::*,
    vectors::{
        cache::CachedEmbeddingModel,
        storage::OpenAIEmbeddings,
        template::{DocumentTemplate, DEFAULT_EDGE_TEMPLATE, DEFAULT_NODE_TEMPLATE},
    },
    version,
};
use std::{
    error::Error,
    fmt::{Display, Formatter},
    future::Future,
    pin::Pin,
    sync::Arc,
};
use tracing::{error, warn};

pub mod graph;
pub mod plugins;
pub(crate) mod schema;
pub(crate) mod sorting;

#[derive(InputObject, Debug, Clone, Default)]
pub struct OpenAIConfig {
    model: String,
    api_base: Option<String>,
    api_key_env: Option<String>,
    org_id: Option<String>,
    project_id: Option<String>,
}

#[derive(OneOfInput, Clone, Debug)]
pub enum EmbeddingModel {
    /// OpenAI embedding models or compatible providers
    OpenAI(OpenAIConfig),
}

impl EmbeddingModel {
    async fn cache<'a>(self, ctx: &Context<'a>) -> GraphResult<CachedEmbeddingModel> {
        let data = ctx.data_unchecked::<Data>();
        match self {
            Self::OpenAI(OpenAIConfig {
                model,
                api_base,
                api_key_env,
                org_id,
                project_id,
            }) => {
                let embeddings = OpenAIEmbeddings {
                    model,
                    api_base,
                    api_key_env,
                    org_id,
                    project_id,
                    dim: None,
                };
                let vector_cache = data.vector_cache.resolve().await?;
                vector_cache.openai(embeddings.into()).await
            }
        }
    }
}

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

/// Checks that the caller has at least READ permission for the graph at `path`.
/// Returns the effective `GraphPermission` (including any stored filter) on success.
/// When denied and the caller has no INTROSPECT on the parent namespace, returns a
/// "Graph does not exist" error to avoid leaking that the graph is present.
fn require_at_least_read(
    ctx: &Context<'_>,
    policy: &Option<Arc<dyn AuthorizationPolicy>>,
    path: &str,
) -> async_graphql::Result<GraphPermission> {
    if let Some(policy) = policy {
        let role = ctx.data::<Option<String>>().ok().and_then(|r| r.as_deref());
        match policy.graph_permissions(ctx, path) {
            Err(msg) => {
                let ns = parent_namespace(path);
                if policy.namespace_permissions(ctx, ns) >= NamespacePermission::Introspect {
                    warn!(
                        role = role.unwrap_or("<no role>"),
                        graph = path,
                        "Access denied by auth policy"
                    );
                    return Err(msg.into());
                } else {
                    // Don't leak graph existence — act as if it doesn't exist.
                    return Err(async_graphql::Error::new(MissingGraph.to_string()));
                }
            }
            Ok(perm) => {
                if let Some(p) = perm.at_least_read() {
                    return Ok(p);
                } else {
                    warn!(
                        role = role.unwrap_or("<no role>"),
                        graph = path,
                        "Introspect-only access — graph() denied; use graphMetadata() instead"
                    );
                    return Err(async_graphql::Error::new(format!(
                        "Access denied: role '{}' has introspect-only access to graph '{path}' — \
                         use graphMetadata(path:) for counts and timestamps, or namespace listings to browse graphs",
                        role.unwrap_or("<no role>")
                    )));
                }
            }
        }
    }
    Ok(GraphPermission::Write)
}

/// Applies a stored data filter (serialised as `serde_json::Value` with optional `node`, `edge`,
/// `graph` keys) to a `DynamicGraph`, returning a new filtered view.
fn apply_graph_filter(
    mut graph: DynamicGraph,
    filter: GraphAccessFilter,
) -> Pin<Box<dyn Future<Output = async_graphql::Result<DynamicGraph>> + Send>> {
    Box::pin(async move {
        use raphtory::db::graph::views::filter::model::{
            edge_filter::CompositeEdgeFilter, node_filter::CompositeNodeFilter, DynView,
        };

        match filter {
            GraphAccessFilter::Node(gql_filter) => {
                let raphtory_filter = CompositeNodeFilter::try_from(gql_filter).map_err(|e| {
                    error!(error = %e, "node filter conversion failed");
                    async_graphql::Error::new("internal error applying access filter")
                })?;
                graph = blocking_compute({
                    let g = graph.clone();
                    move || g.filter(raphtory_filter)
                })
                .await
                .map_err(|e| {
                    error!(error = %e, "node filter apply failed");
                    async_graphql::Error::new("internal error applying access filter")
                })?
                .into_dynamic();
            }
            GraphAccessFilter::Edge(gql_filter) => {
                let raphtory_filter = CompositeEdgeFilter::try_from(gql_filter).map_err(|e| {
                    error!(error = %e, "edge filter conversion failed");
                    async_graphql::Error::new("internal error applying access filter")
                })?;
                graph = blocking_compute({
                    let g = graph.clone();
                    move || g.filter(raphtory_filter)
                })
                .await
                .map_err(|e| {
                    error!(error = %e, "edge filter apply failed");
                    async_graphql::Error::new("internal error applying access filter")
                })?
                .into_dynamic();
            }
            GraphAccessFilter::Graph(gql_filter) => {
                let dyn_view = DynView::try_from(gql_filter).map_err(|e| {
                    error!(error = %e, "graph filter conversion failed");
                    async_graphql::Error::new("internal error applying access filter")
                })?;
                graph = blocking_compute({
                    let g = graph.clone();
                    move || g.filter(dyn_view)
                })
                .await
                .map_err(|e| {
                    error!(error = %e, "graph filter apply failed");
                    async_graphql::Error::new("internal error applying access filter")
                })?
                .into_dynamic();
            }
            GraphAccessFilter::And(filters) => {
                for f in filters {
                    graph = apply_graph_filter(graph, f).await?;
                }
            }
            GraphAccessFilter::Or(filters) => {
                // Group same-type sub-filters and combine with native Or;
                // cross-type sub-filters are applied as independent restrictions.
                let mut node_fs: Vec<GqlNodeFilter> = vec![];
                let mut edge_fs: Vec<GqlEdgeFilter> = vec![];
                let mut rest: Vec<GraphAccessFilter> = vec![];
                for f in filters {
                    match f {
                        GraphAccessFilter::Node(n) => node_fs.push(n),
                        GraphAccessFilter::Edge(e) => edge_fs.push(e),
                        other => rest.push(other),
                    }
                }
                if !node_fs.is_empty() {
                    let combined = if node_fs.len() == 1 {
                        node_fs.pop().unwrap()
                    } else {
                        GqlNodeFilter::Or(node_fs)
                    };
                    graph = apply_graph_filter(graph, GraphAccessFilter::Node(combined)).await?;
                }
                if !edge_fs.is_empty() {
                    let combined = if edge_fs.len() == 1 {
                        edge_fs.pop().unwrap()
                    } else {
                        GqlEdgeFilter::Or(edge_fs)
                    };
                    graph = apply_graph_filter(graph, GraphAccessFilter::Edge(combined)).await?;
                }
                for f in rest {
                    graph = apply_graph_filter(graph, f).await?;
                }
            }
        }

        Ok(graph)
    })
}

/// Returns the namespace portion of a graph path: everything before the last `/`.
/// For top-level graphs (no `/`), returns `""` (the root namespace).
fn parent_namespace(path: &str) -> &str {
    path.rfind('/').map(|i| &path[..i]).unwrap_or("")
}

fn write_denied(role: Option<&str>, msg: impl std::fmt::Display) -> async_graphql::Error {
    match role {
        Some(_) => async_graphql::Error::new(msg.to_string()),
        None => AuthError::RequireWrite.into(),
    }
}

fn require_graph_write(
    ctx: &Context<'_>,
    policy: &Option<Arc<dyn AuthorizationPolicy>>,
    path: &str,
) -> async_graphql::Result<()> {
    match policy {
        None => ctx.require_jwt_write_access().map_err(Into::into),
        Some(p) => {
            let role = ctx.data::<Option<String>>().ok().and_then(|r| r.as_deref());
            p.graph_permissions(ctx, path)
                .map_err(async_graphql::Error::from)?
                .at_least_write()
                .ok_or_else(|| {
                    write_denied(
                        role,
                        format!("Access denied: WRITE permission required for graph '{path}'"),
                    )
                })?;
            Ok(())
        }
    }
}

fn require_namespace_write(
    ctx: &Context<'_>,
    policy: &Option<Arc<dyn AuthorizationPolicy>>,
    ns_path: &str,
    new_path: &str,
    operation: &str,
) -> async_graphql::Result<()> {
    match policy {
        None => ctx.require_jwt_write_access().map_err(Into::into),
        Some(p) => {
            let role = ctx.data::<Option<String>>().ok().and_then(|r| r.as_deref());
            if p.namespace_permissions(ctx, ns_path) < NamespacePermission::Write {
                return Err(write_denied(
                    role,
                    format!("Access denied: WRITE required on namespace '{ns_path}' to {operation} graph '{new_path}'"),
                ));
            }
            Ok(())
        }
    }
}

fn require_graph_read_src(
    ctx: &Context<'_>,
    policy: &Option<Arc<dyn AuthorizationPolicy>>,
    path: &str,
    operation: &str,
) -> async_graphql::Result<()> {
    match policy {
        None => ctx.require_jwt_write_access().map_err(Into::into),
        Some(p) => {
            let role = ctx.data::<Option<String>>().ok().and_then(|r| r.as_deref());
            p.graph_permissions(ctx, path)
                .map_err(async_graphql::Error::from)?
                .at_least_read()
                .ok_or_else(|| {
                    write_denied(
                        role,
                        format!(
                            "Access denied: READ required on source graph '{path}' to {operation}"
                        ),
                    )
                })?;
            Ok(())
        }
    }
}

/// Top-level READ-only query root. Entry points for loading a graph
/// (`graph`, `graphMetadata`), browsing stored graphs (`namespaces`,
/// `namespace`, `root`), downloading a stored graph as a base64 blob
/// (`receiveGraph`), inspecting vectorised variants (`vectorisedGraph`),
/// and a few utility endpoints (`version`, `hello`, `plugins`).
#[derive(ResolvedObject)]
#[graphql(root)]
pub(crate) struct QueryRoot;

#[derive(OneOfInput, Clone, Debug)]
pub enum Template {
    /// The default template.
    Enabled(bool),
    /// A custom template.
    Custom(String),
}

fn resolve(template: Option<Template>, default: &str) -> Option<String> {
    match template? {
        Template::Enabled(false) => None,
        Template::Enabled(true) => Some(default.to_owned()),
        Template::Custom(template) => Some(template),
    }
}

#[ResolvedObjectFields]
impl QueryRoot {
    /// Liveness check — returns a static "hello world" string. Useful for
    /// smoke-testing that the GraphQL server is reachable.
    async fn hello() -> &'static str {
        "Hello world from raphtory-graphql"
    }

    /// Load a graph by path. Returns null if the graph doesn't exist or is
    /// inaccessible. When a READ-scoped filter is attached to the caller's
    /// permissions, that filter is applied before the graph is returned.
    /// `graphType` lets you re-interpret the stored graph at query time —
    /// e.g. read an event-stored graph through persistent semantics. Defaults
    /// to the type the graph was created with.
    /// Requires READ on the graph.

    async fn graph<'a>(
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

        // Permission check: Err (denied or introspect-only) is converted to Ok(None) so the
        // user sees null — indistinguishable from "graph not found". Warnings are logged inside
        // require_at_least_read for cases where the user has namespace INTROSPECT visibility.
        let perms = match require_at_least_read(ctx, &data.auth_policy, path) {
            Ok(p) => p,
            Err(_) => return Ok(None),
        };

        let graph_with_vecs = data.get_graph(path).await?;
        let materialized = match graph_type {
            Some(GqlGraphType::Event) => match graph_with_vecs.graph {
                MaterializedGraph::EventGraph(g) => MaterializedGraph::EventGraph(g),
                MaterializedGraph::PersistentGraph(g) => {
                    MaterializedGraph::EventGraph(g.event_graph())
                }
            },
            Some(GqlGraphType::Persistent) => match graph_with_vecs.graph {
                MaterializedGraph::EventGraph(g) => {
                    MaterializedGraph::PersistentGraph(g.persistent_graph())
                }
                MaterializedGraph::PersistentGraph(g) => MaterializedGraph::PersistentGraph(g),
            },
            None => graph_with_vecs.graph,
        };
        let graph: DynamicGraph = materialized.into_dynamic();

        let graph = if let GraphPermission::Read {
            filter: Some(ref f),
        } = perms
        {
            apply_graph_filter(graph, f.clone()).await?
        } else {
            graph
        };

        Ok(Some(GqlGraph::new(graph_with_vecs.folder, graph)))
    }

    /// Returns lightweight metadata for a graph (node/edge counts,
    /// timestamps) without deserialising the full graph. Returns null if the
    /// graph doesn't exist or is inaccessible.
    /// Requires READ on the graph, or INTROSPECT on its parent namespace.

    async fn graph_metadata<'a>(
        ctx: &Context<'a>,
        #[graphql(desc = "Graph path relative to the root namespace.")] path: String,
    ) -> Result<Option<MetaGraph>> {
        let data = ctx.data_unchecked::<Data>();

        if let Some(policy) = &data.auth_policy {
            let role = ctx.data::<Option<String>>().ok().and_then(|r| r.as_deref());
            if let Err(_) = policy.graph_permissions(ctx, &path) {
                let ns = parent_namespace(&path);
                if policy.namespace_permissions(ctx, ns) >= NamespacePermission::Introspect {
                    warn!(
                        role = role.unwrap_or("<no role>"),
                        graph = path.as_str(),
                        "Access denied by auth policy"
                    );
                }
                // Always return null — permission denial is indistinguishable from "not found"
                // from the user's perspective. The warning above is the only signal in the logs.
                return Ok(None);
            }
        }

        let folder = ExistingGraphFolder::try_from(data.work_dir.clone(), &path)
            .map_err(|e| async_graphql::Error::new(e.to_string()))?;
        Ok(Some(MetaGraph::new(folder)))
    }

    /// Open a graph for writing — returns a `MutableGraph` handle that can
    /// add nodes/edges/properties/metadata.
    /// Requires WRITE on the graph.

    async fn update_graph<'a>(
        ctx: &Context<'a>,
        #[graphql(desc = "Graph path relative to the root namespace.")] path: String,
    ) -> Result<GqlMutableGraph> {
        let data = ctx.data_unchecked::<Data>();
        require_graph_write(ctx, &data.auth_policy, &path)?;

        let graph = data.get_graph(path.as_ref()).await?.into();

        Ok(graph)
    }

    /// Compute and persist embeddings for the nodes and edges of a stored
    /// graph so it can be queried via `vectorisedGraph`.
    /// Requires WRITE access.

    async fn vectorise_graph<'a>(
        ctx: &Context<'a>,
        #[graphql(desc = "Graph path relative to the root namespace.")] path: String,
        #[graphql(desc = "Optional embedding model; defaults to OpenAI's standard model.")]
        model: Option<EmbeddingModel>,
        #[graphql(
            desc = "Optional node-document template (which fields go into each node's text representation); defaults to the built-in template."
        )]
        nodes: Option<Template>,
        #[graphql(desc = "Optional edge-document template; defaults to the built-in template.")]
        edges: Option<Template>,
    ) -> Result<bool> {
        ctx.require_jwt_write_access()?;
        let data = ctx.data_unchecked::<Data>();
        let template = DocumentTemplate {
            node_template: resolve(nodes, DEFAULT_NODE_TEMPLATE),
            edge_template: resolve(edges, DEFAULT_EDGE_TEMPLATE),
        };
        let cached_model = model
            .unwrap_or(EmbeddingModel::OpenAI(Default::default()))
            .cache(ctx)
            .await?;
        let folder = ExistingGraphFolder::try_from(data.work_dir.clone(), &path)?;
        data.vectorise_folder(&folder, &template, cached_model)
            .await?;
        Ok(true)
    }

    /// Open a previously-vectorised graph for similarity queries. Returns null
    /// if the graph has no embeddings (call `vectoriseGraph` first) or is
    /// inaccessible.
    /// Requires READ on the graph.

    async fn vectorised_graph<'a>(
        ctx: &Context<'a>,
        #[graphql(desc = "Graph path relative to the root namespace.")] path: &str,
    ) -> Result<Option<GqlVectorisedGraph>> {
        let data = ctx.data_unchecked::<Data>();
        require_at_least_read(ctx, &data.auth_policy, path)?;
        Ok(data
            .get_graph(path)
            .await
            .ok()
            .and_then(|g| g.vectors)
            .map(|v| v.into()))
    }

    /// Recursively list every namespace under the root. Each namespace is
    /// filtered against the caller's permissions: only namespaces with at
    /// least DISCOVER are returned.
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

    /// Return a specific namespace by path. Errors if no namespace exists at
    /// that path.
    /// Requires INTROSPECT on the namespace to browse its contents.

    async fn namespace<'a>(
        ctx: &Context<'a>,
        #[graphql(
            desc = "Namespace path relative to the root namespace (e.g. `\"team/project\"`)."
        )]
        path: String,
    ) -> Result<Namespace> {
        let data = ctx.data_unchecked::<Data>();
        Ok(Namespace::try_new(data.work_dir.clone(), path)?)
    }

    /// Returns the root namespace. Use it as the entry point for browsing
    /// namespaces and graphs — child listings filter against the caller's
    /// permissions.
    async fn root<'a>(ctx: &Context<'a>) -> Namespace {
        let data = ctx.data_unchecked::<Data>();
        Namespace::root(data.work_dir.clone())
    }

    /// Entry point for READ-only plugins registered with the server (e.g. graph
    /// algorithms exposed as queries). Available plugins are defined at server
    /// startup via the plugin registry.
    async fn plugins<'a>() -> QueryPlugin {
        QueryPlugin::default()
    }

    /// Encode a stored graph as a base64 string for client-side download. If
    /// a READ-scoped filter is attached to the caller's permissions, only the
    /// materialised filtered view is encoded.
    /// Requires READ on the graph.

    async fn receive_graph<'a>(
        ctx: &Context<'a>,
        #[graphql(desc = "Graph path relative to the root namespace.")] path: String,
    ) -> Result<String> {
        let data = ctx.data_unchecked::<Data>();
        let perm = require_at_least_read(ctx, &data.auth_policy, &path)?;
        let raw = data.get_graph(&path).await?.graph;
        let res = if let GraphPermission::Read {
            filter: Some(ref f),
        } = perm
        {
            let filtered = apply_graph_filter(raw.into_dynamic(), f.clone()).await?;
            let materialized = blocking_compute(move || filtered.materialize())
                .await
                .map_err(|e| async_graphql::Error::new(e.to_string()))?;
            url_encode_graph(materialized)?
        } else {
            url_encode_graph(raw)?
        };
        Ok(res)
    }

    /// Version string of the running `raphtory-graphql` server build.
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

    /// Permanently delete a stored graph from the server.
    /// Requires WRITE on the graph and on its parent namespace.

    async fn delete_graph<'a>(
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

    /// Create a new empty graph at the given path. Errors if a graph already
    /// exists there.
    /// Requires WRITE on the parent namespace.

    async fn new_graph<'a>(
        ctx: &Context<'a>,
        #[graphql(desc = "Destination path relative to the root namespace.")] path: String,
        graph_type: GqlGraphType,
    ) -> Result<bool> {
        let data = ctx.data_unchecked::<Data>();
        let ns = parent_namespace(&path);
        require_namespace_write(ctx, &data.auth_policy, ns, &path, "create")?;
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

    /// Move a stored graph to a new path on the server (rename / relocate).
    /// Atomic: copies first, then deletes the source.
    /// Requires WRITE on the source graph and on both the source and
    /// destination namespaces.

    async fn move_graph<'a>(
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
        Self::copy_graph(ctx, path, new_path, overwrite).await?;
        data.delete_graph(path).await?;
        Ok(true)
    }

    /// Duplicate a stored graph to a new path on the server. Source is
    /// preserved.
    /// Requires READ on the source graph and WRITE on the destination namespace.

    async fn copy_graph<'a>(
        ctx: &Context<'a>,
        #[graphql(desc = "Source graph path relative to the root namespace.")] path: &str,
        #[graphql(desc = "Destination path relative to the root namespace.")] new_path: &str,
        #[graphql(
            desc = "If true, allow replacing an existing graph at `newPath`; defaults to false."
        )]
        overwrite: Option<bool>,
    ) -> Result<bool> {
        let data = ctx.data_unchecked::<Data>();
        require_graph_read_src(ctx, &data.auth_policy, path, "copy it")?;
        let dst_ns = parent_namespace(new_path);
        require_namespace_write(ctx, &data.auth_policy, dst_ns, new_path, "create")?;
        // doing this in a more efficient way is not trivial, this at least is correct
        // there are questions like, maybe the new vectorised graph have different rules
        // for the templates or if it needs to be vectorised at all
        let overwrite = overwrite.unwrap_or(false);
        let graph = data.get_graph(path).await?.graph;
        let folder = data.validate_path_for_insert(new_path, overwrite)?;
        data.insert_graph(folder, graph).await?;

        Ok(true)
    }

    /// Stream-upload a graph file using GraphQL multipart upload. The client
    /// sends the file directly; the server stores it under `path`.
    /// Requires WRITE on the destination namespace.

    async fn upload_graph<'a>(
        ctx: &Context<'a>,
        #[graphql(desc = "Destination path relative to the root namespace.")] path: String,
        #[graphql(desc = "Multipart upload of the serialised graph file.")] graph: Upload,
        #[graphql(desc = "If true, replace any graph already at `path`.")] overwrite: bool,
    ) -> Result<String> {
        let data = ctx.data_unchecked::<Data>();
        let dst_ns = parent_namespace(&path);
        require_namespace_write(ctx, &data.auth_policy, dst_ns, &path, "upload")?;
        let in_file = graph.value(ctx)?.content;
        let folder = data.validate_path_for_insert(&path, overwrite)?;
        data.insert_graph_as_bytes(folder, in_file).await?;

        Ok(path)
    }

    /// Send a serialised graph as a base64-encoded string in the request
    /// body. Use for smaller graphs where multipart upload is overkill.
    /// Requires WRITE on the destination namespace.

    async fn send_graph<'a>(
        ctx: &Context<'a>,
        #[graphql(desc = "Destination path relative to the root namespace.")] path: &str,
        #[graphql(desc = "Base64-encoded bincode of the serialised graph.")] graph: String,
        #[graphql(desc = "If true, replace any graph already at `path`.")] overwrite: bool,
    ) -> Result<String> {
        let data = ctx.data_unchecked::<Data>();
        let dst_ns = parent_namespace(path);
        require_namespace_write(ctx, &data.auth_policy, dst_ns, path, "send")?;
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

    /// Persist a subgraph of an existing stored graph as a new graph. The
    /// subgraph contains only the listed nodes and edges between them.
    /// Requires READ on the parent graph and WRITE on the destination namespace.

    async fn create_subgraph<'a>(
        ctx: &Context<'a>,
        #[graphql(desc = "Source graph path relative to the root namespace.")] parent_path: &str,
        #[graphql(desc = "Node ids to include in the subgraph.")] nodes: Vec<GqlNodeId>,
        #[graphql(desc = "Destination path relative to the root namespace.")] new_path: String,
        #[graphql(desc = "If true, replace any graph already at `newPath`.")] overwrite: bool,
    ) -> Result<String> {
        let data = ctx.data_unchecked::<Data>();
        require_graph_read_src(ctx, &data.auth_policy, parent_path, "create a subgraph")?;
        let dst_ns = parent_namespace(&new_path);
        require_namespace_write(ctx, &data.auth_policy, dst_ns, &new_path, "create")?;
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

    /// (Experimental) Build a Tantivy search index for a stored graph so it
    /// can be queried via `searchNodes` / `searchEdges`.
    /// Requires WRITE on the graph.

    async fn create_index<'a>(
        ctx: &Context<'a>,
        #[graphql(desc = "Graph path relative to the root namespace.")] path: &str,
        #[graphql(
            desc = "Optional spec selecting which node/edge property fields to index. Omit to index a default set."
        )]
        index_spec: Option<IndexSpecInput>,
        #[graphql(
            desc = "If true, build the index in memory (faster but lost on restart). If false, persist to disk."
        )]
        in_ram: bool,
    ) -> Result<bool> {
        let data = ctx.data_unchecked::<Data>();
        require_graph_write(ctx, &data.auth_policy, path)?;
        #[cfg(feature = "search")]
        {
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
pub struct App(
    QueryRoot,
    MutRoot,
    Mut,
    PermissionsEntrypointMut,
    PermissionsEntrypointQuery,
);
