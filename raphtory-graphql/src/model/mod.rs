use crate::{
    auth::ContextValidation,
    auth_policy::{AuthPolicyError, AuthorizationPolicy, GraphPermission, NamespacePermission},
    data::Data,
    graph::GraphWithVectors,
    model::{
        graph::{
            collection::GqlCollection,
            filtering::{GraphAccessFilter, GraphRowFilter, HiddenKeys},
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
        graph::views::{
            deletion_graph::PersistentGraph,
            filter::model::{ComposableFilter, DynFilter, DynView, NodeViewFilterOps},
            property_redacted_graph::PropertyRedaction,
        },
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
use std::sync::Arc;
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

#[derive(thiserror::Error, Debug)]
pub(crate) enum PermissionError {
    /// Graph exists but caller has no namespace visibility — hide graph existence.
    #[error("Graph does not exist")]
    GraphNotFound,
    /// Caller has introspect-only access; cannot read graph data.
    #[error(
        "Access denied: role '{role}' has introspect-only access to graph '{graph}' — \
         use graphMetadata(path:) for counts and timestamps, or namespace listings to browse graphs"
    )]
    IntrospectOnly { role: String, graph: String },
    /// Caller has read-only access but the operation requires write.
    #[error("Access denied: WRITE permission required for graph '{graph}'")]
    GraphWriteRequired { graph: String },
    /// Caller lacks write permission on the destination namespace.
    #[error(
        "Access denied: WRITE required on namespace '{namespace}' to {operation} graph '{graph}'"
    )]
    NamespaceWriteRequired {
        namespace: String,
        graph: String,
        operation: String,
    },
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
        return match policy.graph_permissions(ctx, path) {
            Err(msg) => {
                warn!(
                    role = role.unwrap_or("<no role>"),
                    graph = path,
                    "Access denied by auth policy"
                );
                let ns = parent_namespace(path);
                if policy.namespace_permissions(ctx, ns) >= NamespacePermission::Introspect {
                    Err(msg.into())
                } else {
                    // Don't leak graph existence — act as if it doesn't exist.
                    Err(PermissionError::GraphNotFound.into())
                }
            }
            Ok(perm) => {
                if let Some(p) = perm.at_least_read() {
                    Ok(p)
                } else {
                    warn!(
                        role = role.unwrap_or("<no role>"),
                        graph = path,
                        "Introspect-only access — graph() denied; use graphMetadata() instead"
                    );
                    Err(PermissionError::IntrospectOnly {
                        role: role.unwrap_or("<no role>").to_string(),
                        graph: path.to_string(),
                    }
                    .into())
                }
            }
        };
    }
    Ok(GraphPermission::Write)
}

/// Applies a `GraphRowFilter` to a `DynamicGraph`.
/// All filter work (conversion + application) runs inside a single `blocking_compute` call.
async fn apply_graph_filter(
    graph: DynamicGraph,
    row_filter: GraphRowFilter,
) -> async_graphql::Result<DynamicGraph> {
    blocking_compute(move || apply_row_filter_sync(graph, row_filter)).await
}

fn apply_row_filter_sync(
    graph: DynamicGraph,
    filter: GraphRowFilter,
) -> async_graphql::Result<DynamicGraph> {
    // And sub-filters are applied sequentially so that DynView (window/snapshot/layer)
    // sub-filters wrap the graph view before subsequent node/edge predicate filters run.
    // Parallel AndFilter composition loses this ordering because DynView.internal_filter_node
    // always returns true — the window only restricts time semantics, not node predicates.
    if let GraphRowFilter::And(filters) = filter {
        return filters
            .into_iter()
            .try_fold(graph, |g, f| apply_row_filter_sync(g, f));
    }
    let dyn_filter = DynFilter::try_from(filter).map_err(|e| {
        error!(error = %e, "filter conversion failed");
        async_graphql::Error::new("internal error applying access filter")
    })?;
    Ok(graph
        .filter(dyn_filter)
        .map_err(|e| {
            error!(error = %e, "failed to apply filter");
            async_graphql::Error::new("internal error applying access filter")
        })?
        .into_dynamic())
}

/// Extract per-entity hidden property sets from a `GraphAccessFilter`.
fn build_redaction(filter: &GraphAccessFilter) -> PropertyRedaction {
    let hp = filter.hidden_properties.as_ref();
    let hm = filter.hidden_metadata.as_ref();
    fn collect(
        keys: Option<&HiddenKeys>,
        pick: fn(&HiddenKeys) -> Option<&Vec<String>>,
    ) -> std::collections::HashSet<String> {
        keys.and_then(pick)
            .map(|v| v.iter().cloned().collect())
            .unwrap_or_default()
    }
    PropertyRedaction {
        node_hidden_props: collect(hp, |h| h.node.as_ref()),
        node_hidden_meta: collect(hm, |h| h.node.as_ref()),
        edge_hidden_props: collect(hp, |h| h.edge.as_ref()),
        edge_hidden_meta: collect(hm, |h| h.edge.as_ref()),
        graph_hidden_props: collect(hp, |h| h.graph.as_ref()),
        graph_hidden_meta: collect(hm, |h| h.graph.as_ref()),
    }
}

/// Applies the row filter and property redaction from a `GraphAccessFilter` to a graph.
/// Returns the same `DynamicGraph` unchanged when neither filter is set.
async fn apply_access_filter(
    graph: DynamicGraph,
    f: &GraphAccessFilter,
) -> async_graphql::Result<DynamicGraph> {
    let graph = if let Some(ref row_filter) = f.filter {
        apply_graph_filter(graph, row_filter.clone()).await?
    } else {
        graph
    };
    let redaction = build_redaction(f);
    if redaction.has_restrictions() {
        Ok(graph.exclude_properties(&redaction).into_dynamic())
    } else {
        Ok(graph)
    }
}

impl Data {
    /// Loads a graph and applies row filter + property redaction from the caller's access
    /// permission. Returns the graph folder (for metadata) and the filtered `DynamicGraph`.
    /// Permission errors propagate as `Err`; callers that want `None` on denial (e.g. `graph()`)
    /// should match on the result themselves.
    async fn get_graph_with_read_permission(
        &self,
        ctx: &Context<'_>,
        path: &str,
        graph_type: Option<GqlGraphType>,
    ) -> async_graphql::Result<(ExistingGraphFolder, DynamicGraph)> {
        let perm = require_at_least_read(ctx, &self.auth_policy, path)?;
        let gwv = self.get_graph(path).await?;
        let typed_graph = match graph_type {
            Some(GqlGraphType::Event) => match gwv.graph {
                MaterializedGraph::EventGraph(g) => MaterializedGraph::EventGraph(g),
                MaterializedGraph::PersistentGraph(g) => {
                    MaterializedGraph::EventGraph(g.event_graph())
                }
            },
            Some(GqlGraphType::Persistent) => match gwv.graph {
                MaterializedGraph::EventGraph(g) => {
                    MaterializedGraph::PersistentGraph(g.persistent_graph())
                }
                MaterializedGraph::PersistentGraph(g) => MaterializedGraph::PersistentGraph(g),
            },
            None => gwv.graph,
        };
        let raw = typed_graph.into_dynamic();
        let graph = if let GraphPermission::Read {
            filter: Some(ref f),
        } = perm
        {
            apply_access_filter(raw, f).await?
        } else {
            raw
        };
        Ok((gwv.folder, graph))
    }
    
    async fn get_raw_graph_with_read_permission(
        &self,
        ctx: &Context<'_>,
        path: &str,
    ) -> async_graphql::Result<GraphWithVectors> {
        require_at_least_read(ctx, &self.auth_policy, path)?;
        self.get_graph(path)
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))
    }

    /// Checks write permission then returns the raw `GraphWithVectors` for mutation operations.
    async fn get_graph_with_write_permission(
        &self,
        ctx: &Context<'_>,
        path: &str,
    ) -> async_graphql::Result<GraphWithVectors> {
        require_graph_write(ctx, &self.auth_policy, path)?;
        self.get_graph(path)
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))
    }

    /// Checks read permission then returns the vectorised graph for the given path, if any.
    /// Returns `None` for filtered-access users: embeddings are computed from the full graph
    /// and search results cannot be retroactively row-filtered.
    async fn get_vectors_with_read_permission(
        &self,
        ctx: &Context<'_>,
        path: &str,
    ) -> async_graphql::Result<Option<GqlVectorisedGraph>> {
        let perm = require_at_least_read(ctx, &self.auth_policy, path)?;
        if matches!(perm, GraphPermission::Read { filter: Some(_) }) {
            return Ok(None);
        }
        Ok(self
            .get_graph(path)
            .await
            .ok()
            .and_then(|g| g.vectors)
            .map(Into::into))
    }
}

/// Returns the namespace portion of a graph path: everything before the last `/`.
/// For top-level graphs (no `/`), returns `""` (the root namespace).
fn parent_namespace(path: &str) -> &str {
    path.rfind('/').map(|i| &path[..i]).unwrap_or("")
}

fn require_graph_write(
    ctx: &Context<'_>,
    policy: &Option<Arc<dyn AuthorizationPolicy>>,
    path: &str,
) -> async_graphql::Result<()> {
    match policy {
        None => ctx.require_jwt_write_access().map_err(Into::into),
        Some(p) => {
            p.graph_permissions(ctx, path)
                .map_err(async_graphql::Error::from)?
                .at_least_write()
                .ok_or_else(|| {
                    async_graphql::Error::from(PermissionError::GraphWriteRequired {
                        graph: path.to_string(),
                    })
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
            if p.namespace_permissions(ctx, ns_path) < NamespacePermission::Write {
                return Err(PermissionError::NamespaceWriteRequired {
                    namespace: ns_path.to_string(),
                    graph: new_path.to_string(),
                    operation: operation.to_string(),
                }
                .into());
            }
            Ok(())
        }
    }
}

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
    /// Hello world demo
    async fn hello() -> &'static str {
        "Hello world from raphtory-graphql"
    }

    /// Returns a graph

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
        // Permission denial → Ok(None): indistinguishable from "graph not found" for the caller.
        let (folder, graph) = match data
            .get_graph_with_read_permission(ctx, path, graph_type)
            .await
        {
            Ok(x) => x,
            Err(_) => return Ok(None),
        };
        Ok(Some(GqlGraph::new(folder, graph)))
    }

    /// Returns lightweight metadata for a graph (node/edge counts, timestamps) without loading it.
    /// Requires at least INTROSPECT permission.

    async fn graph_metadata<'a>(
        ctx: &Context<'a>,
        #[graphql(desc = "Graph path relative to the root namespace.")] path: String,
    ) -> Result<Option<MetaGraph>> {
        let data = ctx.data_unchecked::<Data>();

        if let Some(policy) = &data.auth_policy {
            let role = ctx.data::<Option<String>>().ok().and_then(|r| r.as_deref());
            if let Err(_) = policy.graph_permissions(ctx, &path) {
                warn!(
                    role = role.unwrap_or("<no role>"),
                    graph = path.as_str(),
                    "Access denied by auth policy"
                );
                return Ok(None);
            }
        }

        let folder = ExistingGraphFolder::try_from(data.work_dir.clone(), &path)
            .map_err(|e| async_graphql::Error::new(e.to_string()))?;
        Ok(Some(MetaGraph::new(folder)))
    }

    /// Update graph query, has side effects to update graph state
    ///
    /// Returns:: GqlMutableGraph

    async fn update_graph<'a>(
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

    /// Update graph query, has side effects to update graph state
    ///
    /// Returns:: GqlMutableGraph

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

    /// Create vectorised graph in the format used for queries
    ///
    /// Returns:: GqlVectorisedGraph

    async fn vectorised_graph<'a>(
        ctx: &Context<'a>,
        #[graphql(desc = "Graph path relative to the root namespace.")] path: &str,
    ) -> Result<Option<GqlVectorisedGraph>> {
        let data = ctx.data_unchecked::<Data>();
        data.get_vectors_with_read_permission(ctx, path).await
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

    /// Encodes graph and returns as string.
    ///
    /// Returns:: Base64 url safe encoded string

    async fn receive_graph<'a>(
        ctx: &Context<'a>,
        #[graphql(desc = "Graph path relative to the root namespace.")] path: String,
    ) -> Result<String> {
        let data = ctx.data_unchecked::<Data>();
        let (_, graph) = data
            .get_graph_with_read_permission(ctx, &path, None)
            .await?;
        let materialized = blocking_compute(move || graph.materialize())
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))?;
        Ok(url_encode_graph(materialized)?)
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

    /// Delete graph from a path on the server.

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

    /// Creates a new graph.

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

    /// Move graph from a path on the server to a new_path on the server.

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

    /// Copy graph from a path on the server to a new_path on the server.

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
        let dst_ns = parent_namespace(new_path);
        require_namespace_write(ctx, &data.auth_policy, dst_ns, new_path, "create")?;
        // doing this in a more efficient way is not trivial, this at least is correct
        // there are questions like, maybe the new vectorised graph have different rules
        // for the templates or if it needs to be vectorised at all
        let overwrite = overwrite.unwrap_or(false);
        let src = data.get_raw_graph_with_read_permission(ctx, path).await?;
        let folder = data.validate_path_for_insert(new_path, overwrite)?;
        data.insert_graph(folder, src.graph).await?;

        Ok(true)
    }

    /// Upload a graph file from a path on the client using GQL multipart uploading.
    ///
    /// Returns::
    /// name of the new graph

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

    /// Send graph bincode as base64 encoded string.
    ///
    /// Returns::
    /// path of the new graph

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

    /// Returns a subgraph given a set of nodes from an existing graph in the server.
    ///
    /// Returns::
    /// name of the new graph

    async fn create_subgraph<'a>(
        ctx: &Context<'a>,
        #[graphql(desc = "Source graph path relative to the root namespace.")] parent_path: &str,
        #[graphql(desc = "Node ids to include in the subgraph.")] nodes: Vec<GqlNodeId>,
        #[graphql(desc = "Destination path relative to the root namespace.")] new_path: String,
        #[graphql(desc = "If true, replace any graph already at `newPath`.")] overwrite: bool,
    ) -> Result<String> {
        let data = ctx.data_unchecked::<Data>();
        let dst_ns = parent_namespace(&new_path);
        require_namespace_write(ctx, &data.auth_policy, dst_ns, &new_path, "create")?;
        let folder = data.validate_path_for_insert(&new_path, overwrite)?;
        let (_, parent_graph) = data
            .get_graph_with_read_permission(ctx, parent_path, None)
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

        data.insert_graph(folder, new_subgraph).await?;
        Ok(new_path)
    }

    /// (Experimental) Creates search index.

    async fn create_index<'a>(
        ctx: &Context<'a>,
        #[graphql(desc = "Graph path relative to the root namespace.")] path: &str,
        #[graphql(
            desc = "Optional spec selecting which node/edge property fields to index. Omit to index a default set."
        )]
        index_spec: Option<IndexSpecInput>,
        in_ram: bool,
    ) -> Result<bool> {
        let data = ctx.data_unchecked::<Data>();
        #[cfg(feature = "search")]
        {
            let graph = data.get_graph_with_write_permission(ctx, path).await?.graph;
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
