use crate::{
    auth::ContextValidation,
    auth_policy::{AuthorizationPolicy, GraphPermission, NamespacePermission},
    config::app_config::AppConfig,
    graph::GraphWithVectors,
    model::{
        blocking_io,
        graph::{
            filtering::{GraphAccessFilter, GraphRowFilter, HiddenKeys},
            namespace::Namespace,
            namespaced_item::NamespacedItem,
            vectorised_graph::GqlVectorisedGraph,
        },
    },
    paths::{
        mark_dirty, ExistingGraphFolder, InternalPathValidationError, PathValidationError,
        ValidGraphPaths, ValidWriteableGraphFolder,
    },
    rayon::blocking_compute,
    GQLError,
};
use async_graphql::Context;
use dynamic_graphql::Enum;
use futures_util::FutureExt;
use moka::future::Cache;
use raphtory::{
    db::{
        api::{
            storage::storage::Config,
            view::{DynamicGraph, Filter, GraphViewOps, IntoDynamic, MaterializedGraph},
        },
        graph::views::{filter::model::DynFilter, property_redacted_graph::PropertyRedaction},
    },
    errors::GraphError,
    serialise::GraphPaths,
    vectors::{
        cache::CachedEmbeddingModel, storage::LazyDiskVectorCache, template::DocumentTemplate,
        vectorisable::Vectorisable, vectorised_graph::VectorisedGraph,
    },
};
use std::{
    fs, io,
    io::{Read, Seek},
    ops::Deref,
    path::{Path, PathBuf},
    sync::Arc,
};
use tracing::{error, warn};
use walkdir::WalkDir;

pub const DIRTY_PATH: &'static str = ".dirty";

#[derive(thiserror::Error, Debug)]
pub enum MutationErrorInner {
    #[error(transparent)]
    GraphError(#[from] GraphError),
    #[error(transparent)]
    IO(#[from] io::Error),
    #[error(transparent)]
    InvalidInternal(#[from] InternalPathValidationError),
}

#[derive(thiserror::Error, Debug)]
pub enum InsertionError {
    #[error("Failed to insert graph {graph}: {error}")]
    Insertion {
        graph: String,
        error: MutationErrorInner,
    },
    #[error(transparent)]
    PathValidation(#[from] PathValidationError),
    #[error("Failed to insert graph {graph}: {error}")]
    GraphError { graph: String, error: GraphError },
}

impl InsertionError {
    pub fn from_inner(graph: &str, error: MutationErrorInner) -> Self {
        InsertionError::Insertion {
            graph: graph.to_string(),
            error,
        }
    }

    pub fn from_graph_err(graph: &str, error: GraphError) -> Self {
        InsertionError::GraphError {
            graph: graph.to_string(),
            error,
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum DeletionError {
    #[error("Failed to delete graph {graph}: {error}")]
    Insertion {
        graph: String,
        error: MutationErrorInner,
    },
    #[error(transparent)]
    PathValidation(#[from] PathValidationError),
}

#[derive(thiserror::Error, Debug)]
pub enum MoveError {
    #[error("Failed to move graph: {0}")]
    Insertion(#[from] InsertionError),
    #[error("Failed to move graph: {0}")]
    Deletion(#[from] DeletionError),
}

impl DeletionError {
    fn from_inner(graph: &str, error: MutationErrorInner) -> Self {
        DeletionError::Insertion {
            graph: graph.to_string(),
            error,
        }
    }
}

/// Get relative path as String joined with `"/"` for use with the validation methods.
/// The path is not validated here!
pub(crate) fn get_relative_path(
    work_dir: &Path,
    path: &Path,
) -> Result<String, InternalPathValidationError> {
    let relative = path.strip_prefix(work_dir)?;
    let mut path_str = String::new();
    let mut components = relative.components().map(|component| {
        component
            .as_os_str()
            .to_str()
            .ok_or(InternalPathValidationError::NonUTFCharacters)
    });
    if let Some(first) = components.next() {
        path_str.push_str(first?);
    }
    for component in components {
        path_str.push('/');
        path_str.push_str(component?);
    }
    Ok(path_str)
}

/// Inner struct with a drop implementation that cleans up the graphs
pub struct DataInner {
    pub(crate) work_dir: PathBuf,
    pub(crate) cache: Cache<String, GraphWithVectors>,
    pub(crate) vector_cache: LazyDiskVectorCache,
    pub(crate) graph_conf: Config,
    pub(crate) auth_policy: Option<Arc<dyn AuthorizationPolicy>>,
}

/// Outer data struct that wraps the inner data to make sure it is only dropped once
#[derive(Clone)]
pub struct Data {
    inner: Arc<DataInner>,
    pub(crate) create_index: bool,
}

impl Deref for Data {
    type Target = DataInner;

    fn deref(&self) -> &Self::Target {
        self.inner.deref()
    }
}

impl Data {
    pub fn new(work_dir: &Path, configs: &AppConfig, graph_conf: Config) -> Self {
        let cache_configs = &configs.cache;

        let cache = Cache::<String, GraphWithVectors>::builder()
            .max_capacity(cache_configs.capacity)
            .time_to_idle(std::time::Duration::from_secs(cache_configs.tti_seconds))
            .async_eviction_listener(|_, graph, cause| {
                // The eviction listener gets called any time a graph is removed from the cache,
                // not just when it is evicted. Only serialize on evictions.
                async move {
                    if !cause.was_evicted() {
                        return;
                    }
                    if let Err(e) =
                        blocking_compute(move || graph.folder.replace_graph_data(graph.graph)).await
                    {
                        error!("Error encoding graph to disk on eviction: {e}");
                    }
                }
                .boxed()
            })
            .build();

        #[cfg(feature = "search")]
        let create_index = configs.index.create_index;
        #[cfg(not(feature = "search"))]
        let create_index = false;

        // TODO: make vector feature optional?

        Self {
            inner: Arc::new(DataInner {
                work_dir: work_dir.to_path_buf(),
                cache,
                vector_cache: LazyDiskVectorCache::new(work_dir.join(".vector-cache")),
                graph_conf,
                auth_policy: None,
            }),
            create_index,
        }
    }

    pub(crate) fn set_auth_policy(&mut self, policy: Arc<dyn AuthorizationPolicy>) {
        Arc::get_mut(&mut self.inner)
            .expect("Data is not uniquely owned when setting auth_policy")
            .auth_policy = Some(policy);
    }

    async fn invalidate(&self, path: &str) {
        self.cache.invalidate(path).await;
        self.cache.run_pending_tasks().await; // make sure the item is actually dropped
    }

    pub fn validate_path_for_insert(
        &self,
        path: &str,
        overwrite: bool,
    ) -> Result<ValidWriteableGraphFolder, PathValidationError> {
        if overwrite {
            ValidWriteableGraphFolder::try_existing_or_new(self.work_dir.clone(), path)
        } else {
            ValidWriteableGraphFolder::try_new(self.work_dir.clone(), path)
        }
    }

    /// # ⚠ Bypasses all permission checks — do not call from resolvers directly.
    /// Use `get_graph_with_read_permission`, `get_raw_graph_with_read_permission`, or
    /// `get_graph_with_write_permission` instead.
    async fn get_graph(&self, path: &str) -> Result<GraphWithVectors, Arc<GQLError>> {
        self.cache
            .try_get_with(path.into(), self.read_graph_from_disk(path))
            .await
    }

    /// Test-only: direct graph load without permission checks.
    #[cfg(test)]
    pub(crate) async fn get_graph_for_test(
        &self,
        path: &str,
    ) -> Result<GraphWithVectors, Arc<GQLError>> {
        self.get_graph(path).await
    }

    pub async fn get_cached_graph(&self, path: &str) -> Option<GraphWithVectors> {
        self.cache.get(path).await
    }

    pub fn has_graph(&self, path: &str) -> bool {
        self.cache.contains_key(path)
            || ExistingGraphFolder::try_from(self.work_dir.clone(), path).is_ok()
    }

    pub async fn insert_graph(
        &self,
        writeable_folder: ValidWriteableGraphFolder,
        graph: MaterializedGraph,
    ) -> Result<(), InsertionError> {
        self.invalidate(writeable_folder.local_path()).await;
        let config = self.graph_conf.clone();
        let graph = blocking_compute(move || {
            writeable_folder.write_graph_data(graph.clone(), config)?;
            let folder = writeable_folder.finish()?;
            let graph = GraphWithVectors::new(graph, None, folder.as_existing()?);
            Ok::<_, InsertionError>(graph)
        })
        .await?;
        self.cache
            .insert(graph.folder.local_path().into(), graph)
            .await;
        // moka's `insert(..).await` is eventually consistent — the entry is
        // queued and may not be visible to `cache.get(..)` immediately. Force
        // the pending insert through so a follow-up `MetaGraph.metadata`
        // hitting the listing path sees the cached graph instead of falling
        // through to `read_constant_graph_properties`, which would read the
        // on-disk graph_props before the writer has flushed them.
        self.cache.run_pending_tasks().await;
        Ok(())
    }

    /// Insert a graph serialized from a graph folder.
    pub async fn insert_graph_as_bytes<R: Read + Seek + Send + 'static>(
        &self,
        folder: ValidWriteableGraphFolder,
        bytes: R,
    ) -> Result<(), InsertionError> {
        self.invalidate(folder.local_path()).await;
        let conf = self.graph_conf.clone();
        blocking_io(move || {
            folder.write_graph_bytes(bytes, conf)?;
            folder.finish()
        })
        .await?;
        Ok(())
    }

    async fn delete_graph_inner(
        &self,
        graph_folder: ExistingGraphFolder,
    ) -> Result<(), MutationErrorInner> {
        let dirty_file = mark_dirty(graph_folder.root())?;
        self.invalidate(graph_folder.local_path()).await;
        blocking_io(move || {
            fs::remove_dir_all(graph_folder.root())?;
            fs::remove_file(dirty_file)?;
            Ok::<_, MutationErrorInner>(())
        })
        .await?;
        Ok(())
    }

    pub async fn delete_graph(&self, path: &str) -> Result<(), DeletionError> {
        let graph_folder = ExistingGraphFolder::try_from(self.work_dir.clone(), path)?;
        self.delete_graph_inner(graph_folder)
            .await
            .map_err(|err| DeletionError::from_inner(path, err))?;
        self.cache.remove(path).await;
        Ok(())
    }

    pub async fn delete_namespace(&self, path: &str) -> Result<(), DeletionError> {
        if path.is_empty() {
            return Err(DeletionError::PathValidation(
                PathValidationError::NamespaceDoesNotExist(path.to_string()),
            ));
        }
        let namespace = Namespace::try_new(self.work_dir.clone(), path.to_string())?;
        for item in namespace.get_all_children() {
            if let NamespacedItem::MetaGraph(g) = item {
                self.invalidate(g.local_path()).await;
                self.cache.remove(g.local_path()).await;
            }
        }
        let root = namespace.current_dir().to_path_buf();
        let dirty_file = mark_dirty(&root).map_err(|err| {
            DeletionError::from_inner(path, MutationErrorInner::InvalidInternal(err))
        })?;
        blocking_io(move || {
            fs::remove_dir_all(&root)?;
            fs::remove_file(dirty_file)?;
            Ok::<_, MutationErrorInner>(())
        })
        .await
        .map_err(|err| DeletionError::from_inner(path, err))?;
        Ok(())
    }

    pub async fn create_namespace(&self, path: &str) -> Result<(), InsertionError> {
        let target = crate::paths::validate_path_for_namespace_create(
            self.work_dir.clone(),
            path,
        )?;
        blocking_io(move || {
            fs::create_dir_all(&target)?;
            Ok::<_, MutationErrorInner>(())
        })
        .await
        .map_err(|err| InsertionError::from_inner(path, err))?;
        Ok(())
    }

    async fn vectorise_with_template(
        &self,
        graph: MaterializedGraph,
        folder: &impl ValidGraphPaths,
        template: &DocumentTemplate,
        model: CachedEmbeddingModel,
    ) -> Option<VectorisedGraph<MaterializedGraph>> {
        let vectors = graph
            .vectorise(
                model,
                template.clone(),
                Some(&folder.graph_folder().vectors_path().ok()?),
                true, // verbose
            )
            .await;
        match vectors {
            Ok(vectors) => Some(vectors),
            Err(error) => {
                let name = folder.local_path();
                warn!("An error occurred when trying to vectorise graph {name}: {error}");
                None
            }
        }
    }

    pub(crate) async fn vectorise_folder(
        &self,
        folder: &ExistingGraphFolder,
        template: &DocumentTemplate,
        model: CachedEmbeddingModel,
    ) -> Result<(), GQLError> {
        let graph = match self.get_cached_graph(folder.local_path()).await {
            None => self.read_graph_from_disk_inner(folder.clone()).await?,
            Some(graph) => graph,
        };
        self.vectorise_with_template(graph.graph, folder, template, model)
            .await;
        self.cache.remove(folder.local_path()).await;
        Ok(())
    }

    pub fn get_all_graph_folders(&self) -> impl Iterator<Item = ExistingGraphFolder> {
        let base_path = self.work_dir.clone();
        WalkDir::new(&self.work_dir)
            .into_iter()
            .filter_map(move |e| {
                let entry = e.ok()?;
                let path = entry.path();
                let relative = get_relative_path(&base_path, path).ok()?;
                let folder = ExistingGraphFolder::try_from(base_path.clone(), &relative).ok()?;
                Some(folder)
            })
    }

    async fn read_graph_from_disk_inner(
        &self,
        folder: ExistingGraphFolder,
    ) -> Result<GraphWithVectors, GraphError> {
        let create_index = self.create_index;
        let config = self.graph_conf.clone();
        let cache = self.vector_cache.clone();
        GraphWithVectors::read_from_folder(&folder, &cache, create_index, config).await
    }

    async fn read_graph_from_disk(&self, path: &str) -> Result<GraphWithVectors, GQLError> {
        let folder = ExistingGraphFolder::try_from(self.work_dir.clone(), path)?;
        Ok(self.read_graph_from_disk_inner(folder).await?)
    }
}

// ---------------------------------------------------------------------------
// Permission types and helpers
// ---------------------------------------------------------------------------

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

#[derive(Enum)]
#[graphql(name = "GraphType")]
pub(crate) enum GqlGraphType {
    /// Persistent.
    Persistent,
    /// Event.
    Event,
}

/// Returns the namespace portion of a graph path: everything before the last `/`.
/// For top-level graphs (no `/`), returns `""` (the root namespace).
pub(crate) fn parent_namespace(path: &str) -> &str {
    path.rfind('/').map(|i| &path[..i]).unwrap_or("")
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

pub(crate) fn require_graph_write(
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

/// Applies a `GraphRowFilter` to a `DynamicGraph`.
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

// ---------------------------------------------------------------------------
// Data permission methods
// ---------------------------------------------------------------------------

impl Data {
    /// Loads and filters the graph using an already-verified permission. Private shared core.
    async fn load_and_filter(
        &self,
        path: &str,
        perm: GraphPermission,
        graph_type: Option<GqlGraphType>,
    ) -> async_graphql::Result<(ExistingGraphFolder, DynamicGraph)> {
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

    /// For the `graph()` resolver: permission denial → `Ok(None)` (null to client, hides
    /// existence and access level). Load failure → `Err` (graph was deleted, etc.).
    pub(crate) async fn get_graph_with_read_permission(
        &self,
        ctx: &Context<'_>,
        path: &str,
        graph_type: Option<GqlGraphType>,
    ) -> async_graphql::Result<Option<(ExistingGraphFolder, DynamicGraph)>> {
        match require_at_least_read(ctx, &self.auth_policy, path) {
            Ok(perm) => self.load_and_filter(path, perm, graph_type).await.map(Some),
            Err(_) => Ok(None),
        }
    }

    /// For resolvers that must surface the specific denial reason (`receive_graph`,
    /// `create_subgraph`). A caller with no namespace visibility gets "does not exist"
    /// (hides graph existence). A caller who can already see the graph in listings gets
    /// "Access denied" (they already know it's there; now they know they need READ).
    pub(crate) async fn get_graph_requiring_read(
        &self,
        ctx: &Context<'_>,
        path: &str,
        graph_type: Option<GqlGraphType>,
    ) -> async_graphql::Result<(ExistingGraphFolder, DynamicGraph)> {
        let perm = require_at_least_read(ctx, &self.auth_policy, path)?;
        self.load_and_filter(path, perm, graph_type).await
    }

    /// Checks read permission then returns the raw `GraphWithVectors` (unfiltered).
    /// Use for copy/move operations where the caller needs the raw storage handle —
    /// O(1) Arc clone, no materialization. Row filters are intentionally not applied:
    /// the destination will have its own access controls.
    pub(crate) async fn get_raw_graph_with_read_permission(
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
    pub(crate) async fn get_graph_with_write_permission(
        &self,
        ctx: &Context<'_>,
        path: &str,
    ) -> async_graphql::Result<GraphWithVectors> {
        require_graph_write(ctx, &self.auth_policy, path)?;
        self.get_graph(path)
            .await
            .map_err(|e| async_graphql::Error::new(e.to_string()))
    }

    /// Checks read permission then returns the vectorised graph, if any.
    /// Returns `None` for filtered-access users: embeddings are computed from the full graph
    /// and search results cannot be retroactively row-filtered.
    pub(crate) async fn get_vectors_with_read_permission(
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

impl Drop for DataInner {
    fn drop(&mut self) {
        // On drop, serialize graphs that don't have underlying storage.
        for (_, graph) in self.cache.iter() {
            if graph.is_dirty() {
                if let Err(e) = graph.folder.replace_graph_data(graph.graph) {
                    error!("Error encoding graph to disk on drop: {e}");
                }
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod data_tests {
    use super::InsertionError;
    use crate::{config::app_config::AppConfigBuilder, data::Data};
    use itertools::Itertools;
    use raphtory::{
        db::api::view::{internal::InternalStorageOps, MaterializedGraph},
        prelude::*,
        serialise::GraphPaths,
    };
    use std::{collections::HashMap, fs, path::Path, time::Duration};
    use tokio::time::sleep;

    fn create_graph_folder(path: &Path) {
        // Use empty graph to create folder structure
        fs::create_dir_all(path).unwrap();
        let graph = Graph::new();
        graph.encode(path).unwrap();
    }

    pub(crate) async fn save_graphs_to_work_dir(
        data: &Data,
        graphs: &HashMap<String, MaterializedGraph>,
    ) -> Result<(), InsertionError> {
        for (name, graph) in graphs.into_iter() {
            let folder = data.validate_path_for_insert(name, true)?;
            data.insert_graph(folder, graph.clone()).await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_save_graphs_to_work_dir() {
        let tmp_work_dir = tempfile::tempdir().unwrap();

        let graph = Graph::new();
        graph.add_metadata([("name", "test_g")]).unwrap();
        graph
            .add_edge(0, 1, 2, [("name", "test_e1")], None)
            .unwrap();
        graph
            .add_edge(0, 1, 3, [("name", "test_e2")], None)
            .unwrap();

        let graph: MaterializedGraph = graph.into();

        let mut graphs = HashMap::new();

        graphs.insert("test_g".to_string(), graph);
        let data = Data::new(tmp_work_dir.path(), &Default::default(), Default::default());

        save_graphs_to_work_dir(&data, &graphs).await.unwrap();

        for graph in graphs.keys() {
            assert!(data.get_graph(graph).await.is_ok(), "could not get {graph}")
        }
    }

    #[tokio::test]
    async fn test_eviction() {
        let tmp_work_dir = tempfile::tempdir().unwrap();

        let graph = Graph::new();
        graph
            .add_edge(0, 1, 2, [("name", "test_e1")], None)
            .unwrap();
        graph
            .add_edge(0, 1, 3, [("name", "test_e2")], None)
            .unwrap();

        graph.encode(&tmp_work_dir.path().join("test_g")).unwrap();
        graph.encode(&tmp_work_dir.path().join("test_g2")).unwrap();

        let configs = AppConfigBuilder::new()
            .with_cache_capacity(1)
            .with_cache_tti_seconds(2)
            .build();

        let data = Data::new(tmp_work_dir.path(), &configs, Default::default());

        assert!(!data.cache.contains_key("test_g"));
        assert!(!data.cache.contains_key("test_g2"));

        // Test size based eviction
        data.get_graph("test_g2").await.unwrap();
        assert!(data.cache.contains_key("test_g2"));
        assert!(!data.cache.contains_key("test_g"));

        data.get_graph("test_g").await.unwrap(); // wait for any eviction
        data.cache.run_pending_tasks().await;
        assert_eq!(data.cache.iter().count(), 1);

        sleep(Duration::from_secs(3)).await;
        assert!(!data.cache.contains_key("test_g"));
        assert!(!data.cache.contains_key("test_g2"));
        // FIXME: this test is not doing anything because calling cache.contains_key() runs
        // any pending evictions. To actually test it we need this assertion:
        //   assert_eq!(data.cache.entry_count(), 0);
        // Which currently does not work because the server task to trigger evictions is not running
        // in this context. The problem is if we do run it by creating a server and calling
        // server.start() the server gets consumed and we loose access to the cache to be able to run
        // the check. If rework the server implementation and this becomes feasible we should change
        // this test
    }

    #[tokio::test]
    async fn test_get_graph_paths() {
        let temp_dir = tempfile::tempdir().unwrap();
        let work_dir = temp_dir.path();

        let g0_path = work_dir.join("g0");
        let g1_path = work_dir.join("g1");
        let g2_path = work_dir.join("shivam/investigations/2024-12-22/g2");
        let g3_path = work_dir.join("shivam/investigations/g3.with.dots"); // Graph
        let g4_path = work_dir.join("shivam/investigations/g4"); // Disk graph dir
        let g5_path = work_dir.join("shivam/investigations/g5"); // Empty dir
        let g6_path = work_dir.join("shivam/investigations/g6"); // File that is not a graph
        let g7_path = work_dir.join(".graph"); // Invalid hidden path

        create_graph_folder(&g0_path);
        create_graph_folder(&g1_path);
        create_graph_folder(&g2_path);
        create_graph_folder(&g3_path);
        create_graph_folder(&g4_path);
        create_graph_folder(&g7_path);

        // Empty, non-graph folder
        fs::create_dir_all(&g5_path).unwrap();

        // Simulate non-graph folder with random files
        fs::create_dir_all(&g6_path).unwrap();
        fs::write(g6_path.join("random-file"), "some-random-content").unwrap();

        let configs = AppConfigBuilder::new()
            .with_cache_capacity(1)
            .with_cache_tti_seconds(2)
            .build();

        let data = Data::new(work_dir, &configs, Default::default());

        let paths = data
            .get_all_graph_folders()
            .into_iter()
            .map(|folder| folder.0.root().to_path_buf())
            .collect_vec();

        assert_eq!(paths.len(), 5);
        assert!(paths.contains(&g0_path));
        assert!(paths.contains(&g1_path));
        assert!(paths.contains(&g2_path));
        assert!(paths.contains(&g3_path));
        assert!(paths.contains(&g4_path));
        assert!(!paths.contains(&g5_path)); // Empty folder is ignored
        assert!(!paths.contains(&g6_path)); // Non-graph folder is ignored
        assert!(!paths.contains(&g7_path)); // Hidden path is ignored

        assert!(data
            .get_graph("shivam/investigations/2024-12-22/g2")
            .await
            .is_ok());

        assert!(data.get_graph("some/random/path").await.is_err());
        assert!(data.get_graph(".graph").await.is_err());
    }

    #[tokio::test]
    async fn test_drop_skips_write_when_graph_is_not_dirty() {
        let tmp_work_dir = tempfile::tempdir().unwrap();

        // Create two graphs and save them to disk
        let graph1 = Graph::new();
        graph1
            .add_edge(0, 1, 2, [("name", "test_e1")], None)
            .unwrap();
        graph1
            .add_edge(0, 1, 3, [("name", "test_e2")], None)
            .unwrap();

        let graph2 = Graph::new();
        graph2
            .add_edge(0, 2, 3, [("name", "test_e3")], None)
            .unwrap();
        graph2
            .add_edge(0, 2, 4, [("name", "test_e4")], None)
            .unwrap();

        let graph1_path = tmp_work_dir.path().join("test_graph1");
        let graph2_path = tmp_work_dir.path().join("test_graph2");
        graph1.encode(&graph1_path).unwrap();
        graph2.encode(&graph2_path).unwrap();

        // Record modification times before any operations
        let graph1_metadata = fs::metadata(&graph1_path).unwrap();
        let graph2_metadata = fs::metadata(&graph2_path).unwrap();
        let graph1_original_time = graph1_metadata.modified().unwrap();
        let graph2_original_time = graph2_metadata.modified().unwrap();

        let configs = AppConfigBuilder::new()
            .with_cache_capacity(10)
            .with_cache_tti_seconds(300)
            .build();

        let data = Data::new(tmp_work_dir.path(), &configs, Default::default());

        let loaded_graph1 = data.get_graph("test_graph1").await.unwrap();
        let loaded_graph2 = data.get_graph("test_graph2").await.unwrap();

        // TODO: This test doesn't work with disk storage right now, make sure modification dates actually update correctly!
        if loaded_graph1.graph.disk_storage_path().is_some() {
            assert!(
                !loaded_graph1.is_dirty(),
                "Graph1 should not be dirty when loaded from disk"
            );
            assert!(
                !loaded_graph2.is_dirty(),
                "Graph2 should not be dirty when loaded from disk"
            );

            // Modify only graph1 to make it dirty
            loaded_graph1.set_dirty(true);
            assert!(
                loaded_graph1.is_dirty(),
                "Graph1 should be dirty after modification"
            );

            // Drop the Data instance - this should trigger serialization
            drop(data);

            // Check modification times after drop
            let graph1_metadata_after = fs::metadata(&graph1_path).unwrap();
            let graph2_metadata_after = fs::metadata(&graph2_path).unwrap();
            let graph1_modified_time = graph1_metadata_after.modified().unwrap();
            let graph2_modified_time = graph2_metadata_after.modified().unwrap();

            // Graph1 (dirty) modification time should be different
            assert_ne!(
                graph1_original_time, graph1_modified_time,
                "Graph1 (dirty) should have been written to disk on drop"
            );

            // Graph2 (not dirty) modification time should be the same
            assert_eq!(
                graph2_original_time, graph2_modified_time,
                "Graph2 (not dirty) should not have been written to disk on drop"
            );
        }
    }

    #[tokio::test]
    async fn test_eviction_skips_write_when_graph_is_not_dirty() {
        let tmp_work_dir = tempfile::tempdir().unwrap();

        // Create two graphs and save them to disk
        let graph1 = Graph::new();
        graph1
            .add_edge(0, 1, 2, [("name", "test_e1")], None)
            .unwrap();
        graph1
            .add_edge(0, 1, 3, [("name", "test_e2")], None)
            .unwrap();

        let graph2 = Graph::new();
        graph2
            .add_edge(0, 2, 3, [("name", "test_e3")], None)
            .unwrap();
        graph2
            .add_edge(0, 2, 4, [("name", "test_e4")], None)
            .unwrap();

        let graph1_path = tmp_work_dir.path().join("test_graph1");
        let graph2_path = tmp_work_dir.path().join("test_graph2");
        graph1.encode(&graph1_path).unwrap();
        graph2.encode(&graph2_path).unwrap();

        // Record modification times before any operations
        let graph1_metadata = fs::metadata(&graph1_path).unwrap();
        let graph2_metadata = fs::metadata(&graph2_path).unwrap();
        let graph1_original_time = graph1_metadata.modified().unwrap();
        let graph2_original_time = graph2_metadata.modified().unwrap();

        // Create cache with time to idle 3 seconds to force eviction
        let configs = AppConfigBuilder::new()
            .with_cache_capacity(10)
            .with_cache_tti_seconds(3)
            .build();

        let data = Data::new(tmp_work_dir.path(), &configs, Default::default());

        // Load first graph
        let loaded_graph1 = data.get_graph("test_graph1").await.unwrap();
        assert!(
            !loaded_graph1.is_dirty(),
            "Graph1 should not be dirty when loaded from disk"
        );

        // Modify graph1 to make it dirty
        loaded_graph1.set_dirty(true);
        assert!(
            loaded_graph1.is_dirty(),
            "Graph1 should be dirty after modification"
        );

        // Load second graph
        println!("Loading second graph");
        let loaded_graph2 = data.get_graph("test_graph2").await.unwrap();
        assert!(
            !loaded_graph2.is_dirty(),
            "Graph2 should not be dirty when loaded from disk"
        );

        // Sleep to trigger eviction
        sleep(Duration::from_secs(3)).await;
        data.cache.run_pending_tasks().await;

        // TODO: This test doesn't work with disk storage right now, make sure modification dates actually update correctly!
        if loaded_graph1.graph.disk_storage_path().is_some() {
            // Check modification times after eviction
            let graph1_metadata_after = fs::metadata(&graph1_path).unwrap();
            let graph2_metadata_after = fs::metadata(&graph2_path).unwrap();
            let graph1_modified_time = graph1_metadata_after.modified().unwrap();
            let graph2_modified_time = graph2_metadata_after.modified().unwrap();

            // Graph1 (dirty) modification time should be different
            assert_ne!(
                graph1_original_time, graph1_modified_time,
                "Graph1 (dirty) should have been written to disk on eviction"
            );

            // Graph2 (not dirty) modification time should be the same
            assert_eq!(
                graph2_original_time, graph2_modified_time,
                "Graph2 (not dirty) should not have been written to disk on eviction"
            );
        }
    }
}
