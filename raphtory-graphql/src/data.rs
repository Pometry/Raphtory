use crate::{
    auth::ContextValidation,
    auth_policy::{AuthorizationPolicy, GraphPermission, PermissionLevel},
    cache::GraphCache,
    config::app_config::AppConfig,
    graph::GraphWithVectors,
    model::{
        blocking_io,
        graph::{
            filtering::{GqlFilter, GraphAccessFilter, HiddenKeys},
            namespace::Namespace,
            namespaced_item::NamespacedItem,
        },
    },
    paths::{
        mark_dirty, ExistingGraphFolder, InternalPathValidationError, PathValidationError,
        UnlockedGraphFolder, ValidGraphPaths, ValidWriteableGraphFolder,
    },
    rayon::blocking_compute,
    GQLError,
};
use async_graphql::{Context, ErrorExtensions};
use dynamic_graphql::Enum;
use raphtory::{
    db::{
        api::{
            storage::storage::Config,
            view::{DynamicGraph, Filter, GraphViewOps, IntoDynamic, MaterializedGraph},
        },
        graph::views::{filter::model::DynFilter, property_redacted_graph::PropertyRedaction},
    },
    errors::GraphError,
    prelude::AdditionOps,
};
use raphtory_api::core::storage::graph_folder::GraphPaths;
use std::{
    cmp::Ordering,
    fs, io,
    io::{Read, Seek},
    ops::Deref,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
};
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};
use tracing::{debug, error, warn};
use walkdir::WalkDir;

#[cfg(feature = "vectors")]
use {
    crate::model::graph::vectorised_graph::GqlVectorisedGraph,
    raphtory::vectors::{
        cache::CachedEmbeddingModel, storage::LazyDiskVectorCache, template::DocumentTemplate,
        vectorisable::Vectorisable, vectorised_graph::VectorisedGraph,
    },
};

#[derive(thiserror::Error, Debug)]
pub enum ParquetPathError {
    #[error("Path {0:?} does not exist or could not be resolved")]
    Unresolvable(PathBuf),
    #[error("Path {0:?} is not allowed: paths within the working directory are not permitted")]
    WithinWorkDir(PathBuf),
    #[error("Path {0:?} is not in the list of allowed paths")]
    NotAllowed(PathBuf),
}

#[derive(thiserror::Error, Debug)]
pub enum MutationErrorInner {
    #[error(transparent)]
    GraphError(#[from] GraphError),
    #[error(transparent)]
    IO(#[from] io::Error),
    #[error(transparent)]
    InvalidInternal(#[from] InternalPathValidationError),
    #[error("Cache operation failed, simultaneous mutation occurred")]
    CacheReplacementError,
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
    work_dir: Arc<RwLock<PathBuf>>,
    pub(crate) cache: GraphCache,
    #[cfg(feature = "vectors")]
    pub(crate) vector_cache: LazyDiskVectorCache,
    pub(crate) graph_conf: Config,
    pub(crate) auth_policy: Option<Arc<dyn AuthorizationPolicy>>,
    pub(crate) allowed_parquet_paths: Vec<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct WorkDirWriteGuard {
    guard: Arc<OwnedRwLockWriteGuard<PathBuf>>,
}

impl WorkDirWriteGuard {
    pub fn path(&self) -> &Path {
        &self.guard
    }

    pub fn to_path_buf(&self) -> PathBuf {
        self.path().to_path_buf()
    }

    pub fn validate_path_for_insert(
        self,
        path: &str,
        overwrite: bool,
    ) -> Result<ValidWriteableGraphFolder, PathValidationError> {
        if overwrite {
            ValidWriteableGraphFolder::try_existing_or_new(self, path)
        } else {
            ValidWriteableGraphFolder::try_new(self, path)
        }
    }
}

impl PartialEq for WorkDirWriteGuard {
    fn eq(&self, other: &Self) -> bool {
        self.path() == other.path()
    }
}

impl Eq for WorkDirWriteGuard {}

impl PartialOrd for WorkDirWriteGuard {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.path().partial_cmp(other.path())
    }
}

impl Ord for WorkDirWriteGuard {
    fn cmp(&self, other: &Self) -> Ordering {
        self.path().cmp(other.path())
    }
}

#[derive(Debug, Clone)]
pub enum WorkDirGuard {
    Read {
        guard: Arc<OwnedRwLockReadGuard<PathBuf>>,
    },
    Write(WorkDirWriteGuard),
}

impl From<WorkDirWriteGuard> for WorkDirGuard {
    fn from(value: WorkDirWriteGuard) -> Self {
        Self::Write(value)
    }
}

impl PartialEq for WorkDirGuard {
    fn eq(&self, other: &Self) -> bool {
        self.path() == other.path()
    }
}

impl Eq for WorkDirGuard {}

impl PartialOrd for WorkDirGuard {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.path().partial_cmp(other.path())
    }
}

impl Ord for WorkDirGuard {
    fn cmp(&self, other: &Self) -> Ordering {
        self.path().cmp(other.path())
    }
}

impl WorkDirGuard {
    pub fn path(&self) -> &Path {
        match self {
            WorkDirGuard::Read { guard } => &guard,
            WorkDirGuard::Write(guard) => guard.path(),
        }
    }

    pub fn to_path_buf(&self) -> PathBuf {
        self.path().to_path_buf()
    }
}

/// Outer data struct that wraps the inner data to make sure it is only dropped once
#[derive(Clone)]
pub struct Data {
    inner: Arc<DataInner>,
}

impl Deref for Data {
    type Target = DataInner;

    fn deref(&self) -> &Self::Target {
        self.inner.deref()
    }
}

/// flushes the graph to avoid errors due to writing to deleted directory
async fn invalidate_graph(old_graph: Option<GraphWithVectors>) {
    if let Some(old_graph) = old_graph {
        let inner = old_graph.into_inner().await;
        blocking_compute(move || {
            if let Err(e) = inner.graph.flush() {
                error!(
                    "Failed to flush old graph {} before replacing: {e}",
                    inner.folder.local_path()
                )
            }
        })
        .await;
    }
}

impl Data {
    pub fn new(work_dir: &Path, configs: &AppConfig, graph_conf: Config) -> Self {
        let cache_configs = &configs.cache;

        let cache = GraphCache::new(cache_configs.capacity as usize);

        Self {
            inner: Arc::new(DataInner {
                work_dir: Arc::new(RwLock::new(work_dir.to_path_buf())),
                cache,
                #[cfg(feature = "vectors")]
                vector_cache: LazyDiskVectorCache::new(work_dir.join(".vector-cache")),
                graph_conf,
                auth_policy: None,
                allowed_parquet_paths: configs.parquet.allowed_paths.clone(),
            }),
        }
    }

    pub async fn work_dir_read(&self) -> WorkDirGuard {
        let guard = Arc::new(self.work_dir.clone().read_owned().await);
        WorkDirGuard::Read { guard }
    }

    pub async fn work_dir_write(&self) -> WorkDirWriteGuard {
        let guard = Arc::new(self.work_dir.clone().write_owned().await);
        WorkDirWriteGuard { guard }
    }

    pub(crate) fn set_auth_policy(&mut self, policy: Arc<dyn AuthorizationPolicy>) {
        Arc::get_mut(&mut self.inner)
            .expect("Data is not uniquely owned when setting auth_policy")
            .auth_policy = Some(policy);
    }

    /// Returns `Ok(())` if `path` is permitted by the parquet allowlist, otherwise an error
    /// describing why the path was rejected.
    /// When `allowed_parquet_paths` is empty, no path is permitted.
    /// As a rule, no paths within the working directory are allowed.
    pub async fn is_parquet_path_allowed(&self, path: &Path) -> Result<(), ParquetPathError> {
        let canonical_path = path
            .canonicalize()
            .map_err(|_| ParquetPathError::Unresolvable(path.to_path_buf()))?;
        if let Ok(canonical_work_dir) = self.work_dir_read().await.to_path_buf().canonicalize() {
            if canonical_path.starts_with(canonical_work_dir) {
                return Err(ParquetPathError::WithinWorkDir(path.to_path_buf()));
            }
        }
        let allowed = self.allowed_parquet_paths.iter().any(|allowed| {
            allowed
                .canonicalize()
                .map(|c| canonical_path.starts_with(c))
                .unwrap_or(false)
        });
        if allowed {
            Ok(())
        } else {
            Err(ParquetPathError::NotAllowed(path.to_path_buf()))
        }
    }

    /// Validates that `ns_path` exists and is a namespace, returning the `Namespace`
    /// so callers can enumerate descendants via `get_all_children()`.
    pub async fn get_namespace(&self, ns_path: &str) -> Result<Namespace, PathValidationError> {
        let work_dir = self.work_dir_read().await;
        Namespace::try_new(work_dir, ns_path.to_string())
    }

    /// # ⚠ Bypasses all permission checks — do not call from resolvers directly.
    /// Use `get_graph_with_read_permission`, `get_raw_graph_with_read_permission`, or
    /// `get_graph_with_write_permission` instead.
    async fn get_graph(&self, path: &str) -> Result<GraphWithVectors, GQLError> {
        self.cache
            .get_or_insert(path, self.read_graph_from_disk(path))
            .await
    }

    /// Test-only: direct graph load without permission checks.
    #[cfg(test)]
    pub(crate) async fn get_graph_for_test(
        &self,
        path: &str,
    ) -> Result<GraphWithVectors, GQLError> {
        self.get_graph(path).await
    }

    pub async fn get_cached_graph(&self, path: &str) -> Option<GraphWithVectors> {
        self.cache.get(path)
    }

    pub async fn insert_graph(
        &self,
        writeable_folder: ValidWriteableGraphFolder,
        graph: MaterializedGraph,
    ) -> Result<(), InsertionError> {
        let key = writeable_folder.local_path().to_owned();
        let config = self.graph_conf.clone();
        self.cache
            .insert_or_replace_with(&key, |old_graph| async {
                invalidate_graph(old_graph).await;
                blocking_compute(move || {
                    let (is_dirty, new_graph) = writeable_folder.write_graph_data(graph, config)?;
                    let folder = writeable_folder.finish()?;
                    let graph = GraphWithVectors::new(new_graph, None, folder.as_existing()?);
                    graph.set_dirty(is_dirty);
                    Ok::<_, InsertionError>(graph)
                })
                .await
            })
            .await?;
        Ok(())
    }

    /// Insert a graph serialized from a graph folder.
    pub async fn insert_graph_as_bytes<R: Read + Seek + Send + 'static>(
        &self,
        folder: ValidWriteableGraphFolder,
        bytes: R,
    ) -> Result<(), InsertionError> {
        let conf = self.graph_conf.clone();
        self.cache
            .invalidate_with(&folder.local_path().to_string(), |old_graph| async {
                invalidate_graph(old_graph).await;
                blocking_io(move || {
                    folder.write_graph_bytes(bytes, conf)?;
                    folder.finish()
                })
                .await
            })
            .await?;
        Ok(())
    }

    async fn delete_graph_inner(
        &self,
        graph_folder: ExistingGraphFolder,
    ) -> Result<(), MutationErrorInner> {
        let key = graph_folder.local_path().to_string();
        let dirty_file = mark_dirty(graph_folder.root())?;
        self.cache
            .invalidate_with(&key, |old_graph| async {
                invalidate_graph(old_graph).await;
                blocking_io(move || {
                    fs::remove_dir_all(graph_folder.root())?;
                    fs::remove_file(dirty_file)?;
                    Ok::<_, MutationErrorInner>(())
                })
                .await
            })
            .await?;
        Ok(())
    }

    pub async fn delete_graph(&self, path: &str) -> Result<(), DeletionError> {
        let work_dir = self.work_dir_write().await;
        let graph_folder = ExistingGraphFolder::try_from(work_dir.into(), path)?;
        self.delete_graph_inner(graph_folder)
            .await
            .map_err(|err| DeletionError::from_inner(path, err))?;
        Ok(())
    }

    pub async fn delete_namespace(
        &self,
        namespace: Namespace,
        descendants: &Vec<NamespacedItem>,
    ) -> Result<(), DeletionError> {
        let path = namespace.local_path();
        if path.is_empty() {
            return Err(DeletionError::PathValidation(
                PathValidationError::EmptyPath,
            ));
        }
        let root = namespace.current_dir().to_path_buf();
        let dirty_file = mark_dirty(&root).map_err(|err| {
            DeletionError::from_inner(path, MutationErrorInner::InvalidInternal(err))
        })?;
        for item in descendants {
            if let NamespacedItem::MetaGraph(g) = item {
                self.cache
                    .invalidate_with(g.local_path(), |old_graph| async {
                        invalidate_graph(old_graph).await;
                    })
                    .await;
            }
        }
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
        let work_dir = self.work_dir_write().await;
        let target =
            crate::paths::validate_path_for_namespace_create(work_dir.to_path_buf(), path)?;
        let mut cleanup_root = target.as_path();
        while let Some(parent) = cleanup_root.parent() {
            if parent.is_dir() {
                break;
            }
            cleanup_root = parent;
        }
        let dirty_file = mark_dirty(cleanup_root).map_err(|err| {
            InsertionError::from_inner(path, MutationErrorInner::InvalidInternal(err))
        })?;
        blocking_io(move || {
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::create_dir(&target)?;
            fs::remove_file(dirty_file)?;
            Ok::<_, MutationErrorInner>(())
        })
        .await
        .map_err(|err| InsertionError::from_inner(path, err))?;
        Ok(())
    }

    /// Rebuild the whole index for a graph.
    #[cfg(feature = "vectors")]
    pub(crate) async fn vectorise_folder(
        &self,
        folder: &ExistingGraphFolder,
        template: &DocumentTemplate,
        model: CachedEmbeddingModel,
    ) -> Result<(), GQLError> {
        let template = template.clone();
        self.index_folder(folder, move |graph, path| async move {
            graph.vectorise(model, template, Some(&path), true).await
        })
        .await
    }

    /// Embed only the entities missing from an existing index. Errors if there is no index yet or
    /// the template or model has changed — rebuilding is what covers those.
    #[cfg(feature = "vectors")]
    pub(crate) async fn vectorise_missing_in_folder(
        &self,
        folder: &ExistingGraphFolder,
        template: &DocumentTemplate,
        model: CachedEmbeddingModel,
    ) -> Result<(), GQLError> {
        let template = template.clone();
        self.index_folder(folder, move |graph, path| async move {
            graph.vectorise_missing(model, template, &path, true).await
        })
        .await
    }

    /// Run an indexing operation against a graph's vectors under the cache guard, keeping the
    /// vectors it already had if the operation fails and reporting the failure to the caller.
    #[cfg(feature = "vectors")]
    async fn index_folder<F, Fut>(
        &self,
        folder: &ExistingGraphFolder,
        index: F,
    ) -> Result<(), GQLError>
    where
        F: FnOnce(MaterializedGraph, PathBuf) -> Fut,
        Fut: std::future::Future<Output = Result<VectorisedGraph<MaterializedGraph>, GraphError>>,
    {
        let vectors_path = folder
            .graph_folder()
            .vectors_path()
            .map_err(GraphError::from)?;

        // The indexing itself runs with no cache guard held: it embeds the whole graph, and holding
        // the entry would block every read of this graph for the duration. Readers keep being served
        // by the vectors currently in the entry while this runs.
        let graph = self.get_graph_unchecked(folder).await?;
        let vectors = index(graph.graph().clone(), vectors_path)
            .await
            .map_err(|error| {
                error!(
                    "An error occurred when trying to vectorise graph {}: {error}",
                    folder.local_path()
                );
                error
            })?;

        // Swapping the new index in is the only part that needs the guard, and it is quick. On the
        // error path above the entry is never touched, so a failed vectorise leaves the graph with
        // exactly the vectors it had.
        let cloned_folder = folder.clone();
        let fallback = graph;
        self.cache
            .insert_or_replace_with(folder.local_path(), |old_graph| async {
                let current = old_graph.unwrap_or(fallback);
                let updated =
                    GraphWithVectors::new(current.graph().clone(), Some(vectors), cloned_folder);
                updated.set_dirty(current.is_dirty());
                Ok::<_, GQLError>(updated)
            })
            .await?;
        Ok(())
    }

    /// The graph for a folder, from the cache or from disk, with no permission check: callers here
    /// have already been authorised.
    #[cfg(feature = "vectors")]
    async fn get_graph_unchecked(
        &self,
        folder: &ExistingGraphFolder,
    ) -> Result<GraphWithVectors, GQLError> {
        match self.cache.get(folder.local_path()) {
            Some(graph) => Ok(graph),
            None => Ok(self.read_graph_from_disk_inner(folder.clone()).await?),
        }
    }

    pub async fn get_all_graph_folders(&self) -> impl Iterator<Item = ExistingGraphFolder> {
        let work_dir = self.work_dir_read().await;
        WalkDir::new(work_dir.path())
            .into_iter()
            .filter_map(move |e| {
                let entry = e.ok()?;
                let path = entry.path();
                let relative = get_relative_path(work_dir.path(), path).ok()?;
                let folder = ExistingGraphFolder::try_from(work_dir.clone(), &relative).ok()?;
                Some(folder)
            })
    }

    async fn read_graph_from_disk_inner(
        &self,
        folder: ExistingGraphFolder,
    ) -> Result<GraphWithVectors, GraphError> {
        let config = self.graph_conf.clone();
        #[cfg(feature = "vectors")]
        let cache = self.vector_cache.clone();
        GraphWithVectors::read_from_folder(
            &folder,
            #[cfg(feature = "vectors")]
            &cache,
            config,
        )
        .await
    }

    async fn read_graph_from_disk(&self, path: &str) -> Result<GraphWithVectors, GQLError> {
        let work_dir = self.work_dir_read().await;
        let folder = ExistingGraphFolder::try_from(work_dir, path)?;
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
        "Access denied: introspect-only access to graph '{graph}' — \
         use graphMetadata(path:) for counts and timestamps, or namespace listings to browse graphs"
    )]
    IntrospectOnly { graph: String },
    /// Caller has read-only access but the operation requires write.
    #[error("Access denied: WRITE permission required for graph '{graph}'")]
    GraphWriteRequired { graph: String },

    /// Caller has filtered read-only access but the opration requires unfiltered read
    #[error("Access denied: unfiltered READ permissions required for graph '{graph}'")]
    GraphUnfilteredReadRequired { graph: String },
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

/// Machine-readable `extensions.code` values attached to authorization errors so
/// the client classifies failures by structure rather than by message wording.
///
/// `GRAPH_NOT_FOUND` must be emitted for both a genuinely missing graph and a
/// forbidden-but-hidden graph, so the two are byte-for-byte indistinguishable to
/// an unauthorized caller.
pub(crate) const CODE_ACCESS_DENIED: &str = "ACCESS_DENIED";
pub(crate) const CODE_GRAPH_NOT_FOUND: &str = "GRAPH_NOT_FOUND";

/// Build an `async_graphql::Error` carrying a `code` in its extensions. The
/// human-readable message is preserved unchanged; only the structured code is
/// added, so the client can branch on it without parsing message text.
pub(crate) fn gql_error_with_code(
    message: impl Into<String>,
    code: &'static str,
) -> async_graphql::Error {
    async_graphql::Error::new(message.into()).extend_with(|_, ext| ext.set("code", code))
}

impl PermissionError {
    /// The `extensions.code` this denial surfaces to the client. `GraphNotFound`
    /// deliberately shares the code a genuinely missing graph produces so a
    /// forbidden graph cannot be distinguished from a nonexistent one.
    pub(crate) fn code(&self) -> &'static str {
        match self {
            PermissionError::GraphNotFound => CODE_GRAPH_NOT_FOUND,
            PermissionError::IntrospectOnly { .. }
            | PermissionError::GraphWriteRequired { .. }
            | PermissionError::GraphUnfilteredReadRequired { .. }
            | PermissionError::NamespaceWriteRequired { .. } => CODE_ACCESS_DENIED,
        }
    }

    /// Convert into an `async_graphql::Error` tagged with the matching `code`.
    pub(crate) fn into_gql_error(self) -> async_graphql::Error {
        let code = self.code();
        gql_error_with_code(self.to_string(), code)
    }
}

#[derive(Enum, Clone, Copy, Debug, PartialEq, Eq)]
#[graphql(name = "GraphType")]
pub enum GqlGraphType {
    /// Persistent.
    Persistent,
    /// Event.
    Event,
}

impl GqlGraphType {
    /// The GraphQL enum literal for this variant, for splicing into a query.
    /// Unquoted by design — GraphQL enum values are not strings.
    pub fn as_gql(&self) -> &'static str {
        match self {
            GqlGraphType::Persistent => "PERSISTENT",
            GqlGraphType::Event => "EVENT",
        }
    }
}

impl FromStr for GqlGraphType {
    type Err = String;

    /// Parses the GraphQL literal. The error names the accepted values,
    /// because this is the boundary where a caller's string (a Python
    /// argument, a config value) becomes a typed graph model.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "EVENT" => Ok(GqlGraphType::Event),
            "PERSISTENT" => Ok(GqlGraphType::Persistent),
            other => Err(format!(
                "invalid graph type `{other}`: expected \"EVENT\" or \"PERSISTENT\""
            )),
        }
    }
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
        return match policy.graph_permissions(ctx, path) {
            Err(msg) => {
                warn!(graph = path, "Access denied by auth policy");
                let ns = parent_namespace(path);
                if policy.namespace_permissions(ctx, ns).is_some() {
                    Err(gql_error_with_code(msg.to_string(), CODE_ACCESS_DENIED))
                } else {
                    Err(PermissionError::GraphNotFound.into_gql_error())
                }
            }
            Ok(perm) => {
                if let Some(p) = perm.at_least_read() {
                    Ok(p)
                } else {
                    warn!(graph = path, "Permission denied: introspect-only access");
                    debug!(
                        graph = path,
                        "Introspect-only grants can read graphMetadata() but not graph(); \
                         use graphMetadata() instead or request a read grant"
                    );
                    Err(PermissionError::IntrospectOnly {
                        graph: path.to_string(),
                    }
                    .into_gql_error())
                }
            }
        };
    }
    Ok(GraphPermission::Write)
}

/// Gives the policy an asynchronous pass at an already-granted permission before its filter is
/// applied (see [`AuthorizationPolicy::refine_permission`]). A no-op without a policy.
async fn refine(
    ctx: &Context<'_>,
    policy: &Option<Arc<dyn AuthorizationPolicy>>,
    path: &str,
    perm: GraphPermission,
) -> async_graphql::Result<GraphPermission> {
    match policy {
        Some(policy) => policy
            .refine_permission(ctx, path, perm)
            .await
            .map_err(|msg| {
                warn!(graph = path, "Access denied while refining permission");
                msg.into()
            }),
        None => Ok(perm),
    }
}

pub(crate) fn require_graph_write(
    ctx: &Context<'_>,
    policy: &Option<Arc<dyn AuthorizationPolicy>>,
    path: &str,
) -> async_graphql::Result<()> {
    if crate::auth::is_read_only(ctx) {
        return Err(gql_error_with_code(
            "Access denied: this context may not write",
            CODE_ACCESS_DENIED,
        ));
    }
    match policy {
        None => ctx
            .require_jwt_write_access()
            .map_err(|e| gql_error_with_code(e.to_string(), CODE_ACCESS_DENIED)),
        Some(p) => {
            p.graph_permissions(ctx, path)
                .map_err(|e| gql_error_with_code(e.to_string(), CODE_ACCESS_DENIED))?
                .at_least_write()
                .ok_or_else(|| {
                    PermissionError::GraphWriteRequired {
                        graph: path.to_string(),
                    }
                    .into_gql_error()
                })?;
            Ok(())
        }
    }
}

/// Applies a row-level `GqlFilter` to a `DynamicGraph`.
async fn apply_graph_filter(
    graph: DynamicGraph,
    row_filter: GqlFilter,
) -> async_graphql::Result<DynamicGraph> {
    blocking_compute(move || apply_row_filter_sync(graph, row_filter)).await
}

fn apply_row_filter_sync(
    graph: DynamicGraph,
    filter: GqlFilter,
) -> async_graphql::Result<DynamicGraph> {
    // And sub-filters are applied sequentially so that DynView (window/snapshot/layer)
    // sub-filters wrap the graph view before subsequent node/edge predicate filters run.
    if let GqlFilter::And(filters) = filter {
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
    ) -> async_graphql::Result<(UnlockedGraphFolder, DynamicGraph)> {
        let gwv = self.get_graph(path).await?;
        let typed_graph = match graph_type {
            Some(GqlGraphType::Event) => match gwv.graph() {
                MaterializedGraph::EventGraph(g) => MaterializedGraph::EventGraph(g.clone()),
                MaterializedGraph::PersistentGraph(g) => {
                    MaterializedGraph::EventGraph(g.event_graph())
                }
            },
            Some(GqlGraphType::Persistent) => match gwv.graph() {
                MaterializedGraph::EventGraph(g) => {
                    MaterializedGraph::PersistentGraph(g.persistent_graph())
                }
                MaterializedGraph::PersistentGraph(g) => {
                    MaterializedGraph::PersistentGraph(g.clone())
                }
            },
            None => gwv.graph().clone(),
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
        Ok((gwv.folder().clone(), graph))
    }

    /// For the `graph()` resolver: permission denial → `Ok(None)` (null to client, hides
    /// existence and access level). Load failure → `Err` (graph was deleted, etc.).
    pub async fn get_graph_with_read_permission(
        &self,
        ctx: &Context<'_>,
        path: &str,
        graph_type: Option<GqlGraphType>,
    ) -> async_graphql::Result<Option<(UnlockedGraphFolder, DynamicGraph)>> {
        match require_at_least_read(ctx, &self.auth_policy, path) {
            Ok(perm) => match refine(ctx, &self.auth_policy, path, perm).await {
                Ok(perm) => self.load_and_filter(path, perm, graph_type).await.map(Some),
                // Refinement denied access — hide the graph, as with any other read denial.
                Err(_) => Ok(None),
            },
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
    ) -> async_graphql::Result<(UnlockedGraphFolder, DynamicGraph)> {
        let perm = require_at_least_read(ctx, &self.auth_policy, path)?;
        let perm = refine(ctx, &self.auth_policy, path, perm).await?;
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
        let res = require_at_least_read(ctx, &self.auth_policy, path)?;
        if res.level() < PermissionLevel::Read {
            return Err(PermissionError::GraphUnfilteredReadRequired {
                graph: path.to_string(),
            }
            .into_gql_error());
        }
        let graph = self.get_graph(path).await?;
        Ok(graph)
    }

    /// Checks write permission then returns the raw `GraphWithVectors` for mutation operations.
    pub(crate) async fn get_graph_with_write_permission(
        &self,
        ctx: &Context<'_>,
        path: &str,
    ) -> async_graphql::Result<GraphWithVectors> {
        require_graph_write(ctx, &self.auth_policy, path)?;
        let graph = self.get_graph(path).await?;
        Ok(graph)
    }

    /// Checks read permission then returns the vectorised graph, if any.
    /// Returns `None` for filtered-access users: embeddings are computed from the full graph
    /// and search results cannot be retroactively row-filtered.
    #[cfg(feature = "vectors")]
    pub(crate) async fn get_vectors_with_read_permission(
        &self,
        ctx: &Context<'_>,
        path: &str,
    ) -> async_graphql::Result<Option<GqlVectorisedGraph>> {
        let perm = require_at_least_read(ctx, &self.auth_policy, path)?;
        if matches!(perm, GraphPermission::Read { filter: Some(_) }) {
            return Ok(None);
        }
        let graph = self.get_graph(path).await?;
        Ok(graph.vectors().cloned().map(|g| g.into()))
    }
}

impl Drop for DataInner {
    fn drop(&mut self) {
        self.cache.flush_and_clear();
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
    };
    use raphtory_api::core::storage::graph_folder::GraphPaths;
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
        let work_dir = data.work_dir_write().await;
        for (name, graph) in graphs.into_iter() {
            let folder = work_dir.clone().validate_path_for_insert(name, true)?;
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

    /// After remote-style mutations, an explicit persist must rewrite the
    /// metadata sidecar so cache-miss namespace listings report true counts.
    #[tokio::test]
    async fn test_persist_refreshes_metadata_sidecar_counts() {
        use crate::paths::ExistingGraphFolder;

        let tmp_work_dir = tempfile::tempdir().unwrap();
        let data = Data::new(tmp_work_dir.path(), &Default::default(), Default::default());

        // A fresh empty graph — its sidecar starts at 0 nodes / 0 edges.
        let path = "people";
        let folder = data
            .work_dir_write()
            .await
            .validate_path_for_insert(path, false)
            .unwrap();
        let empty: MaterializedGraph = Graph::new().into();
        data.insert_graph(folder, empty).await.unwrap();

        // Remote-style mutations against the resident graph, without touching the sidecar.
        let graph = data.get_graph_for_test(path).await.unwrap();
        graph.add_edge(0, "a", "b", NO_PROPS, None).unwrap();
        graph.add_node(0, "c", NO_PROPS, None, None).unwrap();
        graph.set_dirty(true);

        graph.persist().unwrap();

        // The persisted sidecar (what cache-miss listings read) must reflect the writes.
        let read_folder = ExistingGraphFolder::try_from(data.work_dir_read().await, path).unwrap();
        let meta = read_folder.graph_folder().read_metadata().unwrap();
        assert_eq!(meta.node_count, 3, "sidecar node_count stale after persist");
        assert_eq!(meta.edge_count, 1, "sidecar edge_count stale after persist");
    }

    /// Eviction (here via `flush_and_clear`) must also persist true counts —
    /// regression guard now that eviction and explicit flush share `persist`.
    #[tokio::test]
    async fn test_eviction_persists_metadata_sidecar_counts() {
        use crate::paths::ExistingGraphFolder;

        let tmp_work_dir = tempfile::tempdir().unwrap();
        let data = Data::new(tmp_work_dir.path(), &Default::default(), Default::default());

        let path = "people";
        let folder = data
            .work_dir_write()
            .await
            .validate_path_for_insert(path, false)
            .unwrap();
        let empty: MaterializedGraph = Graph::new().into();
        data.insert_graph(folder, empty).await.unwrap();

        let graph = data.get_graph_for_test(path).await.unwrap();
        graph.add_edge(0, "a", "b", NO_PROPS, None).unwrap();
        graph.set_dirty(true);
        drop(graph);

        data.cache.flush_and_clear();

        let read_folder = ExistingGraphFolder::try_from(data.work_dir_read().await, path).unwrap();
        let meta = read_folder.graph_folder().read_metadata().unwrap();
        assert_eq!(
            meta.node_count, 2,
            "sidecar node_count stale after eviction"
        );
        assert_eq!(
            meta.edge_count, 1,
            "sidecar edge_count stale after eviction"
        );
    }

    /// A vectorise that fails partway must not leave the graph without vectors, and the caller
    /// has to be told: returning success while quietly dropping the index is how a live index
    /// disappears with nothing in the response to explain it.
    #[cfg(feature = "vectors")]
    #[tokio::test]
    async fn test_failed_vectorise_reports_and_keeps_the_index() {
        use crate::paths::ExistingGraphFolder;
        use raphtory::vectors::{
            custom::serve_custom_embedding, storage::OpenAIEmbeddings, template::DocumentTemplate,
        };

        fn fake_embedding(text: &str) -> Vec<f32> {
            vec![text.len() as f32, 1.0]
        }

        fn template(prefix: &str) -> DocumentTemplate {
            DocumentTemplate {
                node_template: Some(format!("{prefix} {{{{ properties.doc }}}}")),
                edge_template: None,
            }
        }

        let tmp_work_dir = tempfile::tempdir().unwrap();
        let port = 1751;
        let name = "failing_vg";

        let graph = Graph::new();
        for node in ["alice", "bob"] {
            graph
                .add_node(0, node, [("doc", node.to_string())], None, None)
                .unwrap();
        }
        graph.encode(&tmp_work_dir.path().join(name)).unwrap();

        let configs = AppConfigBuilder::new().build();
        let data = Data::new(tmp_work_dir.path(), &configs, Default::default());
        let embedding_server = serve_custom_embedding(None, port, fake_embedding).await;
        let model = data
            .vector_cache
            .resolve()
            .await
            .unwrap()
            .openai(OpenAIEmbeddings::new("whatever", format!("http://localhost:{port}")).into())
            .await
            .unwrap();
        let folder = ExistingGraphFolder::try_from(data.work_dir_read().await, name).unwrap();

        data.vectorise_folder(&folder, &template("first"), model.clone())
            .await
            .unwrap();
        assert_eq!(
            search_hits(&data, name, "first alice").await,
            2,
            "the first vectorise should have indexed both nodes"
        );

        // the model is already resolved, so the failure lands on the embedding calls that the
        // vectorise itself makes rather than on setting the model up
        embedding_server.stop().await;

        let result = data
            .vectorise_folder(&folder, &template("second"), model)
            .await;
        assert!(
            result.is_err(),
            "a vectorise that could not embed must report the failure"
        );

        let graph = data.get_graph(name).await.unwrap();
        assert!(
            graph.vectors().is_some(),
            "the graph must keep the vectors it had before the failed vectorise"
        );
        assert_eq!(
            search_hits(&data, name, "first alice").await,
            2,
            "the previous index must still answer after a failed vectorise"
        );
    }

    /// Number of documents a similarity search returns for `text`.
    #[cfg(feature = "vectors")]
    async fn search_hits(data: &Data, path: &str, text: &str) -> usize {
        let graph = data.get_graph(path).await.unwrap();
        let vectors = graph.vectors().expect("graph has no vectors");
        let embedding = vectors.embed_text(text).await.unwrap();
        vectors
            .nodes_by_similarity(&embedding, 10, None)
            .execute()
            .await
            .unwrap()
            .get_documents()
            .await
            .unwrap()
            .len()
    }

    /// Vectorising has to work on a graph whose index was loaded from disk by an earlier read,
    /// which is what a restarted server does: something reads the graph, and only then does the
    /// client re-vectorise it.
    #[cfg(feature = "vectors")]
    #[tokio::test]
    async fn test_vectorise_after_reading_a_reloaded_graph() {
        use crate::paths::ExistingGraphFolder;
        use raphtory::vectors::{
            custom::serve_custom_embedding, storage::OpenAIEmbeddings, template::DocumentTemplate,
        };

        fn fake_embedding(text: &str) -> Vec<f32> {
            vec![text.len() as f32, 1.0]
        }

        let tmp_work_dir = tempfile::tempdir().unwrap();
        let port = 1750;
        let name = "reloaded_vg";

        let graph = Graph::new();
        graph
            .add_node(0, name, [("doc", name.to_string())], None, None)
            .unwrap();
        graph.encode(&tmp_work_dir.path().join(name)).unwrap();

        let configs = AppConfigBuilder::new().build();
        let _embedding_server = serve_custom_embedding(None, port, fake_embedding).await;
        let template = DocumentTemplate {
            node_template: Some("{{ properties.doc }}".to_owned()),
            edge_template: None,
        };
        let embeddings = OpenAIEmbeddings::new("whatever", format!("http://localhost:{port}"));

        // first server: builds and persists the index, then goes away
        {
            let data = Data::new(tmp_work_dir.path(), &configs, Default::default());
            let model = data
                .vector_cache
                .resolve()
                .await
                .unwrap()
                .openai(embeddings.clone().into())
                .await
                .unwrap();
            let folder = ExistingGraphFolder::try_from(data.work_dir_read().await, name).unwrap();
            data.vectorise_folder(&folder, &template, model)
                .await
                .unwrap();
        }

        // second server: the read loads the persisted index before anything else touches the
        // embedding cache, and the re-vectorise afterwards must still work
        let data = Data::new(tmp_work_dir.path(), &configs, Default::default());
        assert!(
            data.get_graph(name).await.unwrap().vectors().is_some(),
            "the persisted index should be loaded with the graph"
        );

        let model = data
            .vector_cache
            .resolve()
            .await
            .unwrap()
            .openai(embeddings.into())
            .await
            .unwrap();
        let folder = ExistingGraphFolder::try_from(data.work_dir_read().await, name).unwrap();
        data.vectorise_folder(&folder, &template, model)
            .await
            .unwrap();

        let graph = data.get_graph(name).await.unwrap();
        let vectors = graph
            .vectors()
            .expect("the graph should still have vectors after re-vectorising");
        let embedding = vectors.embed_text(name).await.unwrap();
        let docs = vectors
            .nodes_by_similarity(&embedding, 1, None)
            .execute()
            .await
            .unwrap()
            .get_documents()
            .await
            .unwrap();
        assert!(!docs.is_empty(), "index is empty after re-vectorising");
    }

    /// A vectorised graph that gets evicted has to come back with a working index when it is
    /// next read, because the vectors are reloaded from disk by a different code path than the
    /// one that built them.
    #[cfg(feature = "vectors")]
    #[tokio::test]
    async fn test_eviction_reloads_vectorised_graph() {
        use crate::paths::ExistingGraphFolder;
        use raphtory::vectors::{
            custom::serve_custom_embedding, storage::OpenAIEmbeddings, template::DocumentTemplate,
        };

        fn fake_embedding(text: &str) -> Vec<f32> {
            vec![text.len() as f32, 1.0]
        }

        let tmp_work_dir = tempfile::tempdir().unwrap();
        let port = 1749;

        for name in ["test_vg", "test_vg2"] {
            let graph = Graph::new();
            graph
                .add_node(0, name, [("doc", name.to_string())], None, None)
                .unwrap();
            graph.encode(&tmp_work_dir.path().join(name)).unwrap();
        }

        // capacity 1: reading either graph evicts the other, so the second read of each is a
        // reload from disk
        let configs = AppConfigBuilder::new().with_cache_capacity(1).build();
        let data = Data::new(tmp_work_dir.path(), &configs, Default::default());

        let _embedding_server = serve_custom_embedding(None, port, fake_embedding).await;
        let template = DocumentTemplate {
            node_template: Some("{{ properties.doc }}".to_owned()),
            edge_template: None,
        };
        let model = data
            .vector_cache
            .resolve()
            .await
            .unwrap()
            .openai(OpenAIEmbeddings::new("whatever", format!("http://localhost:{port}")).into())
            .await
            .unwrap();

        for name in ["test_vg", "test_vg2"] {
            let folder = ExistingGraphFolder::try_from(data.work_dir_read().await, name).unwrap();
            data.vectorise_folder(&folder, &template, model.clone())
                .await
                .unwrap();
        }

        // two passes: the first evicts what vectorising left cached, the second reads graphs
        // that can only have come back from disk
        for pass in 0..2 {
            for name in ["test_vg", "test_vg2"] {
                let graph = data.get_graph(name).await.unwrap();
                let vectors = graph
                    .vectors()
                    .unwrap_or_else(|| panic!("pass {pass}: {name} came back without vectors"));
                let embedding = vectors.embed_text(name).await.unwrap();
                let docs = vectors
                    .nodes_by_similarity(&embedding, 1, None)
                    .execute()
                    .await
                    .unwrap()
                    .get_documents()
                    .await
                    .unwrap();
                assert!(
                    !docs.is_empty(),
                    "pass {pass}: {name} reloaded with an empty index"
                );
                // the graph has to be dropped for the cache to be allowed to evict it
                drop(graph);
                assert_eq!(
                    data.cache.iter().count(),
                    1,
                    "pass {pass}: cache should hold only {name}, so the next read reloads"
                );
            }
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

        let configs = AppConfigBuilder::new().with_cache_capacity(1).build();

        let data = Data::new(tmp_work_dir.path(), &configs, Default::default());

        assert!(!data.cache.contains_key("test_g"));
        assert!(!data.cache.contains_key("test_g2"));

        // Test size based eviction
        data.get_graph("test_g2").await.unwrap();
        assert!(data.cache.contains_key("test_g2"));
        assert!(!data.cache.contains_key("test_g"));

        data.get_graph("test_g").await.unwrap(); // wait for any eviction
        assert_eq!(data.cache.iter().count(), 1);
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

        let configs = AppConfigBuilder::new().with_cache_capacity(1).build();

        let data = Data::new(work_dir, &configs, Default::default());

        let paths = data
            .get_all_graph_folders()
            .await
            .into_iter()
            .map(|folder| folder.folder.root().to_path_buf())
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

        let configs = AppConfigBuilder::new().with_cache_capacity(10).build();

        let data = Data::new(tmp_work_dir.path(), &configs, Default::default());

        let loaded_graph1 = data.get_graph("test_graph1").await.unwrap();
        let loaded_graph2 = data.get_graph("test_graph2").await.unwrap();

        // TODO: This test doesn't work with disk storage right now, make sure modification dates actually update correctly!
        if loaded_graph1.graph().disk_storage_path().is_some() {
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
        let configs = AppConfigBuilder::new().with_cache_capacity(10).build();

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

        // TODO: This test doesn't work with disk storage right now, make sure modification dates actually update correctly!
        if loaded_graph1.graph().disk_storage_path().is_some() {
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
