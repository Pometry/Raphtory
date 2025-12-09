use crate::{
    data::DIRTY_PATH,
    model::{blocking_io, GqlGraphError},
    rayon::blocking_compute,
    GQLError,
};
use futures_util::io;
use raphtory::{
    db::api::view::{internal::InternalStorageOps, MaterializedGraph},
    errors::{GraphError, InvalidPathReason},
    prelude::ParquetEncoder,
    serialise::{
        make_data_path, metadata::GraphMetadata, read_data_path, read_dirty_path,
        read_path_pointer, GraphFolder, GraphPaths, InnerGraphFolder, Metadata, RelativePath,
        WriteableGraphFolder, DATA_PATH, META_PATH,
    },
};
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    fs,
    fs::File,
    io::{ErrorKind, Read, Seek, Write},
    ops::Deref,
    path::{Component, Path, PathBuf, StripPrefixError},
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::io::AsyncReadExt;
use tracing::{error, metadata, warn};

pub trait ValidGraphPaths: GraphPaths {
    fn local_path(&self) -> &str;

    fn local_path_string(&self) -> String {
        self.local_path().to_owned()
    }

    fn with_internal_errors<R: WithPath>(
        &self,
        fun: impl FnOnce() -> R,
    ) -> Result<R::Value, PathValidationError> {
        fun().with_path(self.local_path())
    }
}

pub struct ValidPath(PathBuf);

impl ValidPath {
    /// path exists and is a graph
    pub fn is_graph(&self) -> bool {
        self.0.exists() && self.0.join(META_PATH).exists()
    }

    /// path exists and is a namespace
    pub fn is_namespace(&self) -> bool {
        self.0.exists() && !self.0.join(META_PATH).exists()
    }

    pub fn into_path(self) -> PathBuf {
        self.0
    }
}

#[derive(Clone, Debug, PartialOrd, PartialEq, Ord, Eq)]
pub struct ExistingGraphFolder(pub(crate) ValidGraphFolder);

impl GraphPaths for ExistingGraphFolder {
    fn root(&self) -> &Path {
        self.0.root()
    }

    fn data_path(&self) -> Result<InnerGraphFolder, GraphError> {
        self.0.data_path()
    }
}

impl ValidGraphPaths for ExistingGraphFolder {
    fn local_path(&self) -> &str {
        self.0.local_path()
    }
}

impl Deref for ExistingGraphFolder {
    type Target = ValidGraphFolder;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ExistingGraphFolder {
    pub fn try_from(base_path: PathBuf, relative_path: &str) -> Result<Self, PathValidationError> {
        let path = valid_path(base_path, relative_path)?;
        Self::try_from_valid(path, relative_path)
    }

    pub fn try_from_valid(
        base_path: ValidPath,
        relative_path: &str,
    ) -> Result<Self, PathValidationError> {
        let graph_folder: GraphFolder = base_path.into_path().into();
        if graph_folder.is_reserved() {
            Ok(Self(ValidGraphFolder {
                graph_folder: graph_folder,
                local_path: relative_path.to_string(),
            }))
        } else {
            Err(PathValidationError::GraphNotExistsError(
                relative_path.to_string(),
            ))
        }
    }

    fn replace_graph_data_inner(
        &self,
        graph: MaterializedGraph,
    ) -> Result<(), InternalPathValidationError> {
        self.graph_folder.data_path()?.replace_graph(graph)?;
        Ok(())
    }
    pub fn replace_graph_data(&self, graph: MaterializedGraph) -> Result<(), PathValidationError> {
        self.replace_graph_data_inner(graph)
            .with_path(self.local_path())
    }
}

#[derive(Clone, Debug, PartialOrd, PartialEq, Ord, Eq)]
pub struct ValidGraphFolder {
    graph_folder: GraphFolder,
    local_path: String,
}

fn extend_and_validate(
    full_path: &mut PathBuf,
    component: Component,
    user_facing_path: &str,
) -> Result<(), InternalPathValidationError> {
    match component {
        Component::Prefix(_) => {
            return Err(InvalidPathReason::RootNotAllowed(user_facing_path.into()).into())
        }
        Component::RootDir => {
            return Err(InvalidPathReason::RootNotAllowed(user_facing_path.into()).into())
        }
        Component::CurDir => {
            return Err(InvalidPathReason::CurDirNotAllowed(user_facing_path.into()).into())
        }
        Component::ParentDir => {
            return Err(InvalidPathReason::ParentDirNotAllowed(user_facing_path.into()).into())
        }
        Component::Normal(component) => {
            // check if some intermediate path is already a graph
            if full_path.join(META_PATH).exists() {
                return Err(InvalidPathReason::ParentIsGraph(user_facing_path.into()).into());
            }
            full_path.push(component);
            //check for symlinks
            if full_path.is_symlink() {
                return Err(InvalidPathReason::SymlinkNotAllowed(user_facing_path.into()).into());
            }
            ensure_clean_folder(&full_path)?;
        }
    }
    Ok(())
}

pub(crate) fn valid_path(
    base_path: PathBuf,
    relative_path: &str,
) -> Result<ValidPath, PathValidationError> {
    let user_facing_path = PathBuf::from(relative_path);

    if relative_path.contains(r"//") {
        return Err(InvalidPathReason::DoubleForwardSlash(user_facing_path).into());
    }
    if relative_path.contains(r"\") {
        return Err(InvalidPathReason::BackslashError(user_facing_path).into());
    }

    let mut full_path = base_path.clone();
    // fail if any component is a Prefix (C://), tries to access root,
    // tries to access a parent dir or is a symlink which could break out of the working dir
    for component in user_facing_path.components() {
        extend_and_validate(&mut full_path, component, relative_path)
            .with_path(relative_path.to_string())?;
    }
    Ok(ValidPath(full_path))
}

#[derive(Clone, Debug)]
pub struct NewPath {
    path: PathBuf,
    cleanup: Option<CleanupPath>,
}

impl NewPath {
    pub fn is_new(&self) -> bool {
        self.cleanup.is_some()
    }
}

impl PartialEq for NewPath {
    fn eq(&self, other: &Self) -> bool {
        self.path.eq(&other.path)
    }
}

impl PartialOrd for NewPath {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.path.partial_cmp(&other.path)
    }
}

pub(crate) fn create_valid_path(
    base_path: PathBuf,
    relative_path: &str,
) -> Result<NewPath, InternalPathValidationError> {
    let user_facing_path = PathBuf::from(relative_path);

    if relative_path.contains(r"//") {
        return Err(InvalidPathReason::DoubleForwardSlash(user_facing_path).into());
    }
    if relative_path.contains(r"\") {
        return Err(InvalidPathReason::BackslashError(user_facing_path).into());
    }

    let mut full_path = base_path.clone();
    let mut cleanup_marker = None;
    // fail if any component is a Prefix (C://), tries to access root,
    // tries to access a parent dir or is a symlink which could break out of the working dir
    for component in user_facing_path.components() {
        match extend_and_validate(&mut full_path, component, relative_path) {
            Ok(_) => {
                if !full_path.exists() {
                    if cleanup_marker.is_none() {
                        cleanup_marker = Some(CleanupPath {
                            path: full_path.clone(),
                            dirty_marker: mark_dirty(&full_path)?,
                        });
                        fs::create_dir(&full_path)?;
                    }
                }
            }
            Err(error) => {
                if let Some(created_path) = cleanup_marker {
                    created_path.cleanup()?;
                }
                return Err(error.into());
            }
        }
    }
    Ok(NewPath {
        path: full_path,
        cleanup: cleanup_marker,
    })
}

#[derive(Debug, Clone)]
struct CleanupPath {
    path: PathBuf,
    dirty_marker: PathBuf,
}

impl CleanupPath {
    fn persist(&self) -> Result<(), InternalPathValidationError> {
        fs::remove_file(&self.dirty_marker)?;
        Ok(())
    }

    fn cleanup(&self) -> Result<(), InternalPathValidationError> {
        fs::remove_dir_all(&self.path)?;
        fs::remove_file(&self.dirty_marker)?;
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ValidWriteableGraphFolder {
    data_path: WriteableGraphFolder,
    local_path: String,
    dirty_marker: Option<CleanupPath>,
}

impl GraphPaths for ValidWriteableGraphFolder {
    fn root(&self) -> &Path {
        self.data_path.root()
    }

    fn data_path(&self) -> Result<InnerGraphFolder, GraphError> {
        self.data_path.data_path()
    }
}

impl ValidGraphPaths for ValidWriteableGraphFolder {
    fn local_path(&self) -> &str {
        &self.local_path
    }
}

impl ValidWriteableGraphFolder {
    fn new_inner(
        valid_path: NewPath,
        graph_name: &str,
    ) -> Result<Self, InternalPathValidationError> {
        let is_new = valid_path.is_new();
        let graph_folder = GraphFolder::from(valid_path.path);
        if !is_new {
            if !graph_folder.is_reserved() {
                return Err(InternalPathValidationError::GraphIsNamespace);
            }
        }
        let data_path = graph_folder.init_swap()?;
        Ok(Self {
            data_path,
            dirty_marker: valid_path.cleanup,
            local_path: graph_name.to_string(),
        })
    }
    fn new(valid_path: NewPath, graph_name: &str) -> Result<Self, PathValidationError> {
        Self::new_inner(valid_path, graph_name).map_err(|error| {
            PathValidationError::InternalError {
                graph: graph_name.to_string(),
                error,
            }
        })
    }

    pub(crate) fn try_new(
        base_path: PathBuf,
        relative_path: &str,
    ) -> Result<Self, PathValidationError> {
        let path = create_valid_path(base_path, relative_path).map_err(|error| {
            PathValidationError::InternalError {
                graph: relative_path.to_string(),
                error,
            }
        })?;
        if !path.cleanup.is_some() {
            return Err(PathValidationError::GraphExistsError(
                relative_path.to_string(),
            ));
        }
        Self::new(path, relative_path)
    }

    pub(crate) fn try_existing_or_new(
        base_path: PathBuf,
        relative_path: &str,
    ) -> Result<Self, PathValidationError> {
        let path = create_valid_path(base_path, relative_path).map_err(|error| {
            PathValidationError::InternalError {
                graph: relative_path.to_string(),
                error,
            }
        })?;
        Self::new(path, relative_path)
    }

    fn write_graph_data_inner(
        &self,
        graph: MaterializedGraph,
    ) -> Result<(), InternalPathValidationError> {
        self.data_path.data_path()?.replace_graph(graph)?;
        Ok(())
    }
    pub fn write_graph_data(&self, graph: MaterializedGraph) -> Result<(), PathValidationError> {
        self.write_graph_data_inner(graph)
            .with_path(self.local_path())
    }

    pub fn read_graph(&self) -> Result<MaterializedGraph, PathValidationError> {
        self.with_internal_errors(|| self.data_path()?.read_graph())
    }

    pub fn write_graph_bytes<R: Read + Seek + Send + 'static>(
        &self,
        bytes: R,
    ) -> Result<(), PathValidationError> {
        self.data_path
            .data_path()
            .with_path(&self.local_path)?
            .unzip_to_folder(bytes)
            .with_path(&self.local_path)
    }

    /// Swap old and new data and delete the old graph
    pub fn finish(self) -> Result<ValidGraphFolder, PathValidationError> {
        let data_path = self.data_path.finish().with_path(&self.local_path)?;
        if let Some(cleanup) = self.dirty_marker.as_ref() {
            cleanup.persist().with_path(&self.local_path)?;
        }
        Ok(ValidGraphFolder {
            graph_folder: data_path,
            local_path: self.local_path,
        })
    }
}

#[derive(thiserror::Error, Debug)]
pub enum InternalPathValidationError {
    #[error("Path from metadata is invalid: {0}")]
    InvalidPath(#[from] InvalidPathReason),
    #[error(transparent)]
    IOError(io::Error),
    #[error("Graph path should not be nested: {0}")]
    NestedPath(PathBuf),
    #[error("Graph metadata file does not exist")]
    MissingMetadataFile,
    #[error("Reading path from metadata failed: {0}")]
    InvalidMetadata(#[from] serde_json::Error),
    #[error(transparent)]
    GraphError(#[from] GraphError),
    #[error("Graph path should always have a parent")]
    MissingParent,
    #[error(transparent)]
    StripPrefix(#[from] StripPrefixError),
    #[error("Expected a graph but found a namespace")]
    GraphIsNamespace,
    #[error("Expected a namespace but found a graph")]
    NamespaceIsGraph,
    #[error("The path provided contains non-UTF8 characters.")]
    NonUTFCharacters,
}

impl From<io::Error> for InternalPathValidationError {
    fn from(value: io::Error) -> Self {
        error!("Unexpected IO failure: {}", value);
        InternalPathValidationError::IOError(value)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum PathValidationError {
    #[error("Graph {0} already exists")]
    GraphExistsError(String),
    #[error("Graph {0} does not exist")]
    GraphNotExistsError(String),
    #[error("The path provided does not exists as a namespace: {0}")]
    NamespaceDoesNotExist(String),
    #[error(transparent)]
    InvalidPath(#[from] InvalidPathReason),
    #[error("Graph {graph} is corrupted: {error}")]
    InternalError {
        graph: String,
        error: InternalPathValidationError,
    },
    #[error("Unexpected IO error for graph {graph}: {error}")]
    IOError { graph: String, error: io::Error },
}

pub trait WithPath {
    type Value;
    fn with_path<S: Into<String>>(self, graph: S) -> Result<Self::Value, PathValidationError>;
}

impl<V, E: Into<InternalPathValidationError>> WithPath for Result<V, E> {
    type Value = V;
    fn with_path<S: Into<String>>(self, graph: S) -> Result<V, PathValidationError> {
        self.map_err(move |error| PathValidationError::InternalError {
            graph: graph.into(),
            error: error.into(),
        })
    }
}

pub(crate) fn valid_relative_graph_path(
    mut full_path: PathBuf,
    relative_path: &Path,
) -> Result<PathBuf, InternalPathValidationError> {
    let mut components = relative_path.components();
    if let Some(component) = components.next() {
        match component {
            Component::Prefix(_) => {
                Err(InvalidPathReason::RootNotAllowed(
                    relative_path.to_path_buf(),
                ))?;
            }
            Component::RootDir => Err(InvalidPathReason::RootNotAllowed(
                relative_path.to_path_buf(),
            ))?,
            Component::CurDir => Err(InvalidPathReason::CurDirNotAllowed(
                relative_path.to_path_buf(),
            ))?,
            Component::ParentDir => Err(InvalidPathReason::ParentDirNotAllowed(
                relative_path.to_path_buf(),
            ))?,
            Component::Normal(component) => {
                full_path.push(component);
                //check for symlinks
                if full_path.is_symlink() {
                    Err(InvalidPathReason::SymlinkNotAllowed(
                        relative_path.to_path_buf(),
                    ))?
                }
            }
        }
    }
    if components.next().is_some() {
        Err(InternalPathValidationError::NestedPath(
            relative_path.to_path_buf(),
        ))?
    }
    Ok(full_path)
}

fn is_graph(path: &Path) -> bool {
    path.join(META_PATH).is_file()
}

pub(crate) fn ensure_clean_folder(base_path: &Path) -> Result<(), InternalPathValidationError> {
    if base_path.is_dir() {
        match read_dirty_path(base_path) {
            Ok(path) => {
                if let Some(path) = path {
                    warn!("Found dirty path {path}, cleaning...");
                    fs::remove_dir_all(base_path.join(path))?;
                }
            }
            Err(error) => {
                warn!("Found dirty file with invalid path: {error}, cleaning...")
            }
        }
        match fs::remove_file(base_path.join(DIRTY_PATH)) {
            Ok(_) => {}
            Err(err) => match err.kind() {
                ErrorKind::NotFound => {}
                _ => Err(err)?,
            },
        };
    }
    Ok(())
}

/// Mark path as dirty
/// - ensure parent is clean
/// - create dirty file and fsync it
pub(crate) fn mark_dirty(path: &Path) -> Result<PathBuf, InternalPathValidationError> {
    let cleanup_path = path
        .file_name()
        .ok_or(InternalPathValidationError::MissingParent)?
        .to_str()
        .ok_or(InternalPathValidationError::NonUTFCharacters)?
        .to_string();
    let parent = path
        .parent()
        .ok_or(InternalPathValidationError::MissingParent)?;
    ensure_clean_folder(parent)?;
    let dirty_file_path = parent.join(DIRTY_PATH);
    let mut dirty_file = File::create_new(&dirty_file_path)?;
    dirty_file.write_all(&serde_json::to_vec(&RelativePath { path: cleanup_path })?)?;
    // make sure the dirty path is properly recorded before we proceed!
    dirty_file.sync_all()?;
    Ok(dirty_file_path)
}

impl GraphPaths for ValidGraphFolder {
    fn root(&self) -> &Path {
        self.graph_folder.root()
    }

    fn data_path(&self) -> Result<InnerGraphFolder, GraphError> {
        self.graph_folder.data_path()
    }
}

impl ValidGraphPaths for ValidGraphFolder {
    fn local_path(&self) -> &str {
        &self.local_path
    }
}

impl ValidGraphFolder {
    fn with_internal_errors<V>(
        &self,
        map: impl FnOnce() -> Result<V, InternalPathValidationError>,
    ) -> Result<V, PathValidationError> {
        map().with_path(self.local_path())
    }

    pub fn graph_folder(&self) -> &GraphFolder {
        &self.graph_folder
    }
    pub fn created(&self) -> Result<i64, PathValidationError> {
        self.with_internal_errors(|| {
            Ok(self.root_meta_path().metadata()?.created()?.to_millis()?)
        })
    }

    pub fn last_opened(&self) -> Result<i64, PathValidationError> {
        self.with_internal_errors(|| {
            Ok(fs::metadata(self.graph_folder.meta_path()?)?
                .accessed()?
                .to_millis()?)
        })
    }

    pub fn last_updated(&self) -> Result<i64, PathValidationError> {
        self.with_internal_errors(|| {
            Ok(fs::metadata(self.meta_path()?)?.modified()?.to_millis()?)
        })
    }

    pub async fn created_async(&self) -> Result<i64, PathValidationError> {
        let cloned = self.clone();
        blocking_io(move || cloned.created()).await
    }

    pub async fn last_opened_async(&self) -> Result<i64, PathValidationError> {
        let cloned = self.clone();
        blocking_io(move || cloned.last_opened()).await
    }

    pub async fn last_updated_async(&self) -> Result<i64, PathValidationError> {
        let cloned = self.clone();
        blocking_io(move || cloned.last_updated()).await
    }

    pub async fn read_metadata_async(&self) -> Result<GraphMetadata, PathValidationError> {
        let folder: GraphFolder = self.graph_folder.clone();
        blocking_compute(move || folder.read_metadata())
            .await
            .with_path(self.local_path())
    }

    /// This returns the PathBuf used to build multiple GraphError types
    pub fn to_error_path(&self) -> PathBuf {
        self.local_path.to_owned().into()
    }

    pub fn get_graph_name(&self) -> Result<String, PathValidationError> {
        let path: &Path = self.local_path.as_ref();
        let last_component: Component = path
            .components()
            .last()
            .ok_or_else(|| InvalidPathReason::PathNotParsable(self.to_error_path()))?;
        let name = match last_component {
            Component::Normal(value) => value
                .to_str()
                .map(|s| s.to_string())
                .ok_or_else(|| InvalidPathReason::PathNotParsable(self.to_error_path()))?,
            Component::Prefix(_)
            | Component::RootDir
            | Component::CurDir
            | Component::ParentDir => {
                Err(InvalidPathReason::PathNotParsable(self.to_error_path()))?
            }
        };
        Ok(name)
    }
    pub(crate) fn as_existing(&self) -> Result<ExistingGraphFolder, PathValidationError> {
        if self.graph_folder.is_reserved() {
            Ok(ExistingGraphFolder(self.clone()))
        } else {
            Err(PathValidationError::GraphNotExistsError(
                self.local_path.clone(),
            ))
        }
    }
}

trait ToMillis {
    fn to_millis(&self) -> Result<i64, GraphError>;
}
impl ToMillis for SystemTime {
    fn to_millis(&self) -> Result<i64, GraphError> {
        Ok(self.duration_since(UNIX_EPOCH)?.as_millis() as i64)
    }
}
