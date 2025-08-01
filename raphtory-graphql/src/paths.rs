use crate::rayon::blocking_compute;
use raphtory::{
    errors::{GraphError, InvalidPathReason},
    serialise::{metadata::GraphMetadata, GraphFolder},
};
use std::{
    fs,
    ops::Deref,
    path::{Component, Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Clone, Debug, PartialOrd, PartialEq, Ord, Eq)]
pub struct ExistingGraphFolder {
    folder: ValidGraphFolder,
}

impl Deref for ExistingGraphFolder {
    type Target = ValidGraphFolder;

    fn deref(&self) -> &Self::Target {
        &self.folder
    }
}

impl From<ValidGraphFolder> for GraphFolder {
    fn from(value: ValidGraphFolder) -> Self {
        value.folder
    }
}

impl From<ExistingGraphFolder> for GraphFolder {
    fn from(value: ExistingGraphFolder) -> Self {
        value.folder.folder
    }
}
impl ExistingGraphFolder {
    pub(crate) fn try_from(base_path: PathBuf, relative_path: &str) -> Result<Self, GraphError> {
        let graph_folder = ValidGraphFolder::try_from(base_path, relative_path)?;
        if graph_folder.get_meta_path().exists() {
            Ok(Self {
                folder: graph_folder,
            })
        } else {
            Err(GraphError::GraphNotFound(graph_folder.to_error_path()))
        }
    }

    pub(crate) fn get_graph_name(&self) -> Result<String, GraphError> {
        let path = &self.get_base_path();
        let last_component: Component = path.components().last().ok_or_else(|| {
            GraphError::from(InvalidPathReason::PathNotParsable(self.to_error_path()))
        })?;
        match last_component {
            Component::Normal(value) => {
                value
                    .to_str()
                    .map(|s| s.to_string())
                    .ok_or(GraphError::from(InvalidPathReason::PathNotParsable(
                        self.to_error_path(),
                    )))
            }
            Component::Prefix(_)
            | Component::RootDir
            | Component::CurDir
            | Component::ParentDir => Err(GraphError::from(InvalidPathReason::PathNotParsable(
                self.to_error_path(),
            ))),
        }
    }
}

#[derive(Clone, Debug, PartialOrd, PartialEq, Ord, Eq)]
pub struct ValidGraphFolder {
    folder: GraphFolder,
    original_path: String,
}

impl From<ExistingGraphFolder> for ValidGraphFolder {
    fn from(value: ExistingGraphFolder) -> Self {
        value.folder
    }
}

impl Deref for ValidGraphFolder {
    type Target = GraphFolder;

    fn deref(&self) -> &Self::Target {
        &self.folder
    }
}

pub(crate) fn valid_path(
    base_path: PathBuf,
    relative_path: &str,
    namespace: bool,
) -> Result<PathBuf, InvalidPathReason> {
    let user_facing_path = PathBuf::from(relative_path);

    if relative_path.contains(r"//") {
        return Err(InvalidPathReason::DoubleForwardSlash(user_facing_path));
    }
    if relative_path.contains(r"\") {
        return Err(InvalidPathReason::BackslashError(user_facing_path));
    }

    let mut full_path = base_path.clone();
    // fail if any component is a Prefix (C://), tries to access root,
    // tries to access a parent dir or is a symlink which could break out of the working dir
    for component in user_facing_path.components() {
        match component {
            Component::Prefix(_) => {
                return Err(InvalidPathReason::RootNotAllowed(user_facing_path))
            }
            Component::RootDir => return Err(InvalidPathReason::RootNotAllowed(user_facing_path)),
            Component::CurDir => return Err(InvalidPathReason::CurDirNotAllowed(user_facing_path)),
            Component::ParentDir => {
                return Err(InvalidPathReason::ParentDirNotAllowed(user_facing_path))
            }
            Component::Normal(component) => {
                // check if some intermediate path is already a graph
                if full_path.join(".raph").exists() {
                    return Err(InvalidPathReason::ParentIsGraph(user_facing_path));
                }
                full_path.push(component);
                //check if the path with the component is a graph
                if namespace && full_path.join(".raph").exists() {
                    return Err(InvalidPathReason::ParentIsGraph(user_facing_path));
                }
                //check for symlinks
                if full_path.is_symlink() {
                    return Err(InvalidPathReason::SymlinkNotAllowed(user_facing_path));
                }
            }
        }
    }
    Ok(full_path)
}

impl ValidGraphFolder {
    pub(crate) fn try_from(
        base_path: PathBuf,
        relative_path: &str,
    ) -> Result<Self, InvalidPathReason> {
        let full_path = valid_path(base_path, relative_path, false)?;
        Ok(Self {
            original_path: relative_path.to_owned(),
            folder: GraphFolder::from(full_path),
        })
    }

    pub fn created(&self) -> Result<i64, GraphError> {
        fs::metadata(self.get_graph_path())?.created()?.to_millis()
    }

    pub fn last_opened(&self) -> Result<i64, GraphError> {
        fs::metadata(self.get_graph_path())?.accessed()?.to_millis()
    }

    pub fn last_updated(&self) -> Result<i64, GraphError> {
        fs::metadata(self.get_graph_path())?.modified()?.to_millis()
    }

    pub async fn created_async(&self) -> Result<i64, GraphError> {
        let metadata = tokio::fs::metadata(self.get_graph_path()).await?;
        metadata.created()?.to_millis()
    }

    pub async fn last_opened_async(&self) -> Result<i64, GraphError> {
        let metadata = tokio::fs::metadata(self.get_graph_path()).await?;
        metadata.accessed()?.to_millis()
    }

    pub async fn last_updated_async(&self) -> Result<i64, GraphError> {
        let metadata = tokio::fs::metadata(self.get_graph_path()).await?;
        metadata.modified()?.to_millis()
    }

    pub async fn read_metadata_async(&self) -> Result<GraphMetadata, GraphError> {
        let folder = self.folder.clone();
        blocking_compute(move || folder.read_metadata()).await
    }

    pub fn get_original_path_str(&self) -> &str {
        &self.original_path
    }

    pub fn get_original_path(&self) -> &Path {
        &Path::new(&self.original_path)
    }

    /// This returns the PathBuf used to build multiple GraphError types
    pub fn to_error_path(&self) -> PathBuf {
        self.original_path.to_owned().into()
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
