use std::{
    ops::Deref,
    path::{Component, Path, PathBuf},
};

use raphtory::core::utils::errors::InvalidPathReason::*;
use raphtory::{core::utils::errors::GraphError, prelude::*};
use raphtory::{core::utils::errors::InvalidPathReason, serialise::GraphFolder};

#[derive(Clone)]
pub(crate) struct ExistingGraphFolder {
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

// const VECTORS_FILE_NAME: &str = "vectors";

// struct VectorsPath(PathBuf);

// impl VectorsPath {
//     fn path(&self) -> &Path {
//         &self.0
//     }
// }

impl ExistingGraphFolder {
    pub(crate) fn try_from(base_path: PathBuf, relative_path: &str) -> Result<Self, GraphError> {
        let graph_folder = ValidGraphFolder::try_from(base_path, relative_path)?;
        if graph_folder.get_graph_path().exists() {
            Ok(Self {
                folder: graph_folder,
            })
        } else {
            // TODO: review if it is ok using GraphError::GraphNotFound instead of PathDoesNotExist here
            Err(GraphError::GraphNotFound(graph_folder.to_error_path()))
        }
    }

    // pub(crate) fn get_vector_path_to_write(&self) -> VectorsPath {
    //     VectorsPath(self.base_path.join(VECTORS_FILE_NAME))
    // }

    pub(crate) fn get_graph_name(&self) -> Result<String, GraphError> {
        let path = &self.get_base_path();
        let last_component: Component = path
            .components()
            .last()
            .ok_or_else(|| GraphError::from(PathNotParsable(self.to_error_path())))?;
        match last_component {
            Component::Normal(value) => value
                .to_str()
                .map(|s| s.to_string())
                .ok_or(GraphError::from(PathNotParsable(self.to_error_path()))),
            Component::Prefix(_)
            | Component::RootDir
            | Component::CurDir
            | Component::ParentDir => Err(GraphError::from(PathNotParsable(self.to_error_path()))),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ValidGraphFolder {
    // TODO: make this not public so that we guarantee calling try_from is the only way of creating this struct
    original_path: String,
    folder: GraphFolder,
    // pub(crate) user_facing_path: PathBuf,
    // pub(crate) base_path: PathBuf,
    // pub(crate) graph_path: PathBuf, // TODO: maybe these two can always be inferred from base_path
    // pub(crate) vectors_path: PathBuf,
}

impl Deref for ValidGraphFolder {
    type Target = GraphFolder;

    fn deref(&self) -> &Self::Target {
        &self.folder
    }
}

impl ValidGraphFolder {
    pub(crate) fn try_from(
        base_path: PathBuf,
        relative_path: &str,
    ) -> Result<Self, InvalidPathReason> {
        let user_facing_path = PathBuf::from(relative_path);
        // check for errors in the path
        //additionally ban any backslash
        if relative_path.contains(r"\") {
            return Err(BackslashError(user_facing_path));
        }
        if relative_path.contains(r"//") {
            return Err(DoubleForwardSlash(user_facing_path));
        }

        let mut full_path = base_path;
        // fail if any component is a Prefix (C://), tries to access root,
        // tries to access a parent dir or is a symlink which could break out of the working dir
        for component in user_facing_path.components() {
            match component {
                Component::Prefix(_) => return Err(RootNotAllowed(user_facing_path)),
                Component::RootDir => return Err(RootNotAllowed(user_facing_path)),
                Component::CurDir => return Err(CurDirNotAllowed(user_facing_path)),
                Component::ParentDir => return Err(ParentDirNotAllowed(user_facing_path)),
                Component::Normal(component) => {
                    //check for symlinks
                    full_path.push(component);
                    if full_path.is_symlink() {
                        return Err(SymlinkNotAllowed(user_facing_path));
                    }
                }
            }
        }
        Ok(Self {
            original_path: relative_path.to_owned(),
            folder: GraphFolder::from(full_path),
        })
    }

    pub(crate) fn get_original_path_str(&self) -> &str {
        &self.original_path
    }

    pub(crate) fn get_original_path(&self) -> &Path {
        &Path::new(&self.original_path)
    }

    /// This returns the PathBuf used to build multiple GraphError types
    pub(crate) fn to_error_path(&self) -> PathBuf {
        self.original_path.to_owned().into()
    }
}
