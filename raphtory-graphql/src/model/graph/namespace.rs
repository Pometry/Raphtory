use crate::{
    data::{get_relative_path, Data},
    model::graph::meta_graph::MetaGraph,
    paths::{ExistingGraphFolder, ValidGraphFolder},
};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use std::path::PathBuf;
use walkdir::WalkDir;

#[derive(ResolvedObject)]
pub(crate) struct Namespace {
    base_dir: PathBuf,
    current_dir: PathBuf,
}

impl Namespace {
    pub fn new(base_dir: PathBuf, current_dir: PathBuf) -> Self {
        Self {
            base_dir,
            current_dir,
        }
    }

    fn get_all_graph_folders(&self) -> Vec<ExistingGraphFolder> {
        let base_path = self.base_dir.clone();
        WalkDir::new(&self.current_dir)
            .max_depth(1)
            .into_iter()
            .filter_map(|e| {
                let entry = e.ok()?;
                let path = entry.path();
                let relative = get_relative_path(base_path.clone(), path).ok()?;
                let relative_str = relative.to_str()?; // potential UTF8 error here
                let cleaned = relative_str.replace(r"\", "/");
                let folder = ExistingGraphFolder::try_from(base_path.clone(), &cleaned).ok()?;
                Some(folder)
            })
            .collect()
    }
}

#[ResolvedObjectFields]
impl Namespace {
    async fn graphs(&self) -> Vec<MetaGraph> {
        self.get_all_graph_folders()
            .iter()
            .map(|g| MetaGraph::new(g.clone()))
            .collect()
    }
    async fn path(&self) -> Option<String> {
        let relative = get_relative_path(self.base_dir.clone(), self.current_dir.as_path()).ok()?;
        let relative_str = relative.to_str()?; // potential UTF8 error here
        Some(relative_str.replace(r"\", "/"))
    }

    async fn parent(&self) -> Option<Namespace> {
        let parent = self.current_dir.parent()?.to_path_buf();
        if parent.starts_with(&self.base_dir) {
            Some(Namespace::new(self.base_dir.clone(), parent))
        } else {
            None
        }
    }

    async fn children(&self) -> Vec<Namespace> {
        WalkDir::new(&self.current_dir)
            .max_depth(1)
            .into_iter()
            .filter_map(|e| {
                let entry = e.ok()?;
                let path = entry.path();
                if path.is_dir() {
                    if path.join(".raph").exists() || path.is_symlink() || path == self.current_dir
                    {
                        //is graph dir, escapes folder or is the top level dir
                        None
                    } else {
                        Some(Namespace::new(self.base_dir.clone(), path.to_path_buf()))
                    }
                } else {
                    None
                }
            })
            .collect()
    }
}
