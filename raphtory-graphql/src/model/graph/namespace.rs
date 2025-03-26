use crate::{
    data::get_relative_path,
    model::graph::meta_graph::MetaGraph,
    paths::{valid_path, ExistingGraphFolder},
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
                let relative = get_relative_path(base_path.clone(), path, false).ok()?;
                let folder = ExistingGraphFolder::try_from(base_path.clone(), &relative).ok()?;
                Some(folder)
            })
            .collect()
    }

    pub(crate) fn get_all_children(&self) -> Vec<Namespace> {
        WalkDir::new(&self.current_dir)
            .into_iter()
            .filter_map(|e| {
                let entry = e.ok()?;
                let file_name = entry.file_name().to_str()?;
                let path = entry.path();
                if path.is_dir()
                    && path != self.current_dir
                    && valid_path(self.current_dir.clone(), file_name, true).is_ok()
                {
                    Some(Namespace::new(self.base_dir.clone(), path.to_path_buf()))
                } else {
                    None
                }
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
        get_relative_path(self.base_dir.clone(), self.current_dir.as_path(), true)
            .ok()
            .map(|s| s.to_string())
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
                let file_name = entry.file_name().to_str()?;
                let path = entry.path();
                if path.is_dir()
                    && path != self.current_dir
                    && valid_path(self.current_dir.clone(), file_name, true).is_ok()
                {
                    Some(Namespace::new(self.base_dir.clone(), path.to_path_buf()))
                } else {
                    None
                }
            })
            .collect()
    }
}
