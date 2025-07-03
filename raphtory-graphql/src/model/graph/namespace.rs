use crate::{
    data::get_relative_path,
    model::graph::{collection::GqlCollection, meta_graph::MetaGraph},
    paths::{valid_path, ExistingGraphFolder, ValidGraphFolder},
    rayon::blocking_compute,
};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use itertools::Itertools;
use raphtory::errors::InvalidPathReason;
use std::path::PathBuf;
use walkdir::WalkDir;

#[derive(ResolvedObject, Clone, Ord, Eq, PartialEq, PartialOrd)]
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

    pub(crate) fn get_all_namespaces(&self) -> Vec<Namespace> {
        let base_path = self.base_dir.clone();
        WalkDir::new(&self.current_dir)
            .into_iter()
            .filter_map(|e| {
                let entry = e.ok()?;
                let path = entry.path();
                if path.is_dir() && get_relative_path(base_path.clone(), path, true).is_ok() {
                    Some(Namespace::new(self.base_dir.clone(), path.to_path_buf()))
                } else {
                    None
                }
            })
            .sorted()
            .collect()
    }
}

#[ResolvedObjectFields]
impl Namespace {
    async fn graphs(&self) -> GqlCollection<MetaGraph> {
        let self_clone = self.clone();
        blocking_compute(move || {
            GqlCollection::new(
                self_clone
                    .get_all_graph_folders()
                    .into_iter()
                    .sorted_by(|a, b| {
                        let a_as_valid_folder: ValidGraphFolder = a.clone().into();
                        let b_as_valid_folder: ValidGraphFolder = b.clone().into();
                        a_as_valid_folder
                            .get_original_path_str()
                            .cmp(b_as_valid_folder.get_original_path_str())
                    })
                    .map(|g| MetaGraph::new(g.clone()))
                    .collect(),
            )
        })
        .await
    }
    async fn path(&self) -> Result<String, InvalidPathReason> {
        get_relative_path(self.base_dir.clone(), self.current_dir.as_path(), true)
    }

    async fn parent(&self) -> Option<Namespace> {
        let parent = self.current_dir.parent()?.to_path_buf();
        if parent.starts_with(&self.base_dir) {
            Some(Namespace::new(self.base_dir.clone(), parent))
        } else {
            None
        }
    }

    async fn children(&self) -> GqlCollection<Namespace> {
        let self_clone = self.clone();
        blocking_compute(move || {
            GqlCollection::new(
                WalkDir::new(&self_clone.current_dir)
                    .max_depth(1)
                    .into_iter()
                    .filter_map(|e| {
                        let entry = e.ok()?;
                        let file_name = entry.file_name().to_str()?;
                        let path = entry.path();
                        if path.is_dir()
                            && path != self_clone.current_dir
                            && valid_path(self_clone.current_dir.clone(), file_name, true).is_ok()
                        {
                            Some(Namespace::new(
                                self_clone.base_dir.clone(),
                                path.to_path_buf(),
                            ))
                        } else {
                            None
                        }
                    })
                    .sorted()
                    .collect(),
            )
        })
        .await
    }
}
