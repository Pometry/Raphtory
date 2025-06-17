use crate::model::graph::namespaced_item::NamespacedItem;
use crate::model::graph::namespaced_items::NamespacedItems;
use crate::{
    data::get_relative_path,
    model::graph::{meta_graph::MetaGraph, meta_graphs::MetaGraphs, namespaces::Namespaces},
    paths::{valid_path, ExistingGraphFolder},
};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use itertools::Itertools;
use raphtory::errors::InvalidPathReason;
use std::path::PathBuf;
use tokio::task::spawn_blocking;
use walkdir::WalkDir;

#[derive(ResolvedObject, Clone, PartialOrd, PartialEq, Ord, Eq)]
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

    fn get_all_children(&self) -> impl Iterator<Item = NamespacedItem> + use<'_> {
        WalkDir::new(&self.current_dir)
            .max_depth(1)
            .into_iter()
            .flatten()
            .filter_map(|entry| {
                let path = entry.path();
                let file_name = entry.file_name().to_str()?;
                if path.is_dir() {
                    if path != self.current_dir
                        && valid_path(self.current_dir.clone(), file_name, true).is_ok()
                    {
                        Some(NamespacedItem::Namespace(Namespace::new(
                            self.base_dir.clone(),
                            path.to_path_buf(),
                        )))
                    } else {
                        let base_path = self.base_dir.clone();
                        let relative = get_relative_path(base_path.clone(), path, false).ok()?;
                        let folder =
                            ExistingGraphFolder::try_from(base_path.clone(), &relative).ok()?;
                        Some(NamespacedItem::MetaGraph(MetaGraph::new(folder)))
                    }
                } else {
                    None
                }
            })
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
            .collect()
    }
}

#[ResolvedObjectFields]
impl Namespace {
    async fn graphs(&self) -> MetaGraphs {
        let self_clone = self.clone();
        spawn_blocking(move || {
            MetaGraphs::new(
                self_clone
                    .get_all_children()
                    .into_iter()
                    .filter_map(|g| match g {
                        NamespacedItem::MetaGraph(g) => Some(g),
                        NamespacedItem::Namespace(_) => None,
                    })
                    .sorted()
                    .collect(),
            )
        })
        .await
        .unwrap()
    }
    async fn path(&self) -> Result<String, InvalidPathReason> {
        let self_clone = self.clone();
        spawn_blocking(move || {
            get_relative_path(
                self_clone.base_dir.clone(),
                self_clone.current_dir.as_path(),
                true,
            )
        })
        .await
        .unwrap()
    }

    async fn parent(&self) -> Option<Namespace> {
        let self_clone = self.clone();
        spawn_blocking(move || {
            let parent = self_clone.current_dir.parent()?.to_path_buf();
            if parent.starts_with(&self_clone.base_dir) {
                Some(Namespace::new(self_clone.base_dir.clone(), parent))
            } else {
                None
            }
        })
        .await
        .unwrap()
    }

    async fn children(&self) -> Namespaces {
        let self_clone = self.clone();
        spawn_blocking(move || {
            Namespaces::new(
                self_clone
                    .get_all_children()
                    .filter_map(|item| match item {
                        NamespacedItem::MetaGraph(_) => None,
                        NamespacedItem::Namespace(n) => Some(n),
                    })
                    .sorted()
                    .collect(),
            )
        })
        .await
        .unwrap()
    }

    async fn items(&self) -> NamespacedItems {
        let self_clone = self.clone();
        spawn_blocking(move || NamespacedItems {
            items: self_clone.get_all_children().sorted().collect(),
        })
        .await
        .unwrap()
    }
}
