use crate::{
    data::get_relative_path,
    model::graph::{
        collection::GqlCollection, meta_graph::MetaGraph, namespaced_item::NamespacedItem,
    },
    paths::{
        valid_path, valid_relative_graph_path, ExistingGraphFolder, InternalPathValidationError,
        PathValidationError, ValidPath,
    },
    rayon::blocking_compute,
};
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use itertools::{join, Itertools};
use raphtory::errors::InvalidPathReason;
use std::path::PathBuf;
use walkdir::WalkDir;

#[derive(ResolvedObject, Clone, Ord, Eq, PartialEq, PartialOrd)]
pub(crate) struct Namespace {
    current_dir: PathBuf,  // always validated
    relative_path: String, // relative to the root working directory
}

pub struct NamespaceIter {
    it: walkdir::IntoIter,
    root: Namespace,
}

impl Iterator for NamespaceIter {
    type Item = NamespacedItem;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.it.next() {
                None => return None,
                Some(Ok(entry)) => {
                    let path = entry.path();
                    if path.is_dir() {
                        match get_relative_path(&self.root.current_dir, path) {
                            Ok(relative) => {
                                match self.root.try_new_child(&relative) {
                                    Ok(child) => {
                                        match &child {
                                            NamespacedItem::Namespace(_) => {}
                                            NamespacedItem::MetaGraph(_) => {
                                                self.it.skip_current_dir() // graphs should not be traversed further
                                            }
                                        }
                                        return Some(child);
                                    }
                                    Err(_) => {
                                        self.it.skip_current_dir() // not a valid path
                                    }
                                }
                            }
                            Err(_) => {
                                self.it.skip_current_dir() // not a valid path and shouldn't be traversed further}
                            }
                        }
                    }
                }
                _ => {} // skip errors
            };
        }
    }
}

impl Namespace {
    pub fn root(root: PathBuf) -> Self {
        Self {
            current_dir: root,
            relative_path: "".to_owned(),
        }
    }

    pub fn try_new(root: PathBuf, relative_path: String) -> Result<Self, PathValidationError> {
        let current_dir = valid_path(root, relative_path.as_str())?;
        Self::try_from_valid(current_dir, &relative_path)
    }

    /// Create a namespace from a valid path if it exists and is a namespace
    pub fn try_from_valid(
        current_dir: ValidPath,
        relative_path: impl Into<String>,
    ) -> Result<Self, PathValidationError> {
        if current_dir.is_namespace() {
            Ok(Self {
                current_dir: current_dir.into_path(),
                relative_path: relative_path.into(),
            })
        } else {
            Err(PathValidationError::NamespaceDoesNotExist(
                relative_path.into(),
            ))
        }
    }

    pub fn new_child_namespace(&self, relative_path: &str) -> Result<Self, PathValidationError> {
        let current_dir = valid_path(self.current_dir.clone(), relative_path)?;
        let relative_path = [&self.relative_path, relative_path].join("/");
        Self::try_from_valid(current_dir, relative_path)
    }

    pub fn try_new_child(&self, file_name: &str) -> Result<NamespacedItem, PathValidationError> {
        let current_dir = valid_path(self.current_dir.clone(), file_name)?;
        let relative_path = if self.relative_path.is_empty() {
            file_name.to_owned()
        } else {
            [&self.relative_path, file_name].join("/")
        };
        let child = if current_dir.is_namespace() {
            NamespacedItem::Namespace(Self::try_from_valid(current_dir, relative_path)?)
        } else {
            NamespacedItem::MetaGraph(MetaGraph::new(ExistingGraphFolder::try_from_valid(
                current_dir,
                &relative_path,
            )?))
        };
        Ok(child)
    }

    /// Non-recursively list children
    pub fn get_children(&self) -> impl Iterator<Item = NamespacedItem> + use<'_> {
        WalkDir::new(&self.current_dir)
            .min_depth(1)
            .max_depth(1)
            .into_iter()
            .flatten()
            .filter_map(|entry| {
                let path = entry.path();
                if path.is_dir() {
                    let file_name = entry.file_name().to_str()?;
                    self.try_new_child(file_name).ok()
                } else {
                    None
                }
            })
    }

    /// Recursively list all children
    pub fn get_all_children(&self) -> impl Iterator<Item = NamespacedItem> {
        let it = WalkDir::new(&self.current_dir).into_iter();
        let root = self.clone();
        NamespaceIter { it, root }
    }
}

#[ResolvedObjectFields]
impl Namespace {
    async fn graphs(&self) -> GqlCollection<MetaGraph> {
        let self_clone = self.clone();
        blocking_compute(move || {
            GqlCollection::new(
                self_clone
                    .get_children()
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
    }
    async fn path(&self) -> String {
        self.relative_path.clone()
    }

    async fn parent(&self) -> Option<Namespace> {
        if self.relative_path.is_empty() {
            None
        } else {
            let parent = self.current_dir.parent()?.to_path_buf();
            let relative_path = self
                .relative_path
                .rsplit_once("/")
                .map_or("", |(parent, _)| parent);
            Some(Self {
                current_dir: parent,
                relative_path: relative_path.to_owned(),
            })
        }
    }

    async fn children(&self) -> GqlCollection<Namespace> {
        let self_clone = self.clone();
        blocking_compute(move || {
            GqlCollection::new(
                self_clone
                    .get_children()
                    .filter_map(|item| match item {
                        NamespacedItem::MetaGraph(_) => None,
                        NamespacedItem::Namespace(n) => Some(n),
                    })
                    .sorted()
                    .collect(),
            )
        })
        .await
    }

    // Fetch the collection of namespaces/graphs in this namespace.
    // Namespaces will be listed before graphs.
    async fn items(&self) -> GqlCollection<NamespacedItem> {
        let self_clone = self.clone();
        blocking_compute(move || GqlCollection::new(self_clone.get_children().sorted().collect()))
            .await
    }
}
