use crate::{
    auth_policy::{AuthorizationPolicy, NamespacePermission},
    data::{get_relative_path, Data},
    model::graph::{
        collection::GqlCollection, meta_graph::MetaGraph, namespaced_item::NamespacedItem,
    },
    paths::{ExistingGraphFolder, PathValidationError, ValidPath},
    rayon::blocking_compute,
};
use async_graphql::Context;
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields};
use itertools::Itertools;
use std::{path::PathBuf, sync::Arc};
use walkdir::WalkDir;

/// A directory-like container for graphs and nested namespaces. Graphs are
/// addressed by path (e.g. `"team/project/graph"`), and every segment except
/// the last is a namespace. Use to browse what's stored on the server without
/// loading any graph data.
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
        let current_dir = ValidPath::try_new(root, relative_path.as_str())?;
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

    pub fn try_new_child(&self, file_name: &str) -> Result<NamespacedItem, PathValidationError> {
        let current_dir = ValidPath::try_new(self.current_dir.clone(), file_name)?;
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

fn is_graph_visible(
    ctx: &Context<'_>,
    policy: &Option<Arc<dyn AuthorizationPolicy>>,
    g: &MetaGraph,
) -> bool {
    policy
        .as_ref()
        .map_or(true, |p| p.graph_permissions(ctx, &g.local_path()).is_ok())
}

fn is_namespace_visible(
    ctx: &Context<'_>,
    policy: &Option<Arc<dyn AuthorizationPolicy>>,
    n: &Namespace,
) -> bool {
    policy.as_ref().map_or(true, |p| {
        p.namespace_permissions(ctx, &n.relative_path) >= NamespacePermission::Discover
    })
}

#[ResolvedObjectFields]
impl Namespace {
    /// Graphs directly inside this namespace (excludes graphs in nested
    /// namespaces). Filtered by the caller's permissions — only graphs the
    /// caller is allowed to see are returned.
    async fn graphs(&self, ctx: &Context<'_>) -> GqlCollection<MetaGraph> {
        let data = ctx.data_unchecked::<Data>();
        let self_clone = self.clone();
        let items = blocking_compute(move || self_clone.get_children().collect::<Vec<_>>()).await;
        GqlCollection::new(
            items
                .into_iter()
                .filter_map(|item| match item {
                    NamespacedItem::MetaGraph(g)
                        if is_graph_visible(ctx, &data.auth_policy, &g) =>
                    {
                        Some(g)
                    }
                    _ => None,
                })
                .sorted()
                .collect(),
        )
    }
    /// Path of this namespace relative to the root namespace. Empty string for
    /// the root namespace itself.
    async fn path(&self) -> String {
        self.relative_path.clone()
    }

    /// Parent namespace, or null at the root.
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

    /// Sub-namespaces directly inside this one (one level down, not recursive).
    /// Filtered by permissions.
    async fn children(&self, ctx: &Context<'_>) -> GqlCollection<Namespace> {
        let data = ctx.data_unchecked::<Data>();
        let self_clone = self.clone();
        let items = blocking_compute(move || self_clone.get_children().collect::<Vec<_>>()).await;
        GqlCollection::new(
            items
                .into_iter()
                .filter_map(|item| match item {
                    NamespacedItem::Namespace(n)
                        if is_namespace_visible(ctx, &data.auth_policy, &n) =>
                    {
                        Some(n)
                    }
                    _ => None,
                })
                .sorted()
                .collect(),
        )
    }

    /// Everything in this namespace — sub-namespaces and graphs — as a single
    /// heterogeneous collection. Sub-namespaces are listed before graphs.
    /// Filtered by permissions.
    async fn items(&self, ctx: &Context<'_>) -> GqlCollection<NamespacedItem> {
        let data = ctx.data_unchecked::<Data>();
        let self_clone = self.clone();
        let all_items =
            blocking_compute(move || self_clone.get_children().collect::<Vec<_>>()).await;
        GqlCollection::new(
            all_items
                .into_iter()
                .filter(|item| match item {
                    NamespacedItem::MetaGraph(g) => is_graph_visible(ctx, &data.auth_policy, g),
                    NamespacedItem::Namespace(n) => is_namespace_visible(ctx, &data.auth_policy, n),
                })
                .sorted()
                .collect(),
        )
    }
}
