use crate::{
    auth::Access,
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
    policy: &Option<Arc<dyn AuthorizationPolicy>>,
    is_admin: bool,
    role: Option<&str>,
    g: &MetaGraph,
) -> bool {
    policy.as_ref().map_or(true, |p| {
        p.graph_permissions(is_admin, role, &g.local_path()).is_ok()
    })
}

fn is_namespace_visible(
    policy: &Option<Arc<dyn AuthorizationPolicy>>,
    is_admin: bool,
    role: Option<&str>,
    n: &Namespace,
) -> bool {
    policy.as_ref().map_or(true, |p| {
        p.namespace_permissions(is_admin, role, &n.relative_path) >= NamespacePermission::Discover
    })
}

#[ResolvedObjectFields]
impl Namespace {
    async fn graphs(&self, ctx: &Context<'_>) -> GqlCollection<MetaGraph> {
        let data = ctx.data_unchecked::<Data>();
        let is_admin = ctx.data::<Access>().is_ok_and(|a| a == &Access::Rw);
        let role: Option<String> = ctx.data::<Option<String>>().ok().and_then(|r| r.clone());
        let policy = data.auth_policy.clone();
        let self_clone = self.clone();
        blocking_compute(move || {
            GqlCollection::new(
                self_clone
                    .get_children()
                    .filter_map(|g| match g {
                        NamespacedItem::MetaGraph(g)
                            if is_graph_visible(&policy, is_admin, role.as_deref(), &g) =>
                        {
                            Some(g)
                        }
                        _ => None,
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

    async fn children(&self, ctx: &Context<'_>) -> GqlCollection<Namespace> {
        let data = ctx.data_unchecked::<Data>();
        let is_admin = ctx.data::<Access>().is_ok_and(|a| a == &Access::Rw);
        let role: Option<String> = ctx.data::<Option<String>>().ok().and_then(|r| r.clone());
        let policy = data.auth_policy.clone();
        let self_clone = self.clone();
        blocking_compute(move || {
            GqlCollection::new(
                self_clone
                    .get_children()
                    .filter_map(|item| match item {
                        NamespacedItem::Namespace(n)
                            if is_namespace_visible(&policy, is_admin, role.as_deref(), &n) =>
                        {
                            Some(n)
                        }
                        _ => None,
                    })
                    .sorted()
                    .collect(),
            )
        })
        .await
    }

    // Fetch the collection of namespaces/graphs in this namespace.
    // Namespaces will be listed before graphs.
    async fn items(&self, ctx: &Context<'_>) -> GqlCollection<NamespacedItem> {
        let data = ctx.data_unchecked::<Data>();
        let is_admin = ctx.data::<Access>().is_ok_and(|a| a == &Access::Rw);
        let role: Option<String> = ctx.data::<Option<String>>().ok().and_then(|r| r.clone());
        let policy = data.auth_policy.clone();
        let self_clone = self.clone();
        blocking_compute(move || {
            GqlCollection::new(
                self_clone
                    .get_children()
                    .filter(|item| match item {
                        NamespacedItem::MetaGraph(g) => {
                            is_graph_visible(&policy, is_admin, role.as_deref(), g)
                        }
                        NamespacedItem::Namespace(n) => {
                            is_namespace_visible(&policy, is_admin, role.as_deref(), n)
                        }
                    })
                    .sorted()
                    .collect(),
            )
        })
        .await
    }
}
