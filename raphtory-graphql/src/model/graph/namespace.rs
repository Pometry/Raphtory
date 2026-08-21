use crate::{
    auth_policy::AuthorizationPolicy,
    data::{get_relative_path, Data, WorkDirGuard},
    model::graph::{
        collection::GqlCollection,
        meta_graph::MetaGraph,
        namespace_filtering::{
            sort_graphs, MetaGraphFilter, MetaGraphSort, NamespaceFilter, NamespaceSort,
            NamespacedItemFilter,
        },
        namespaced_item::NamespacedItem,
    },
    paths::{ExistingGraphFolder, PathValidationError, ValidPath},
    rayon::blocking_compute,
};
use async_graphql::Context;
use dynamic_graphql::{ResolvedObject, ResolvedObjectFields, Result};
use itertools::Itertools;
use std::{cmp::Ordering, path::PathBuf, sync::Arc};
use walkdir::WalkDir;

/// A directory-like container for graphs and nested namespaces. Graphs are
/// addressed by path (e.g. `"team/project/graph"`), and every segment except
/// the last is a namespace. Use to browse what's stored on the server without
/// loading any graph data.
#[derive(ResolvedObject, Clone)]
pub struct Namespace {
    guard: WorkDirGuard,
    current_dir: PathBuf,  // always validated
    relative_path: String, // relative to the root working directory
}

impl Namespace {
    /// Path relative to the root working directory, as used by `path`.
    pub(crate) fn path_str(&self) -> &str {
        &self.relative_path
    }
}

impl PartialEq for Namespace {
    fn eq(&self, other: &Self) -> bool {
        self.current_dir == other.current_dir
    }
}

impl PartialOrd for Namespace {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.current_dir.partial_cmp(&other.current_dir)
    }
}

impl Eq for Namespace {}

impl Ord for Namespace {
    fn cmp(&self, other: &Self) -> Ordering {
        self.current_dir.cmp(&other.current_dir)
    }
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
    pub fn root(root: WorkDirGuard) -> Self {
        let current_dir = root.to_path_buf();
        Self {
            guard: root,
            current_dir,
            relative_path: "".to_owned(),
        }
    }

    pub fn local_path(&self) -> &str {
        &self.relative_path
    }

    pub fn try_new(root: WorkDirGuard, relative_path: String) -> Result<Self, PathValidationError> {
        let current_dir = ValidPath::try_new(root.to_path_buf(), relative_path.as_str())?;
        Self::try_from_valid(root, current_dir, &relative_path)
    }

    /// Create a namespace from a valid path if it exists and is a namespace
    pub fn try_from_valid(
        guard: WorkDirGuard,
        current_dir: ValidPath,
        relative_path: impl Into<String>,
    ) -> Result<Self, PathValidationError> {
        if current_dir.is_namespace() {
            Ok(Self {
                guard,
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
            NamespacedItem::Namespace(Self::try_from_valid(
                self.guard.clone(),
                current_dir,
                relative_path,
            )?)
        } else {
            NamespacedItem::MetaGraph(MetaGraph::new(ExistingGraphFolder::try_from_valid(
                self.guard.clone(),
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
        let it = WalkDir::new(&self.current_dir).min_depth(1).into_iter();
        let root = self.clone();
        NamespaceIter { it, root }
    }

    /// Recursively list self and all children.
    pub fn self_and_all_children(&self) -> impl Iterator<Item = NamespacedItem> {
        std::iter::once(NamespacedItem::Namespace(self.clone())).chain(self.get_all_children())
    }

    pub fn current_dir(&self) -> &std::path::Path {
        &self.current_dir
    }

    pub fn relative_path(&self) -> &str {
        &self.relative_path
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

pub(crate) fn is_namespace_visible(
    ctx: &Context<'_>,
    policy: &Option<Arc<dyn AuthorizationPolicy>>,
    n: &Namespace,
) -> bool {
    policy.as_ref().map_or(true, |p| {
        p.namespace_permissions(ctx, &n.relative_path).is_some()
    })
}

#[ResolvedObjectFields]
impl Namespace {
    /// Graphs directly inside this namespace (excludes graphs in nested
    /// namespaces). Filtered by the caller's permissions — only graphs the
    /// caller is allowed to see are returned.
    ///
    /// `filter` and `sort` are applied before the returned collection is paged,
    /// so `count` reflects the filtered total and `page` slices the sorted
    /// order.
    pub async fn graphs(
        &self,
        ctx: &Context<'_>,
        #[graphql(desc = "Restricts which graphs are listed.")] filter: Option<MetaGraphFilter>,
        #[graphql(desc = "Sort keys applied in order, before paging.")] sort: Option<
            Vec<MetaGraphSort>,
        >,
    ) -> Result<GqlCollection<MetaGraph>> {
        let data = ctx.data_unchecked::<Data>();
        let self_clone = self.clone();
        let items = blocking_compute(move || self_clone.get_children().collect::<Vec<_>>()).await;
        let visible = items.into_iter().filter_map(|item| match item {
            NamespacedItem::MetaGraph(g) if is_graph_visible(ctx, &data.auth_policy, &g) => Some(g),
            _ => None,
        });

        let mut graphs = match &filter {
            None => visible.collect::<Vec<_>>(),
            Some(filter) => {
                let mut kept = Vec::new();
                for graph in visible {
                    if filter.matches(&graph, ctx, data).await? {
                        kept.push(graph);
                    }
                }
                kept
            }
        };

        sort_graphs(&mut graphs, sort.as_deref(), ctx, data).await?;

        Ok(GqlCollection::new(graphs.into()))
    }
    /// Path of this namespace relative to the root namespace. Empty string for
    /// the root namespace itself.
    pub async fn path(&self) -> String {
        self.relative_path.clone()
    }

    /// Most recent `lastUpdated` across the graphs directly inside this
    /// namespace, or null when it holds none.
    ///
    /// Computed here so a listing can show a folder's recency without the client
    /// walking every graph in every folder it displays.
    async fn last_updated(&self, ctx: &Context<'_>) -> Result<Option<i64>> {
        let data = ctx.data_unchecked::<Data>();
        let self_clone = self.clone();
        let items = blocking_compute(move || self_clone.get_children().collect::<Vec<_>>()).await;
        let mut latest: Option<i64> = None;
        for item in items {
            if let NamespacedItem::MetaGraph(g) = item {
                if is_graph_visible(ctx, &data.auth_policy, &g) {
                    let updated = g.last_updated_value().await?;
                    latest = Some(latest.map_or(updated, |current: i64| current.max(updated)));
                }
            }
        }
        Ok(latest)
    }

    /// Parent namespace, or null at the root.
    pub async fn parent(&self) -> Option<Namespace> {
        if self.relative_path.is_empty() {
            None
        } else {
            let parent = self.current_dir.parent()?.to_path_buf();
            let relative_path = self
                .relative_path
                .rsplit_once("/")
                .map_or("", |(parent, _)| parent);
            let guard = self.guard.clone();
            Some(Self {
                guard,
                current_dir: parent,
                relative_path: relative_path.to_owned(),
            })
        }
    }

    /// Sub-namespaces directly inside this one (one level down, not recursive).
    /// Filtered by permissions.
    ///
    /// `filter` and `sort` are applied before the returned collection is paged.
    pub async fn children(
        &self,
        ctx: &Context<'_>,
        #[graphql(desc = "Restricts which sub-namespaces are listed.")] filter: Option<
            NamespaceFilter,
        >,
        #[graphql(desc = "Ordering applied before paging.")] sort: Option<NamespaceSort>,
    ) -> GqlCollection<Namespace> {
        let data = ctx.data_unchecked::<Data>();
        let self_clone = self.clone();
        let items = blocking_compute(move || self_clone.get_children().collect::<Vec<_>>()).await;
        let mut namespaces = items
            .into_iter()
            .filter_map(|item| match item {
                NamespacedItem::Namespace(n)
                    if is_namespace_visible(ctx, &data.auth_policy, &n) =>
                {
                    Some(n)
                }
                _ => None,
            })
            .filter(|n| filter.as_ref().map_or(true, |f| f.matches(n)))
            .sorted()
            .collect::<Vec<_>>();

        if sort.as_ref().and_then(|s| s.reverse) == Some(true) {
            namespaces.reverse();
        }

        GqlCollection::new(namespaces.into())
    }

    /// Everything in this namespace — sub-namespaces and graphs — as a single
    /// heterogeneous collection. Sub-namespaces are listed before graphs.
    /// Filtered by permissions.
    ///
    /// `filter` and `sort` are applied before the returned collection is paged.
    /// `sort` orders the graphs; sub-namespaces keep path order and stay ahead of
    /// them, so a client paging this collection walks folders before graphs
    /// regardless of how the graphs are ordered.
    pub async fn items(
        &self,
        ctx: &Context<'_>,
        #[graphql(desc = "Restricts which items are listed.")] filter: Option<NamespacedItemFilter>,
        #[graphql(desc = "Sort keys for the graphs, applied in order before paging.")] sort: Option<
            Vec<MetaGraphSort>,
        >,
    ) -> Result<GqlCollection<NamespacedItem>> {
        let data = ctx.data_unchecked::<Data>();
        let self_clone = self.clone();
        let all_items =
            blocking_compute(move || self_clone.get_children().collect::<Vec<_>>()).await;
        let visible = all_items.into_iter().filter(|item| match item {
            NamespacedItem::MetaGraph(g) => is_graph_visible(ctx, &data.auth_policy, g),
            NamespacedItem::Namespace(n) => is_namespace_visible(ctx, &data.auth_policy, n),
        });

        let mut namespaces = Vec::new();
        let mut graphs = Vec::new();
        for item in visible {
            if let Some(filter) = &filter {
                if !filter.matches(&item, ctx, data).await? {
                    continue;
                }
            }
            match item {
                NamespacedItem::Namespace(n) => namespaces.push(n),
                NamespacedItem::MetaGraph(g) => graphs.push(g),
            }
        }

        namespaces.sort();
        sort_graphs(&mut graphs, sort.as_deref(), ctx, data).await?;

        let items = namespaces
            .into_iter()
            .map(NamespacedItem::Namespace)
            .chain(graphs.into_iter().map(NamespacedItem::MetaGraph))
            .collect::<Vec<_>>();
        Ok(GqlCollection::new(items.into()))
    }
}
