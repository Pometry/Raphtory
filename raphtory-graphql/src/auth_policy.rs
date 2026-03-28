use crate::model::graph::filtering::GraphAccessFilter;

/// The effective permission level a principal has on a specific graph.
/// Variants are ordered by the hierarchy: `Write` > `Read{filter:None}` > `Read{filter:Some}` > `Introspect`.
/// A filtered `Read` is less powerful than an unfiltered `Read` because it sees a restricted view.
#[derive(Clone)]
pub enum GraphPermission {
    /// May query graph metadata (counts, schema) but not read data.
    Introspect,
    /// May read graph data; optionally restricted by a data filter.
    Read { filter: Option<GraphAccessFilter> },
    /// May read and mutate the graph (implies `Read` and `Introspect`, never filtered).
    Write,
}

impl GraphPermission {
    /// Numeric level used for ordering: `Introspect`=0, `Read{Some}`=1, `Read{None}`=2, `Write`=3.
    fn level(&self) -> u8 {
        match self {
            GraphPermission::Introspect => 0,
            GraphPermission::Read { filter: Some(_) } => 1,
            GraphPermission::Read { filter: None } => 2,
            GraphPermission::Write => 3,
        }
    }

    /// Returns `true` if the permission level is `Read` or higher.
    pub fn is_at_least_read(&self) -> bool {
        self.level() >= 1
    }

    /// Returns `true` only for `Write` permission.
    pub fn is_write(&self) -> bool {
        self.level() >= 3
    }

    /// Returns `Some(self)` if at least `Read` (filtered or not), `None` otherwise.
    /// Use with `?` to gate access and preserve the permission value for filter extraction.
    pub fn at_least_read(self) -> Option<Self> {
        self.is_at_least_read().then_some(self)
    }

    /// Returns `Some(self)` if `Write`, `None` otherwise.
    pub fn at_least_write(self) -> Option<Self> {
        self.is_write().then_some(self)
    }
}

impl PartialEq for GraphPermission {
    fn eq(&self, other: &Self) -> bool {
        self.level() == other.level()
    }
}

impl Eq for GraphPermission {}

impl PartialOrd for GraphPermission {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for GraphPermission {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.level().cmp(&other.level())
    }
}

/// The effective permission level a principal has on a namespace.
/// Variants are ordered lowest to highest so that `PartialOrd`/`Ord` reflect the hierarchy.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum NamespacePermission {
    /// No access — namespace is invisible.
    Denied,
    /// Namespace is visible in parent `children()` listings but cannot be browsed.
    Discover,
    /// Namespace is browseable; graphs inside are visible as MetaGraph in `graphs()`.
    Introspect,
    /// All descendant graphs are fully readable.
    Read,
    /// All descendants are writable; `newGraph` is allowed.
    Write,
}

pub trait AuthorizationPolicy: Send + Sync + 'static {
    /// Resolves the effective permission level for a principal on a graph.
    /// Returns `Err(denial message)` only when access is entirely denied (not even introspect).
    /// Admin principals (`"access": "rw"` JWT) always yield `Write`.
    /// Empty store (no roles configured) yields `Read` — fail open for reads,
    /// but write still requires an explicit `Write` grant.
    /// The implementation is responsible for extracting principal identity from `ctx`.
    fn graph_permissions(
        &self,
        ctx: &async_graphql::Context<'_>,
        path: &str,
    ) -> Result<GraphPermission, String>;

    /// Resolves the effective namespace permission for a principal.
    /// Admin principals always yield `Write`.
    /// Empty store yields `Read` (fail open, consistent with graph_permissions).
    /// Missing role yields `Denied`.
    /// The implementation is responsible for extracting principal identity from `ctx`.
    fn namespace_permissions(
        &self,
        ctx: &async_graphql::Context<'_>,
        path: &str,
    ) -> NamespacePermission;
}
