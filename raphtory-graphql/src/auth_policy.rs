use crate::model::graph::filtering::GraphAccessFilter;

/// The effective permission level a principal has on a specific graph.
/// Variants are ordered by the hierarchy: `Write` > `Read` > `Introspect`.
#[derive(Clone)]
pub enum GraphPermission {
    /// May query graph metadata (counts, schema) but not read data.
    Introspect,
    /// May read graph data; optionally restricted by a data filter.
    Read { filter: Option<GraphAccessFilter> },
    /// May read and mutate the graph (implies `Read` and `Introspect`, never filtered).
    Write,
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
