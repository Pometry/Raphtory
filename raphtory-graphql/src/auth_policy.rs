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

pub trait AuthorizationPolicy: Send + Sync + 'static {
    /// Resolves the effective permission level for a principal on a graph.
    /// Returns `Err(denial message)` only when access is entirely denied (not even introspect).
    /// Admin bypass (`is_admin = true`) always yields `Write`.
    /// Empty store (no roles configured) yields `Read` for non-admins — fail open for reads,
    /// but write still requires an explicit `Write` grant.
    fn graph_permissions(
        &self,
        is_admin: bool,
        role: Option<&str>,
        path: &str,
    ) -> Result<GraphPermission, String>;
}

/// A no-op policy that grants full access to everyone.
/// Used when no auth policy has been configured on the server.
pub struct NoopPolicy;

impl AuthorizationPolicy for NoopPolicy {
    fn graph_permissions(&self, _: bool, _: Option<&str>, _: &str) -> Result<GraphPermission, String> {
        Ok(GraphPermission::Write)
    }
}
