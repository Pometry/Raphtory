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
    /// Returns `Some(true)` to allow access, `Some(false)` to deny, `None` if the role has no
    /// entry covering this graph (treated as denied when a policy is active).
    fn check_graph_access(&self, role: Option<&str>, path: &str) -> Option<bool>;

    /// Returns `true` if the role may perform graph-level introspection
    /// (countNodes, countEdges, uniqueLayers, schema).
    fn check_graph_introspection(&self, role: Option<&str>, path: &str) -> bool;

    /// Returns `true` if the role may write to this graph (addNode, addEdge, updateGraph, newGraph).
    /// `"a": "rw"` users bypass this check entirely — it is only called for `"a": "ro"` users.
    fn check_graph_write_access(&self, role: Option<&str>, path: &str) -> bool;

    /// Returns a filter to apply transparently when this role queries the graph.
    /// Returns `None` if no filter is configured (full access to graph data).
    /// `"access": "rw"` admin users bypass this — it is never called for them.
    fn get_graph_data_filter(&self, role: Option<&str>, path: &str) -> Option<GraphAccessFilter>;

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
    ) -> Result<GraphPermission, String> {
        if is_admin {
            return Ok(GraphPermission::Write);
        }
        match self.check_graph_access(role, path) {
            // No roles configured: fail open for reads, but not writes
            None => Ok(GraphPermission::Read { filter: None }),
            Some(true) => {
                if self.check_graph_write_access(role, path) {
                    Ok(GraphPermission::Write)
                } else {
                    Ok(GraphPermission::Read { filter: self.get_graph_data_filter(role, path) })
                }
            }
            Some(false) => {
                if self.check_graph_introspection(role, path) {
                    Ok(GraphPermission::Introspect)
                } else {
                    Err(format!(
                        "Access denied: role '{}' is not permitted to access graph '{path}'",
                        role.unwrap_or("<no role>")
                    ))
                }
            }
        }
    }
}

/// A no-op policy that permits all reads and leaves writes to the `PermissionsPlugin`.
/// Used when no auth policy has been configured on the server.
pub struct NoopPolicy;

impl AuthorizationPolicy for NoopPolicy {
    fn check_graph_access(&self, _: Option<&str>, _: &str) -> Option<bool> {
        Some(true)
    }

    fn check_graph_introspection(&self, _: Option<&str>, _: &str) -> bool {
        true
    }

    fn check_graph_write_access(&self, _: Option<&str>, _: &str) -> bool {
        true
    }

    fn get_graph_data_filter(&self, _: Option<&str>, _: &str) -> Option<GraphAccessFilter> {
        None
    }
}
