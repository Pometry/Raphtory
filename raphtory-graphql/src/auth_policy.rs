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
}
