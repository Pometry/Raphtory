use crate::model::graph::filtering::GraphAccessFilter;
use futures_util::future::BoxFuture;

/// Opaque error returned by [`AuthorizationPolicy::graph_permissions`] when access is entirely
/// denied. The message is intended for logging only; callers must not surface it to end users.
#[derive(Debug)]
pub struct AuthPolicyError(String);

impl AuthPolicyError {
    pub fn new(msg: impl Into<String>) -> Self {
        Self(msg.into())
    }
}

impl std::fmt::Display for AuthPolicyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

// async_graphql's blanket `impl<T: Display + Send + Sync + 'static> From<T> for Error` covers
// AuthPolicyError automatically via its Display impl.

/// Ordered permission levels for comparison; variants derive `Ord` directly.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum PermissionLevel {
    /// May query graph metadata only (counts, schema), not data.
    Introspect,
    /// May read data but through a restricted filter.
    ReadRedacted,
    /// May read all graph data without restrictions.
    Read,
    /// May read and mutate the graph.
    Write,
}

/// The effective permission level a principal has on a specific graph.
#[derive(Clone)]
pub enum GraphPermission {
    /// May query graph metadata (counts, schema) but not read data.
    Introspect,
    /// May read graph data; optionally restricted by a row filter and/or property redaction.
    Read { filter: Option<GraphAccessFilter> },
    /// May read and mutate the graph (implies `Read` and `Introspect`, never filtered).
    Write,
}

impl GraphPermission {
    pub fn level(&self) -> PermissionLevel {
        match self {
            GraphPermission::Introspect => PermissionLevel::Introspect,
            GraphPermission::Read { filter: Some(_) } => PermissionLevel::ReadRedacted,
            GraphPermission::Read { filter: None } => PermissionLevel::Read,
            GraphPermission::Write => PermissionLevel::Write,
        }
    }

    /// Returns `true` if the permission level is `Read` or higher.
    pub fn is_at_least_read(&self) -> bool {
        self.level() >= PermissionLevel::ReadRedacted
    }

    /// Returns `true` only for `Write` permission.
    pub fn is_write(&self) -> bool {
        self.level() >= PermissionLevel::Write
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
    /// Namespace is listable; graphs and child namespaces are visible.
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
    ) -> Result<GraphPermission, AuthPolicyError>;

    /// Resolves the effective namespace permission for a principal.
    /// Returns `None` if the principal has no access to this namespace (it is invisible).
    /// The implementation is responsible for extracting principal identity from `ctx`.
    /// Resolves the effective permission on a namespace.
    /// Returns `Err` only when the principal cannot be established at all, which is a
    /// server-side fault rather than a caller who lacks access; a caller with no grant on
    /// this namespace is `Ok(None)`.
    fn namespace_permissions(
        &self,
        ctx: &async_graphql::Context<'_>,
        path: &str,
    ) -> Result<Option<NamespacePermission>, AuthPolicyError>;

    /// Optional asynchronous refinement of an already-resolved permission.
    ///
    /// Called on the read path once [`Self::graph_permissions`] has granted at least read access,
    /// and before the permission's filter is applied. Unlike `graph_permissions` this is `async`,
    /// so an implementation may perform I/O (further lookups, queries) while deciding the final
    /// permission.
    ///
    /// The default returns `perm` unchanged: policies that need no refinement — and the no-policy
    /// case — are unaffected. Returning `Err` denies the request.
    fn refine_permission<'a>(
        &'a self,
        _ctx: &'a async_graphql::Context<'_>,
        _path: &'a str,
        perm: GraphPermission,
    ) -> BoxFuture<'a, Result<GraphPermission, AuthPolicyError>> {
        Box::pin(std::future::ready(Ok(perm)))
    }

    /// Called after a graph is successfully created to auto-grant `Write` for the creator's role.
    /// Returns an error if the grant cannot be persisted; the caller is responsible for rolling
    /// back the graph creation so the store and filesystem stay consistent.
    /// Default no-op — only meaningful when a policy and a role claim are present.
    fn on_graph_created(
        &self,
        _ctx: &async_graphql::Context<'_>,
        _path: &str,
    ) -> Result<(), String> {
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod auth_policy_tests {
    use super::{AuthPolicyError, AuthorizationPolicy, GraphPermission, NamespacePermission};
    use std::collections::HashMap;

    /// Test-only authorization policy: every path must be configured explicitly via
    /// [`Self::with_namespace`] / [`Self::with_graph`]. Unknown namespaces return
    /// `None` and unknown graphs return `Err`. This is stricter than the production
    /// policy's fail-open contract — that's intentional, so a missing `with_*` call
    /// in a test surfaces as an obvious failure rather than as a silent allow.
    #[derive(Default)]
    pub(crate) struct FakePolicy {
        namespaces: HashMap<String, NamespacePermission>,
        graphs: HashMap<String, GraphPermission>,
    }

    #[allow(dead_code)]
    impl FakePolicy {
        pub(crate) fn with_namespace(mut self, path: &str, perm: NamespacePermission) -> Self {
            self.namespaces.insert(path.to_string(), perm);
            self
        }
        pub(crate) fn with_graph(mut self, path: &str, perm: GraphPermission) -> Self {
            self.graphs.insert(path.to_string(), perm);
            self
        }
    }

    impl AuthorizationPolicy for FakePolicy {
        fn graph_permissions(
            &self,
            _ctx: &async_graphql::Context<'_>,
            path: &str,
        ) -> Result<GraphPermission, AuthPolicyError> {
            match self.graphs.get(path) {
                Some(p) => Ok(p.clone()),
                None => Err(AuthPolicyError::new(format!(
                    "no permission for graph {path}"
                ))),
            }
        }
        fn namespace_permissions(
            &self,
            _ctx: &async_graphql::Context<'_>,
            path: &str,
        ) -> Result<Option<NamespacePermission>, AuthPolicyError> {
            Ok(self.namespaces.get(path).cloned())
        }
    }
}
