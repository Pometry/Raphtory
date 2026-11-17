use crate::{
    auth::ContextValidation,
    auth_policy::{AuthPolicyError, AuthorizationPolicy, GraphPermission, NamespacePermission},
    GQLError,
};
use async_graphql::{Context, ErrorExtensions};
use std::sync::Arc;
use tracing::{debug, error, warn};

#[derive(thiserror::Error, Debug)]
pub enum PermissionError {
    /// Graph exists but caller has no namespace visibility — hide graph existence.
    #[error("Graph does not exist")]
    GraphNotFound,
    /// Caller has no grant on the graph at all but can see its namespace. The denial names
    /// the path the caller asked for, and a path with no graph behind it gets the same
    /// denial, so the answer does not tell them whether the graph exists.
    #[error("Access denied: no permission for graph '{graph}'")]
    GraphAccessDenied { graph: String },
    /// Caller has introspect-only access; cannot read graph data.
    #[error(
        "Access denied: introspect-only access to graph '{graph}' — \
         use graphMetadata(path:) for counts and timestamps, or namespace listings to browse graphs"
    )]
    IntrospectOnly { graph: String },
    /// Caller has read-only access but the operation requires write.
    #[error("Access denied: WRITE permission required for graph '{graph}'")]
    GraphWriteRequired { graph: String },

    /// Caller has filtered read-only access but the opration requires unfiltered read
    #[error("Access denied: unfiltered READ permissions required for graph '{graph}'")]
    GraphUnfilteredReadRequired { graph: String },
    /// Caller lacks write permission on the destination namespace.
    #[error(
        "Access denied: WRITE required on namespace '{namespace}' to {operation} graph '{graph}'"
    )]
    NamespaceWriteRequired {
        namespace: String,
        graph: String,
        operation: String,
    },
    /// The request runs in a read-only context (an ABAC probe), which may never write.
    #[error("Access denied: this context may not write")]
    ReadOnlyContext,
    #[error("Internal server error, access policy is misconfigured")]
    AuthPolicyError,
}

impl From<AuthPolicyError> for PermissionError {
    fn from(value: AuthPolicyError) -> Self {
        error!("Auth policy failure: {value}");
        PermissionError::AuthPolicyError
    }
}

/// Machine-readable `extensions.code` values attached to authorization errors so
/// the client classifies failures by structure rather than by message wording.
///
/// `GRAPH_NOT_FOUND` must be emitted for both a genuinely missing graph and a
/// forbidden-but-hidden graph, so the two are byte-for-byte indistinguishable to
/// an unauthorized caller.
pub const CODE_ACCESS_DENIED: &str = "ACCESS_DENIED";
pub const CODE_GRAPH_NOT_FOUND: &str = "GRAPH_NOT_FOUND";
pub const CODE_AUTH_ERROR: &str = "AUTH_ERROR";

/// Build an `async_graphql::Error` carrying a `code` in its extensions. The
/// human-readable message is preserved unchanged; only the structured code is
/// added, so the client can branch on it without parsing message text.
pub fn gql_error_with_code(
    error: impl Into<async_graphql::Error>,
    code: &'static str,
) -> async_graphql::Error {
    error.into().extend_with(|_, ext| ext.set("code", code))
}

impl PermissionError {
    /// The `extensions.code` this denial surfaces to the client. `GraphNotFound`
    /// deliberately shares the code a genuinely missing graph produces so a
    /// forbidden graph cannot be distinguished from a nonexistent one.
    pub fn code(&self) -> &'static str {
        match self {
            PermissionError::GraphNotFound => CODE_GRAPH_NOT_FOUND,
            PermissionError::GraphAccessDenied { .. }
            | PermissionError::IntrospectOnly { .. }
            | PermissionError::GraphWriteRequired { .. }
            | PermissionError::GraphUnfilteredReadRequired { .. }
            | PermissionError::NamespaceWriteRequired { .. }
            | PermissionError::ReadOnlyContext => CODE_ACCESS_DENIED,
            PermissionError::AuthPolicyError => CODE_AUTH_ERROR,
        }
    }

    /// Convert into an `async_graphql::Error` tagged with the matching `code`.
    pub fn into_gql_error(self) -> async_graphql::Error {
        let code = self.code();
        gql_error_with_code(self.to_string(), code)
    }
}

/// Returns the namespace portion of a graph path: everything before the last `/`.
/// For top-level graphs (no `/`), returns `""` (the root namespace).
pub(crate) fn parent_namespace(path: &str) -> &str {
    path.rfind('/').map(|i| &path[..i]).unwrap_or("")
}

/// Checks that the caller has at least READ permission for the graph at `path`.
/// Returns the effective `GraphPermission` (including any stored filter) on success.
/// The caller's grant on the graph, or the error that hides the graph when they have none.
/// When denied and the caller has no INTROSPECT on the parent namespace, returns a
/// "Graph does not exist" error to avoid leaking that the graph is present.
/// A misconfigured authorisation policy logs the error and returns a generic error message to the
/// caller instead of silently hiding everything.
fn graph_grant(
    ctx: &Context<'_>,
    policy: &dyn AuthorizationPolicy,
    path: &str,
) -> Result<GraphPermission, GQLError> {
    match policy.graph_permissions(ctx, path) {
        Err(e) => {
            error!(
                graph = path,
                error = %e,
                "Authorization policy could not resolve graph permissions"
            );
            Err(PermissionError::AuthPolicyError.into())
        }
        Ok(None) => {
            warn!(graph = path, "Access denied by auth policy");
            // A caller who cannot see the namespace is told the graph does not exist. One
            // who can is denied by name, which is safe only because a path with no graph
            // behind it is denied the same way. A policy fault resolving the namespace is
            // reported as a generic policy error, same as if the graph permission fails to resolve.
            let namespace = parent_namespace(path);
            let disclosable = match policy.namespace_permissions(ctx, namespace) {
                Ok(perm) => perm.is_some(),
                Err(e) => {
                    error!(
                        namespace,
                        error = %e,
                        "Authorization policy could not resolve namespace permissions when checking graph visibility."
                    );
                    return Err(PermissionError::AuthPolicyError.into());
                }
            };
            if disclosable {
                Err(PermissionError::GraphAccessDenied {
                    graph: path.to_string(),
                }
                .into())
            } else {
                Err(PermissionError::GraphNotFound.into())
            }
        }
        Ok(Some(perm)) => Ok(perm),
    }
}

pub(crate) fn require_at_least_read(
    ctx: &Context<'_>,
    policy: &Option<Arc<dyn AuthorizationPolicy>>,
    path: &str,
) -> Result<GraphPermission, GQLError> {
    if let Some(policy) = policy {
        let perm = graph_grant(ctx, policy.as_ref(), path)?;
        return if let Some(p) = perm.at_least_read() {
            Ok(p)
        } else {
            warn!(graph = path, "Permission denied: introspect-only access");
            debug!(
                graph = path,
                "Introspect-only grants can read graphMetadata() but not graph(); \
                 use graphMetadata() instead or request a read grant"
            );
            Err(PermissionError::IntrospectOnly {
                graph: path.to_string(),
            }
            .into())
        };
    }
    Ok(GraphPermission::Write)
}

/// Gives the policy an asynchronous pass at an already-granted permission before its filter is
/// applied (see [`AuthorizationPolicy::refine_permission`]). A no-op without a policy.
pub(crate) async fn refine(
    ctx: &Context<'_>,
    policy: &Option<Arc<dyn AuthorizationPolicy>>,
    path: &str,
    perm: GraphPermission,
) -> Result<GraphPermission, GQLError> {
    match policy {
        Some(policy) => policy
            .refine_permission(ctx, path, perm)
            .await
            .map_err(|msg| {
                error!(
                    graph = path,
                    "Authpolicy failure when refining permissions: {msg}"
                );
                PermissionError::AuthPolicyError.into()
            }),
        None => Ok(perm),
    }
}

pub(crate) fn require_namespace_write(
    ctx: &Context<'_>,
    policy: &Option<Arc<dyn AuthorizationPolicy>>,
    ns_path: &str,
    new_path: &str,
    operation: &str,
) -> Result<(), GQLError> {
    if ctx.is_read_only() {
        return Err(PermissionError::ReadOnlyContext.into());
    }
    match policy {
        None => Ok(ctx.require_jwt_write_access()?),
        Some(p) => {
            let ns_perm = p.namespace_permissions(ctx, ns_path).map_err(|e| {
                error!(
                    namespace = ns_path,
                    error = %e,
                    "Authorization policy could not resolve namespace permissions"
                );
                PermissionError::AuthPolicyError
            })?;
            if ns_perm < Some(NamespacePermission::Write) {
                return Err(PermissionError::NamespaceWriteRequired {
                    namespace: ns_path.to_string(),
                    graph: new_path.to_string(),
                    operation: operation.to_string(),
                }
                .into());
            }
            Ok(())
        }
    }
}

pub(crate) fn require_graph_write(
    ctx: &Context<'_>,
    policy: &Option<Arc<dyn AuthorizationPolicy>>,
    path: &str,
) -> Result<(), GQLError> {
    if ctx.is_read_only() {
        return Err(PermissionError::ReadOnlyContext.into());
    }
    match policy {
        None => Ok(ctx.require_jwt_write_access()?),
        Some(p) => {
            graph_grant(ctx, p.as_ref(), path)?
                .at_least_write()
                .ok_or_else(|| PermissionError::GraphWriteRequired {
                    graph: path.to_string(),
                })?;
            Ok(())
        }
    }
}
