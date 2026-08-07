use crate::{
    config::{app_config::AppConfig, auth_config::PublicKey},
    data::{gql_error_with_code, CODE_ACCESS_DENIED},
};
use async_graphql::{
    async_trait,
    extensions::{Extension, ExtensionContext, ExtensionFactory, NextParseQuery},
    http::{create_multipart_mixed_stream, is_accept_multipart_mixed},
    parser::types::{ExecutableDocument, OperationType},
    BatchRequest, Context, Executor, ServerError, ServerResult, Variables,
};
use async_graphql_poem::{GraphQLBatchRequest, GraphQLBatchResponse, GraphQLRequest};
use futures_util::{future::BoxFuture, StreamExt};
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use poem::{
    error::{BadRequest, TooManyRequests, Unauthorized},
    Body, Endpoint, FromRequest, IntoResponse, Request, Response, Result,
};
use reqwest::header::AUTHORIZATION;
use serde::Deserialize;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::{RwLock, Semaphore};
use tracing::{debug, warn};

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Access {
    Ro,
    Rw,
}

impl Default for Access {
    /// Tokens from an external IdP (SSO/OIDC) carry no `access` claim; they are read-only and
    /// subject to RBAC. Write/admin access requires an explicit `"access": "rw"`.
    fn default() -> Self {
        Access::Ro
    }
}

#[derive(Deserialize, Debug, Clone)]
pub(crate) struct TokenClaims {
    /// Defaults to read-only when absent — external IdP tokens don't carry this claim.
    #[serde(default)]
    pub(crate) access: Access,
    /// The `role` claim, either a string or an array of strings. Read via [`roles_from_value`]; a
    /// custom claim name may be selected with `role_claim`.
    #[serde(default)]
    pub(crate) role: Option<serde_json::Value>,
    /// Every other claim in the token, kept verbatim so authorization policies can read claims
    /// this crate has no opinion about (`sub`, tenant identifiers, and so on).
    #[serde(flatten)]
    pub(crate) other: HashMap<String, serde_json::Value>,
}

/// The validated token's claims beyond `access`/`role`, injected into the GraphQL context for
/// authorization policies to consult. Empty when no token was presented.
#[derive(Clone, Debug, Default)]
pub struct TokenClaimValues(pub HashMap<String, serde_json::Value>);

impl TokenClaimValues {
    /// Convenience accessor for a string-valued claim.
    pub fn string(&self, name: &str) -> Option<&str> {
        self.0.get(name).and_then(|v| v.as_str())
    }
}

/// The roles carried by the validated token (a request may hold several), injected into the GraphQL
/// context for authorization policies. Empty when no token was presented.
#[derive(Clone, Debug, Default)]
pub struct Roles(pub Vec<String>);

/// Resolves the JWT decoding key(s) used to verify a bearer token. The default
/// [`StaticKeyResolver`] returns a single configured key; an extension may register a resolver that
/// fetches keys dynamically (e.g. SSO/OIDC JWKS by `kid`).
pub trait KeyResolver: Send + Sync {
    /// The decoding key and the algorithm(s) permitted for a token carrying `kid` (`None` when the
    /// token has no `kid`), or `None` if no key matches.
    fn resolve<'a>(
        &'a self,
        kid: Option<&'a str>,
    ) -> BoxFuture<'a, Option<(DecodingKey, Vec<Algorithm>)>>;
}

/// The default resolver: one statically-configured key (`auth.public_key`), used for every token.
pub struct StaticKeyResolver {
    key: DecodingKey,
    algorithms: Vec<Algorithm>,
}

impl StaticKeyResolver {
    pub fn new(key: DecodingKey, algorithms: Vec<Algorithm>) -> Self {
        Self { key, algorithms }
    }
}

impl KeyResolver for StaticKeyResolver {
    fn resolve<'a>(
        &'a self,
        _kid: Option<&'a str>,
    ) -> BoxFuture<'a, Option<(DecodingKey, Vec<Algorithm>)>> {
        Box::pin(async move { Some((self.key.clone(), self.algorithms.clone())) })
    }
}

// TODO: maybe this should be renamed as it doens't only take care of auth anymore
pub struct AuthenticatedGraphQL<E> {
    executor: E,
    config: AppConfig,
    semaphore: Option<Semaphore>,
    lock: Option<RwLock<()>>,
    /// Resolves the JWT verification key. `None` when no auth is configured.
    key_resolver: Option<Arc<dyn KeyResolver>>,
}

impl<E> AuthenticatedGraphQL<E> {
    /// Create a GraphQL endpoint. `key_resolver` is a resolver registered by an extension (e.g. an
    /// SSO/JWKS resolver); when `None`, a static key from `auth.public_key` is used if configured.
    pub fn new(executor: E, config: AppConfig, key_resolver: Option<Arc<dyn KeyResolver>>) -> Self {
        let semaphore = config.concurrency.heavy_query_limit.map(|limit| {
            println!("Server running with concurrency limited to {limit} for heavy queries");
            Semaphore::new(limit)
        });
        let lock = if config.concurrency.exclusive_writes {
            println!("Server running with exclusive writes");
            Some(RwLock::new(()))
        } else {
            None
        };
        // A registered resolver (e.g. SSO/JWKS) wins; otherwise fall back to the static public key.
        let key_resolver = key_resolver.or_else(|| {
            config.auth.public_key.as_ref().map(|pk| {
                Arc::new(StaticKeyResolver::new(
                    pk.decoding_key.clone(),
                    pk.algorithms.clone(),
                )) as Arc<dyn KeyResolver>
            })
        });
        Self {
            executor,
            config,
            semaphore,
            lock,
            key_resolver,
        }
    }
}

impl<E> AuthenticatedGraphQL<E>
where
    E: Executor,
{
    async fn execute(&self, request: BatchRequest) -> Response {
        GraphQLBatchResponse(self.executor.execute_batch(request).await).into_response()
    }

    async fn execute_read_query(&self, req: BatchRequest) -> Result<Response> {
        let is_heavy = match &req {
            BatchRequest::Single(request) => is_query_heavy(&request.query),
            BatchRequest::Batch(requests) => requests
                .iter()
                .any(|request| is_query_heavy(&request.query)),
        };
        if is_heavy {
            if let Some(semaphore) = &self.semaphore {
                match semaphore.acquire().await {
                    Ok(_permit) => Ok(self.execute(req).await),
                    Err(error) => Err(TooManyRequests(error)),
                }
            } else {
                Ok(self.execute(req).await)
            }
        } else {
            Ok(self.execute(req).await)
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum AuthError {
    #[error("The requested endpoint requires at least read access")]
    RequireRead,
    #[error("The requested endpoint requires write access")]
    RequireWrite,
    #[error("Query batching is disabled on this server")]
    BatchingDisabled,
    #[error("Batch size {actual} exceeds the maximum allowed {max}")]
    BatchSizeExceeded { max: usize, actual: usize },
}

impl From<AuthError> for ServerError {
    fn from(value: AuthError) -> Self {
        ServerError::new(value.to_string(), None)
    }
}

// this is copied over from async_graphql_poem::GraphQL, but including the bits to extract the role from the header
// I found no alternative way of doing this because the data field inside of poem::Request data is not mapped into async_graphql::Request.data
// So either:
// - I have access to headers and can include the role in the data, but then gets lost along the way
// - or I hook into async_graphql by implementing Extension::prepare_request, where I can actually include data into the request, but don't have access to any headers there
impl<E> Endpoint for AuthenticatedGraphQL<E>
where
    E: Executor,
{
    type Output = Response;

    async fn call(&self, req: Request) -> Result<Self::Output> {
        // here ANY error when trying to validate the Authorization header is equivalent to it not being present at all
        let auth = &self.config.auth;
        let (access, roles, claim_values) = match &self.key_resolver {
            // if auth is not setup, we give write access to all requests
            None => (Access::Rw, Vec::new(), TokenClaimValues::default()),
            Some(resolver) => {
                let claims = match req.header(AUTHORIZATION) {
                    Some(header) => {
                        extract_claims(
                            header,
                            resolver.as_ref(),
                            auth.audience.as_deref(),
                            auth.issuer.as_deref(),
                            auth.role_claim.as_deref(),
                        )
                        .await
                    }
                    None => None,
                };
                match claims {
                    Some((access, roles, other)) => {
                        debug!(roles = ?roles, "JWT validated successfully");
                        (access, roles, TokenClaimValues(other))
                    }
                    None => {
                        if auth.require_auth_for_reads {
                            warn!("Request missing valid JWT — rejecting (require_auth_for_reads=true)");
                            return Err(Unauthorized(AuthError::RequireRead));
                        } else {
                            debug!("No valid JWT but require_auth_for_reads=false — granting read access");
                            (Access::Ro, Vec::new(), TokenClaimValues::default())
                        }
                    }
                }
            }
        };

        let is_accept_multipart_mixed = req
            .header("accept")
            .map(is_accept_multipart_mixed)
            .unwrap_or_default();

        if is_accept_multipart_mixed {
            let (req, mut body) = req.split();
            let req = GraphQLRequest::from_request(&req, &mut body).await?;
            let req = req.0.data(access).data(Roles(roles)).data(claim_values);
            let stream = self.executor.execute_stream(req, None);
            Ok(Response::builder()
                .header("content-type", "multipart/mixed; boundary=graphql")
                .body(Body::from_bytes_stream(
                    create_multipart_mixed_stream(stream, Duration::from_secs(30))
                        .map(Ok::<_, std::io::Error>),
                )))
        } else {
            let (req, mut body) = req.split();
            let batch_req = GraphQLBatchRequest::from_request(&req, &mut body).await?.0;

            if let BatchRequest::Batch(requests) = &batch_req {
                if self.config.concurrency.disable_batching {
                    return Err(BadRequest(AuthError::BatchingDisabled));
                }
                if let Some(max) = self.config.concurrency.max_batch_size {
                    let actual = requests.len();
                    if actual > max {
                        return Err(BadRequest(AuthError::BatchSizeExceeded { max, actual }));
                    }
                }
            }

            let req = batch_req.data(access).data(Roles(roles)).data(claim_values);

            let contains_update = match &req {
                BatchRequest::Single(request) => is_exclusive_write(&request.query),
                BatchRequest::Batch(requests) => requests
                    .iter()
                    .any(|request| is_exclusive_write(&request.query)),
            };
            if contains_update {
                if let Some(lock) = &self.lock {
                    let _guard = lock.write().await;
                    Ok(self.execute(req).await)
                } else {
                    Ok(self.execute(req).await)
                }
            } else {
                if let Some(lock) = &self.lock {
                    let _guard = lock.read().await;
                    self.execute_read_query(req).await
                } else {
                    self.execute_read_query(req).await
                }
            }
        }
    }
}

fn is_exclusive_write(query: &str) -> bool {
    is_operation(query, "updateGraph") || is_operation(query, "deleteNamespace")
}

fn is_operation(query: &str, op: &str) -> bool {
    query
        .split(|c: char| !c.is_alphanumeric() && c != '_')
        .any(|token| token == op)
}

fn is_query_heavy(query: &str) -> bool {
    query.contains("outComponent")
        || query.contains("inComponent")
        || query.contains("edges")
        || query.contains("outEdges")
        || query.contains("inEdges")
        || query.contains("neighbours")
        || query.contains("outNeighbours")
        || query.contains("inNeighbours")
        || query.contains("algorithm")
}

/// Verify a bearer token: select the decoding key via the [`KeyResolver`] (by the token's `kid`),
/// validate signature + `nbf` + `aud` + `iss`, and return the claims (with the role remapped from a
/// custom claim if configured).
async fn extract_claims(
    header: &str,
    resolver: &dyn KeyResolver,
    audience: Option<&str>,
    issuer: Option<&str>,
    role_claim: Option<&str>,
) -> Option<(Access, Vec<String>, HashMap<String, serde_json::Value>)> {
    let jwt = header.strip_prefix("Bearer ").or_else(|| {
        warn!("Authorization header is missing or does not start with 'Bearer '");
        None
    })?;
    let kid = decode_header(jwt).ok()?.kid;
    let (decoding_key, algorithms) = resolver.resolve(kid.as_deref()).await?;

    let mut validation = Validation::new(algorithms[0]);
    validation.algorithms = algorithms;
    validation.validate_nbf = true; // reject not-yet-valid tokens (nbf in the future)
                                    // Require the claims we validate to be present, so a token that simply omits a configured
                                    // audience/issuer is rejected rather than skipping the check. `exp` stays optional.
    let mut required: Vec<&str> = Vec::new();
    // Validate `aud` against the configured audience, or disable the check so SSO/OIDC tokens
    // (which always carry an `aud`) are accepted.
    match audience {
        Some(aud) => {
            validation.set_audience(&[aud]);
            required.push("aud");
        }
        None => validation.validate_aud = false,
    }
    if let Some(iss) = issuer {
        validation.set_issuer(&[iss]);
        required.push("iss");
    }
    validation.set_required_spec_claims(&required);
    match decode::<TokenClaims>(jwt, &decoding_key, &validation) {
        Ok(token_data) => {
            let claims = token_data.claims;
            let roles = effective_roles(&claims, role_claim);
            Some((claims.access, roles, claims.other))
        }
        Err(e) => {
            warn!(error = %e, "JWT validation failed");
            None
        }
    }
}

/// The caller's roles, read from the `role` claim or a custom claim named by `role_claim`. The claim
/// may be a single string or an array of strings (e.g. an SSO `roles`/`groups` claim); every entry
/// becomes a role, and the authorization policy merges their grants.
fn effective_roles(claims: &TokenClaims, role_claim: Option<&str>) -> Vec<String> {
    let name = role_claim.unwrap_or("role");
    let value = if name == "role" {
        claims.role.as_ref()
    } else {
        claims.other.get(name)
    };
    value.map(roles_from_value).unwrap_or_default()
}

/// The roles carried by a claim that is either a string or an array of strings. Non-string array
/// entries are ignored.
fn roles_from_value(v: &serde_json::Value) -> Vec<String> {
    match v {
        serde_json::Value::String(s) => vec![s.clone()],
        serde_json::Value::Array(items) => items
            .iter()
            .filter_map(|x| x.as_str().map(String::from))
            .collect(),
        _ => Vec::new(),
    }
}

pub(crate) trait ContextValidation {
    fn require_jwt_write_access(&self) -> Result<(), AuthError>;
}

/// Check that the request carries a write-access JWT (`"access": "rw"`).
/// For use in dynamic resolver ops that run under `query { ... }` and are
/// therefore not covered by the `MutationAuth` extension.
pub fn require_jwt_write_access_dynamic(
    ctx: &async_graphql::dynamic::ResolverContext,
) -> Result<(), async_graphql::Error> {
    if ctx.data::<Access>().is_ok_and(|a| a == &Access::Rw) {
        Ok(())
    } else {
        Err(gql_error_with_code(
            "Access denied: write access required",
            CODE_ACCESS_DENIED,
        ))
    }
}

impl<'a> ContextValidation for &Context<'a> {
    fn require_jwt_write_access(&self) -> Result<(), AuthError> {
        match self.data::<Access>() {
            Ok(access) if access == &Access::Rw => Ok(()),
            _ => Err(AuthError::RequireWrite),
        }
    }
}

pub(crate) struct MutationAuth;

impl ExtensionFactory for MutationAuth {
    fn create(&self) -> Arc<dyn Extension> {
        Arc::new(MutationAuth)
    }
}

#[async_trait::async_trait]
impl Extension for MutationAuth {
    async fn parse_query(
        &self,
        ctx: &ExtensionContext<'_>,
        query: &str,
        variables: &Variables,
        next: NextParseQuery<'_>,
    ) -> ServerResult<ExecutableDocument> {
        next.run(ctx, query, variables).await.and_then(|doc| {
            let mutation = doc
                .operations
                .iter()
                .any(|op| op.1.node.ty == OperationType::Mutation);
            if mutation && ctx.data::<Access>() != Ok(&Access::Rw) {
                // If a policy is active, allow "ro" users through to resolvers —
                // each resolver enforces its own per-graph or admin-only check.
                // Without a policy (OSS), preserve the original blanket deny.
                let policy_active = ctx
                    .data::<crate::data::Data>()
                    .map(|d| d.auth_policy.is_some())
                    .unwrap_or(false);
                if !policy_active {
                    return Err(AuthError::RequireWrite.into());
                }
            }
            Ok(doc)
        })
    }
}
