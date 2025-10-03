use crate::config::auth_config::{AuthConfig, PublicKey};
use async_graphql::{
    async_trait,
    extensions::{Extension, ExtensionContext, ExtensionFactory, NextParseQuery},
    http::{create_multipart_mixed_stream, is_accept_multipart_mixed},
    parser::types::{ExecutableDocument, OperationType},
    BatchRequest, Context, Executor, ServerError, ServerResult, Variables,
};
use async_graphql_poem::{GraphQLBatchRequest, GraphQLBatchResponse, GraphQLRequest};
use futures_util::StreamExt;
use jsonwebtoken::{decode, Algorithm, Validation};
use poem::{
    error::{TooManyRequests, Unauthorized},
    Body, Endpoint, FromRequest, IntoResponse, Request, Response, Result,
};
use reqwest::header::AUTHORIZATION;
use serde::Deserialize;
use std::{sync::Arc, time::Duration};
use tokio::sync::{RwLock, Semaphore};

#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Access {
    Ro,
    Rw,
}

#[derive(Deserialize, Debug, Clone)]
pub(crate) struct TokenClaims {
    pub(crate) a: Access,
}

pub struct AuthenticatedGraphQL<E> {
    executor: E,
    config: AuthConfig,
    semaphore: Option<Semaphore>,
    lock: Option<RwLock<()>>,
}

impl<E> AuthenticatedGraphQL<E> {
    /// Create a GraphQL endpoint.
    pub fn new(executor: E, config: AuthConfig) -> Self {
        Self {
            executor,
            config,
            semaphore: std::env::var("CONCURRENCY_LIMIT").ok().and_then(|limit| {
                let limit = limit.parse::<usize>().ok()?;
                println!("Server running with concurrency limited to {limit} for heavy queries");
                Some(Semaphore::new(limit))
            }),
            lock: std::env::var("RAPHTORY_THREADSAFE")
                .ok()
                .and_then(|thread_safe| {
                    if thread_safe == "1" {
                        println!("Server running in threadsafe mode");
                        Some(RwLock::new(()))
                    } else {
                        None
                    }
                }),
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
        let access = match &self.config.public_key {
            Some(public_key) => {
                let presented_access = req
                    .header(AUTHORIZATION)
                    .and_then(|header| extract_access_from_header(header, public_key));
                match presented_access {
                    Some(access) => access,
                    None => {
                        if self.config.enabled_for_reads {
                            return Err(Unauthorized(AuthError::RequireRead));
                        } else {
                            Access::Ro // if read access is not required, we give read access to all requests
                        }
                    }
                }
            }
            None => Access::Rw, // if auth is not setup, we give write access to all requests
        };

        let is_accept_multipart_mixed = req
            .header("accept")
            .map(is_accept_multipart_mixed)
            .unwrap_or_default();

        if is_accept_multipart_mixed {
            let (req, mut body) = req.split();
            let req = GraphQLRequest::from_request(&req, &mut body).await?;
            let req = req.0.data(access);
            let stream = self.executor.execute_stream(req, None);
            Ok(Response::builder()
                .header("content-type", "multipart/mixed; boundary=graphql")
                .body(Body::from_bytes_stream(
                    create_multipart_mixed_stream(stream, Duration::from_secs(30))
                        .map(Ok::<_, std::io::Error>),
                )))
        } else {
            let (req, mut body) = req.split();
            let req = GraphQLBatchRequest::from_request(&req, &mut body).await?;
            let req = req.0.data(access);

            let contains_update = match &req {
                BatchRequest::Single(request) => request.query.contains("updateGraph"),
                BatchRequest::Batch(requests) => requests
                    .iter()
                    .any(|request| request.query.contains("updateGraph")),
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

fn is_query_heavy(query: &str) -> bool {
    query.contains("outComponent")
        || query.contains("inComponent")
        || query.contains("edges")
        || query.contains("outEdges")
        || query.contains("inEdges")
        || query.contains("neighbours")
        || query.contains("outNeighbours")
        || query.contains("inNeighbours")
}

fn extract_access_from_header(header: &str, public_key: &PublicKey) -> Option<Access> {
    if header.starts_with("Bearer ") {
        let jwt = header.replace("Bearer ", "");
        let mut validation = Validation::new(Algorithm::EdDSA);
        validation.set_required_spec_claims::<String>(&[]); // we don't require 'exp' to be present
        let decoded = decode::<TokenClaims>(&jwt, &public_key.decoding_key, &validation);
        Some(decoded.ok()?.claims.a)
    } else {
        None
    }
}

pub(crate) trait ContextValidation {
    fn require_write_access(&self) -> Result<(), AuthError>;
}

impl<'a> ContextValidation for &Context<'a> {
    fn require_write_access(&self) -> Result<(), AuthError> {
        if self.data::<Access>().is_ok_and(|role| role == &Access::Rw) {
            Ok(())
        } else {
            Err(AuthError::RequireWrite)
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
                Err(AuthError::RequireWrite.into())
            } else {
                Ok(doc)
            }
        })
    }
}
