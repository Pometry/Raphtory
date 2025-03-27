use std::time::Duration;

use async_graphql::{
    http::{create_multipart_mixed_stream, is_accept_multipart_mixed},
    Executor,
};
use async_graphql_poem::{GraphQLBatchRequest, GraphQLBatchResponse, GraphQLRequest};
use futures_util::StreamExt;
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use poem::{Body, Endpoint, FromRequest, IntoResponse, Request, Response, Result};
use reqwest::header::AUTHORIZATION;
use serde::Deserialize;

#[derive(Clone, Debug, Deserialize)]
pub(crate) enum Role {
    Read,
    Write,
}

pub struct AuthenticatedGraphQL<E> {
    executor: E,
    secret: String,
}

impl<E> AuthenticatedGraphQL<E> {
    /// Create a GraphQL endpoint.
    pub fn new(executor: E, secret: String) -> Self {
        Self { executor, secret }
    }

    fn extract_role_from_header(&self, header: &str) -> Option<Role> {
        if header.starts_with("Bearer ") {
            let jwt = header.replace("Bearer ", "");
            let decoded = decode::<Role>(
                &jwt,
                &DecodingKey::from_secret(self.secret.as_ref()),
                &Validation::new(Algorithm::HS256),
            );
            Some(decoded.ok()?.claims)
        } else {
            None
        }
    }
}

impl<E> Endpoint for AuthenticatedGraphQL<E>
where
    E: Executor,
{
    type Output = Response;

    async fn call(&self, req: Request) -> Result<Self::Output> {
        let role = req
            .header(AUTHORIZATION)
            .and_then(|header| self.extract_role_from_header(header));

        let is_accept_multipart_mixed = req
            .header("accept")
            .map(is_accept_multipart_mixed)
            .unwrap_or_default();

        if is_accept_multipart_mixed {
            let (req, mut body) = req.split();
            let req = GraphQLRequest::from_request(&req, &mut body).await?;
            // FIXME: a bit verbose innit?
            let req = if let Some(role) = role {
                req.0.data(role)
            } else {
                req.0
            };
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
            let req = if let Some(role) = role {
                req.0.data(role)
            } else {
                req.0
            };
            Ok(GraphQLBatchResponse(self.executor.execute_batch(req).await).into_response())
        }
    }
}
