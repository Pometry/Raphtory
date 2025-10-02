use crate::auth::AuthenticatedGraphQL;
use async_graphql::dynamic::Schema;
use poem::{
    endpoint::StaticFilesEndpoint,
    handler,
    http::{Method, StatusCode},
    post,
    web::Json,
    Endpoint, EndpointExt, IntoResponse, Request, Response, RouteMethod,
};
use serde::Serialize;
use std::future::Future;

#[derive(Serialize)]
struct Health {
    healthy: bool,
}

#[derive(Serialize)]
struct Version {
    version: String,
}

#[handler]
pub(crate) async fn health() -> impl IntoResponse {
    let health = Health { healthy: true };
    (StatusCode::OK, Json(health))
}

#[handler]
pub(crate) async fn version() -> impl IntoResponse {
    let v = Version {
        version: String::from(raphtory::version()),
    };
    (StatusCode::OK, Json(v))
}

pub(crate) struct IndexEndpoint<G> {
    gql: G,
}

impl<G> IndexEndpoint<G> {
    pub(crate) fn new(gql: G) -> IndexEndpoint<G> {
        IndexEndpoint { gql }
    }
}

impl<G> Endpoint for IndexEndpoint<G>
where
    G: Endpoint<Output = Response>,
{
    type Output = Response;

    async fn call(&self, req: Request) -> poem::Result<Self::Output> {
        if req.method() == Method::POST {
            self.gql.call(req).await
        } else {
            StaticFilesEndpoint::new(env!("RAPHTORY_UI_INDEX_PATH"))
                .index_file("index.html")
                .fallback_to_index()
                .call(req)
                .await
        }
    }
}
