use poem::{
    endpoint::{EmbeddedFileEndpoint, EmbeddedFilesEndpoint},
    handler,
    http::{Method, StatusCode},
    web::Json,
    Endpoint, IntoResponse, Request, Response,
};
use rust_embed::Embed;
use serde::Serialize;

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

#[derive(Embed)]
#[folder = "resources/"]
struct ResourcesFolder;

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

    async fn call(&self, mut req: Request) -> poem::Result<Self::Output> {
        if req.method() == Method::POST {
            self.gql.call(req).await
        } else {
            let path = req.uri().path().trim_start_matches('/');

            if !path.is_empty()
                && ResourcesFolder::get(path).is_none()
                && ResourcesFolder::get(&format!("{path}/index.html")).is_none()
            {
                EmbeddedFileEndpoint::<ResourcesFolder>::new("index.html")
                    .call(req)
                    .await
            } else {
                EmbeddedFilesEndpoint::<ResourcesFolder>::new()
                    .call(req)
                    .await
            }
        }
    }
}
