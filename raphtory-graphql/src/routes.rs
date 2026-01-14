use poem::{
    endpoint::{EmbeddedFileEndpoint, EmbeddedFilesEndpoint, StaticFilesEndpoint},
    handler,
    http::{Method, StatusCode},
    web::Json,
    Endpoint, IntoResponse, Request, Response,
};
use rust_embed::Embed;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Serialize, Deserialize)]
pub(crate) struct Health {
    pub(crate) healthy: bool,
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
#[folder = "$RAPHTORY_UI_INDEX_PATH"]
struct PublicFolder;

pub(crate) struct PublicFilesEndpoint<G> {
    public_dir: Option<PathBuf>,
    gql: G,
}

impl<G> PublicFilesEndpoint<G> {
    pub(crate) fn new(public_dir: Option<PathBuf>, gql: G) -> PublicFilesEndpoint<G> {
        PublicFilesEndpoint { public_dir, gql }
    }
}

impl<G> Endpoint for PublicFilesEndpoint<G>
where
    G: Endpoint<Output = Response>,
{
    type Output = Response;

    async fn call(&self, req: Request) -> poem::Result<Self::Output> {
        if req.method() == Method::POST {
            self.gql.call(req).await
        } else if let Some(public_dir) = &self.public_dir {
            StaticFilesEndpoint::new(public_dir)
                .fallback_to_index()
                .call(req)
                .await
        } else {
            let path = req.uri().path().trim_start_matches('/');

            if !path.is_empty()
                && PublicFolder::get(path).is_none()
                && PublicFolder::get(&format!("{path}/index.html")).is_none()
            {
                EmbeddedFileEndpoint::<PublicFolder>::new("index.html")
                    .call(req)
                    .await
            } else {
                EmbeddedFilesEndpoint::<PublicFolder>::new().call(req).await
            }
        }
    }
}
