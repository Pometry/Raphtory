use futures_util::future::join;
use poem::{
    endpoint::{EmbeddedFileEndpoint, EmbeddedFilesEndpoint, StaticFilesEndpoint},
    handler,
    http::{Method, StatusCode},
    web::{Json, Query},
    Endpoint, IntoResponse, Request, Response,
};
use rust_embed::Embed;
use serde::{Deserialize, Serialize};
use std::{path::PathBuf, time::Duration};

use crate::rayon::{blocking_compute, blocking_write};

#[derive(Serialize, Deserialize)]
pub(crate) struct Health {
    pub(crate) healthy: bool,
}

#[derive(Serialize)]
struct Version {
    version: String,
}

#[derive(Deserialize)]
struct HealthQuery {
    timeout: Option<u64>, // seconds
}

#[handler]
pub(crate) async fn health(Query(params): Query<HealthQuery>) -> impl IntoResponse {
    // using blocking_compute and blocking_write to identify deadlocks on any of the two rayon pools
    let result = tokio::time::timeout(
        Duration::from_secs(params.timeout.unwrap_or(10)),
        join(blocking_compute(|| {}), blocking_write(|| {})),
    )
    .await;
    match result {
        Ok(_) => (StatusCode::OK, Json(Health { healthy: true })),
        Err(_) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(Health { healthy: false }),
        ),
    }
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
            let file_name = req.uri().path().split('/').last().unwrap_or("");

            if file_name.contains("worker") && file_name.ends_with("js") {
                // Always return the worker from root
                EmbeddedFileEndpoint::<PublicFolder>::new(file_name)
                    .call(req)
                    .await
            } else if !path.is_empty()
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
