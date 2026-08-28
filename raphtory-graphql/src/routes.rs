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
    disable_ui: bool,
    gql: G,
}

impl<G> PublicFilesEndpoint<G> {
    pub(crate) fn new(
        public_dir: Option<PathBuf>,
        disable_ui: bool,
        gql: G,
    ) -> PublicFilesEndpoint<G> {
        PublicFilesEndpoint {
            public_dir,
            disable_ui,
            gql,
        }
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
        } else if self.disable_ui {
            Ok(StatusCode::NOT_FOUND.into_response())
        } else if let Some(public_dir) = &self.public_dir {
            StaticFilesEndpoint::new(public_dir)
                .index_file("index.html")
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

#[cfg(test)]
mod tests {
    use super::PublicFilesEndpoint;
    use poem::{
        endpoint::make_sync,
        http::{Method, StatusCode},
        Endpoint, Request, Response,
    };
    use std::{fs, path::Path};
    use tempfile::tempdir;

    fn public_dir_endpoint(dir: &Path) -> impl Endpoint<Output = Response> {
        PublicFilesEndpoint::new(
            Some(dir.to_path_buf()),
            false,
            make_sync(|_| Response::builder().body("gql")),
        )
    }

    #[tokio::test]
    async fn disable_ui_returns_404_for_get_but_post_reaches_gql() {
        let endpoint =
            PublicFilesEndpoint::new(None, true, make_sync(|_| Response::builder().body("gql")));
        // GET (the UI) is gone.
        assert_eq!(get(&endpoint, "/").await.status(), StatusCode::NOT_FOUND);
        // POST still reaches the GraphQL executor.
        let post = Request::builder()
            .method(Method::POST)
            .uri("/".parse().unwrap())
            .finish();
        let resp = endpoint.call(post).await.unwrap();
        assert_eq!(resp.into_body().into_string().await.unwrap(), "gql");
    }

    async fn get(endpoint: &impl Endpoint<Output = Response>, path: &str) -> Response {
        let req = Request::builder()
            .method(Method::GET)
            .uri(path.parse().unwrap())
            .finish();
        endpoint
            .call(req)
            .await
            .unwrap_or_else(|err| err.into_response())
    }

    #[tokio::test]
    async fn public_dir_serves_index_for_spa_routes() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("index.html"), "<html>ui</html>").unwrap();
        let endpoint = public_dir_endpoint(dir.path());

        for path in ["/", "/index.html", "/graphs", "/graphs/nested/route"] {
            let resp = get(&endpoint, path).await;
            assert_eq!(resp.status(), StatusCode::OK, "GET {path}");
            assert_eq!(
                resp.into_body().into_string().await.unwrap(),
                "<html>ui</html>",
                "GET {path}"
            );
        }
    }

    #[tokio::test]
    async fn public_dir_missing_index_returns_404() {
        let dir = tempdir().unwrap(); // exists but has no index.html
        let endpoint = public_dir_endpoint(dir.path());
        assert_eq!(get(&endpoint, "/").await.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            get(&endpoint, "/graphs").await.status(),
            StatusCode::NOT_FOUND
        );
    }

    #[tokio::test]
    async fn public_dir_nonexistent_returns_404() {
        // public_dir points at a path that doesn't exist at all (misconfiguration).
        let dir = tempdir().unwrap();
        let missing = dir.path().join("does-not-exist");
        let endpoint = public_dir_endpoint(&missing);
        assert_eq!(get(&endpoint, "/").await.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            get(&endpoint, "/assets/app.js").await.status(),
            StatusCode::NOT_FOUND
        );
    }

    #[tokio::test]
    async fn public_dir_serves_real_files() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("index.html"), "<html>ui</html>").unwrap();
        fs::create_dir(dir.path().join("assets")).unwrap();
        fs::write(dir.path().join("assets").join("app.js"), "js-content").unwrap();
        let endpoint = public_dir_endpoint(dir.path());

        let resp = get(&endpoint, "/assets/app.js").await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.into_body().into_string().await.unwrap(), "js-content");
    }
}
