use bytes::Bytes;
use futures_util::future::join;
use poem::{
    endpoint::{EmbeddedFileEndpoint, EmbeddedFilesEndpoint, StaticFilesEndpoint},
    handler,
    http::{header, Method, StatusCode},
    web::{Json, Query},
    Body, Endpoint, IntoResponse, Request, Response,
};
use rust_embed::Embed;
use serde::{Deserialize, Serialize};
use std::{
    hash::{DefaultHasher, Hash, Hasher},
    path::PathBuf,
    time::Duration,
};

use crate::rayon::{blocking_write, probe_compute};

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
    // Round-trips every pool: one that cannot run a no-op within the timeout (deadlocked, or
    // saturated beyond use) reports unhealthy.
    let result = tokio::time::timeout(
        Duration::from_secs(params.timeout.unwrap_or(10)),
        join(probe_compute(), blocking_write(|| {})),
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
    base_path: Option<String>,
    bundled_page: Option<TaggedPage>,
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
            base_path: None,
            bundled_page: None,
            gql,
        }
    }

    /// The path prefix the server is reached under, when a reverse proxy mounts it below the
    /// site root. It is written into every HTML page so the UI can find its own routes and the
    /// GraphQL endpoint.
    pub(crate) fn with_base_path(mut self, base_path: Option<String>) -> Self {
        self.base_path = base_path
            .map(|path| path.trim_end_matches('/').to_owned())
            .filter(|path| !path.is_empty());
        // The bundled page never changes while the server runs, so it is tagged once, here.
        // Pages in `public_dir` can change on disk and are tagged as they are served.
        self.bundled_page = match (&self.base_path, &self.public_dir) {
            (Some(base_path), None) => TaggedPage::bundled(base_path),
            _ => None,
        };
        self
    }

    async fn page(&self, req: Request) -> poem::Result<Response> {
        if let Some(public_dir) = &self.public_dir {
            let response = StaticFilesEndpoint::new(public_dir)
                .index_file("index.html")
                .fallback_to_index()
                .call(req)
                .await?;
            match &self.base_path {
                Some(base_path) => with_base_path_meta(response, base_path).await,
                None => Ok(response),
            }
        } else {
            let path = req.uri().path().trim_start_matches('/');
            let file_name = req.uri().path().split('/').next_back().unwrap_or("");

            if file_name.contains("worker") && file_name.ends_with("js") {
                // Always return the worker from root
                EmbeddedFileEndpoint::<PublicFolder>::new(file_name)
                    .call(req)
                    .await
            } else if path.is_empty()
                || path == "index.html"
                || (PublicFolder::get(path).is_none()
                    && PublicFolder::get(&format!("{path}/index.html")).is_none())
            {
                match &self.bundled_page {
                    Some(page) => Ok(page.respond(&req)),
                    None => {
                        EmbeddedFileEndpoint::<PublicFolder>::new("index.html")
                            .call(req)
                            .await
                    }
                }
            } else {
                EmbeddedFilesEndpoint::<PublicFolder>::new().call(req).await
            }
        }
    }
}

/// Writes the base path into a page, as a tag at the top of its `<head>`.
fn tag_base_path(html: &str, base_path: &str) -> String {
    let escaped = base_path
        .replace('&', "&amp;")
        .replace('"', "&quot;")
        .replace('<', "&lt;");
    let meta = format!(r#"<head><meta name="raphtory-base-path" content="{escaped}" />"#);
    html.replacen("<head>", &meta, 1)
}

/// The bundled UI page with the base path written into it.
struct TaggedPage {
    body: Bytes,
    etag: String,
}

impl TaggedPage {
    fn bundled(base_path: &str) -> Option<Self> {
        let page = PublicFolder::get("index.html")?;
        let body = tag_base_path(&String::from_utf8_lossy(&page.data), base_path);
        // Its own validator: the bundled file's no longer describes this body.
        let mut hasher = DefaultHasher::new();
        body.hash(&mut hasher);
        Some(Self {
            etag: format!("\"{:x}\"", hasher.finish()),
            body: body.into(),
        })
    }

    fn respond(&self, req: &Request) -> Response {
        let unchanged = req
            .headers()
            .get(header::IF_NONE_MATCH)
            .is_some_and(|seen| seen.as_bytes() == self.etag.as_bytes());
        if unchanged {
            return StatusCode::NOT_MODIFIED.into_response();
        }
        Response::builder()
            .content_type("text/html; charset=utf-8")
            .header(header::ETAG, &self.etag)
            .body(self.body.clone())
    }
}

/// Tags an HTML page served from `public_dir`; any other response is returned as is.
async fn with_base_path_meta(response: Response, base_path: &str) -> poem::Result<Response> {
    let is_html = response
        .content_type()
        .is_some_and(|content_type| content_type.starts_with("text/html"));
    if !is_html || !response.is_success() {
        return Ok(response);
    }
    let (mut parts, body) = response.into_parts();
    let html = body.into_string().await?;
    // The body no longer matches the length or the validators of the file it came from.
    for stale in [header::CONTENT_LENGTH, header::ETAG, header::LAST_MODIFIED] {
        parts.headers.remove(stale);
    }
    Ok(Response::from_parts(
        parts,
        Body::from_string(tag_base_path(&html, base_path)),
    ))
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
        } else {
            self.page(req).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::PublicFilesEndpoint;
    use poem::{
        endpoint::make_sync,
        http::{header, Method, StatusCode},
        Endpoint, IntoResponse, Request, Response,
    };
    use std::{fs, path::Path};
    use tempfile::tempdir;

    fn public_dir_endpoint(dir: &Path) -> PublicFilesEndpoint<impl Endpoint<Output = Response>> {
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

    #[tokio::test]
    async fn base_path_is_written_into_pages_only() {
        let dir = tempdir().unwrap();
        fs::write(
            dir.path().join("index.html"),
            "<html><head></head>ui</html>",
        )
        .unwrap();
        fs::write(dir.path().join("app.js"), "<head>").unwrap();
        let endpoint = public_dir_endpoint(dir.path()).with_base_path(Some("/proxy/1736/".into()));

        for path in ["/", "/graph/nested/route"] {
            let resp = get(&endpoint, path).await;
            assert_eq!(resp.status(), StatusCode::OK, "GET {path}");
            assert_eq!(
                resp.into_body().into_string().await.unwrap(),
                r#"<html><head><meta name="raphtory-base-path" content="/proxy/1736" /></head>ui</html>"#,
                "GET {path}"
            );
        }
        // a file that is not a page is served untouched
        let resp = get(&endpoint, "/app.js").await;
        assert_eq!(resp.into_body().into_string().await.unwrap(), "<head>");
    }

    #[tokio::test]
    async fn bundled_page_carries_the_base_path_and_can_be_revalidated() {
        let gql = make_sync(|_| Response::builder().body("gql"));
        let endpoint =
            PublicFilesEndpoint::new(None, false, gql).with_base_path(Some("/proxy/1736".into()));
        let tag = r#"<head><meta name="raphtory-base-path" content="/proxy/1736" />"#;

        let mut etags = vec![];
        for path in ["/", "/index.html", "/graph/nested/route"] {
            let resp = get(&endpoint, path).await;
            assert_eq!(resp.status(), StatusCode::OK, "GET {path}");
            etags.push(resp.header(header::ETAG).unwrap().to_owned());
            let page = resp.into_body().into_string().await.unwrap();
            assert_eq!(page.matches(tag).count(), 1, "GET {path}");
        }
        assert!(etags.iter().all(|etag| etag == &etags[0]));

        // a browser that already has this page is told so instead of being sent it again
        let revalidate = Request::builder()
            .method(Method::GET)
            .uri("/graph/nested/route".parse().unwrap())
            .header(header::IF_NONE_MATCH, &etags[0])
            .finish();
        let resp = endpoint.call(revalidate).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_MODIFIED);

        // without a base path the bundled page is served as it is
        let plain = PublicFilesEndpoint::new(None, false, make_sync(|_| "gql".into_response()));
        let page = get(&plain, "/")
            .await
            .into_body()
            .into_string()
            .await
            .unwrap();
        assert!(!page.contains(r#"<meta name="raphtory-base-path""#));
    }
}
