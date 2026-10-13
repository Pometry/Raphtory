//! Tests of the SPARQL 1.1 Protocol endpoint, through the routes of the server.
use crate::{
    auth_policy::{
        auth_policy_tests::FakePolicy, AuthorizationPolicy, GraphPermission, NamespacePermission,
    },
    config::app_config::{AppConfig, AppConfigBuilder},
    server::GraphServer,
    test_support::rdf::{graph, pairs, people, rows, AS_OF_3, KNOWN_BY, KNOWN_BY_NOW, KNOWS, NOW},
};
use itertools::Itertools;
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use oxigraph::io::RdfFormat;
use poem::{
    endpoint::BoxEndpoint,
    http::{Method, StatusCode},
    Endpoint, EndpointExt, Request, Response,
};
use raphtory::{db::api::storage::storage::Args, prelude::*};
use raphtory_rdf::QueryResultsFormat;
use serde_json::{json, Value as Json};
use std::{fs, sync::Arc, time::Duration};
use tempfile::TempDir;
use tokio::time::timeout;
use url::form_urlencoded::Serializer;

const JSON: &str = "application/sparql-results+json";
const TEXT: &str = "text/plain; charset=utf-8";

/// A server with the graphs `pg` and `ns/inner graph`, both [`people`], and the routes it
/// serves.
struct Server {
    _work_dir: TempDir,
    app: BoxEndpoint<'static, Response>,
}

impl Server {
    async fn new(config: AppConfig, policy: Option<Arc<dyn AuthorizationPolicy>>) -> Self {
        let work_dir = TempDir::new().unwrap();
        let pg = people();
        pg.encode(work_dir.path().join("pg")).unwrap();
        fs::create_dir(work_dir.path().join("ns")).unwrap();
        pg.encode(work_dir.path().join("ns").join("inner graph"))
            .unwrap();
        let mut server =
            GraphServer::new(work_dir.path().to_path_buf(), Some(config), Args::default())
                .await
                .unwrap();
        if let Some(policy) = policy {
            server = server.with_auth_policy(policy);
        }
        let app = server.generate_endpoint(None).await.unwrap();
        Self {
            _work_dir: work_dir,
            app: app.map_to_response().boxed(),
        }
    }

    async fn default() -> Self {
        Self::new(AppConfig::default(), None).await
    }

    async fn with(config: impl FnOnce(&mut AppConfig)) -> Self {
        let mut app_config = AppConfig::default();
        config(&mut app_config);
        Self::new(app_config, None).await
    }

    /// A server whose policy grants `permission` on `pg` and `ns/inner graph`.
    async fn as_user(permission: GraphPermission) -> Self {
        let policy = FakePolicy::default()
            .with_namespace("", NamespacePermission::Read)
            .with_graph("pg", permission.clone())
            .with_graph("ns/inner graph", permission);
        Self::new(AppConfig::default(), Some(Arc::new(policy))).await
    }

    async fn send(&self, request: Request) -> Reply {
        let response = self.app.call(request).await.unwrap();
        let status = response.status();
        let header = |name: &str| response.header(name).map(str::to_owned).unwrap_or_default();
        let content_type = header("content-type");
        let allow = header("allow");
        let vary = response
            .headers()
            .get_all("vary")
            .iter()
            .filter_map(|value| value.to_str().ok())
            .join(", ");
        let body = response.into_body().into_string().await.unwrap();
        Reply {
            status,
            content_type,
            allow,
            vary,
            body,
        }
    }

    /// `GET /sparql/<graph>?<params>` with `headers`.
    async fn get(&self, graph: &str, params: &[(&str, &str)], headers: &[(&str, &str)]) -> Reply {
        let mut request = Request::builder()
            .method(Method::GET)
            .uri(format!("/sparql/{graph}?{}", form(params)).parse().unwrap());
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        self.send(request.finish()).await
    }

    /// `POST /sparql/pg` with `body` of type `content_type`.
    async fn post(&self, content_type: &str, body: impl Into<String>) -> Reply {
        let request = Request::builder()
            .method(Method::POST)
            .uri("/sparql/pg".parse().unwrap())
            .header("content-type", content_type);
        self.send(request.body(body.into())).await
    }

    /// The results of `query` on `pg`, sent with GET.
    async fn select(&self, query: &str) -> Reply {
        self.get("pg", &[("query", query)], &[]).await
    }
}

#[derive(Debug)]
struct Reply {
    status: StatusCode,
    content_type: String,
    allow: String,
    vary: String,
    body: String,
}

impl Reply {
    /// Checks the status and content type and returns the body.
    fn expect(self, status: StatusCode, content_type: &str) -> String {
        assert_eq!(
            (self.status, self.content_type.as_str()),
            (status, content_type),
            "{}",
            self.body
        );
        self.body
    }

    fn ok(self, content_type: &str) -> String {
        self.expect(StatusCode::OK, content_type)
    }

    fn json(self) -> String {
        self.ok(JSON)
    }

    /// Checks that the request failed with `status` and returns the message.
    fn error(self, status: StatusCode) -> String {
        self.expect(status, TEXT)
    }
}

fn form(params: &[(&str, &str)]) -> String {
    Serializer::new(String::new()).extend_pairs(params).finish()
}

#[tokio::test]
async fn every_query_form_of_the_protocol() {
    let server = Server::default().await;
    let reply = server
        .get("pg", &[("query", KNOWS)], &[("origin", "http://ex")])
        .await;
    assert_eq!(reply.vary, "Origin, Accept");
    assert_eq!(rows(&reply.json()), pairs(NOW));
    let reply = server
        .post(
            "application/x-www-form-urlencoded; charset=UTF-8",
            form(&[("query", KNOWS)]),
        )
        .await;
    assert_eq!(rows(&reply.json()), pairs(NOW));
    let reply = server.post("application/sparql-query", KNOWS).await;
    assert_eq!(rows(&reply.json()), pairs(NOW));
    // unknown parameters (`format` and `output` of SPARQLWrapper) are ignored
    let params = [("query", KNOWS), ("format", "xml"), ("output", "xml")];
    let reply = server.get("pg", &params, &[]).await;
    assert_eq!(rows(&reply.json()), pairs(NOW));
    let reply = server
        .get("ns/inner%20graph", &[("query", KNOWS)], &[])
        .await;
    assert_eq!(rows(&reply.json()), pairs(NOW));

    let ask = server
        .select("ASK { <http://ex/bob> <http://ex/knows> <http://ex/carol> }")
        .await;
    assert_eq!(ask.json(), r#"{"head":{},"boolean":true}"#);
    let turtle = server.select(KNOWN_BY).await.ok("text/turtle");
    assert_eq!(
        graph(&turtle, RdfFormat::Turtle),
        graph(KNOWN_BY_NOW, RdfFormat::NTriples)
    );

    let options = Request::builder()
        .method(Method::OPTIONS)
        .uri("/sparql/pg".parse().unwrap())
        .finish();
    let reply = server.send(options).await;
    assert_eq!(
        (reply.status, reply.allow.as_str()),
        (StatusCode::NO_CONTENT, "GET, POST, OPTIONS")
    );
}

#[tokio::test]
async fn the_view_is_the_current_state_unless_a_time_graph_is_read() {
    let server = Server::default().await;
    // the time graph holds `alice knows bob`, retracted since
    let as_of_3 = "PREFIX ex: <http://ex/> \
                   SELECT ?s ?o { GRAPH raphtory:asof:3 { ?s ex:knows ?o } } ORDER BY ?s ?o";
    assert_eq!(rows(&server.select(as_of_3).await.json()), pairs(AS_OF_3));
    // a time graph that is only an argument keeps the current state
    let since_3 = "PREFIX ex: <http://ex/> SELECT ?s ?o { ?s ex:knows ?o \
                   BIND(raphtory:validFrom(?s, ex:knows, ?o, raphtory:asof:3) AS ?since) } \
                   ORDER BY ?s ?o";
    assert_eq!(rows(&server.select(since_3).await.json()), pairs(NOW));
    let reply = server
        .get("pg", &[("query", KNOWS), ("asof", "3")], &[])
        .await;
    assert_eq!(rows(&reply.json()), pairs(AS_OF_3));
}

#[tokio::test]
async fn the_accept_header_picks_the_format() {
    let server = Server::default().await;
    let accept = |accept| [("accept", accept)];
    server
        .get("pg", &[("query", KNOWS)], &accept("text/csv"))
        .await
        .ok(QueryResultsFormat::Csv.media_type());
    let nt = server
        .get(
            "pg",
            &[("query", KNOWN_BY)],
            &accept("text/turtle;q=0.5, application/n-triples"),
        )
        .await
        .ok("application/n-triples");
    assert_eq!(
        graph(&nt, RdfFormat::NTriples),
        graph(KNOWN_BY_NOW, RdfFormat::NTriples)
    );
    server
        .get(
            "pg",
            &[("query", KNOWS)],
            &accept("application/sparql-results+json;q=0, */*"),
        )
        .await
        .ok("application/json");
    server
        .get("pg", &[("query", KNOWS)], &accept("text/turtle"))
        .await
        .error(StatusCode::NOT_ACCEPTABLE);
}

/// No access looks like a missing graph, and the errors of the GraphQL request are the errors
/// of the request.
#[tokio::test]
async fn permissions_and_errors() {
    let server = Server::as_user(GraphPermission::Introspect).await;
    for (graph, name) in [("pg", "pg"), ("ns/inner%20graph", "ns/inner graph")] {
        let error = server
            .get(graph, &[("query", KNOWS)], &[])
            .await
            .error(StatusCode::NOT_FOUND);
        assert_eq!(error, format!("Graph '{name}' does not exist"));
    }

    let server =
        Server::with(|config| config.concurrency.max_sparql_triple_patterns = Some(0)).await;
    server.select(KNOWS).await.error(StatusCode::BAD_REQUEST);
    let error = server
        .get("missing", &[("query", KNOWS)], &[])
        .await
        .error(StatusCode::NOT_FOUND);
    assert_eq!(error, "Graph 'missing' does not exist");
}

#[tokio::test]
async fn queries_take_a_heavy_query_slot() {
    let server = Server::with(|config| config.concurrency.heavy_query_limit = Some(0)).await;
    assert!(timeout(Duration::from_millis(200), server.select(KNOWS))
        .await
        .is_err());
    let light = Request::builder()
        .method(Method::POST)
        .uri("/".parse().unwrap())
        .content_type("application/json")
        .body(json!({ "query": "{ __typename }" }).to_string());
    let reply = timeout(Duration::from_secs(10), server.send(light))
        .await
        .unwrap();
    assert_eq!(reply.status, StatusCode::OK);
}

/// The public key that verifies tokens, and its private key (both from
/// `python/tests/test_auth.py`).
const PUBLIC_KEY: &str = "MCowBQYDK2VwAyEADdrWr1kTLj+wSHlr45eneXmOjlHo3N1DjLIvDa2ozno=";
const PRIVATE_KEY: &str = "-----BEGIN PRIVATE KEY-----
MC4CAQAwBQYDK2VwBCIEIFzEcSO/duEjjX4qKxDVy4uLqfmiEIA6bEw1qiPyzTQg
-----END PRIVATE KEY-----";

/// The `Authorization` header of a token with `claims`, signed with [`PRIVATE_KEY`].
fn bearer(claims: Json) -> String {
    let key = EncodingKey::from_ed_pem(PRIVATE_KEY.as_bytes()).unwrap();
    let token = encode(&Header::new(Algorithm::EdDSA), &claims, &key).unwrap();
    format!("Bearer {token}")
}

#[tokio::test]
async fn the_token_of_the_request_is_used() {
    let mut config = AppConfigBuilder::new();
    config
        .with_auth_public_key(Some(PUBLIC_KEY.to_owned()))
        .unwrap()
        .with_require_auth_for_reads(true);
    let server = Server::new(config.build(), None).await;
    let token = bearer(json!({ "access": "ro" }));
    let reply = server
        .get(
            "pg",
            &[("query", KNOWS)],
            &[("authorization", token.as_str())],
        )
        .await;
    assert_eq!(rows(&reply.json()), pairs(NOW));
    assert_eq!(server.select(KNOWS).await.status, StatusCode::UNAUTHORIZED);
}
