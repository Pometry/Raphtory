//! Tests of the SPARQL 1.1 Protocol endpoint, through the routes of the server.
use crate::{
    auth::{Access, Roles, TokenClaimValues},
    auth_policy::{
        auth_policy_tests::FakePolicy, AuthPolicyError, AuthorizationPolicy, GraphPermission,
        NamespacePermission,
    },
    config::app_config::{AppConfig, AppConfigBuilder},
    server::GraphServer,
    sparql::{run_limited, SparqlError},
    test_support::rdf::{
        graph, pairs, people, rows, without_carol, AS_OF_3, COUNT, KNOWN_BY, KNOWN_BY_NOW, KNOWS,
        NOW, SINCE,
    },
};
use async_graphql::Context;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use oxigraph::{
    io::RdfFormat,
    model::{Literal, NamedNode, NamedNodeRef},
};
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

const JSON: &str = "application/sparql-results+json";
const TEXT: &str = "text/plain; charset=utf-8";

/// `alice` knows `carol` (layer `knows`) and met `bob` in 2023 (layer `2023`, so the predicate
/// `<raphtory:2023>`, which does not end in an XML name and so cannot be written in RDF/XML).
fn years() -> Graph {
    let g = Graph::new();
    g.add_edge(1, "alice", "bob", NO_PROPS, Some("2023"))
        .unwrap();
    g.add_edge(1, "alice", "carol", NO_PROPS, Some("knows"))
        .unwrap();
    g
}

/// A server with the graphs `pg` (`people`), `eg` (its events as an event graph),
/// `ns/inner graph` (`people`) and `years`, and the routes it serves.
struct Server {
    _work_dir: TempDir,
    app: BoxEndpoint<'static, Response>,
}

impl Server {
    async fn new(config: AppConfig, policy: Option<Arc<dyn AuthorizationPolicy>>) -> Self {
        let work_dir = TempDir::new().unwrap();
        let pg = people();
        pg.encode(work_dir.path().join("pg")).unwrap();
        pg.event_graph().encode(work_dir.path().join("eg")).unwrap();
        fs::create_dir(work_dir.path().join("ns")).unwrap();
        pg.encode(work_dir.path().join("ns").join("inner graph"))
            .unwrap();
        years().encode(work_dir.path().join("years")).unwrap();
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

    /// A server whose policy grants `permission` on `pg` and `eg`.
    async fn as_user(permission: GraphPermission) -> Self {
        let policy = FakePolicy::default()
            .with_namespace("", NamespacePermission::Read)
            .with_graph("pg", permission.clone())
            .with_graph("eg", permission);
        Self::new(AppConfig::default(), Some(Arc::new(policy))).await
    }

    async fn send(&self, request: Request) -> Reply {
        let response = self.app.call(request).await.unwrap();
        let status = response.status();
        let header = |name: &str| response.header(name).map(str::to_owned).unwrap_or_default();
        let content_type = header("content-type");
        let allow = header("allow");
        let vary = header("vary");
        let body = response.into_body().into_string().await.unwrap();
        Reply {
            status,
            content_type,
            allow,
            vary,
            body,
        }
    }

    /// `GET /sparql/pg?<params>`.
    async fn get(&self, params: &[(&str, &str)], accept: Option<&str>) -> Reply {
        self.get_from("pg", params, accept).await
    }

    async fn get_from(&self, graph: &str, params: &[(&str, &str)], accept: Option<&str>) -> Reply {
        let accept = accept.map(|accept| ("accept", accept));
        self.get_with(graph, params, accept.as_slice()).await
    }

    /// `GET /sparql/<graph>?<params>` with `headers`, sent to the host `example.org`.
    async fn get_with(
        &self,
        graph: &str,
        params: &[(&str, &str)],
        headers: &[(&str, &str)],
    ) -> Reply {
        let mut request = Request::builder()
            .method(Method::GET)
            .uri(format!("/sparql/{graph}?{}", form(params)).parse().unwrap())
            .header("host", "example.org");
        for (name, value) in headers {
            request = request.header(*name, *value);
        }
        self.send(request.finish()).await
    }

    /// `POST /sparql/pg?<url params>` with `body` of type `content_type`.
    async fn post(
        &self,
        url_params: &[(&str, &str)],
        content_type: &str,
        body: impl Into<String>,
        accept: Option<&str>,
    ) -> Reply {
        let mut request = Request::builder()
            .method(Method::POST)
            .uri(format!("/sparql/pg?{}", form(url_params)).parse().unwrap())
            .header("content-type", content_type);
        if let Some(accept) = accept {
            request = request.header("accept", accept);
        }
        self.send(request.body(body.into())).await
    }

    /// The SPARQL Results JSON of `query` on `pg`, sent with GET.
    async fn select(&self, query: &str) -> Reply {
        self.get(&[("query", query)], None).await
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
    url::form_urlencoded::Serializer::new(String::new())
        .extend_pairs(params)
        .finish()
}

/// The IRI of `name` in the SPARQL service description vocabulary.
fn sd(name: &str) -> NamedNode {
    NamedNode::new_unchecked(format!(
        "http://www.w3.org/ns/sparql-service-description#{name}"
    ))
}

#[tokio::test]
async fn every_query_form_of_the_protocol() {
    let server = Server::default().await;
    // GET
    let reply = server.select(KNOWS).await;
    assert_eq!(reply.vary, "Accept");
    assert_eq!(rows(&reply.json()), pairs(NOW));
    // POST of a form, also with a charset
    for content_type in [
        "application/x-www-form-urlencoded",
        "application/x-www-form-urlencoded; charset=UTF-8",
    ] {
        let reply = server
            .post(&[], content_type, form(&[("query", KNOWS)]), None)
            .await;
        assert_eq!(rows(&reply.json()), pairs(NOW), "{content_type}");
    }
    // POST of the query
    let reply = server
        .post(&[], "application/sparql-query", KNOWS, None)
        .await;
    assert_eq!(rows(&reply.json()), pairs(NOW));
    // unknown parameters (`format` and `output` of SPARQLWrapper) are ignored
    let reply = server
        .get(
            &[("query", KNOWS), ("format", "xml"), ("output", "xml")],
            None,
        )
        .await;
    assert_eq!(rows(&reply.json()), pairs(NOW));
    // a graph in a namespace
    let reply = server
        .get_from("ns/inner%20graph", &[("query", KNOWS)], None)
        .await;
    assert_eq!(rows(&reply.json()), pairs(NOW));

    // ASK and CONSTRUCT
    let ask = server
        .select("ASK { <http://ex/bob> <http://ex/knows> <http://ex/carol> }")
        .await;
    assert_eq!(ask.json(), r#"{"head":{},"boolean":true}"#);
    let turtle = server.select(KNOWN_BY).await.ok("text/turtle");
    assert_eq!(
        graph(&turtle, RdfFormat::Turtle),
        graph(KNOWN_BY_NOW, RdfFormat::NTriples)
    );
}

#[tokio::test]
async fn the_endpoint_queries_the_current_state_unless_a_time_graph_is_read() {
    let server = Server::default().await;
    assert_eq!(rows(&server.select(KNOWS).await.json()), pairs(NOW));
    // the time graph holds `alice knows bob`, retracted since
    let as_of_3 = "PREFIX ex: <http://ex/> \
                   SELECT ?s ?o { GRAPH raphtory:asof:3 { ?s ex:knows ?o } } ORDER BY ?s ?o";
    assert_eq!(rows(&server.select(as_of_3).await.json()), pairs(AS_OF_3));
    // a time graph that is only a reference time keeps the current state
    let since_3 = "PREFIX ex: <http://ex/> SELECT ?s ?o { ?s ex:knows ?o \
                   BIND(raphtory:validFrom(?s, ex:knows, ?o, raphtory:asof:3) AS ?since) } \
                   ORDER BY ?s ?o";
    assert_eq!(rows(&server.select(since_3).await.json()), pairs(NOW));
}

#[tokio::test]
async fn the_accept_header_picks_the_format() {
    let server = Server::default().await;
    let csv = server.get(&[("query", KNOWS)], Some("text/csv")).await;
    csv.ok(QueryResultsFormat::Csv.media_type());
    let nt = server
        .get(&[("query", KNOWN_BY)], Some("application/n-triples"))
        .await
        .ok(RdfFormat::NTriples.media_type());
    assert_eq!(
        graph(&nt, RdfFormat::NTriples),
        graph(KNOWN_BY_NOW, RdfFormat::NTriples)
    );

    // a literal SPARQL Results XML cannot hold is written in the next format the Accept header
    // allows
    let control = "SELECT ?v { BIND(\"a\\u0001b\" AS ?v) }";
    let xml_first = "application/sparql-results+xml, application/sparql-results+json;q=0.5";
    let reply = server.get(&[("query", control)], Some(xml_first)).await;
    assert_eq!(
        rows(&reply.json()),
        [[Some(Literal::new_simple_literal("a\u{1}b").into())]]
    );
    // XML when it holds the results
    let reply = server.get(&[("query", KNOWS)], Some(xml_first)).await;
    reply.ok(QueryResultsFormat::Xml.media_type());

    // the same for triples RDF/XML cannot hold
    let from_years = |query: &'static str| {
        let server = &server;
        async move {
            server
                .get_from(
                    "years",
                    &[("query", query)],
                    Some("application/rdf+xml, text/turtle;q=0.5"),
                )
                .await
        }
    };
    let all = from_years("CONSTRUCT WHERE { ?s ?p ?o }")
        .await
        .ok(RdfFormat::Turtle.media_type());
    let expected = "<raphtory:alice> <raphtory:2023> <raphtory:bob> .
                    <raphtory:alice> <raphtory:knows> <raphtory:carol> .";
    assert_eq!(
        graph(&all, RdfFormat::Turtle),
        graph(expected, RdfFormat::NTriples)
    );
    let knows = from_years("CONSTRUCT WHERE { ?s <raphtory:knows> ?o }")
        .await
        .ok(RdfFormat::RdfXml.media_type());
    let expected = "<raphtory:alice> <raphtory:knows> <raphtory:carol> .";
    assert_eq!(
        graph(&knows, RdfFormat::RdfXml),
        graph(expected, RdfFormat::NTriples)
    );
}

#[tokio::test]
async fn dataset_parameters_set_the_dataset() {
    let server = Server::default().await;
    let all = "SELECT ?s ?o { ?s <http://ex/knows> ?o } ORDER BY ?s ?o";
    // the default graph as of 3, replacing `FROM`
    let from_now = "SELECT ?s ?o FROM raphtory:asof:9 { ?s <http://ex/knows> ?o } ORDER BY ?s ?o";
    let reply = server
        .get(
            &[
                ("query", from_now),
                ("default-graph-uri", "raphtory:asof:3"),
            ],
            None,
        )
        .await;
    assert_eq!(rows(&reply.json()), pairs(AS_OF_3));
    // with the query in the body, the dataset is in the URL
    let reply = server
        .post(
            &[("default-graph-uri", "raphtory:asof:3")],
            "application/sparql-query",
            all,
            None,
        )
        .await;
    assert_eq!(rows(&reply.json()), pairs(AS_OF_3));

    // named graphs; a graph named twice counts once
    let graphs = "SELECT ?g (COUNT(*) AS ?n) { GRAPH ?g { ?s <http://ex/knows> ?o } } \
                  GROUP BY ?g ORDER BY ?g";
    let reply = server
        .get(
            &[
                ("query", graphs),
                ("named-graph-uri", "raphtory:asof:3"),
                ("named-graph-uri", "raphtory:asof:9"),
                ("named-graph-uri", "raphtory:asof:3"),
            ],
            None,
        )
        .await;
    let row = |graph: &str| {
        [
            Some(NamedNode::new_unchecked(graph).into()),
            Some(Literal::from(2).into()),
        ]
    };
    assert_eq!(
        rows(&reply.json()),
        [row("raphtory:asof:3"), row("raphtory:asof:9")]
    );
}

#[tokio::test]
async fn asof_queries_the_graph_as_of_a_time() {
    let server = Server::default().await;
    let reply = server.get(&[("query", KNOWS), ("asof", "3")], None).await;
    assert_eq!(rows(&reply.json()), pairs(AS_OF_3));
}

#[tokio::test]
async fn the_service_description() {
    let server = Server::default().await;
    let request = Request::builder()
        .method(Method::GET)
        .uri("/sparql/pg".parse().unwrap())
        .header("host", "example.org:1736")
        .finish();
    let turtle = server.send(request).await.ok("text/turtle");
    let description = graph(&turtle, RdfFormat::Turtle);
    let has = |predicate: &str, object: &str| {
        description
            .subjects_for_predicate_object(&sd(predicate), NamedNodeRef::new_unchecked(object))
            .next()
            .is_some()
    };
    assert!(
        has("endpoint", "http://example.org:1736/sparql/pg"),
        "{turtle}"
    );
    for format in ["SPARQL_Results_JSON", "JSON-LD"] {
        assert!(
            has(
                "resultFormat",
                &format!("http://www.w3.org/ns/formats/{format}")
            ),
            "{turtle}"
        );
    }
    assert!(has("extensionFunction", "raphtory:validFrom"), "{turtle}");

    // negotiated
    let request = Request::builder()
        .method(Method::GET)
        .uri("/sparql/ns/inner%20graph".parse().unwrap())
        .header("host", "example.org")
        .header("accept", "application/n-triples")
        .finish();
    let nt = server.send(request).await.ok("application/n-triples");
    let endpoint = NamedNodeRef::new_unchecked("http://example.org/sparql/ns/inner%20graph");
    assert!(
        graph(&nt, RdfFormat::NTriples)
            .subjects_for_predicate_object(&sd("endpoint"), endpoint)
            .next()
            .is_some(),
        "{nt}"
    );
}

#[tokio::test]
async fn the_endpoint_keeps_the_limits_of_the_server() {
    let server = Server::default().await;
    let body = " ".repeat(1024 * 1024);
    let reply = server
        .post(&[], "application/sparql-query", body, None)
        .await;
    reply.error(StatusCode::PAYLOAD_TOO_LARGE);
    let capped = Server::with(|config| config.concurrency.max_sparql_result_bytes = Some(10)).await;
    capped
        .select(KNOWS)
        .await
        .error(StatusCode::PAYLOAD_TOO_LARGE);
    let disabled = Server::with(|config| config.concurrency.disable_lists = true).await;
    disabled
        .select(COUNT)
        .await
        .error(StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn a_panic_is_an_error() {
    let result = run_limited(None, |_| -> Result<(), SparqlError> { panic!("boom") }).await;
    assert!(matches!(result, Err(SparqlError::Panicked)));
}

/// A query waits for a `heavy_query_limit` slot; the service description takes none.
#[tokio::test]
async fn queries_take_a_heavy_query_slot() {
    let server = Server::with(|config| config.concurrency.heavy_query_limit = Some(0)).await;
    assert!(timeout(Duration::from_millis(200), server.select(KNOWS))
        .await
        .is_err());
    server.get(&[], None).await.ok("text/turtle");
}

/// Access control applies to every kind of request, and no access looks exactly like a missing
/// graph.
#[tokio::test]
async fn permissions_and_access_filters_apply() {
    let query: &[(&str, &str)] = &[("query", KNOWS)];
    let query_as_of_3: &[(&str, &str)] = &[("query", KNOWS), ("asof", "3")];
    let description: &[(&str, &str)] = &[];
    let since: &[(&str, &str)] = &[("query", SINCE)];

    let server = Server::as_user(without_carol()).await;
    assert_eq!(
        rows(&server.get(query, None).await.json()),
        pairs(&[("dave", "alice")])
    );
    assert_eq!(
        rows(&server.get(query_as_of_3, None).await.json()),
        pairs(&[("alice", "bob")])
    );
    // no temporal functions for a row-filtered read, and none in its service description
    server.get(since, None).await.error(StatusCode::BAD_REQUEST);
    let turtle = server.get(description, None).await.ok("text/turtle");
    assert!(graph(&turtle, RdfFormat::Turtle)
        .triples_for_predicate(&sd("extensionFunction"))
        .next()
        .is_none());

    // an unfiltered read has them
    let server = Server::as_user(GraphPermission::Read { filter: None }).await;
    assert_eq!(
        rows(&server.get(since, None).await.json()),
        [[Some(Literal::from(1).into())]]
    );
    server.get(description, None).await.ok("text/turtle");

    // introspection only and no grant look the same as no graph
    let server = Server::as_user(GraphPermission::Introspect).await;
    for params in [query, query_as_of_3, description] {
        for (graph, name) in [("pg", "pg"), ("ns/inner%20graph", "ns/inner graph")] {
            let error = server
                .get_from(graph, params, None)
                .await
                .error(StatusCode::NOT_FOUND);
            assert_eq!(
                error,
                format!("Graph '{name}' does not exist"),
                "{params:?}"
            );
        }
    }
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
    let token = jsonwebtoken::encode(&Header::new(Algorithm::EdDSA), &claims, &key).unwrap();
    format!("Bearer {token}")
}

/// The config of a server that verifies tokens with [`PUBLIC_KEY`] and needs one for reads.
fn reads_need_a_token() -> AppConfig {
    let mut config = AppConfigBuilder::new();
    config
        .with_auth_public_key(Some(PUBLIC_KEY.to_owned()))
        .unwrap()
        .with_require_auth_for_reads(true);
    config.build()
}

/// A policy that reads the caller's identity from the context, as those of deployments do: it
/// grants reads of `pg` to the role `analyst`, of `eg` to the claim `"team": "graph"`, and of
/// every graph to write access.
struct TokenPolicy;

impl AuthorizationPolicy for TokenPolicy {
    fn graph_permissions(
        &self,
        ctx: &Context<'_>,
        path: &str,
    ) -> Result<Option<GraphPermission>, AuthPolicyError> {
        let missing = |what: &str| AuthPolicyError::new(format!("no {what} in the context"));
        let access = ctx.data::<Access>().map_err(|_| missing("access"))?;
        let roles = Roles::from_context(ctx).map_err(|_| missing("roles"))?;
        let claims = ctx
            .data::<TokenClaimValues>()
            .map_err(|_| missing("claims"))?;
        let granted = *access == Access::Rw
            || match path {
                "pg" => roles.as_slice().iter().any(|role| role == "analyst"),
                "eg" => claims.string("team") == Some("graph"),
                _ => false,
            };
        Ok(granted.then_some(GraphPermission::Read { filter: None }))
    }

    fn namespace_permissions(
        &self,
        _ctx: &Context<'_>,
        _path: &str,
    ) -> Result<Option<NamespacePermission>, AuthPolicyError> {
        Ok(None)
    }
}

#[tokio::test]
async fn a_valid_token_reads_with_its_roles_and_claims() {
    let server = Server::new(reads_need_a_token(), Some(Arc::new(TokenPolicy))).await;
    let analyst = bearer(json!({ "access": "ro", "role": "analyst" }));
    let graph_team = bearer(json!({ "access": "ro", "team": "graph" }));
    let guest = bearer(json!({ "access": "ro", "role": "guest" }));
    let admin = bearer(json!({ "access": "rw" }));
    let read =
        |graph: &'static str, params: &'static [(&'static str, &'static str)], token: &str| {
            let server = &server;
            let token = token.to_owned();
            async move {
                server
                    .get_with(graph, params, &[("authorization", token.as_str())])
                    .await
            }
        };
    let knows: &[(&str, &str)] = &[("query", KNOWS)];
    let description: &[(&str, &str)] = &[];

    // the role, the claim and the access of the token reach the policy
    assert_eq!(rows(&read("pg", knows, &analyst).await.json()), pairs(NOW));
    assert_eq!(
        rows(&read("eg", knows, &graph_team).await.json()),
        pairs(NOW)
    );
    assert_eq!(
        rows(&read("ns/inner%20graph", knows, &admin).await.json()),
        pairs(NOW)
    );
    read("pg", description, &analyst).await.ok("text/turtle");
    // a read-only token without the role is not granted the graph
    read("pg", knows, &guest).await.error(StatusCode::NOT_FOUND);
    // without a token
    assert_eq!(server.select(KNOWS).await.status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn options_lists_the_allowed_methods() {
    let server = Server::default().await;
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
