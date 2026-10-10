//! Tests of the SPARQL 1.1 Protocol endpoint, through the routes of the server.
use crate::{
    auth::{Access, Roles, TokenClaimValues},
    auth_policy::{
        auth_policy_tests::FakePolicy, AuthPolicyError, AuthorizationPolicy, GraphPermission,
        NamespacePermission,
    },
    config::app_config::{AppConfig, AppConfigBuilder},
    server::GraphServer,
    sparql::Running,
    test_support::rdf::{
        pairs, people, read_with, rows, slow_count, triples, AS_OF_3, COUNT, KNOWN_BY, KNOWS, NOW,
        SINCE,
    },
};
use async_graphql::Context;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use oxigraph::io::JsonLdProfileSet;
use poem::{
    endpoint::BoxEndpoint,
    http::{Method, StatusCode},
    Endpoint, EndpointExt, Request, Response,
};
use raphtory::{
    db::api::storage::storage::Args,
    prelude::*,
    rdf::{QueryResultsFormat, RdfFormat},
};
use serde_json::{json, Value as Json};
use std::{
    fs,
    sync::Arc,
    time::{Duration, Instant},
};
use tempfile::TempDir;
use url::form_urlencoded::byte_serialize;

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
        people().encode(work_dir.path().join("pg")).unwrap();
        people()
            .event_graph()
            .encode(work_dir.path().join("eg"))
            .unwrap();
        fs::create_dir(work_dir.path().join("ns")).unwrap();
        people()
            .encode(work_dir.path().join("ns").join("inner graph"))
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
        let allow_origin = header("access-control-allow-origin");
        let vary = header("vary");
        let body = response.into_body().into_string().await.unwrap();
        Reply {
            status,
            content_type,
            allow,
            allow_origin,
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

    /// `GET /sparql/<graph>?<params>` with `headers`.
    async fn get_with(
        &self,
        graph: &str,
        params: &[(&str, &str)],
        headers: &[(&str, &str)],
    ) -> Reply {
        let mut request = Request::builder()
            .method(Method::GET)
            .uri(format!("/sparql/{graph}?{}", form(params)).parse().unwrap());
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
    allow_origin: String,
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

fn url_encode(s: &str) -> String {
    byte_serialize(s.as_bytes()).collect()
}

/// The results of `query` on the current state of `pg`, as `sparql_to_writer` writes them.
fn local(query: &str, format: QueryResultsFormat) -> String {
    let mut out = Vec::new();
    RdfGraph::from(people())
        .valid()
        .sparql_to_writer(query, &mut out, format)
        .unwrap();
    String::from_utf8(out).unwrap()
}

/// The triples of `KNOWN_BY` on the current state of `pg`.
const KNOWN_BY_NOW: [&str; 2] = [
    "<http://ex/alice> <http://ex/knownBy> <http://ex/dave> .",
    "<http://ex/carol> <http://ex/knownBy> <http://ex/bob> .",
];

#[tokio::test]
async fn every_query_form_of_the_protocol() {
    let server = Server::default().await;
    let expected = local(KNOWS, QueryResultsFormat::Json);
    assert_eq!(rows(&expected), pairs(NOW));
    // GET
    let reply = server.select(KNOWS).await;
    assert_eq!(reply.vary, "Accept");
    assert_eq!(reply.json(), expected);
    // POST of a form, also with a charset
    for content_type in [
        "application/x-www-form-urlencoded",
        "application/x-www-form-urlencoded; charset=UTF-8",
    ] {
        let reply = server
            .post(&[], content_type, form(&[("query", KNOWS)]), None)
            .await;
        assert_eq!(reply.json(), expected, "{content_type}");
    }
    // POST of the query
    let reply = server
        .post(&[], "application/sparql-query", KNOWS, None)
        .await;
    assert_eq!(reply.json(), expected);
    // unknown parameters (`format` and `output` of SPARQLWrapper) are ignored
    let reply = server
        .get(
            &[("query", KNOWS), ("format", "xml"), ("output", "xml")],
            None,
        )
        .await;
    assert_eq!(reply.json(), expected);
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
    assert_eq!(triples(&turtle, RdfFormat::Turtle), KNOWN_BY_NOW);
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
    // an event graph is read with persistent semantics
    let reply = server.get_from("eg", &[("query", KNOWS)], None).await;
    assert_eq!(rows(&reply.json()), pairs(NOW));
}

#[tokio::test]
async fn the_accept_header_picks_the_format() {
    let server = Server::default().await;
    let select = |accept: &'static str| {
        let server = &server;
        async move { server.get(&[("query", KNOWS)], Some(accept)).await }
    };
    let construct = |accept: &'static str| {
        let server = &server;
        async move { server.get(&[("query", KNOWN_BY)], Some(accept)).await }
    };

    for (accept, format) in [
        ("*/*", QueryResultsFormat::Json),
        ("application/json", QueryResultsFormat::Json),
        ("application/sparql-results+xml", QueryResultsFormat::Xml),
        ("application/xml", QueryResultsFormat::Xml),
        ("text/csv", QueryResultsFormat::Csv),
        ("text/tab-separated-values", QueryResultsFormat::Tsv),
        // SPARQLWrapper and YASGUI
        (
            "application/sparql-results+json,application/json,text/javascript,application/javascript",
            QueryResultsFormat::Json,
        ),
        (
            "application/sparql-results+json,*/*;q=0.9",
            QueryResultsFormat::Json,
        ),
    ] {
        let body = select(accept).await.ok(format.media_type());
        assert_eq!(body, local(KNOWS, format), "{accept}");
    }

    let json_ld = RdfFormat::JsonLd {
        profile: JsonLdProfileSet::empty(),
    };
    for (accept, format) in [
        ("*/*", RdfFormat::Turtle),
        ("application/n-triples", RdfFormat::NTriples),
        ("application/rdf+xml", RdfFormat::RdfXml),
        ("application/xml", RdfFormat::RdfXml),
        ("application/n-quads", RdfFormat::NQuads),
        ("application/trig", RdfFormat::TriG),
        ("application/ld+json", json_ld),
        ("application/json", json_ld),
        // rdflib and YASGUI
        (
            "application/sparql-results+xml, application/rdf+xml",
            RdfFormat::RdfXml,
        ),
        (
            "application/sparql-results+json,*/*;q=0.9",
            RdfFormat::Turtle,
        ),
    ] {
        let body = construct(accept).await.ok(format.media_type());
        assert_eq!(triples(&body, format), KNOWN_BY_NOW, "{accept}");
    }

    // nothing acceptable: 406 before the query runs, or once its form is known
    select("text/plain").await.error(StatusCode::NOT_ACCEPTABLE);
    construct("text/csv")
        .await
        .error(StatusCode::NOT_ACCEPTABLE);
    select("text/turtle")
        .await
        .error(StatusCode::NOT_ACCEPTABLE);

    // a literal SPARQL Results XML cannot hold is written in the next format the Accept header
    // allows
    let control = "SELECT ?v { BIND(\"a\\u0001b\" AS ?v) }";
    let xml_first = "application/sparql-results+xml, application/sparql-results+json;q=0.5";
    let reply = server.get(&[("query", control)], Some(xml_first)).await;
    assert_eq!(
        reply.ok(QueryResultsFormat::Json.media_type()),
        local(control, QueryResultsFormat::Json)
    );
    // XML when it holds the results
    let reply = server.get(&[("query", KNOWS)], Some(xml_first)).await;
    assert_eq!(
        reply.ok(QueryResultsFormat::Xml.media_type()),
        local(KNOWS, QueryResultsFormat::Xml)
    );

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
    assert_eq!(
        triples(&all, RdfFormat::Turtle),
        [
            "<raphtory:alice> <raphtory:2023> <raphtory:bob> .",
            "<raphtory:alice> <raphtory:knows> <raphtory:carol> .",
        ]
    );
    let knows = from_years("CONSTRUCT WHERE { ?s <raphtory:knows> ?o }")
        .await
        .ok(RdfFormat::RdfXml.media_type());
    assert_eq!(
        triples(&knows, RdfFormat::RdfXml),
        ["<raphtory:alice> <raphtory:knows> <raphtory:carol> ."]
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
    assert_eq!(
        rows(&reply.json()),
        [["raphtory:asof:3", "2"], ["raphtory:asof:9", "2"]]
    );
}

#[tokio::test]
async fn asof_queries_the_graph_as_of_a_time() {
    let server = Server::default().await;
    // as `FROM <raphtory:asof:T>`, before, between and after the retraction
    for t in ["0", "3", "6", "9"] {
        let from = format!(
            "PREFIX ex: <http://ex/> SELECT ?s ?o FROM <raphtory:asof:{t}> {{ ?s ex:knows ?o }} \
             ORDER BY ?s ?o"
        );
        let from = rows(&server.select(&from).await.json());
        let reply = server.get(&[("query", KNOWS), ("asof", t)], None).await;
        assert_eq!(rows(&reply.json()), from, "asof={t}");
    }
    // a timezone offset in a URL, its `+` written as is (so read as a space)
    let uri = format!(
        "/sparql/pg?asof=1970-01-01T01:00:00.003+01:00&query={}",
        url_encode(KNOWS)
    );
    let reply = server
        .send(
            Request::builder()
                .method(Method::GET)
                .uri(uri.parse().unwrap())
                .finish(),
        )
        .await;
    assert_eq!(rows(&reply.json()), pairs(AS_OF_3));
    // in a form
    let reply = server
        .post(
            &[],
            "application/x-www-form-urlencoded",
            form(&[("query", KNOWS), ("asof", "3")]),
            None,
        )
        .await;
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
    let triples = triples(&turtle, RdfFormat::Turtle);
    let sd = |name: &str| format!("<http://www.w3.org/ns/sparql-service-description#{name}>");
    let has = |predicate: &str, object: &str| {
        triples
            .iter()
            .any(|triple| triple.ends_with(&format!(" {predicate} {object} .")))
    };
    assert!(
        has(&sd("endpoint"), "<http://example.org:1736/sparql/pg>"),
        "{turtle}"
    );
    for format in ["SPARQL_Results_JSON", "JSON-LD"] {
        assert!(
            has(
                &sd("resultFormat"),
                &format!("<http://www.w3.org/ns/formats/{format}>")
            ),
            "{turtle}"
        );
    }
    assert!(
        has(&sd("extensionFunction"), "<raphtory:validFrom>"),
        "{turtle}"
    );

    // negotiated, behind a proxy
    let request = Request::builder()
        .method(Method::GET)
        .uri("/sparql/ns/inner%20graph".parse().unwrap())
        .header("accept", "application/n-triples")
        .header("x-forwarded-proto", "https")
        .header("x-forwarded-host", "raphtory.example")
        .finish();
    let nt = server.send(request).await.ok("application/n-triples");
    assert!(
        nt.contains(&format!(
            "{} <https://raphtory.example/sparql/ns/inner%20graph> .",
            sd("endpoint")
        )),
        "{nt}"
    );
}

#[tokio::test]
async fn protocol_errors_have_their_status() {
    let server = Server::default().await;
    let disabled = Server::with(|config| config.concurrency.disable_lists = true).await;
    let update = "INSERT DATA { <http://ex/a> <http://ex/b> <http://ex/c> }";
    let put = Request::builder()
        .method(Method::PUT)
        .uri("/sparql/pg".parse().unwrap())
        .finish();
    let put = server.send(put).await;
    assert_eq!(put.allow, "GET, POST, OPTIONS");

    let replies = [
        (
            StatusCode::BAD_REQUEST,
            server.select("SELECT ?x WHERE { ?x").await,
        ),
        (
            StatusCode::BAD_REQUEST,
            server.get(&[("query", KNOWS), ("asof", "foo")], None).await,
        ),
        (
            StatusCode::PAYLOAD_TOO_LARGE,
            server
                .post(
                    &[],
                    "application/sparql-query",
                    " ".repeat(1024 * 1024),
                    None,
                )
                .await,
        ),
        (
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            server.post(&[], "application/json", "{}", None).await,
        ),
        (StatusCode::METHOD_NOT_ALLOWED, put),
        (
            StatusCode::NOT_FOUND,
            server.get_from("nope", &[("query", KNOWS)], None).await,
        ),
        (
            StatusCode::NOT_ACCEPTABLE,
            server.get(&[], Some("text/csv")).await,
        ),
        (
            StatusCode::NOT_IMPLEMENTED,
            server
                .post(&[], "application/sparql-update", update, None)
                .await,
        ),
        (
            StatusCode::SERVICE_UNAVAILABLE,
            disabled.select(COUNT).await,
        ),
    ];
    for (row, (status, reply)) in replies.into_iter().enumerate() {
        assert_eq!(
            (reply.status, reply.content_type.as_str()),
            (status, TEXT),
            "row {row}: {}",
            reply.body
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slow_queries_time_out() {
    let server = Arc::new(
        Server::with(|config| config.concurrency.sparql_timeout = Some(Duration::from_millis(200)))
            .await,
    );
    let query = slow_count("endpoint timeout");
    let slow = {
        let server = server.clone();
        let query = query.clone();
        tokio::spawn(async move { server.select(&query).await })
    };
    // Time the query from when it runs: before that it waits for the process-global compute
    // pool, which other tests (such as `rayon::deadlock_tests`) can hold for seconds.
    let start = Instant::now();
    while Running::count(&query) == 0 && !slow.is_finished() {
        assert!(start.elapsed() < Duration::from_secs(60), "never ran");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let running = Instant::now();
    let error = slow.await.unwrap().error(StatusCode::GATEWAY_TIMEOUT);
    assert_eq!(
        error,
        "SPARQL query timed out: it ran longer than its time limit of 200ms (`sparql_timeout`)"
    );
    assert!(running.elapsed() < Duration::from_secs(10));
    assert_eq!(rows(&server.select(COUNT).await.json()), [["4"]]);
}

/// The endpoint and the GraphQL `sparql` field share the `heavy_query_limit` slots.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queries_share_the_heavy_query_limit_with_graphql() {
    let server = Arc::new(
        Server::with(|config| {
            config.concurrency.heavy_query_limit = Some(1);
            config.concurrency.sparql_timeout = Some(Duration::from_millis(1500));
        })
        .await,
    );
    let query = slow_count("endpoint slot");
    let slow = {
        let server = server.clone();
        let query = query.clone();
        tokio::spawn(async move { server.select(&query).await })
    };
    let start = Instant::now();
    while Running::count(&query) == 0 {
        assert!(start.elapsed() < Duration::from_secs(30), "never ran");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let started = Instant::now();
    let graphql = Request::builder()
        .method(Method::POST)
        .uri("/".parse().unwrap())
        .header("content-type", "application/json")
        .body(
            json!({ "query": r#"{ rdfGraph(path: "pg") { sparql(query: "ASK {}") } }"# })
                .to_string(),
        );
    let reply = server.send(graphql).await;
    let waited = started.elapsed();
    assert_eq!(
        serde_json::from_str::<Json>(&reply.body).unwrap(),
        json!({ "data": { "rdfGraph": { "sparql": r#"{"head":{},"boolean":true}"# } } })
    );
    assert!(
        waited >= Duration::from_millis(1000),
        "the GraphQL query ran after {waited:?}, while the endpoint query held the slot"
    );
    slow.await.unwrap().error(StatusCode::GATEWAY_TIMEOUT);
}

/// Access control applies to every kind of request, and no access looks exactly like a missing
/// graph.
#[tokio::test]
async fn permissions_and_access_filters_apply() {
    let query: &[(&str, &str)] = &[("query", KNOWS)];
    let query_as_of_3: &[(&str, &str)] = &[("query", KNOWS), ("asof", "3")];
    let description: &[(&str, &str)] = &[];
    let since: [&[(&str, &str)]; 2] = [&[("query", SINCE)], &[("query", SINCE), ("asof", "3")]];

    let server = Server::as_user(read_with(json!({
        "filter": { "node": { "name": { "where": { "ne": { "str": "http://ex/carol" } } } } }
    })))
    .await;
    assert_eq!(
        rows(&server.get(query, None).await.json()),
        pairs(&[("dave", "alice")])
    );
    assert_eq!(
        rows(&server.get(query_as_of_3, None).await.json()),
        pairs(&[("alice", "bob")])
    );
    // no temporal functions for a row-filtered read
    for params in since {
        let error = server
            .get(params, None)
            .await
            .error(StatusCode::BAD_REQUEST);
        assert!(error.contains("validFromTime"), "{params:?}: {error}");
    }

    // an unfiltered read has them
    let server = Server::as_user(GraphPermission::Read { filter: None }).await;
    for params in since {
        assert_eq!(
            rows(&server.get(params, None).await.json()),
            [["1"]],
            "{params:?}"
        );
    }
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
    // a graph the policy does not grant looks like one that does not exist
    for (graph, params, token) in [("pg", knows, &guest), ("eg", description, &analyst)] {
        let error = read(graph, params, token)
            .await
            .error(StatusCode::NOT_FOUND);
        assert_eq!(error, format!("Graph '{graph}' does not exist"));
    }
    // without a token, or with one that does not verify
    for reply in [
        server.select(KNOWS).await,
        read("pg", knows, "Bearer not.a.token").await,
    ] {
        assert_eq!(reply.status, StatusCode::UNAUTHORIZED, "{}", reply.body);
    }
}

#[tokio::test]
async fn browsers_can_call_the_endpoint_from_other_origins() {
    let server = Server::default().await;
    let preflight = Request::builder()
        .method(Method::OPTIONS)
        .uri("/sparql/pg".parse().unwrap())
        .header("origin", "https://yasgui.example")
        .header("access-control-request-method", "POST")
        .header("access-control-request-headers", "content-type")
        .finish();
    let reply = server.send(preflight).await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.body);
    assert_eq!(reply.allow_origin, "https://yasgui.example");
    let request = Request::builder()
        .method(Method::POST)
        .uri("/sparql/pg".parse().unwrap())
        .header("origin", "https://yasgui.example")
        .header("content-type", "application/x-www-form-urlencoded")
        .body(form(&[("query", COUNT)]));
    let reply = server.send(request).await;
    assert_eq!(reply.allow_origin, "https://yasgui.example");
    assert_eq!(rows(&reply.json()), [["4"]]);
    // without an origin
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
