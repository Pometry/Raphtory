//! Tests of the SPARQL 1.1 Protocol endpoint, through the routes of the server.
use crate::{
    auth::{Access, Roles, TokenClaimValues},
    auth_policy::{
        auth_policy_tests::FakePolicy, AuthPolicyError, AuthorizationPolicy, GraphPermission,
        NamespacePermission,
    },
    config::app_config::{AppConfig, AppConfigBuilder},
    model::graph::filtering::GraphAccessFilter,
    server::GraphServer,
    sparql::Running,
};
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use poem::{
    endpoint::BoxEndpoint,
    http::{Method, StatusCode},
    Endpoint, EndpointExt, Request, Response,
};
use raphtory::{
    db::api::storage::storage::Args,
    prelude::*,
    rdf::{QueryResultsFormat, RdfFormat, RdfParser, RdfViewOps, SparqlFormat},
};
use serde_json::{json, Value as Json};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tempfile::TempDir;
use url::form_urlencoded::byte_serialize;

const PEOPLE: &str = r#"
    @prefix ex: <http://ex/> .
    ex:alice ex:knows ex:bob ; ex:age 42 .
    ex:bob ex:knows ex:carol .
    ex:carol ex:name "Carol" .
"#;
const KNOWS: &str = "PREFIX ex: <http://ex/> SELECT ?s ?o { ?s ex:knows ?o } ORDER BY ?s ?o";
const COUNT: &str = "SELECT (COUNT(*) AS ?n) { ?s ?p ?o }";
const KNOWN_BY: &str =
    "PREFIX ex: <http://ex/> CONSTRUCT { ?o ex:knownBy ?s } WHERE { ?s ex:knows ?o }";
/// When `bob knows carol` started holding: at 1.
const SINCE: &str = "PREFIX ex: <http://ex/> \
    SELECT (raphtory:validFromTime(ex:bob, ex:knows, ex:carol) AS ?t) {}";
const JSON: &str = "application/sparql-results+json";
const TEXT: &str = "text/plain; charset=utf-8";

/// Asserted at 1: `alice knows bob`, `alice age 42`, `bob knows carol`, `carol name "Carol"`.
/// `alice knows bob` is retracted at 5, and `dave knows alice` asserted at 7.
fn people() -> PersistentGraph {
    let pg = PersistentGraph::new();
    pg.load_rdf(1, PEOPLE.as_bytes(), RdfFormat::Turtle, None)
        .unwrap();
    let triple = |s: &str, o: &str| format!("<http://ex/{s}> <http://ex/knows> <http://ex/{o}> .");
    pg.retract_rdf(
        5,
        triple("alice", "bob").as_bytes(),
        RdfFormat::NTriples,
        None,
    )
    .unwrap();
    pg.load_rdf(
        7,
        triple("dave", "alice").as_bytes(),
        RdfFormat::NTriples,
        None,
    )
    .unwrap();
    pg
}

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
        std::fs::create_dir(work_dir.path().join("ns")).unwrap();
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

/// The values of the solutions of SPARQL Results JSON, `""` for an unbound variable.
fn rows(results: &str) -> Vec<Vec<String>> {
    let results: Json = serde_json::from_str(results).unwrap();
    let vars: Vec<&str> = results["head"]["vars"]
        .as_array()
        .unwrap()
        .iter()
        .map(|var| var.as_str().unwrap())
        .collect();
    results["results"]["bindings"]
        .as_array()
        .unwrap()
        .iter()
        .map(|solution| {
            vars.iter()
                .map(|var| solution[var]["value"].as_str().unwrap_or("").to_owned())
                .collect()
        })
        .collect()
}

fn pairs(pairs: &[(&str, &str)]) -> Vec<Vec<String>> {
    pairs
        .iter()
        .map(|(s, o)| vec![format!("http://ex/{s}"), format!("http://ex/{o}")])
        .collect()
}

/// The results of `query` on `pg`, as `sparql_to_writer` writes them.
fn local(query: &str, format: impl Into<SparqlFormat>) -> String {
    let mut out = Vec::new();
    people().sparql_to_writer(query, &mut out, format).unwrap();
    String::from_utf8(out).unwrap()
}

/// The triples of an RDF document, in N-Triples form and sorted.
fn triples(document: &str, format: RdfFormat) -> Vec<String> {
    let mut triples: Vec<String> = RdfParser::from_format(format)
        .for_slice(document.as_bytes())
        .map(|quad| format!("{} .", quad.unwrap()))
        .collect();
    triples.sort();
    triples
}

const NOW: &[(&str, &str)] = &[("bob", "carol"), ("dave", "alice")];
const AS_OF_3: &[(&str, &str)] = &[("alice", "bob"), ("bob", "carol")];

#[tokio::test]
async fn every_query_form_of_the_protocol() {
    let server = Server::default().await;
    let expected = local(KNOWS, QueryResultsFormat::Json);
    assert_eq!(rows(&expected), pairs(NOW));
    // GET
    assert_eq!(server.select(KNOWS).await.json(), expected);
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

    // ASK, CONSTRUCT and DESCRIBE
    let ask = server
        .select("ASK { <http://ex/bob> <http://ex/knows> <http://ex/carol> }")
        .await;
    assert_eq!(ask.json(), r#"{"head":{},"boolean":true}"#);
    let turtle = server.select(KNOWN_BY).await.ok("text/turtle");
    assert_eq!(turtle, local(KNOWN_BY, RdfFormat::Turtle));
    assert_eq!(
        triples(&turtle, RdfFormat::Turtle),
        [
            "<http://ex/alice> <http://ex/knownBy> <http://ex/dave> .",
            "<http://ex/carol> <http://ex/knownBy> <http://ex/bob> .",
        ]
    );
    let describe = server
        .select("DESCRIBE <http://ex/carol>")
        .await
        .ok("text/turtle");
    assert_eq!(
        triples(&describe, RdfFormat::Turtle),
        ["<http://ex/carol> <http://ex/name> \"Carol\" ."]
    );
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
        // q-values
        (
            "text/csv;q=0.5, application/sparql-results+xml;q=0.8, text/turtle",
            QueryResultsFormat::Xml,
        ),
        (
            "text/csv;q=0, text/tab-separated-values;q=0.5, */*;q=0.1",
            QueryResultsFormat::Tsv,
        ),
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
    assert_eq!(
        select("text/csv").await.body,
        "s,o\r\nhttp://ex/bob,http://ex/carol\r\nhttp://ex/dave,http://ex/alice\r\n"
    );

    for (accept, format) in [
        ("*/*", RdfFormat::Turtle),
        ("application/n-triples", RdfFormat::NTriples),
        ("application/rdf+xml", RdfFormat::RdfXml),
        ("application/xml", RdfFormat::RdfXml),
        ("application/n-quads", RdfFormat::NQuads),
        ("application/trig", RdfFormat::TriG),
        (
            "application/n-triples;q=0.4, application/rdf+xml;q=0.9",
            RdfFormat::RdfXml,
        ),
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
        assert_eq!(body, local(KNOWN_BY, format), "{accept}");
    }
    for accept in ["application/ld+json", "application/json"] {
        let body = construct(accept).await.ok("application/ld+json");
        assert_eq!(
            triples(
                &body,
                RdfFormat::from_media_type("application/ld+json").unwrap()
            ),
            triples(&local(KNOWN_BY, RdfFormat::Turtle), RdfFormat::Turtle)
        );
    }

    // nothing acceptable: 406 before the query runs, or once its form is known
    for accept in ["text/plain", "*/*;q=0", "image/png, text/html"] {
        let error = select(accept).await.error(StatusCode::NOT_ACCEPTABLE);
        assert!(error.contains("text/turtle"), "{error}");
    }
    let error = construct("text/csv")
        .await
        .error(StatusCode::NOT_ACCEPTABLE);
    assert_eq!(
        error,
        "none of the formats the Accept header allows can hold CONSTRUCT results"
    );
    let error = select("text/turtle")
        .await
        .error(StatusCode::NOT_ACCEPTABLE);
    assert_eq!(
        error,
        "none of the formats the Accept header allows can hold SELECT results"
    );
    // a literal SPARQL Results XML cannot hold
    let control = "SELECT ?v { BIND(\"a\\u0001b\" AS ?v) }";
    let error = server
        .get(
            &[("query", control)],
            Some("application/sparql-results+xml"),
        )
        .await
        .error(StatusCode::NOT_ACCEPTABLE);
    assert!(error.contains("use JSON"), "{error}");
    // ... in the next format the Accept header allows, if any
    let xml_first = "application/sparql-results+xml, application/sparql-results+json;q=0.5";
    for (accept, format) in [
        (xml_first, QueryResultsFormat::Json),
        (
            "application/sparql-results+xml, text/csv;q=0.5, */*;q=0.1",
            QueryResultsFormat::Csv,
        ),
    ] {
        let reply = server.get(&[("query", control)], Some(accept)).await;
        assert_eq!(
            reply.ok(format.media_type()),
            local(control, format),
            "{accept}"
        );
    }
    // XML when it holds the results
    for query in [KNOWS, "ASK {}"] {
        let reply = server.get(&[("query", query)], Some(xml_first)).await;
        assert_eq!(
            reply.ok(QueryResultsFormat::Xml.media_type()),
            local(query, QueryResultsFormat::Xml),
            "{query}"
        );
    }
    // the formats of the other form of query do not count
    let error = server
        .get(&[("query", KNOWN_BY)], Some(xml_first))
        .await
        .error(StatusCode::NOT_ACCEPTABLE);
    assert_eq!(
        error,
        "none of the formats the Accept header allows can hold CONSTRUCT results"
    );
    let reply = server
        .get(
            &[("query", KNOWN_BY)],
            Some("application/sparql-results+xml, application/rdf+xml;q=0.1"),
        )
        .await;
    assert_eq!(
        reply.ok("application/rdf+xml"),
        local(KNOWN_BY, RdfFormat::RdfXml)
    );
}

/// The results of `query` on `years`, as `sparql_to_writer` writes them.
fn local_years(query: &str, format: impl Into<SparqlFormat>) -> String {
    let mut out = Vec::new();
    years().sparql_to_writer(query, &mut out, format).unwrap();
    String::from_utf8(out).unwrap()
}

#[tokio::test]
async fn rdf_xml_never_drops_triples() {
    let server = Server::default().await;
    let construct = |query: &'static str, accept: &'static str| {
        let server = &server;
        async move {
            server
                .get_from("years", &[("query", query)], Some(accept))
                .await
        }
    };
    let all = "CONSTRUCT WHERE { ?s ?p ?o }";
    assert_eq!(
        triples(&local_years(all, RdfFormat::NTriples), RdfFormat::NTriples),
        [
            "<raphtory:alice> <raphtory:2023> <raphtory:bob> .",
            "<raphtory:alice> <raphtory:knows> <raphtory:carol> .",
        ]
    );
    // With RDF/XML the only RDF format allowed, results it cannot hold are refused, not cut
    // short. rdflib's SPARQLStore sends the second header.
    for accept in [
        "application/rdf+xml",
        "application/sparql-results+xml, application/rdf+xml",
    ] {
        let error = construct(all, accept)
            .await
            .error(StatusCode::NOT_ACCEPTABLE);
        assert_eq!(
            error,
            "RDF/XML cannot hold 1 of the 2 triples of the results (such as triples whose \
             predicate does not end in an XML name); accept another RDF format: text/turtle, \
             application/n-triples, application/ld+json, application/n-quads or application/trig",
            "{accept}"
        );
    }
    let met = "CONSTRUCT WHERE { ?s <raphtory:2023> ?o }";
    let error = construct(met, "application/rdf+xml")
        .await
        .error(StatusCode::NOT_ACCEPTABLE);
    assert!(
        error.starts_with("RDF/XML cannot hold 1 of the 1 triples"),
        "{error}"
    );
    // otherwise they are in the next RDF format allowed
    for (accept, format) in [
        ("application/rdf+xml, text/turtle;q=0.5", RdfFormat::Turtle),
        (
            "application/rdf+xml, application/n-triples;q=0.9, text/turtle;q=0.5",
            RdfFormat::NTriples,
        ),
        ("application/rdf+xml, */*;q=0.1", RdfFormat::Turtle),
    ] {
        let body = construct(all, accept).await.ok(format.media_type());
        assert_eq!(body, local_years(all, format), "{accept}");
    }
    let describe = "DESCRIBE <raphtory:alice>";
    let reply = construct(describe, "application/rdf+xml, application/n-triples;q=0.5").await;
    assert_eq!(
        triples(&reply.ok("application/n-triples"), RdfFormat::NTriples),
        triples(&local_years(all, RdfFormat::NTriples), RdfFormat::NTriples)
    );
    // and in RDF/XML when it holds them all
    let knows = "CONSTRUCT WHERE { ?s <raphtory:knows> ?o }";
    let body = construct(knows, "application/rdf+xml, text/turtle;q=0.5")
        .await
        .ok("application/rdf+xml");
    assert_eq!(body, local_years(knows, RdfFormat::RdfXml));
    assert!(body.contains("raphtory:carol"), "{body}");
}

#[tokio::test]
async fn dataset_parameters_set_the_dataset() {
    let server = Server::default().await;
    let all = "SELECT ?s ?o { ?s <http://ex/knows> ?o } ORDER BY ?s ?o";
    // the default graph as of 3, also replacing `FROM`
    let as_of_3 = server
        .get(
            &[("query", all), ("default-graph-uri", "raphtory:asof:3")],
            None,
        )
        .await;
    assert_eq!(rows(&as_of_3.json()), pairs(AS_OF_3));
    let from_now = "SELECT ?s ?o FROM raphtory:asof:9 { ?s <http://ex/knows> ?o } ORDER BY ?s ?o";
    assert_eq!(rows(&server.select(from_now).await.json()), pairs(NOW));
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
    // in a form, and with a date-time
    let reply = server
        .post(
            &[],
            "application/x-www-form-urlencoded",
            form(&[
                ("query", all),
                (
                    "default-graph-uri",
                    "raphtory:asof:1970-01-01T00:00:00.003Z",
                ),
            ]),
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

    // named graphs, repeated
    let graphs = "SELECT ?g (COUNT(*) AS ?n) { GRAPH ?g { ?s <http://ex/knows> ?o } } \
                  GROUP BY ?g ORDER BY ?g";
    let reply = server
        .get(
            &[
                ("query", graphs),
                ("named-graph-uri", "raphtory:asof:3"),
                ("named-graph-uri", "raphtory:asof:9"),
            ],
            None,
        )
        .await;
    assert_eq!(
        rows(&reply.json()),
        [["raphtory:asof:3", "2"], ["raphtory:asof:9", "2"]]
    );
    // a graph named twice counts once
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
    let reply = server
        .get(
            &[
                ("query", all),
                ("default-graph-uri", "raphtory:asof:3"),
                ("default-graph-uri", "raphtory:asof:3"),
            ],
            None,
        )
        .await;
    assert_eq!(rows(&reply.json()), pairs(AS_OF_3));
    // the protocol dataset replaces the whole dataset: no named graphs here
    let reply = server
        .get(
            &[("query", graphs), ("default-graph-uri", "raphtory:asof:3")],
            None,
        )
        .await;
    assert_eq!(rows(&reply.json()), Vec::<Vec<String>>::new());

    // only time graphs
    for iri in ["http://ex/g", "not an iri", "raphtory:Alice"] {
        let error = server
            .get(&[("query", all), ("default-graph-uri", iri)], None)
            .await
            .error(StatusCode::BAD_REQUEST);
        assert!(error.contains("can only name time graphs"), "{error}");
    }
    let error = server
        .get(
            &[("query", all), ("named-graph-uri", "raphtory:asof:soon")],
            None,
        )
        .await
        .error(StatusCode::BAD_REQUEST);
    assert!(
        error.starts_with("invalid time graph <raphtory:asof:soon>"),
        "{error}"
    );
}

#[tokio::test]
async fn graph_type_reads_the_graph_as_events_or_persistent() {
    let server = Server::default().await;
    let ask = "ASK { <http://ex/alice> <http://ex/knows> <http://ex/bob> }";
    let yes = r#"{"head":{},"boolean":true}"#;
    let no = r#"{"head":{},"boolean":false}"#;
    for (graph, graph_type, expected) in [
        ("eg", None, yes),
        ("eg", Some("persistent"), no),
        ("eg", Some("PERSISTENT"), no),
        ("pg", None, no),
        ("pg", Some("event"), yes),
    ] {
        let mut params = vec![("query", ask)];
        params.extend(graph_type.map(|graph_type| ("graph_type", graph_type)));
        let reply = server.get_from(graph, &params, None).await;
        assert_eq!(reply.json(), expected, "{graph} {graph_type:?}");
    }
    let error = server
        .get(&[("query", ask), ("graph_type", "eventually")], None)
        .await
        .error(StatusCode::BAD_REQUEST);
    assert_eq!(
        error,
        "invalid `graph_type`: expected `event` or `persistent`"
    );
}

#[tokio::test]
async fn graphs_in_namespaces() {
    let server = Server::default().await;
    let reply = server
        .get_from("ns/inner%20graph", &[("query", KNOWS)], None)
        .await;
    assert_eq!(rows(&reply.json()), pairs(NOW));
    let error = server
        .get_from("ns/nope", &[("query", KNOWS)], None)
        .await
        .error(StatusCode::NOT_FOUND);
    assert_eq!(error, "Graph 'ns/nope' does not exist");
}

#[tokio::test]
async fn updates_are_refused() {
    let server = Server::default().await;
    let update = "INSERT DATA { <http://ex/a> <http://ex/b> <http://ex/c> }";
    let refused = "SPARQL Update is not supported: this endpoint answers queries only";
    let replies = [
        server
            .post(
                &[],
                "application/x-www-form-urlencoded",
                form(&[("update", update)]),
                None,
            )
            .await,
        server
            .post(&[], "application/sparql-update", update, None)
            .await,
        server.get(&[("update", update)], None).await,
    ];
    for reply in replies {
        assert_eq!(reply.error(StatusCode::NOT_IMPLEMENTED), refused);
    }
    // nothing changed
    assert_eq!(rows(&server.select(COUNT).await.json()), [["4"]]);
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
        has(
            "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
            &sd("Service")
        ),
        "{turtle}"
    );
    assert!(
        has(&sd("endpoint"), "<http://example.org:1736/sparql/pg>"),
        "{turtle}"
    );
    assert!(has(&sd("supportedLanguage"), &sd("SPARQL11Query")));
    for format in [
        "SPARQL_Results_JSON",
        "SPARQL_Results_CSV",
        "Turtle",
        "JSON-LD",
    ] {
        assert!(
            has(
                &sd("resultFormat"),
                &format!("<http://www.w3.org/ns/formats/{format}>")
            ),
            "{format}"
        );
    }
    assert!(has(&sd("extensionFunction"), "<raphtory:validFrom>"));

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
    let error = server
        .get(&[], Some("text/csv"))
        .await
        .error(StatusCode::NOT_ACCEPTABLE);
    assert!(error.contains("text/turtle"), "{error}");

    // like a query, it needs a graph that exists
    for graph in ["nope", "ns/nope"] {
        let error = server
            .get_from(graph, &[], None)
            .await
            .error(StatusCode::NOT_FOUND);
        assert_eq!(error, format!("Graph '{graph}' does not exist"));
    }
    let error = server
        .get_from("..%2Fpg", &[], None)
        .await
        .error(StatusCode::BAD_REQUEST);
    assert!(error.starts_with("Invalid path"), "{error}");
    server
        .get(&[("graph_type", "event")], None)
        .await
        .ok("text/turtle");
    let error = server
        .get(&[("graph_type", "eventually")], None)
        .await
        .error(StatusCode::BAD_REQUEST);
    assert_eq!(
        error,
        "invalid `graph_type`: expected `event` or `persistent`"
    );
}

#[tokio::test]
async fn bad_requests() {
    let server = Server::default().await;
    let error = server
        .select("SELECT ?x WHERE { ?x")
        .await
        .error(StatusCode::BAD_REQUEST);
    assert!(error.starts_with("SPARQL syntax error"), "{error}");
    let error = server
        .select("SELECT (<http://ex/nope>(1) AS ?x) {}")
        .await
        .error(StatusCode::BAD_REQUEST);
    assert!(error.starts_with("SPARQL evaluation error"), "{error}");
    let nested = format!(
        "SELECT * {{ FILTER({}1{}) }}",
        "(".repeat(500),
        ")".repeat(500)
    );
    let error = server.select(&nested).await.error(StatusCode::BAD_REQUEST);
    assert!(
        error.contains("brackets nest more than 128 deep"),
        "{error}"
    );
    // SERVICE makes no request
    let error = server
        .select("SELECT * { SERVICE <http://127.0.0.1:9/sparql> { ?s ?p ?o } }")
        .await
        .error(StatusCode::BAD_REQUEST);
    assert!(error.contains("is not supported"), "{error}");

    // the protocol
    let error = server
        .post(&[], "application/x-www-form-urlencoded", "", None)
        .await
        .error(StatusCode::BAD_REQUEST);
    assert_eq!(error, "the request has no `query`");
    let error = server
        .get(&[("query", KNOWS), ("query", COUNT)], None)
        .await
        .error(StatusCode::BAD_REQUEST);
    assert_eq!(error, "the request has more than one `query`");
    let error = server
        .post(&[("query", KNOWS)], "application/sparql-query", COUNT, None)
        .await
        .error(StatusCode::BAD_REQUEST);
    assert_eq!(error, "the request has more than one `query`");
    let error = server
        .post(&[], "application/json", "{}", None)
        .await
        .error(StatusCode::UNSUPPORTED_MEDIA_TYPE);
    assert!(error.contains("application/sparql-query"), "{error}");
    let invalid_utf8 = Request::builder()
        .method(Method::POST)
        .uri("/sparql/pg".parse().unwrap())
        .header("content-type", "application/sparql-query")
        .body(vec![b'A', b'S', b'K', 0xff]);
    let error = server
        .send(invalid_utf8)
        .await
        .error(StatusCode::BAD_REQUEST);
    assert_eq!(error, "the query is not UTF-8");
    let put = Request::builder()
        .method(Method::PUT)
        .uri("/sparql/pg".parse().unwrap())
        .finish();
    let reply = server.send(put).await;
    assert_eq!(reply.allow, "GET, POST, OPTIONS");
    reply.error(StatusCode::METHOD_NOT_ALLOWED);

    // graphs
    let error = server
        .get_from("nope", &[("query", KNOWS)], None)
        .await
        .error(StatusCode::NOT_FOUND);
    assert_eq!(error, "Graph 'nope' does not exist");
    let error = server
        .get_from("..%2Fpg", &[("query", KNOWS)], None)
        .await
        .error(StatusCode::BAD_REQUEST);
    assert!(error.starts_with("Invalid path"), "{error}");
}

#[tokio::test]
async fn the_limits_of_the_server_apply() {
    // the default length limit, 16 KiB
    let server = Server::default().await;
    let padded = |length: usize| format!("{COUNT}{}", " ".repeat(length - COUNT.len()));
    assert_eq!(
        rows(&server.select(&padded(16 * 1024)).await.json()),
        [["4"]]
    );
    let too_long = server
        .post(&[], "application/sparql-query", padded(16 * 1024 + 1), None)
        .await
        .error(StatusCode::BAD_REQUEST);
    assert_eq!(
        too_long,
        "SPARQL query too long: 16385 bytes, the limit is 16384 bytes \
         (`max_sparql_query_length`)"
    );
    // a body far longer than any form of an allowed query is not read
    let huge = server
        .post(&[], "application/sparql-query", padded(1024 * 1024), None)
        .await
        .error(StatusCode::PAYLOAD_TOO_LARGE);
    assert!(huge.contains("`max_sparql_query_length`"), "{huge}");
    // the default limit of 100 triple patterns
    let star = |n: usize| {
        let patterns: String = (0..n).map(|i| format!("?s ?p{i} ?o{i} . ")).collect();
        format!("SELECT ?s {{ {patterns} }}")
    };
    server.select(&star(100)).await.json();
    let error = server
        .select(&star(101))
        .await
        .error(StatusCode::BAD_REQUEST);
    assert!(
        error.starts_with("SPARQL query too complex: 101 triple patterns"),
        "{error}"
    );
    assert!(error.ends_with("(`max_sparql_triple_patterns`)"), "{error}");

    let server = Server::with(|config| config.concurrency.max_sparql_query_length = Some(40)).await;
    let error = server
        .select(&padded(41))
        .await
        .error(StatusCode::BAD_REQUEST);
    assert!(
        error.starts_with("SPARQL query too long: 41 bytes"),
        "{error}"
    );

    let server = Server::with(|config| config.concurrency.disable_lists = true).await;
    for reply in [server.select(COUNT).await, server.get(&[], None).await] {
        assert_eq!(
            reply.error(StatusCode::SERVICE_UNAVAILABLE),
            "SPARQL queries are disabled on this server (`disable_lists`)"
        );
    }
}

/// A query over the 4 triples of `pg` that effectively never finishes (4^16 solutions).
fn slow_count(tag: &str) -> String {
    let patterns: String = (0..16).map(|i| format!("?s{i} ?p{i} ?o{i} . ")).collect();
    format!("# {tag}\nSELECT (COUNT(*) AS ?n) {{ {patterns} }}")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slow_queries_time_out() {
    let server =
        Arc::new(Server::with(|config| config.concurrency.sparql_timeout = Some(0.2)).await);
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
            config.concurrency.sparql_timeout = Some(1.5);
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
            json!({ "query": r#"{ graph(path: "pg") { sparql(query: "ASK {}") } }"# }).to_string(),
        );
    let reply = server.send(graphql).await;
    let waited = started.elapsed();
    assert_eq!(
        serde_json::from_str::<Json>(&reply.body).unwrap(),
        json!({ "data": { "graph": { "sparql": r#"{"head":{},"boolean":true}"# } } })
    );
    assert!(
        waited >= Duration::from_millis(1000),
        "the GraphQL query ran after {waited:?}, while the endpoint query held the slot"
    );
    slow.await.unwrap().error(StatusCode::GATEWAY_TIMEOUT);
}

/// A row-filtered read permission (see [`GraphAccessFilter`]).
fn read_with(filter: Json) -> GraphPermission {
    GraphPermission::Read {
        filter: Some(serde_json::from_value::<GraphAccessFilter>(filter).unwrap()),
    }
}

#[tokio::test]
async fn permissions_and_access_filters_apply() {
    let server = Server::as_user(read_with(json!({
        "filter": { "node": { "name": { "where": { "ne": { "str": "http://ex/carol" } } } } }
    })))
    .await;
    assert_eq!(
        rows(&server.select(KNOWS).await.json()),
        pairs(&[("dave", "alice")])
    );
    assert_eq!(rows(&server.select(COUNT).await.json()), [["2"]]);
    let ask = "ASK { ?s ?p <http://ex/carol> }";
    let reply = server
        .get(
            &[("query", ask), ("default-graph-uri", "raphtory:asof:3")],
            None,
        )
        .await;
    assert_eq!(reply.json(), r#"{"head":{},"boolean":false}"#);
    let hidden = server
        .get(&[("query", "DESCRIBE <http://ex/carol>")], None)
        .await
        .ok("text/turtle");
    assert_eq!(
        hidden,
        local("DESCRIBE <http://ex/nobody>", RdfFormat::Turtle)
    );
    // no temporal functions for a row-filtered read
    let error = server.select(SINCE).await.error(StatusCode::BAD_REQUEST);
    assert!(error.contains("validFromTime"), "{error}");

    // an unfiltered read has them
    let server = Server::as_user(GraphPermission::Read { filter: None }).await;
    assert_eq!(rows(&server.select(SINCE).await.json()), [["1"]]);
    server.get(&[], None).await.ok("text/turtle");

    // introspection only, no grant and no graph look the same, also to the service description
    let server = Server::as_user(GraphPermission::Introspect).await;
    let introspect = server.select(KNOWS).await.error(StatusCode::NOT_FOUND);
    assert_eq!(introspect, "Graph 'pg' does not exist");
    let introspect = server.get(&[], None).await.error(StatusCode::NOT_FOUND);
    assert_eq!(introspect, "Graph 'pg' does not exist");
    for params in [&[("query", KNOWS)][..], &[]] {
        let no_grant = server
            .get_from("ns/inner%20graph", params, None)
            .await
            .error(StatusCode::NOT_FOUND);
        assert_eq!(no_grant, "Graph 'ns/inner graph' does not exist");
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
        ctx: &async_graphql::Context<'_>,
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
        _ctx: &async_graphql::Context<'_>,
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

    // the role, the claim and the access of the token reach the policy
    assert_eq!(rows(&read("pg", knows, &analyst).await.json()), pairs(NOW));
    assert_eq!(
        rows(&read("eg", knows, &graph_team).await.json()),
        pairs(&[("alice", "bob"), ("bob", "carol"), ("dave", "alice")])
    );
    assert_eq!(
        rows(&read("ns/inner%20graph", knows, &admin).await.json()),
        pairs(NOW)
    );
    read("pg", &[], &analyst).await.ok("text/turtle");
    // a graph the policy does not grant looks like one that does not exist
    for (graph, name, token) in [
        ("pg", "pg", &graph_team),
        ("pg", "pg", &guest),
        ("eg", "eg", &analyst),
        ("ns/inner%20graph", "ns/inner graph", &analyst),
    ] {
        for params in [knows, &[]] {
            let error = read(graph, params, token)
                .await
                .error(StatusCode::NOT_FOUND);
            assert_eq!(error, format!("Graph '{name}' does not exist"));
        }
    }
    // without a token
    let reply = server.select(KNOWS).await;
    assert_eq!(reply.status, StatusCode::UNAUTHORIZED, "{}", reply.body);
}

#[tokio::test]
async fn reads_can_require_a_token() {
    let server = Server::new(reads_need_a_token(), None).await;
    for reply in [
        server.select(COUNT).await,
        server.get(&[], None).await,
        server
            .send(
                Request::builder()
                    .method(Method::GET)
                    .uri(
                        format!("/sparql/pg?query={}", url_encode(COUNT))
                            .parse()
                            .unwrap(),
                    )
                    .header("authorization", "Bearer not.a.token")
                    .finish(),
            )
            .await,
    ] {
        assert_eq!(reply.status, StatusCode::UNAUTHORIZED, "{}", reply.body);
    }
}

#[tokio::test]
async fn answers_vary_with_the_accept_header() {
    let server = Server::default().await;
    let replies = [
        server.select(KNOWS).await,
        server
            .get(&[("query", KNOWN_BY)], Some("application/n-triples"))
            .await,
        server
            .post(&[], "application/sparql-query", KNOWS, Some("text/csv"))
            .await,
        server.get(&[], None).await,
        server.get(&[("query", KNOWS)], Some("text/turtle")).await,
        server.get(&[], Some("text/csv")).await,
        server.select("SELECT ?x WHERE { ?x").await,
    ];
    let answers: Vec<_> = replies
        .iter()
        .map(|reply| (reply.status, reply.vary.as_str()))
        .collect();
    let ok = (StatusCode::OK, "Accept");
    let not_acceptable = (StatusCode::NOT_ACCEPTABLE, "Accept");
    assert_eq!(
        answers,
        [
            ok,
            ok,
            ok,
            ok,
            not_acceptable,
            not_acceptable,
            (StatusCode::BAD_REQUEST, "Accept")
        ]
    );
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

/// Results are the RDF terms of the graph, whichever way the query was sent.
#[tokio::test]
async fn terms_are_kept() {
    let server = Server::default().await;
    let json = server
        .select("SELECT ?o { ?s <http://ex/age> ?o }")
        .await
        .json();
    let results: Json = serde_json::from_str(&json).unwrap();
    assert_eq!(
        results["results"]["bindings"][0]["o"],
        json!({
            "type": "literal",
            "datatype": "http://www.w3.org/2001/XMLSchema#integer",
            "value": "42",
        })
    );
    let nt = server
        .get(
            &[("query", "CONSTRUCT { ?s ?p ?o } WHERE { ?s <http://ex/name> ?o BIND(<http://ex/name> AS ?p) }")],
            Some("application/n-triples"),
        )
        .await
        .ok("application/n-triples");
    assert_eq!(nt, "<http://ex/carol> <http://ex/name> \"Carol\" .\n");
}
