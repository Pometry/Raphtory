//! The SPARQL 1.1 Protocol endpoint: `/sparql/<graph path>` answers SPARQL queries on a graph.
//!
//! A request reads the graph through the `rdfGraph` field with the caller's credentials, so it
//! gets the same permission checks and access filters, and runs the query on `valid()` of it,
//! the current state. A query that reads a time graph `<raphtory:asof:T>`
//! ([`QueryOutline::reads_time_graphs`]), or a request whose dataset names one, runs on the whole
//! graph instead, since a time graph already holds the state at
//! its time; `asof=T` runs it on `at(T)`.
use crate::{
    auth::{Access, AuthenticatedGraphQL, Roles, TokenClaimValues},
    config::concurrency_config::ConcurrencyConfig,
    model::graph::rdf_graph::{GqlRdfGraph, RdfGraphLookup},
    paths::PathValidationError,
    sparql::{accept, check_enabled, check_length, run_sparql, SparqlError},
    GQLError,
};
use async_graphql::{Executor, Request as GraphQLRequest, ServerError, Variables};
use oxigraph::io::{JsonLdProfileSet, RdfSerializer};
use poem::{
    error::ReadBodyError,
    http::{
        header::{HeaderValue, ACCEPT, ALLOW, CONTENT_TYPE, HOST, VARY},
        Method, StatusCode,
    },
    Body, Endpoint, Request, Response,
};
use raphtory::{
    db::api::view::TimeOps,
    errors::GraphError,
    rdf::{
        model::{vocab::rdf, BlankNode, NamedNode, Triple},
        QueryForm, QueryOutline, QueryResultsFormat, RdfError, RdfFormat, RdfView, SparqlDataset,
        SparqlFormat, SparqlOptions, TimeGraph, WriteStats, VALID_FROM, VALID_FROM_TIME, VALID_TO,
        VALID_TO_TIME,
    },
};
use serde_json::json;
use std::{collections::HashSet, sync::Arc};

/// The methods the endpoint answers.
const ALLOWED: &str = "GET, POST, OPTIONS";

/// The formats of `SELECT` and `ASK` results, the default first, with the media types each is
/// served for.
const RESULTS_FORMATS: &[(QueryResultsFormat, &[&str])] = &[
    (
        QueryResultsFormat::Json,
        &["application/sparql-results+json", "application/json"],
    ),
    (
        QueryResultsFormat::Xml,
        &[
            "application/sparql-results+xml",
            "application/xml",
            "text/xml",
        ],
    ),
    (QueryResultsFormat::Csv, &["text/csv"]),
    (QueryResultsFormat::Tsv, &["text/tab-separated-values"]),
];

/// The formats of `CONSTRUCT` and `DESCRIBE` results and of the service description, the
/// default first, with the media types each is served for.
const GRAPH_FORMATS: &[(RdfFormat, &[&str])] = &[
    (
        RdfFormat::Turtle,
        &["text/turtle", "application/x-turtle", "application/turtle"],
    ),
    (RdfFormat::NTriples, &["application/n-triples"]),
    (
        RdfFormat::RdfXml,
        &["application/rdf+xml", "application/xml", "text/xml"],
    ),
    (
        RdfFormat::JsonLd {
            profile: JsonLdProfileSet::empty(),
        },
        &["application/ld+json", "application/json"],
    ),
    (RdfFormat::NQuads, &["application/n-quads"]),
    (RdfFormat::TriG, &["application/trig"]),
];

/// A format of query results: a SPARQL results format for `SELECT` and `ASK`, an RDF format for
/// `CONSTRUCT` and `DESCRIBE`.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Format {
    Results(QueryResultsFormat),
    Graph(RdfFormat),
}

impl Format {
    fn sparql_format(self) -> SparqlFormat {
        match self {
            Self::Results(format) => format.into(),
            Self::Graph(format) => format.into(),
        }
    }

    fn media_type(self) -> &'static str {
        match self {
            Self::Results(format) => format.media_type(),
            Self::Graph(format) => format.media_type(),
        }
    }

    /// Whether some results do not fit the format: SPARQL Results XML cannot hold literals with
    /// control characters and RDF 1.2 triple terms, and RDF/XML skips triples such as those
    /// whose predicate does not end in an XML name (see `RdfView::sparql_to_writer`).
    fn may_not_hold(self) -> bool {
        matches!(
            self,
            Self::Results(QueryResultsFormat::Xml) | Self::Graph(RdfFormat::RdfXml)
        )
    }
}

/// The formats the client accepts for each form of query, the preferred first.
#[derive(Clone, Debug)]
struct Formats {
    results: Vec<Format>,
    graph: Vec<Format>,
}

impl Formats {
    fn negotiate(accept: Option<&str>) -> Self {
        Self {
            results: accept::rank(accept, RESULTS_FORMATS)
                .into_iter()
                .map(Format::Results)
                .collect(),
            graph: accept::rank(accept, GRAPH_FORMATS)
                .into_iter()
                .map(Format::Graph)
                .collect(),
        }
    }

    fn is_empty(&self) -> bool {
        self.results.is_empty() && self.graph.is_empty()
    }

    /// Runs `query`, of the form `form`, and returns its results and their media type: in the
    /// preferred format the client accepts for the form that holds them all.
    fn write(
        &self,
        view: &RdfView,
        query: &str,
        form: QueryForm,
        options: &SparqlOptions,
    ) -> Result<(Vec<u8>, &'static str), SparqlError> {
        let formats = if form.is_graph() {
            &self.graph
        } else {
            &self.results
        };
        let Some((&preferred, others)) = formats.split_first() else {
            return Err(SparqlError::NotAcceptable { form });
        };
        if others.is_empty() || !preferred.may_not_hold() {
            return write_in(preferred, |out, format| {
                view.sparql_to_writer_with(query, out, format, options)
            });
        }
        // The preferred format may not hold every result: run the query once and write its
        // results in the accepted formats in turn, until one holds them all.
        let results = view.sparql_with(query, options)?;
        let mut not_held = None;
        for &format in formats {
            match write_in(format, |out, format| results.write(out, format)) {
                Err(error) if cannot_hold(&error) => not_held = Some(error),
                written => return written,
            }
        }
        Err(not_held.unwrap_or(SparqlError::NotAcceptable { form }))
    }
}

/// Writes results in `format` with `write`, failing if the format cannot hold them all.
fn write_in(
    format: Format,
    write: impl FnOnce(&mut Vec<u8>, SparqlFormat) -> Result<WriteStats, GraphError>,
) -> Result<(Vec<u8>, &'static str), SparqlError> {
    let mut out = Vec::new();
    let stats = write(&mut out, format.sparql_format())?;
    if stats.skipped > 0 {
        return Err(SparqlError::RdfXmlSkipped {
            skipped: stats.skipped,
            triples: stats.written + stats.skipped,
        });
    }
    Ok((out, format.media_type()))
}

/// Whether `error` is results that their format cannot hold, which another format can.
fn cannot_hold(error: &SparqlError) -> bool {
    matches!(
        error,
        SparqlError::RdfXmlSkipped { .. }
            | SparqlError::Query(GraphError::Rdf(
                RdfError::XmlUnsafeLiteral(_) | RdfError::XmlTripleTerm(_)
            ))
    )
}

/// A failed request: its status and a message for the plain-text body.
struct Failure {
    status: StatusCode,
    message: String,
}

impl Failure {
    fn into_response(self) -> Response {
        Response::builder()
            .status(self.status)
            .content_type("text/plain; charset=utf-8")
            .body(self.message)
    }
}

impl From<&SparqlError> for Failure {
    fn from(error: &SparqlError) -> Self {
        text(status_of(error), error.to_string())
    }
}

impl From<poem::Error> for Failure {
    fn from(error: poem::Error) -> Self {
        text(error.status(), error.to_string())
    }
}

fn text(status: StatusCode, message: impl Into<String>) -> Failure {
    Failure {
        status,
        message: message.into(),
    }
}

/// The status of a failed query.
fn status_of(error: &SparqlError) -> StatusCode {
    match error {
        SparqlError::Disabled => StatusCode::SERVICE_UNAVAILABLE,
        SparqlError::TooLong { .. } => StatusCode::BAD_REQUEST,
        SparqlError::NotAcceptable { .. } | SparqlError::RdfXmlSkipped { .. } => {
            StatusCode::NOT_ACCEPTABLE
        }
        #[cfg(feature = "shacl")]
        SparqlError::RecursiveShapes => StatusCode::BAD_REQUEST,
        SparqlError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        SparqlError::Query(GraphError::Rdf(error)) => match error {
            RdfError::Timeout { .. } => StatusCode::GATEWAY_TIMEOUT,
            RdfError::Cancelled => StatusCode::SERVICE_UNAVAILABLE,
            RdfError::WrongResultsFormat { .. }
            | RdfError::XmlUnsafeLiteral(_)
            | RdfError::XmlTripleTerm(_) => StatusCode::NOT_ACCEPTABLE,
            RdfError::SparqlSyntax(_)
            | RdfError::SparqlTooDeep { .. }
            | RdfError::TooManyPatterns { .. }
            | RdfError::SparqlEvaluation(_)
            | RdfError::InvalidTimeGraph { .. }
            | RdfError::Iri(_) => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        },
        SparqlError::Query(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// The response for a graph that does not exist or that the caller cannot read; the two are
/// indistinguishable.
fn not_found(path: &str) -> Failure {
    text(
        StatusCode::NOT_FOUND,
        format!("Graph '{path}' does not exist"),
    )
}

/// The response for a GraphQL error of `rdfGraph(path:)`, which failed to load the graph.
fn failure(error: &ServerError, path: &str) -> Failure {
    let mut graph_error = error.source::<GQLError>();
    while let Some(GQLError::Arc(inner)) = graph_error {
        graph_error = Some(inner.as_ref());
    }
    match graph_error {
        Some(GQLError::Validation(
            PathValidationError::GraphNotExistsError(_)
            | PathValidationError::NamespaceDoesNotExist(_),
        )) => not_found(path),
        Some(GQLError::Validation(
            error @ (PathValidationError::InvalidPath { .. } | PathValidationError::EmptyPath),
        )) => text(StatusCode::BAD_REQUEST, error.to_string()),
        _ => text(StatusCode::INTERNAL_SERVER_ERROR, error.message.clone()),
    }
}

/// The parameters of a protocol request.
#[derive(Default)]
struct Params {
    query: Option<String>,
    update: bool,
    default_graphs: Vec<String>,
    named_graphs: Vec<String>,
    asof: Option<String>,
}

impl Params {
    fn add(&mut self, name: &str, value: String) -> Result<(), Failure> {
        let once = |slot: &mut Option<String>, value| match slot {
            Some(_) => Err(text(
                StatusCode::BAD_REQUEST,
                format!("the request has more than one `{name}`"),
            )),
            None => {
                *slot = Some(value);
                Ok(())
            }
        };
        match name {
            "query" => once(&mut self.query, value)?,
            "update" => self.update = true,
            "default-graph-uri" => self.default_graphs.push(value),
            "named-graph-uri" => self.named_graphs.push(value),
            "asof" => once(&mut self.asof, value)?,
            // other parameters (such as the `format` and `output` of some clients) are ignored
            _ => {}
        }
        Ok(())
    }

    fn add_form(&mut self, form: &[u8]) -> Result<(), Failure> {
        for (name, value) in url::form_urlencoded::parse(form) {
            self.add(&name, value.into_owned())?;
        }
        Ok(())
    }

    /// The dataset of `default-graph-uri` and `named-graph-uri`, if the request sets one.
    fn dataset(&self) -> Result<Option<SparqlDataset>, Failure> {
        if self.default_graphs.is_empty() && self.named_graphs.is_empty() {
            return Ok(None);
        }
        let time_graphs = |iris: &[String]| -> Result<Vec<NamedNode>, Failure> {
            // The named graphs are a set, and the default graph a merge, so a graph named twice
            // counts once.
            let mut named = HashSet::new();
            iris.iter()
                .filter(|iri| named.insert(iri.as_str()))
                .map(|iri| {
                    let time_graph = NamedNode::new(iri.as_str())
                        .ok()
                        .and_then(|node| TimeGraph::parse(&node).transpose());
                    match time_graph {
                        Some(Ok(graph)) => Ok(graph.iri),
                        Some(Err(error)) => Err(text(StatusCode::BAD_REQUEST, error.to_string())),
                        None => Err(text(
                            StatusCode::BAD_REQUEST,
                            format!(
                                "<{iri}> is not a graph of this endpoint: a dataset can only \
                                 name time graphs <raphtory:asof:T>"
                            ),
                        )),
                    }
                })
                .collect()
        };
        Ok(Some(SparqlDataset {
            default_graphs: time_graphs(&self.default_graphs)?,
            named_graphs: time_graphs(&self.named_graphs)?,
        }))
    }

    /// The time of `asof` in epoch milliseconds, parsed as the time of `<raphtory:asof:T>`.
    ///
    /// Parameters are form-encoded, where `+` stands for a space, so the `+` of a timezone offset
    /// pasted as is into a URL (`?asof=2024-01-01T00:00:00+01:00`) arrives as a space. A date-time
    /// that does not parse and ends in a space and an offset `hh:mm` is therefore read again with
    /// that space as `+`.
    fn asof(&self) -> Result<Option<i64>, Failure> {
        let Some(time) = self.asof.as_deref() else {
            return Ok(None);
        };
        let error = match TimeGraph::parse_time(time) {
            Ok(at) => return Ok(Some(at)),
            Err(error) => error,
        };
        time.rsplit_once(' ')
            .filter(|(date_time, offset)| date_time.contains('T') && is_offset(offset))
            .and_then(|(date_time, offset)| {
                TimeGraph::parse_time(&format!("{date_time}+{offset}")).ok()
            })
            .map(Some)
            .ok_or_else(|| text(StatusCode::BAD_REQUEST, format!("invalid `asof`: {error}")))
    }
}

/// Whether `offset` is a timezone offset `hh:mm`.
fn is_offset(offset: &str) -> bool {
    let bytes = offset.as_bytes();
    bytes.len() == 5 && bytes[2] == b':' && [0, 1, 3, 4].iter().all(|&i| bytes[i].is_ascii_digit())
}

/// The media type of a header value, lower-cased and without parameters.
fn essence(value: &str) -> String {
    value
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase()
}

/// The SPARQL 1.1 Protocol endpoint for the graphs of a server.
pub(crate) struct SparqlEndpoint<E> {
    graphql: Arc<AuthenticatedGraphQL<E>>,
    config: ConcurrencyConfig,
}

impl<E> SparqlEndpoint<E> {
    /// An endpoint that reads graphs through `graphql`, the GraphQL endpoint of the server, and
    /// shares its `exclusive_writes` lock and `heavy_query_limit` slots.
    pub(crate) fn new(graphql: Arc<AuthenticatedGraphQL<E>>, config: ConcurrencyConfig) -> Self {
        Self { graphql, config }
    }
}

impl<E: Executor> Endpoint for SparqlEndpoint<E> {
    type Output = Response;

    async fn call(&self, req: Request) -> poem::Result<Response> {
        let response = match *req.method() {
            Method::GET | Method::POST => {
                let mut response = self
                    .respond(req)
                    .await
                    .unwrap_or_else(Failure::into_response);
                // the format of the answer depends on the `Accept` header
                response
                    .headers_mut()
                    .append(VARY, HeaderValue::from_static("Accept"));
                response
            }
            Method::OPTIONS => Response::builder()
                .status(StatusCode::NO_CONTENT)
                .header(ALLOW, ALLOWED)
                .finish(),
            _ => {
                let mut response = text(
                    StatusCode::METHOD_NOT_ALLOWED,
                    "the SPARQL endpoint answers GET and POST requests",
                )
                .into_response();
                response
                    .headers_mut()
                    .insert(ALLOW, HeaderValue::from_static(ALLOWED));
                response
            }
        };
        Ok(response)
    }
}

/// The GraphQL query that reads the graph of a request into its [`RdfGraphLookup`].
const LOOKUP: &str = "query SparqlEndpoint($path: String!) { rdfGraph(path: $path) { path } }";

/// The caller of a request, as the GraphQL endpoint authenticates it.
type Caller = (Access, Roles, TokenClaimValues);

impl<E: Executor> SparqlEndpoint<E> {
    /// Answers a GET or POST request.
    async fn respond(&self, mut req: Request) -> Result<Response, Failure> {
        let path = req.raw_path_param("path").unwrap_or_default().to_owned();
        let caller = self.graphql.authenticate(&req).await?;
        check_enabled(Some(&self.config)).map_err(|error| Failure::from(&error))?;

        let mut params = Params::default();
        if let Some(query) = req.uri().query() {
            params.add_form(query.as_bytes())?;
        }
        if req.method() == Method::POST {
            let content_type = req.header(CONTENT_TYPE).map(essence).unwrap_or_default();
            if content_type == "application/sparql-update" {
                params.update = true;
            } else {
                let body = self.read_body(req.take_body()).await?;
                match content_type.as_str() {
                    "application/x-www-form-urlencoded" => params.add_form(&body)?,
                    "application/sparql-query" => {
                        let query = String::from_utf8(body)
                            .map_err(|_| text(StatusCode::BAD_REQUEST, "the query is not UTF-8"))?;
                        params.add("query", query)?;
                    }
                    _ => {
                        return Err(text(
                            StatusCode::UNSUPPORTED_MEDIA_TYPE,
                            "a SPARQL query POST must have the content type \
                             application/x-www-form-urlencoded or application/sparql-query",
                        ));
                    }
                }
            }
        }
        if params.update {
            return Err(text(
                StatusCode::NOT_IMPLEMENTED,
                "SPARQL Update is not supported: this endpoint answers queries only",
            ));
        }
        let accept = accept_header(&req);
        let asof = params.asof()?;
        let Some(query) = params.query.take() else {
            if req.method() == Method::POST {
                return Err(text(StatusCode::BAD_REQUEST, "the request has no `query`"));
            }
            return self.describe(&req, &path, accept.as_deref(), caller).await;
        };
        check_length(&self.config, &query).map_err(|error| Failure::from(&error))?;
        let dataset = params.dataset()?;
        let formats = Formats::negotiate(accept.as_deref());
        if formats.is_empty() {
            return Err(text(
                StatusCode::NOT_ACCEPTABLE,
                "the Accept header allows no format of this endpoint: SPARQL results as \
                 application/sparql-results+json, application/sparql-results+xml, text/csv or \
                 text/tab-separated-values; RDF as text/turtle, application/n-triples, \
                 application/rdf+xml, application/ld+json, application/n-quads or \
                 application/trig",
            ));
        }
        let run = async {
            let graph = self.rdf_graph(&path, caller).await?;
            let view = graph
                .view()
                .map_err(|error| Failure::from(&SparqlError::from(error)))?;
            let has_dataset = dataset.is_some();
            run_sparql(
                Some(&self.config),
                view,
                graph.temporal_functions(),
                query,
                dataset,
                move |view, query, options| {
                    let outline = QueryOutline::parse(query)?;
                    let view = match asof {
                        Some(at) => view.at(at).into_dynamic(),
                        None if has_dataset || outline.reads_time_graphs => view.clone(),
                        None => view.valid().into_dynamic(),
                    };
                    formats.write(&view, query, outline.form, options)
                },
            )
            .await
            .map_err(|error| Failure::from(&error))
        };
        let (body, media_type) = self.graphql.read(true, run).await??;
        Ok(Response::builder().content_type(media_type).body(body))
    }

    /// The service description of the endpoint of the graph at `path`, in the RDF format
    /// `accept` prefers. Like a query, it needs a graph that `caller` can read.
    async fn describe(
        &self,
        req: &Request,
        path: &str,
        accept: Option<&str>,
        caller: Caller,
    ) -> Result<Response, Failure> {
        let Some(&format) = accept::rank(accept, GRAPH_FORMATS).first() else {
            return Err(text(
                StatusCode::NOT_ACCEPTABLE,
                "the Accept header allows no RDF format of this endpoint: text/turtle, \
                 application/n-triples, application/rdf+xml, application/ld+json, \
                 application/n-quads or application/trig",
            ));
        };
        self.graphql
            .read(false, self.rdf_graph(path, caller))
            .await??;
        service_description(req, format)
    }

    /// The graph at `path` as `rdfGraph` reads it for `caller`.
    async fn rdf_graph(&self, path: &str, caller: Caller) -> Result<GqlRdfGraph, Failure> {
        let (access, roles, claims) = caller;
        let lookup = Arc::new(RdfGraphLookup::default());
        let request = GraphQLRequest::new(LOOKUP)
            .variables(Variables::from_json(json!({ "path": path })))
            .data(access)
            .data(roles)
            .data(claims)
            .data(lookup.clone());
        let response = self.graphql.executor().execute(request).await;
        if let Some(error) = response.errors.first() {
            return Err(failure(error, path));
        }
        // `rdfGraph` is null: the caller cannot read the graph
        Arc::into_inner(lookup)
            .and_then(RdfGraphLookup::into_inner)
            .ok_or_else(|| not_found(path))
    }

    /// The body of a POST, at most a little more than a form holding the longest query.
    async fn read_body(&self, body: Body) -> Result<Vec<u8>, Failure> {
        let unreadable = |error: ReadBodyError| text(StatusCode::BAD_REQUEST, error.to_string());
        let Some(max) = self.config.max_sparql_query_length else {
            return body.into_vec().await.map_err(unreadable);
        };
        let limit = max.saturating_mul(3).saturating_add(64 * 1024);
        match body.into_bytes_limit(limit).await {
            Ok(body) => Ok(body.into()),
            Err(ReadBodyError::PayloadTooLarge) => Err(text(
                StatusCode::PAYLOAD_TOO_LARGE,
                format!(
                    "request body too large for a SPARQL query of at most {max} bytes \
                     (`max_sparql_query_length`)"
                ),
            )),
            Err(error) => Err(unreadable(error)),
        }
    }
}

/// The `Accept` headers of `req`, joined.
fn accept_header(req: &Request) -> Option<String> {
    let values: Vec<&str> = req
        .headers()
        .get_all(ACCEPT)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .collect();
    (!values.is_empty()).then(|| values.join(","))
}

/// The absolute URL of the endpoint `req` was sent to.
fn endpoint_url(req: &Request) -> NamedNode {
    let header = |name: &str| {
        req.header(name)
            .and_then(|value| value.split(',').next())
            .map(str::trim)
            .filter(|value| !value.is_empty())
    };
    let scheme = header("x-forwarded-proto").unwrap_or("http");
    let host = header("x-forwarded-host")
        .or_else(|| header(HOST.as_str()))
        .unwrap_or("localhost");
    let path = req.uri().path();
    NamedNode::new(format!("{scheme}://{host}{path}"))
        .unwrap_or_else(|_| NamedNode::new_unchecked(format!("http://localhost{path}")))
}

const SD: &str = "http://www.w3.org/ns/sparql-service-description#";

/// The SPARQL 1.1 Service Description of the endpoint `req` was sent to, in `format`.
fn service_description(req: &Request, format: RdfFormat) -> Result<Response, Failure> {
    let sd = |name: &str| NamedNode::new_unchecked(format!("{SD}{name}"));
    let iri = |iri: &str| NamedNode::new_unchecked(iri);
    let service = BlankNode::default();
    let mut triples = vec![
        Triple::new(service.clone(), rdf::TYPE, sd("Service")),
        Triple::new(service.clone(), sd("endpoint"), endpoint_url(req)),
        Triple::new(
            service.clone(),
            sd("supportedLanguage"),
            sd("SPARQL11Query"),
        ),
    ];
    let results_formats = RESULTS_FORMATS.iter().map(|(format, _)| format.iri());
    let graph_formats = GRAPH_FORMATS.iter().map(|(format, _)| match format {
        // oxigraph's IRI for JSON-LD is not the one of the W3C formats registry
        RdfFormat::JsonLd { .. } => "http://www.w3.org/ns/formats/JSON-LD",
        format => format.iri(),
    });
    for format in results_formats.chain(graph_formats) {
        triples.push(Triple::new(
            service.clone(),
            sd("resultFormat"),
            iri(format),
        ));
    }
    for function in [VALID_FROM, VALID_TO, VALID_FROM_TIME, VALID_TO_TIME] {
        triples.push(Triple::new(
            service.clone(),
            sd("extensionFunction"),
            iri(function),
        ));
    }
    let serializer = RdfSerializer::from_format(format)
        .with_prefix("sd", SD)
        .and_then(|serializer| serializer.with_prefix("formats", "http://www.w3.org/ns/formats/"))
        .expect("valid prefixes");
    let mut writer = serializer.for_writer(Vec::new());
    let written = triples
        .iter()
        .try_for_each(|triple| writer.serialize_triple(triple))
        .and_then(|()| writer.finish());
    match written {
        Ok(body) => Ok(Response::builder()
            .content_type(format.media_type())
            .body(body)),
        Err(error) => Err(text(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("cannot write the service description: {error}"),
        )),
    }
}
