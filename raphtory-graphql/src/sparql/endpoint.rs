//! The SPARQL 1.1 Protocol endpoint: `/sparql/<graph path>` answers SPARQL queries on a graph.
//!
//! A query runs as the GraphQL query `graph(path:, graphType:) { sparql(query:) }` (with
//! `before(time: T + 1)` in between for `asof=T`) with the caller's credentials, so it gets the same
//! permission checks, access filters and limits. The [`ProtocolCall`] in the request data
//! carries the negotiated formats and dataset to the `sparql` field and the serialized results
//! back.
use crate::{
    auth::{Access, AuthenticatedGraphQL, Roles, TokenClaimValues},
    config::concurrency_config::ConcurrencyConfig,
    data::GqlGraphType,
    model::graph::graph::GqlGraph,
    paths::PathValidationError,
    sparql::{accept, check_query, run_sparql, SparqlError},
    GQLError,
};
use async_graphql::{Executor, ServerError, Variables};
use poem::{
    http::{
        header::{HeaderValue, ACCEPT, ALLOW, CONTENT_TYPE, HOST, VARY},
        Method, StatusCode,
    },
    Body, Endpoint, Request, Response,
};
use raphtory::{
    db::api::view::DynamicGraph,
    errors::GraphError,
    rdf::{
        model::{vocab::rdf, BlankNode, NamedNode, Triple},
        QueryResultsFormat, RdfError, RdfFormat, RdfSerializer, RdfViewOps, SparqlDataset,
        SparqlFormat, SparqlOptions, SparqlWriteStats, TimeGraph, VALID_FROM, VALID_FROM_TIME,
        VALID_TO, VALID_TO_TIME,
    },
};
use raphtory_api::core::{storage::timeindex::AsTime, utils::time::TryIntoTime};
use serde_json::json;
use std::{
    collections::HashSet,
    io,
    sync::{Arc, Mutex},
};

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
const GRAPH_FORMATS: &[(GraphFormat, &[&str])] = &[
    (
        GraphFormat::Turtle,
        &["text/turtle", "application/x-turtle", "application/turtle"],
    ),
    (GraphFormat::NTriples, &["application/n-triples"]),
    (
        GraphFormat::RdfXml,
        &["application/rdf+xml", "application/xml", "text/xml"],
    ),
    (
        GraphFormat::JsonLd,
        &["application/ld+json", "application/json"],
    ),
    (GraphFormat::NQuads, &["application/n-quads"]),
    (GraphFormat::TriG, &["application/trig"]),
];

/// The RDF formats of [`GRAPH_FORMATS`].
#[derive(Clone, Copy, Debug, PartialEq)]
enum GraphFormat {
    Turtle,
    NTriples,
    RdfXml,
    JsonLd,
    NQuads,
    TriG,
}

impl GraphFormat {
    fn rdf_format(self) -> RdfFormat {
        match self {
            Self::Turtle => RdfFormat::Turtle,
            Self::NTriples => RdfFormat::NTriples,
            Self::RdfXml => RdfFormat::RdfXml,
            Self::JsonLd => {
                RdfFormat::from_media_type("application/ld+json").expect("JSON-LD is supported")
            }
            Self::NQuads => RdfFormat::NQuads,
            Self::TriG => RdfFormat::TriG,
        }
    }

    /// The W3C IRI of the format, for the service description.
    fn iri(self) -> &'static str {
        match self {
            Self::Turtle => "http://www.w3.org/ns/formats/Turtle",
            Self::NTriples => "http://www.w3.org/ns/formats/N-Triples",
            Self::RdfXml => "http://www.w3.org/ns/formats/RDF_XML",
            Self::JsonLd => "http://www.w3.org/ns/formats/JSON-LD",
            Self::NQuads => "http://www.w3.org/ns/formats/N-Quads",
            Self::TriG => "http://www.w3.org/ns/formats/TriG",
        }
    }
}

/// The W3C IRI of a results format, for the service description.
fn results_format_iri(format: QueryResultsFormat) -> &'static str {
    match format {
        QueryResultsFormat::Json => "http://www.w3.org/ns/formats/SPARQL_Results_JSON",
        QueryResultsFormat::Xml => "http://www.w3.org/ns/formats/SPARQL_Results_XML",
        QueryResultsFormat::Csv => "http://www.w3.org/ns/formats/SPARQL_Results_CSV",
        _ => "http://www.w3.org/ns/formats/SPARQL_Results_TSV",
    }
}

/// A format of query results: a SPARQL results format for `SELECT` and `ASK`, an RDF format for
/// `CONSTRUCT` and `DESCRIBE`.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Format {
    Results(QueryResultsFormat),
    Graph(GraphFormat),
}

impl Format {
    fn sparql_format(self) -> SparqlFormat {
        match self {
            Self::Results(format) => format.into(),
            Self::Graph(format) => format.rdf_format().into(),
        }
    }

    fn media_type(self) -> &'static str {
        match self {
            Self::Results(format) => format.media_type(),
            Self::Graph(format) => format.rdf_format().media_type(),
        }
    }

    /// Whether some results do not fit the format: SPARQL Results XML cannot hold literals with
    /// control characters and RDF 1.2 triple terms, and RDF/XML skips triples such as those
    /// whose predicate does not end in an XML name (see `RdfViewOps::sparql_to_writer`).
    fn may_not_hold(self) -> bool {
        matches!(
            self,
            Self::Results(QueryResultsFormat::Xml) | Self::Graph(GraphFormat::RdfXml)
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

    /// Runs `query` and returns its results and their media type: in the preferred format the
    /// client accepts for the form of the query that holds them all.
    fn write(
        &self,
        view: &DynamicGraph,
        query: &str,
        options: &SparqlOptions,
    ) -> Result<(Vec<u8>, &'static str), SparqlError> {
        let (form, formats) = self.for_form(view, query, options)?;
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
        Err(not_held.unwrap_or_else(|| SparqlError::NotAcceptable { form }))
    }

    /// The form of `query` (`SELECT`, ...) and the formats accepted for its results, found once
    /// the query parses and before it runs.
    fn for_form(
        &self,
        view: &DynamicGraph,
        query: &str,
        options: &SparqlOptions,
    ) -> Result<(&'static str, &[Format]), SparqlError> {
        // A format with no slot fits no query, and is refused before the query runs.
        let no_format = SparqlFormat {
            results: None,
            graph: None,
        };
        match view.sparql_to_writer_with(query, io::sink(), no_format, options) {
            Err(GraphError::Rdf(RdfError::WrongResultsFormat { form, .. })) => {
                let formats = match form {
                    "CONSTRUCT" | "DESCRIBE" => &self.graph,
                    _ => &self.results,
                };
                Ok((form, formats))
            }
            Err(error) => Err(error.into()),
            Ok(_) => Err(SparqlError::Internal(
                "the form of the SPARQL query is unknown".to_owned(),
            )),
        }
    }
}

/// Writes results in `format` with `write`, failing if the format cannot hold them all.
fn write_in(
    format: Format,
    write: impl FnOnce(&mut Vec<u8>, SparqlFormat) -> Result<SparqlWriteStats, GraphError>,
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

/// What the endpoint passes to the `sparql` field in the request data.
pub(crate) struct ProtocolCall {
    formats: Formats,
    dataset: Option<SparqlDataset>,
    /// The serialized results and their media type, once the query has run.
    results: Mutex<Option<(Vec<u8>, &'static str)>>,
}

impl ProtocolCall {
    /// Runs `query` on `graph` for the caller of `ctx` and keeps the results.
    pub(crate) async fn run(
        &self,
        ctx: &async_graphql::Context<'_>,
        graph: &GqlGraph,
        query: String,
    ) -> Result<(), SparqlError> {
        let formats = self.formats.clone();
        let results = run_sparql(
            ctx,
            graph,
            query,
            self.dataset.clone(),
            move |view, q, o| formats.write(view, q, o),
        )
        .await?;
        *self.results.lock().unwrap() = Some(results);
        Ok(())
    }
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
        SparqlError::InvalidTimeout(_) | SparqlError::Internal(_) => {
            StatusCode::INTERNAL_SERVER_ERROR
        }
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

/// The response for a GraphQL error of the query.
fn failure(error: &ServerError, path: &str) -> Failure {
    if let Some(error) = error.source::<SparqlError>() {
        return error.into();
    }
    // `graph(path:)` failed to load the graph
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
    graph_type: Option<String>,
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
            "graph_type" => once(&mut self.graph_type, value)?,
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

    fn graph_type(&self) -> Result<Option<GqlGraphType>, Failure> {
        match self.graph_type.as_deref().map(str::to_ascii_lowercase) {
            None => Ok(None),
            Some(t) if t == "event" => Ok(Some(GqlGraphType::Event)),
            Some(t) if t == "persistent" => Ok(Some(GqlGraphType::Persistent)),
            Some(_) => Err(text(
                StatusCode::BAD_REQUEST,
                "invalid `graph_type`: expected `event` or `persistent`",
            )),
        }
    }

    /// The time of `asof` in epoch milliseconds, parsed as in `<raphtory:asof:T>`.
    ///
    /// Parameters are form-encoded, where `+` stands for a space, so the `+` of a timezone offset
    /// pasted as is into a URL (`?asof=2024-01-01T00:00:00+01:00`) arrives as a space. A time that
    /// does not parse is therefore read again with its last space as `+`.
    fn asof(&self) -> Result<Option<i64>, Failure> {
        let Some(time) = self.asof.as_deref() else {
            return Ok(None);
        };
        let parse = |time: &str| match time.parse::<i64>() {
            Ok(at) => Some(at),
            Err(_) => time.try_into_time().ok().map(|at| at.t()),
        };
        let with_plus = || {
            let (before, after) = time.rsplit_once(' ')?;
            parse(&format!("{before}+{after}"))
        };
        match parse(time).or_else(with_plus) {
            Some(at) => Ok(Some(at)),
            None => Err(text(
                StatusCode::BAD_REQUEST,
                format!(
                    "invalid `asof`: cannot parse \"{time}\" as a time: expected epoch \
                     milliseconds or a date-time such as 2024-01-01, 2024-01-01T00:00:00 or \
                     2024-01-01T00:00:00Z"
                ),
            )),
        }
    }
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
    /// An endpoint that runs queries through `graphql`, the GraphQL endpoint of the server, and
    /// shares its `heavy_query_limit` slots.
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

/// The GraphQL query a protocol request runs.
const QUERY: &str = "query SparqlProtocol($path: String!, $graphType: GraphType, $query: String!) \
    { graph(path: $path, graphType: $graphType) { sparql(query: $query) } }";

/// The GraphQL query a protocol request with `asof=T` runs: on the history up to and including
/// `T`, so the triples as of `T`, with the time graphs of earlier times.
const QUERY_AS_OF: &str = "query SparqlProtocol($path: String!, $graphType: GraphType, \
    $end: TimeInput!, $query: String!) { graph(path: $path, graphType: $graphType) \
    { before(time: $end) { sparql(query: $query) } } }";

/// The GraphQL query that checks that the caller can read the graph of a request.
const LOOKUP: &str = "query SparqlService($path: String!, $graphType: GraphType) \
    { graph(path: $path, graphType: $graphType) { path } }";

/// The caller of a request, as the GraphQL endpoint authenticates it.
type Caller = (Access, Roles, TokenClaimValues);

/// The GraphQL request of `query` with `variables`, made by `caller`.
fn graphql_request(
    query: &str,
    variables: serde_json::Value,
    (access, roles, claims): Caller,
) -> async_graphql::Request {
    async_graphql::Request::new(query)
        .variables(Variables::from_json(variables))
        .data(access)
        .data(roles)
        .data(claims)
}

impl<E: Executor> SparqlEndpoint<E> {
    /// Answers a GET or POST request.
    async fn respond(&self, mut req: Request) -> Result<Response, Failure> {
        let path = req.raw_path_param("path").unwrap_or_default().to_owned();
        let caller = self.graphql.authenticate(&req).await?;
        let config = Some(&self.config);
        // `disable_lists` turns the endpoint off
        check_query(config, "").map_err(|error| Failure::from(&error))?;

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
        let Some(query) = params.query.take() else {
            if req.method() == Method::POST {
                return Err(text(StatusCode::BAD_REQUEST, "the request has no `query`"));
            }
            let graph_type = params.graph_type()?;
            params.asof()?;
            return self
                .describe(&req, &path, graph_type, accept.as_deref(), caller)
                .await;
        };
        check_query(config, &query).map_err(|error| Failure::from(&error))?;
        let dataset = params.dataset()?;
        let graph_type = params.graph_type()?;
        let asof = params.asof()?;
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

        let call = Arc::new(ProtocolCall {
            formats,
            dataset,
            results: Mutex::new(None),
        });
        let mut variables = json!({
            "path": path,
            "graphType": graph_type.map(|graph_type| graph_type.as_gql()),
            "query": query,
        });
        let document = match asof {
            Some(asof) => {
                variables["end"] = json!(asof.saturating_add(1));
                QUERY_AS_OF
            }
            None => QUERY,
        };
        let request = graphql_request(document, variables, caller).data(call.clone());
        let response = self.graphql.execute_read(request, true).await?;
        if let Some((body, media_type)) = call.results.lock().unwrap().take() {
            return Ok(Response::builder().content_type(media_type).body(body));
        }
        Err(match response.errors.first() {
            Some(error) => failure(error, &path),
            // `graph` is null: the caller cannot read the graph
            None => not_found(&path),
        })
    }

    /// The service description of the endpoint of the graph at `path`, in the RDF format
    /// `accept` prefers. Like a query, it needs a graph that `caller` can read.
    async fn describe(
        &self,
        req: &Request,
        path: &str,
        graph_type: Option<GqlGraphType>,
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
        let variables = json!({
            "path": path,
            "graphType": graph_type.map(|graph_type| graph_type.as_gql()),
        });
        let request = graphql_request(LOOKUP, variables, caller);
        let response = self.graphql.execute_read(request, false).await?;
        if let Some(error) = response.errors.first() {
            return Err(failure(error, path));
        }
        match response.data.into_json() {
            Ok(data) if data["graph"].is_object() => service_description(req, format),
            // `graph` is null: the caller cannot read the graph
            _ => Err(not_found(path)),
        }
    }

    /// The body of a POST, at most a little more than a form holding the longest query.
    async fn read_body(&self, body: Body) -> Result<Vec<u8>, Failure> {
        let unreadable =
            |error: poem::error::ReadBodyError| text(StatusCode::BAD_REQUEST, error.to_string());
        let Some(max) = self.config.max_sparql_query_length else {
            return body.into_vec().await.map_err(unreadable);
        };
        let limit = max.saturating_mul(3).saturating_add(64 * 1024);
        match body.into_bytes_limit(limit).await {
            Ok(body) => Ok(body.into()),
            Err(poem::error::ReadBodyError::PayloadTooLarge) => Err(text(
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
fn service_description(req: &Request, format: GraphFormat) -> Result<Response, Failure> {
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
    let results_formats = RESULTS_FORMATS
        .iter()
        .map(|(format, _)| results_format_iri(*format));
    let graph_formats = GRAPH_FORMATS.iter().map(|(format, _)| format.iri());
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
    let rdf_format = format.rdf_format();
    let serializer = RdfSerializer::from_format(rdf_format)
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
            .content_type(rdf_format.media_type())
            .body(body)),
        Err(error) => Err(text(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("cannot write the service description: {error}"),
        )),
    }
}
