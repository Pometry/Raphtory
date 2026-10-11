//! The SPARQL 1.1 Protocol endpoint: `/sparql/<graph path>` answers SPARQL queries on a graph.
//!
//! A request reads the graph through the `rdfGraph` field with the caller's credentials, so it
//! gets the same permission checks and access filters, and runs the query on `valid()` of it,
//! the current state. A query that reads a time graph `<raphtory:asof:T>`
//! ([`QueryOutline::reads_time_graphs`]), or a request whose dataset names one, runs on the whole
//! graph instead, since a time graph already holds the state at its time. In such a query the
//! default graph holds every triple ever asserted or retracted, and the current state can be
//! named as a time graph at the present time. `asof=T` runs the query on `at(T)`.
use crate::{
    auth::{AuthenticatedGraphQL, Caller},
    config::concurrency_config::ConcurrencyConfig,
    model::graph::rdf_graph::{GqlRdfGraph, RdfGraphLookup},
    paths::PathValidationError,
    sparql::{accept, check_enabled, check_length, run_sparql, write_capped, Capped, SparqlError},
    GQLError,
};
use async_graphql::{Executor, Request as GraphQLRequest, ServerError, Variables};
use itertools::Itertools;
use oxigraph::{
    io::{RdfFormat as OxRdfFormat, RdfSerializer},
    model::{vocab::rdf, BlankNode, IriParseError},
};
use poem::{
    error::{ReadBodyError, ResponseError},
    http::{
        header::{HeaderValue, ALLOW, HOST, VARY},
        Method, StatusCode,
    },
    web::{headers::Mime, Accept},
    Body, Endpoint, FromRequest, Request, Response,
};
use raphtory::db::api::view::{IntoDynHop, TimeOps};
use raphtory_api::core::utils::time::ParseTimeError;
use raphtory_rdf::{
    NamedNode, QueryForm, QueryOutline, QueryResultsFormat, RdfError, RdfFormat, RdfView,
    SparqlDataset, SparqlFormat, SparqlOptions, TimeGraph, Triple, WriteStats, VALID_FROM,
    VALID_FROM_TIME, VALID_TO, VALID_TO_TIME,
};
use serde_json::json;
use std::{io, sync::Arc};
use thiserror::Error;
use url::form_urlencoded;

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
        RdfFormat::JsonLd,
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
    /// control characters and RDF 1.2 triple terms, and RDF/XML cannot hold triples such as
    /// those whose predicate does not end in an XML name (see `RdfView::sparql_to_writer`).
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
    fn negotiate(accept: &[Mime]) -> Self {
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
        config: &ConcurrencyConfig,
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
            return write_in(preferred, config, |out, format| {
                view.sparql_to_writer(query, out, format, options)
            });
        }
        // The preferred format may not hold every result: run the query once and write its
        // results in the accepted formats in turn, until one holds them all.
        let results = view.sparql(query, options)?;
        let mut not_held = None;
        for &format in formats {
            match write_in(format, config, |out, format| results.write(out, format)) {
                Err(error) if cannot_hold(&error) => not_held = Some(error),
                written => return written,
            }
        }
        Err(not_held.unwrap_or(SparqlError::NotAcceptable { form }))
    }
}

/// Writes results in `format` with `write`, within the limits of `config`.
fn write_in(
    format: Format,
    config: &ConcurrencyConfig,
    write: impl FnOnce(&mut Capped<Vec<u8>>, SparqlFormat) -> Result<WriteStats, RdfError>,
) -> Result<(Vec<u8>, &'static str), SparqlError> {
    let out = write_capped(config, |out| write(out, format.sparql_format()))?;
    Ok((out, format.media_type()))
}

/// Whether `error` is results that their format cannot hold, which another format can.
fn cannot_hold(error: &SparqlError) -> bool {
    matches!(
        error,
        SparqlError::Query(
            RdfError::XmlUnsafeLiteral(_)
                | RdfError::XmlTripleTerm(_)
                | RdfError::RdfXmlSkipped { .. }
        )
    )
}

/// The status of a failed query.
fn status_of(error: &SparqlError) -> StatusCode {
    match error {
        SparqlError::Disabled => StatusCode::SERVICE_UNAVAILABLE,
        SparqlError::TooLong { .. } => StatusCode::BAD_REQUEST,
        SparqlError::NotAcceptable { .. } => StatusCode::NOT_ACCEPTABLE,
        SparqlError::TooLarge { .. } => StatusCode::PAYLOAD_TOO_LARGE,
        #[cfg(feature = "shacl")]
        SparqlError::RecursiveShapes => StatusCode::BAD_REQUEST,
        SparqlError::Panicked => StatusCode::INTERNAL_SERVER_ERROR,
        SparqlError::Query(error) => match error {
            RdfError::WrongResultsFormat { .. }
            | RdfError::XmlUnsafeLiteral(_)
            | RdfError::XmlTripleTerm(_)
            | RdfError::RdfXmlSkipped { .. } => StatusCode::NOT_ACCEPTABLE,
            RdfError::SparqlSyntax(_)
            | RdfError::SparqlTooDeep { .. }
            | RdfError::TooManyPatterns { .. }
            | RdfError::SparqlEvaluation(_)
            | RdfError::InvalidTimeGraph { .. }
            | RdfError::Iri(_) => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        },
    }
}

/// Why a protocol request failed. The response is the message as plain text.
#[derive(Debug, Error)]
enum EndpointError {
    #[error("the request has more than one `{0}`")]
    DuplicateParam(&'static str),
    #[error("the query is not UTF-8")]
    QueryNotUtf8,
    #[error(
        "a SPARQL query POST must have the content type application/x-www-form-urlencoded or \
         application/sparql-query"
    )]
    UnsupportedMediaType,
    #[error("SPARQL Update is not supported: this endpoint answers queries only")]
    UpdateNotSupported,
    #[error("the request has no `query`")]
    NoQuery,
    #[error(
        "the Accept header allows no format of this endpoint: SPARQL results as {}; RDF as {}",
        media_types(RESULTS_FORMATS),
        media_types(GRAPH_FORMATS)
    )]
    NoAcceptableFormat,
    #[error(
        "the Accept header allows no RDF format of this endpoint: {}",
        media_types(GRAPH_FORMATS)
    )]
    NoRdfFormat,
    #[error("invalid `asof`: {0}")]
    InvalidAsof(ParseTimeError),
    #[error("invalid dataset IRI <{iri}>: {error}")]
    InvalidDatasetIri { iri: String, error: IriParseError },
    #[error(
        "<{0}> is not a graph of this endpoint: a dataset can only name time graphs \
         <raphtory:asof:T>"
    )]
    NotATimeGraph(String),
    #[error(transparent)]
    InvalidTimeGraph(RdfError),
    /// A graph that does not exist or that the caller cannot read; the two are
    /// indistinguishable.
    #[error(transparent)]
    GraphNotFound(PathValidationError),
    /// `rdfGraph(path:)` rejected the path.
    #[error("{}", .0.message)]
    InvalidPath(ServerError),
    /// `rdfGraph(path:)` failed to load the graph.
    #[error("{}", .0.message)]
    Lookup(ServerError),
    #[error(
        "request body too large for a SPARQL query of at most {max} bytes \
         (`max_sparql_query_length`)"
    )]
    BodyTooLarge { max: usize },
    #[error("the `Host` header does not make the endpoint URL <{0}> an IRI")]
    InvalidHost(String),
    #[error("cannot write the service description in {0}")]
    NoSerializer(RdfFormat),
    #[error("cannot write the service description: {0}")]
    ServiceDescription(#[from] io::Error),
    #[error("the SPARQL endpoint answers GET and POST requests")]
    MethodNotAllowed,
    #[error(transparent)]
    Sparql(#[from] SparqlError),
}

impl ResponseError for EndpointError {
    fn status(&self) -> StatusCode {
        match self {
            Self::DuplicateParam(_)
            | Self::QueryNotUtf8
            | Self::NoQuery
            | Self::InvalidAsof(_)
            | Self::InvalidDatasetIri { .. }
            | Self::NotATimeGraph(_)
            | Self::InvalidTimeGraph(_)
            | Self::InvalidPath(_)
            | Self::InvalidHost(_) => StatusCode::BAD_REQUEST,
            Self::UnsupportedMediaType => StatusCode::UNSUPPORTED_MEDIA_TYPE,
            Self::UpdateNotSupported => StatusCode::NOT_IMPLEMENTED,
            Self::NoAcceptableFormat | Self::NoRdfFormat => StatusCode::NOT_ACCEPTABLE,
            Self::GraphNotFound(_) => StatusCode::NOT_FOUND,
            Self::Lookup(_) | Self::NoSerializer(_) | Self::ServiceDescription(_) => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
            Self::BodyTooLarge { .. } => StatusCode::PAYLOAD_TOO_LARGE,
            Self::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
            Self::Sparql(error) => status_of(error),
        }
    }
}

/// Every media type `formats` are served for.
fn media_types<T>(formats: &[(T, &[&str])]) -> String {
    formats
        .iter()
        .flat_map(|(_, types)| types.iter())
        .join(", ")
}

/// The error of `rdfGraph(path:)`, which failed to load the graph.
fn lookup_error(error: ServerError, path: &str) -> EndpointError {
    let mut graph_error = error.source::<GQLError>();
    while let Some(GQLError::Arc(inner)) = graph_error {
        graph_error = Some(inner.as_ref());
    }
    match graph_error {
        Some(GQLError::Validation(
            PathValidationError::GraphNotExistsError(_)
            | PathValidationError::NamespaceDoesNotExist(_),
        )) => graph_not_found(path),
        Some(GQLError::Validation(
            PathValidationError::InvalidPath { .. } | PathValidationError::EmptyPath,
        )) => EndpointError::InvalidPath(error),
        _ => EndpointError::Lookup(error),
    }
}

fn graph_not_found(path: &str) -> EndpointError {
    EndpointError::GraphNotFound(PathValidationError::GraphNotExistsError(path.to_owned()))
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
    fn add(&mut self, name: &str, value: String) -> Result<(), EndpointError> {
        let once = |slot: &mut Option<String>, name, value| match slot {
            Some(_) => Err(EndpointError::DuplicateParam(name)),
            None => {
                *slot = Some(value);
                Ok(())
            }
        };
        match name {
            "query" => once(&mut self.query, "query", value)?,
            "update" => self.update = true,
            "default-graph-uri" => self.default_graphs.push(value),
            "named-graph-uri" => self.named_graphs.push(value),
            "asof" => once(&mut self.asof, "asof", value)?,
            // other parameters (such as the `format` and `output` of some clients) are ignored
            _ => {}
        }
        Ok(())
    }

    fn add_form(&mut self, form: &[u8]) -> Result<(), EndpointError> {
        for (name, value) in form_urlencoded::parse(form) {
            self.add(&name, value.into_owned())?;
        }
        Ok(())
    }

    /// The dataset of `default-graph-uri` and `named-graph-uri`, if the request sets one.
    fn dataset(&self) -> Result<Option<SparqlDataset>, EndpointError> {
        if self.default_graphs.is_empty() && self.named_graphs.is_empty() {
            return Ok(None);
        }
        let time_graphs = |iris: &[String]| -> Result<Vec<NamedNode>, EndpointError> {
            iris.iter()
                .map(|iri| {
                    let node = NamedNode::new(iri.as_str()).map_err(|error| {
                        EndpointError::InvalidDatasetIri {
                            iri: iri.clone(),
                            error,
                        }
                    })?;
                    match TimeGraph::parse(&node).map_err(EndpointError::InvalidTimeGraph)? {
                        Some(graph) => Ok(graph.iri),
                        None => Err(EndpointError::NotATimeGraph(iri.clone())),
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
    fn asof(&self) -> Result<Option<i64>, EndpointError> {
        self.asof
            .as_deref()
            .map(TimeGraph::parse_time)
            .transpose()
            .map_err(EndpointError::InvalidAsof)
    }
}

/// The SPARQL 1.1 Protocol endpoint for the graphs of a server.
pub(crate) struct SparqlEndpoint<E> {
    graphql: Arc<AuthenticatedGraphQL<E>>,
}

impl<E> SparqlEndpoint<E> {
    /// An endpoint that reads graphs through `graphql`, the GraphQL endpoint of the server, and
    /// shares its configuration, `exclusive_writes` lock and `heavy_query_limit` slots.
    pub(crate) fn new(graphql: Arc<AuthenticatedGraphQL<E>>) -> Self {
        Self { graphql }
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
                    .unwrap_or_else(|error| error.into_response());
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
                let mut response = EndpointError::MethodNotAllowed.as_response();
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

impl<E: Executor> SparqlEndpoint<E> {
    /// Answers a GET or POST request.
    async fn respond(&self, mut req: Request) -> poem::Result<Response> {
        let path = req.raw_path_param("path").unwrap_or_default().to_owned();
        let caller = self.graphql.authenticate(&req).await?;
        let config = &self.graphql.config().concurrency;
        check_enabled(Some(config)).map_err(EndpointError::from)?;

        let mut params = Params::default();
        if let Some(query) = req.uri().query() {
            params.add_form(query.as_bytes())?;
        }
        if req.method() == Method::POST {
            let content_type = req
                .content_type()
                .and_then(|value| value.parse::<Mime>().ok());
            match content_type.as_ref().map(Mime::essence_str) {
                Some("application/sparql-update") => params.update = true,
                Some("application/x-www-form-urlencoded") => {
                    params.add_form(&self.read_body(req.take_body()).await?)?
                }
                Some("application/sparql-query") => {
                    let query = String::from_utf8(self.read_body(req.take_body()).await?)
                        .map_err(|_| EndpointError::QueryNotUtf8)?;
                    params.add("query", query)?;
                }
                _ => return Err(EndpointError::UnsupportedMediaType.into()),
            }
        }
        if params.update {
            return Err(EndpointError::UpdateNotSupported.into());
        }
        let Accept(accept) = Accept::from_request_without_body(&req).await?;
        let asof = params.asof()?;
        let Some(query) = params.query.take() else {
            if req.method() == Method::POST {
                return Err(EndpointError::NoQuery.into());
            }
            return self.describe(&req, &path, &accept, &caller).await;
        };
        check_length(config, &query).map_err(EndpointError::from)?;
        let dataset = params.dataset()?;
        let formats = Formats::negotiate(&accept);
        if formats.is_empty() {
            return Err(EndpointError::NoAcceptableFormat.into());
        }
        let run = async {
            let graph = self.rdf_graph(&path, &caller).await?;
            let has_dataset = dataset.is_some();
            let results = run_sparql(
                Some(config),
                &graph,
                query,
                dataset,
                move |view, query, options, config| {
                    let outline = QueryOutline::parse(query)?;
                    let view = match asof {
                        Some(at) => view.at(at).into_dyn_hop(),
                        None if has_dataset || outline.reads_time_graphs => view.clone(),
                        None => view.valid().into_dyn_hop(),
                    };
                    formats.write(&view, query, outline.form, options, config)
                },
            )
            .await?;
            Ok::<_, EndpointError>(results)
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
        accept: &[Mime],
        caller: &Caller,
    ) -> poem::Result<Response> {
        let Some(&format) = accept::rank(accept, GRAPH_FORMATS).first() else {
            return Err(EndpointError::NoRdfFormat.into());
        };
        let graph = self
            .graphql
            .read(false, self.rdf_graph(path, caller))
            .await??;
        Ok(service_description(
            req,
            format,
            graph.temporal_functions(),
        )?)
    }

    /// The graph at `path` as `rdfGraph` reads it for `caller`.
    async fn rdf_graph(&self, path: &str, caller: &Caller) -> Result<GqlRdfGraph, EndpointError> {
        let lookup = Arc::new(RdfGraphLookup::default());
        let mut request = GraphQLRequest::new(LOOKUP)
            .variables(Variables::from_json(json!({ "path": path })))
            .data(lookup.clone());
        caller.attach(&mut request);
        let response = self.graphql.executor().execute(request).await;
        if let Some(error) = response.errors.into_iter().next() {
            return Err(lookup_error(error, path));
        }
        // `rdfGraph` is null: the caller cannot read the graph
        lookup.get().ok_or_else(|| graph_not_found(path))
    }

    /// The body of a POST, at most a little more than a form holding the longest query.
    async fn read_body(&self, body: Body) -> poem::Result<Vec<u8>> {
        let Some(max) = self.graphql.config().concurrency.max_sparql_query_length else {
            return Ok(body.into_vec().await?);
        };
        // percent-encoding can triple each byte of the query; 64 KiB covers the other form fields.
        let limit = max.saturating_mul(3).saturating_add(64 * 1024);
        match body.into_bytes_limit(limit).await {
            Ok(body) => Ok(body.into()),
            Err(ReadBodyError::PayloadTooLarge) => Err(EndpointError::BodyTooLarge { max }.into()),
            Err(error) => Err(error.into()),
        }
    }
}

/// The absolute URL of the endpoint `req` was sent to, from its `Host` header.
fn endpoint_url(req: &Request) -> Result<NamedNode, EndpointError> {
    let host = req.header(HOST).unwrap_or_default();
    let iri = format!("http://{host}{}", req.uri().path());
    match NamedNode::new(iri.as_str()) {
        Ok(url) if !host.is_empty() => Ok(url),
        _ => Err(EndpointError::InvalidHost(iri)),
    }
}

const SD: &str = "http://www.w3.org/ns/sparql-service-description#";

/// The SPARQL 1.1 Service Description of the endpoint `req` was sent to, in `format`. It lists
/// the temporal functions if `temporal_functions`, that is if the caller's queries may call them.
fn service_description(
    req: &Request,
    format: RdfFormat,
    temporal_functions: bool,
) -> Result<Response, EndpointError> {
    let sd = |name: &str| NamedNode::new_unchecked(format!("{SD}{name}"));
    let iri = |iri: &str| NamedNode::new_unchecked(iri);
    let service = BlankNode::default();
    let mut triples = vec![
        Triple::new(service.clone(), rdf::TYPE, sd("Service")),
        Triple::new(service.clone(), sd("endpoint"), endpoint_url(req)?),
        Triple::new(
            service.clone(),
            sd("supportedLanguage"),
            sd("SPARQL11Query"),
        ),
    ];
    let results_formats = RESULTS_FORMATS.iter().map(|(format, _)| format.iri());
    let graph_formats = GRAPH_FORMATS.iter().map(|(format, _)| match format {
        // oxigraph's IRI for JSON-LD is not the one of the W3C formats registry
        RdfFormat::JsonLd => "http://www.w3.org/ns/formats/JSON-LD",
        format => format.iri(),
    });
    for format in results_formats.chain(graph_formats) {
        triples.push(Triple::new(
            service.clone(),
            sd("resultFormat"),
            iri(format),
        ));
    }
    if temporal_functions {
        for function in [VALID_FROM, VALID_TO, VALID_FROM_TIME, VALID_TO_TIME] {
            triples.push(Triple::new(
                service.clone(),
                sd("extensionFunction"),
                iri(function),
            ));
        }
    }
    let serializer_format = OxRdfFormat::from_media_type(format.media_type())
        .ok_or(EndpointError::NoSerializer(format))?;
    let mut writer = RdfSerializer::from_format(serializer_format)
        .with_prefix("sd", SD)
        .and_then(|serializer| serializer.with_prefix("formats", "http://www.w3.org/ns/formats/"))
        .map_err(io::Error::other)?
        .for_writer(Vec::new());
    for triple in &triples {
        writer.serialize_triple(triple)?;
    }
    Ok(Response::builder()
        .content_type(format.media_type())
        .body(writer.finish()?))
}
