//! The SPARQL 1.1 Protocol endpoint: `/sparql/<graph path>` answers a SPARQL query by sending
//! it, with the request's `Authorization` header, to the GraphQL endpoint as the `sparql` field
//! of `rdfGraph(path:)`, so it gets the same permission checks, access filters and limits.
//!
//! The query runs on `valid` of the graph, the current state, unless it reads a time graph
//! `<raphtory:asof:T>` ([`QueryOutline::reads_time_graphs`]): then it runs on the whole graph,
//! since a time graph already holds the state at its time. `asof=T` runs it on
//! `before(time: T + 1) { valid }`.
//!
//! The query is parsed, to choose the view and the format, before the GraphQL endpoint
//! authenticates the request.
use crate::{
    model::graph::rdf_graph::GqlSparqlFormat,
    paths::PathValidationError,
    rayon::blocking_compute,
    sparql::{accept, SparqlError, CODE_RESULTS_TOO_LARGE},
};
use async_graphql::Value;
use itertools::Itertools;
use poem::{
    error::ResponseError,
    http::{
        header::{HeaderValue, ALLOW, AUTHORIZATION},
        Method, StatusCode,
    },
    web::{headers::Mime, Accept},
    Endpoint, FromRequest, IntoResponse, Request, Response,
};
use raphtory_api::core::utils::time::ParseTimeError;
use raphtory_rdf::{QueryForm, QueryOutline, SparqlFormat, SparqlOptions, TimeGraph};
use serde_json::{json, Value as Json};
use thiserror::Error;
use url::form_urlencoded;

const ALLOWED: &str = "GET, POST, OPTIONS";

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
    #[error(
        "`default-graph-uri` and `named-graph-uri` are not supported: name time graphs \
         <raphtory:asof:T> in the query"
    )]
    DatasetNotSupported,
    #[error("the request has no `query`")]
    NoQuery,
    #[error("invalid `asof`: {0}")]
    InvalidAsof(ParseTimeError),
    #[error(transparent)]
    Query(SparqlError),
    #[error("the Accept header allows no format of {} results", .0.name())]
    NotAcceptable(QueryForm),
    /// The status and the messages of the errors of the GraphQL request.
    #[error("{1}")]
    GraphQL(StatusCode, String),
    /// A graph that does not exist or that the caller cannot read; the two are
    /// indistinguishable.
    #[error(transparent)]
    GraphNotFound(PathValidationError),
    #[error("the SPARQL endpoint answers GET and POST requests")]
    MethodNotAllowed,
}

impl ResponseError for EndpointError {
    fn status(&self) -> StatusCode {
        match self {
            Self::DuplicateParam(_)
            | Self::QueryNotUtf8
            | Self::DatasetNotSupported
            | Self::NoQuery
            | Self::InvalidAsof(_)
            | Self::Query(_) => StatusCode::BAD_REQUEST,
            Self::GraphQL(status, _) => *status,
            Self::UnsupportedMediaType => StatusCode::UNSUPPORTED_MEDIA_TYPE,
            Self::UpdateNotSupported => StatusCode::NOT_IMPLEMENTED,
            Self::NotAcceptable(_) => StatusCode::NOT_ACCEPTABLE,
            Self::GraphNotFound(_) => StatusCode::NOT_FOUND,
            Self::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
        }
    }
}

#[derive(Default)]
struct Params {
    query: Option<String>,
    update: bool,
    dataset: bool,
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
            "default-graph-uri" | "named-graph-uri" => self.dataset = true,
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
}

/// The SPARQL 1.1 Protocol endpoint for the graphs of a server.
pub(crate) struct SparqlEndpoint<G> {
    graphql: G,
    options: SparqlOptions,
}

impl<G> SparqlEndpoint<G> {
    /// An endpoint that sends queries to `graphql`, the GraphQL endpoint of the server, whose
    /// SPARQL queries run with `options`.
    pub(crate) fn new(graphql: G, options: SparqlOptions) -> Self {
        Self { graphql, options }
    }
}

impl<G: Endpoint> Endpoint for SparqlEndpoint<G> {
    type Output = Response;

    async fn call(&self, req: Request) -> poem::Result<Response> {
        let response = match *req.method() {
            Method::GET | Method::POST => self
                .respond(req)
                .await
                .unwrap_or_else(|error| error.into_response()),
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

impl<G: Endpoint> SparqlEndpoint<G> {
    async fn respond(&self, mut req: Request) -> poem::Result<Response> {
        let path = req.raw_path_param("path").unwrap_or_default().to_owned();
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
                    params.add_form(&req.take_body().into_vec().await?)?
                }
                Some("application/sparql-query") => {
                    let query = String::from_utf8(req.take_body().into_vec().await?)
                        .map_err(|_| EndpointError::QueryNotUtf8)?;
                    params.add("query", query)?;
                }
                _ => return Err(EndpointError::UnsupportedMediaType.into()),
            }
        }
        if params.update {
            return Err(EndpointError::UpdateNotSupported.into());
        }
        if params.dataset {
            return Err(EndpointError::DatasetNotSupported.into());
        }
        let query = params.query.ok_or(EndpointError::NoQuery)?;
        let asof = params
            .asof
            .as_deref()
            .map(TimeGraph::parse_time)
            .transpose()
            .map_err(EndpointError::InvalidAsof)?;
        let (parsed, options) = (query.clone(), self.options.clone());
        let outline = blocking_compute(move || QueryOutline::parse(&parsed, &options))
            .await
            .map_err(|error| EndpointError::Query(error.into()))?;
        let Accept(accept) = Accept::from_request_without_body(&req).await?;
        let (format, media_type) =
            SparqlFormat::from_media_types(outline.form, accept::ranges(&accept))
                .and_then(|(format, media_type)| {
                    Some((GqlSparqlFormat::naming(outline.form, format)?, media_type))
                })
                .ok_or(EndpointError::NotAcceptable(outline.form))?;

        let sparql = "sparql(query: $query, format: $format)";
        let (view, results) = match asof {
            Some(time) => (
                format!(
                    "before(time: {}) {{ valid {{ {sparql} }} }}",
                    time.saturating_add(1)
                ),
                "/data/rdfGraph/before/valid/sparql",
            ),
            None if outline.reads_time_graphs => (sparql.to_owned(), "/data/rdfGraph/sparql"),
            None => (
                format!("valid {{ {sparql} }}"),
                "/data/rdfGraph/valid/sparql",
            ),
        };
        let request = json!({
            "query": format!(
                "query SparqlEndpoint($path: String!, $query: String!, $format: SparqlFormat!) \
                 {{ rdfGraph(path: $path) {{ {view} }} }}"
            ),
            "variables": { "path": path, "query": query, "format": Value::from(&format) },
        });
        let mut graphql = Request::builder()
            .method(Method::POST)
            .content_type("application/json");
        if let Some(authorization) = req.headers().get(AUTHORIZATION) {
            graphql = graphql.header(AUTHORIZATION, authorization.clone());
        }
        let response = self
            .graphql
            .call(graphql.body(request.to_string()))
            .await?
            .into_response();
        let mut response: Json = response.into_body().into_json().await?;

        if let Some(errors) = response["errors"].as_array().filter(|e| !e.is_empty()) {
            // `rdfGraph` is the only root field, so no data means it failed (a missing graph or
            // one that cannot be loaded); an error below it leaves data
            let status = if response["data"].is_null() {
                StatusCode::NOT_FOUND
            } else if errors[0]["extensions"]["code"] == CODE_RESULTS_TOO_LARGE {
                StatusCode::PAYLOAD_TOO_LARGE
            } else {
                StatusCode::BAD_REQUEST
            };
            let messages = errors
                .iter()
                .filter_map(|error| error["message"].as_str())
                .join("\n");
            return Err(EndpointError::GraphQL(status, messages).into());
        }
        match response.pointer_mut(results).map(Json::take) {
            Some(Json::String(results)) => {
                Ok(Response::builder().content_type(media_type).body(results))
            }
            // `rdfGraph` is null: the caller cannot read the graph
            None => {
                let error = PathValidationError::GraphNotExistsError(path);
                Err(EndpointError::GraphNotFound(error).into())
            }
            // the query failed unexpectedly
            Some(_) => Ok(StatusCode::INTERNAL_SERVER_ERROR.into_response()),
        }
    }
}
