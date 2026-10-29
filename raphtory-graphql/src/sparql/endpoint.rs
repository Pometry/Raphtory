//! The SPARQL 1.1 Protocol endpoint `/sparql/<graph path>`: it runs the query as the `sparql`
//! field of `rdfGraph(path:)` on the GraphQL endpoint, with the same permission checks and limits.
use crate::{
    auth::AuthenticatedGraphQL,
    paths::PathValidationError,
    rayon::blocking_compute,
    sparql::{config::CODE_RESULTS_TOO_LARGE, format::GqlSparqlFormat, SparqlError},
};
use async_graphql::{Executor, Value};
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
use std::sync::Arc;
use thiserror::Error;
use url::form_urlencoded;

const ALLOWED: &str = "GET, POST, OPTIONS";

const QUERY: &str = "query SparqlEndpoint($path: String!, $expr: FilterExpr, $query: String!, \
                     $format: SparqlFormat!) { rdfGraph(path: $path) { filter(expr: $expr) { \
                     sparql(query: $query, format: $format) } } }";

// poem's `Accept` is sorted best first
fn media_ranges(accept: &[Mime]) -> Vec<(&str, f32)> {
    if accept.is_empty() {
        return vec![("*/*", 1.0)];
    }
    accept
        .iter()
        .filter_map(|range| {
            let q = match range.get_param("q") {
                Some(q) => q
                    .as_str()
                    .parse::<f32>()
                    .ok()
                    .filter(|q| (0.0..=1.0).contains(q))?,
                None => 1.0,
            };
            Some((range.essence_str(), q))
        })
        .collect()
}

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
    #[error("the request has no `query`")]
    NoQuery,
    #[error("invalid `asof`: {0}")]
    InvalidAsof(ParseTimeError),
    #[error(transparent)]
    Query(SparqlError),
    #[error("the Accept header allows no format of {} results", .0.name())]
    NotAcceptable(QueryForm),
    #[error("{1}")]
    GraphQL(StatusCode, String),
    // also a graph the caller cannot read, so the two are indistinguishable
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
            | Self::NoQuery
            | Self::InvalidAsof(_)
            | Self::Query(_) => StatusCode::BAD_REQUEST,
            Self::GraphQL(status, _) => *status,
            Self::UnsupportedMediaType => StatusCode::UNSUPPORTED_MEDIA_TYPE,
            Self::NotAcceptable(_) => StatusCode::NOT_ACCEPTABLE,
            Self::GraphNotFound(_) => StatusCode::NOT_FOUND,
            Self::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
        }
    }
}

#[derive(Default)]
struct Params {
    query: Option<String>,
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
            "asof" => once(&mut self.asof, "asof", value)?,
            // such as the `format` and `output` of some clients
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

pub(crate) struct SparqlEndpoint<E> {
    graphql: Arc<AuthenticatedGraphQL<E>>,
    options: SparqlOptions,
}

impl<E> SparqlEndpoint<E> {
    pub(crate) fn new(graphql: Arc<AuthenticatedGraphQL<E>>, options: SparqlOptions) -> Self {
        Self { graphql, options }
    }
}

impl<E: Executor> Endpoint for SparqlEndpoint<E> {
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

impl<E: Executor> SparqlEndpoint<E> {
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
        let query = params.query.ok_or(EndpointError::NoQuery)?;
        let asof = params
            .asof
            .as_deref()
            .map(TimeGraph::parse_time)
            .transpose()
            .map_err(EndpointError::InvalidAsof)?;
        // parsed before the GraphQL endpoint authenticates, to choose the view and the format
        let (parsed, options) = (query.clone(), self.options.clone());
        let outline = blocking_compute(move || QueryOutline::parse(&parsed, &options))
            .await
            .map_err(|error| EndpointError::Query(error.into()))?;
        let Accept(accept) = Accept::from_request_without_body(&req).await?;
        let (format, media_type) =
            SparqlFormat::from_media_types(outline.form, media_ranges(&accept))
                .and_then(|(format, media_type)| {
                    Some((GqlSparqlFormat::naming(outline.form, format)?, media_type))
                })
                .ok_or(EndpointError::NotAcceptable(outline.form))?;

        // the current state, or the whole graph for a query reading time graphs (they hold theirs),
        // through the identity filter of a null `expr`
        let expr = match asof {
            Some(time) => {
                json!({ "view": [{ "before": time.saturating_add(1) }, { "kind": "VALID" }] })
            }
            None if outline.reads_time_graphs => Json::Null,
            None => json!({ "view": [{ "kind": "VALID" }] }),
        };
        let body = json!({
            "query": QUERY,
            "variables": {
                "path": path,
                "expr": expr,
                "query": query,
                "format": Value::from(&format),
            },
        });
        let mut graphql = Request::builder()
            .method(Method::POST)
            .content_type("application/json");
        if let Some(authorization) = req.headers().get(AUTHORIZATION) {
            graphql = graphql.header(AUTHORIZATION, authorization.clone());
        }
        let mut response: Json = self
            .graphql
            .call(graphql.body(body.to_string()))
            .await?
            .into_body()
            .into_json()
            .await?;

        if let Some(errors) = response["errors"]
            .as_array()
            .filter(|errors| !errors.is_empty())
        {
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
        match response
            .pointer_mut("/data/rdfGraph/filter/sparql")
            .map(Json::take)
        {
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
