//! SPARQL queries over graph views (feature `rdf`), shared by the `sparql` field of `Graph` and
//! the SPARQL 1.1 Protocol endpoint at `/sparql/<graph path>`.
mod accept;
pub(crate) mod endpoint;
#[cfg(test)]
mod tests;

use crate::{
    config::concurrency_config::{
        sparql_timeout_duration, ConcurrencyConfig, DEFAULT_MAX_SPARQL_QUERY_LENGTH,
        DEFAULT_MAX_SPARQL_TRIPLE_PATTERNS, DEFAULT_SPARQL_TIMEOUT,
    },
    data::Data,
    model::graph::graph::GqlGraph,
    paths::ValidGraphPaths,
    rayon::blocking_compute,
};
use async_graphql::Context;
use raphtory::{
    db::api::view::DynamicGraph,
    errors::GraphError,
    rdf::{sparql_stack_size, CancellationToken, RdfError, SparqlDataset, SparqlOptions},
};
use std::{fmt, thread};

/// Why a SPARQL query failed. The messages are the errors of the GraphQL `sparql` field.
#[derive(Debug)]
pub(crate) enum SparqlError {
    /// The server sets `disable_lists`.
    Disabled,
    /// The query is longer than `max_sparql_query_length`.
    TooLong { length: usize, max: usize },
    /// The server's `sparql_timeout` is invalid.
    InvalidTimeout(String),
    /// Parsing, evaluating or writing the query failed.
    Query(GraphError),
    /// The client accepts no format for the results of this form of query (`SELECT`, ...).
    NotAcceptable { form: &'static str },
    /// RDF/XML, the only RDF format the client accepts, cannot hold `skipped` of the `triples`
    /// triples of the results.
    RdfXmlSkipped { skipped: usize, triples: usize },
    /// The query could not run.
    Internal(String),
}

impl fmt::Display for SparqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Disabled => write!(
                f,
                "SPARQL queries are disabled on this server (`disable_lists`)"
            ),
            Self::TooLong { length, max } => write!(
                f,
                "SPARQL query too long: {length} bytes, the limit is {max} bytes \
                 (`max_sparql_query_length`)"
            ),
            Self::InvalidTimeout(error) => write!(f, "{error} (`sparql_timeout`)"),
            // name the setting that limits the query
            Self::Query(error @ GraphError::Rdf(RdfError::Timeout { .. })) => {
                write!(f, "{error} (`sparql_timeout`)")
            }
            Self::Query(error @ GraphError::Rdf(RdfError::TooManyPatterns { .. })) => {
                write!(f, "{error} (`max_sparql_triple_patterns`)")
            }
            Self::Query(error) => write!(f, "{error}"),
            Self::NotAcceptable { form } => write!(
                f,
                "none of the formats the Accept header allows can hold {form} results"
            ),
            Self::RdfXmlSkipped { skipped, triples } => write!(
                f,
                "RDF/XML cannot hold {skipped} of the {triples} triples of the results (such as \
                 triples whose predicate does not end in an XML name); accept another RDF format: \
                 text/turtle, application/n-triples, application/ld+json, application/n-quads or \
                 application/trig"
            ),
            Self::Internal(error) => write!(f, "{error}"),
        }
    }
}

impl From<GraphError> for SparqlError {
    fn from(error: GraphError) -> Self {
        Self::Query(error)
    }
}

/// Fails if the server disables SPARQL queries or `query` is too long.
pub(crate) fn check_query(
    config: Option<&ConcurrencyConfig>,
    query: &str,
) -> Result<(), SparqlError> {
    // `SELECT * { ?s ?p ?o }` lists every edge, so SPARQL is a bulk list endpoint.
    if config.is_some_and(|config| config.disable_lists) {
        return Err(SparqlError::Disabled);
    }
    let max_length = config.map_or(Some(DEFAULT_MAX_SPARQL_QUERY_LENGTH), |config| {
        config.max_sparql_query_length
    });
    match max_length.filter(|&max| query.len() > max) {
        Some(max) => Err(SparqlError::TooLong {
            length: query.len(),
            max,
        }),
        None => Ok(()),
    }
}

/// Runs `query` on the view of `graph` as the caller of `ctx` may: within the server's limits,
/// with the temporal functions only if the caller's read has no row filter, and on a thread
/// with a stack sized to the query. `write` evaluates the query and serializes its results.
pub(crate) async fn run_sparql<T: Send + 'static>(
    ctx: &Context<'_>,
    graph: &GqlGraph,
    query: String,
    dataset: Option<SparqlDataset>,
    write: impl FnOnce(&DynamicGraph, &str, &SparqlOptions) -> Result<T, SparqlError> + Send + 'static,
) -> Result<T, SparqlError> {
    let config = ctx.data_opt::<ConcurrencyConfig>();
    check_query(config, &query)?;
    // The temporal functions read unfiltered history, which a row filter must hide.
    let data = ctx.data_unchecked::<Data>();
    let temporal_functions = !data
        .read_has_row_filter(ctx, graph.folder().local_path())
        .await;
    let timeout = config
        .map_or(Some(DEFAULT_SPARQL_TIMEOUT), |config| config.sparql_timeout)
        .map(sparql_timeout_duration)
        .transpose()
        .map_err(SparqlError::InvalidTimeout)?;
    let max_patterns = config.map_or(Some(DEFAULT_MAX_SPARQL_TRIPLE_PATTERNS), |config| {
        config.max_sparql_triple_patterns
    });
    let token = CancellationToken::new();
    // held until the query is done, or dropped with the request
    let _cancel_on_drop = CancelOnDrop(token.clone());
    let view = graph.graph().clone();
    let options = SparqlOptions::default()
        .with_temporal_functions(temporal_functions)
        .with_timeout(timeout)
        .with_max_triple_patterns(max_patterns)
        .with_cancellation_token(token)
        .with_dataset(dataset);
    blocking_compute(move || {
        // Own thread with a stack sized to the query: a stack overflow would abort the server.
        thread::scope(|scope| {
            thread::Builder::new()
                .name("RAP-sparql".to_owned())
                .stack_size(sparql_stack_size(query.len()))
                .spawn_scoped(scope, || {
                    #[cfg(test)]
                    let _running = Running::new(&query);
                    write(&view, &query, &options)
                })
                .map_err(|error| {
                    SparqlError::Internal(format!("cannot run the SPARQL query: {error}"))
                })?
                .join()
                .unwrap_or_else(|_| {
                    Err(SparqlError::Internal(
                        "the SPARQL query failed unexpectedly".to_owned(),
                    ))
                })
        })
    })
    .await
}

/// Cancels the query when the request future is dropped (e.g. on server shutdown). Client
/// disconnects do not drop it; those queries run until `sparql_timeout`.
struct CancelOnDrop(CancellationToken);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

/// The SPARQL queries running (tests check when a query stops).
#[cfg(test)]
static RUNNING: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());

/// Lists a running query in [`RUNNING`] while it lives.
#[cfg(test)]
pub(crate) struct Running(String);

#[cfg(test)]
impl Running {
    fn new(query: &str) -> Self {
        RUNNING.lock().unwrap().push(query.to_owned());
        Running(query.to_owned())
    }

    /// How many times `query` is running.
    pub(crate) fn count(query: &str) -> usize {
        RUNNING
            .lock()
            .unwrap()
            .iter()
            .filter(|q| *q == query)
            .count()
    }
}

#[cfg(test)]
impl Drop for Running {
    fn drop(&mut self) {
        let mut running = RUNNING.lock().unwrap();
        if let Some(i) = running.iter().position(|q| *q == self.0) {
            running.remove(i);
        }
    }
}
