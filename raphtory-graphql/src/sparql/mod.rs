//! SPARQL queries over RDF graph views (feature `rdf`), shared by the `sparql` field of
//! `RdfGraph` and the SPARQL 1.1 Protocol endpoint at `/sparql/<graph path>`, and the limits of
//! the server that the RDF fields of `RdfGraph` run within.
mod accept;
pub(crate) mod endpoint;
#[cfg(test)]
mod tests;

use crate::{config::concurrency_config::ConcurrencyConfig, rayon::blocking_compute};
use raphtory::{
    errors::GraphError,
    rdf::{CancellationToken, QueryForm, RdfError, RdfView, SparqlDataset, SparqlOptions},
};
#[cfg(test)]
use std::sync::Mutex;
use std::{
    fmt,
    panic::{catch_unwind, AssertUnwindSafe},
};
use tracing::error;

/// Why a SPARQL query or an RDF export failed. The messages are the errors of the GraphQL
/// fields.
#[derive(Debug)]
pub(crate) enum SparqlError {
    /// The server sets `disable_lists`.
    Disabled,
    /// The query is longer than `max_sparql_query_length`.
    TooLong { length: usize, max: usize },
    /// Parsing, evaluating or writing the query failed.
    Query(GraphError),
    /// The client accepts no format for the results of this form of query.
    NotAcceptable { form: QueryForm },
    /// RDF/XML, the only RDF format the client accepts, cannot hold `skipped` of the `triples`
    /// triples of the results.
    RdfXmlSkipped { skipped: usize, triples: usize },
    /// The shapes of `validateShacl` are recursive, which a reader could use to overflow the
    /// validator's stack and abort the server.
    #[cfg(feature = "shacl")]
    RecursiveShapes,
    /// The query could not run.
    Internal(String),
}

impl fmt::Display for SparqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Disabled => write!(
                f,
                "SPARQL queries, RDF exports and SHACL validation are disabled on this server \
                 (`disable_lists`)"
            ),
            Self::TooLong { length, max } => write!(
                f,
                "SPARQL query too long: {length} bytes, the limit is {max} bytes \
                 (`max_sparql_query_length`)"
            ),
            // name the setting that limits the query
            Self::Query(
                error @ GraphError::Rdf(RdfError::Timeout { .. } | RdfError::ExportTimeout { .. }),
            ) => {
                write!(f, "{error} (`sparql_timeout`)")
            }
            #[cfg(feature = "shacl")]
            Self::Query(error @ GraphError::Rdf(RdfError::ShaclTimeout { .. })) => {
                write!(f, "{error} (`sparql_timeout`)")
            }
            Self::Query(error @ GraphError::Rdf(RdfError::TooManyPatterns { .. })) => {
                write!(f, "{error} (`max_sparql_triple_patterns`)")
            }
            Self::Query(error) => write!(f, "{error}"),
            Self::NotAcceptable { form } => write!(
                f,
                "none of the formats the Accept header allows can hold {} results",
                form.name()
            ),
            Self::RdfXmlSkipped { skipped, triples } => write!(
                f,
                "RDF/XML cannot hold {skipped} of the {triples} triples of the results (such as \
                 triples whose predicate does not end in an XML name); accept another RDF format: \
                 text/turtle, application/n-triples, application/ld+json, application/n-quads or \
                 application/trig"
            ),
            #[cfg(feature = "shacl")]
            Self::RecursiveShapes => write!(
                f,
                "recursive SHACL shapes (a shape that refers to itself) cannot be validated on \
                 this server"
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

/// Fails if the server disables SPARQL queries, RDF exports and SHACL validation.
pub(crate) fn check_enabled(config: Option<&ConcurrencyConfig>) -> Result<(), SparqlError> {
    // `SELECT * { ?s ?p ?o }` lists every edge, so SPARQL is a bulk list endpoint.
    if config.is_some_and(|config| config.disable_lists) {
        return Err(SparqlError::Disabled);
    }
    Ok(())
}

/// Fails if `query` is longer than `max_sparql_query_length`.
pub(crate) fn check_length(config: &ConcurrencyConfig, query: &str) -> Result<(), SparqlError> {
    match config
        .max_sparql_query_length
        .filter(|&max| query.len() > max)
    {
        Some(max) => Err(SparqlError::TooLong {
            length: query.len(),
            max,
        }),
        None => Ok(()),
    }
}

/// Runs `query` on `view` within the server's limits of `config`. `write` evaluates the query
/// and serializes its results.
pub(crate) async fn run_sparql<T: Send + 'static>(
    config: Option<&ConcurrencyConfig>,
    view: RdfView,
    temporal_functions: bool,
    query: String,
    dataset: Option<SparqlDataset>,
    write: impl FnOnce(&RdfView, &str, &SparqlOptions) -> Result<T, SparqlError> + Send + 'static,
) -> Result<T, SparqlError> {
    run_limited(config, move |config, token| {
        check_length(config, &query)?;
        let options = SparqlOptions::default()
            .with_temporal_functions(temporal_functions)
            .with_timeout(config.sparql_timeout)
            .with_max_triple_patterns(config.max_sparql_triple_patterns)
            .with_cancellation_token(token)
            .with_dataset(dataset);
        #[cfg(test)]
        let _running = Running::new(&query);
        write(&view, &query, &options)
    })
    .await
}

/// Runs `f` on the compute pool with the limits of `config` (the defaults if the server sets
/// none) and a token that is cancelled when the request is dropped, unless the server sets
/// `disable_lists`.
pub(crate) async fn run_limited<T: Send + 'static>(
    config: Option<&ConcurrencyConfig>,
    f: impl FnOnce(&ConcurrencyConfig, CancellationToken) -> Result<T, SparqlError> + Send + 'static,
) -> Result<T, SparqlError> {
    check_enabled(config)?;
    let config = config.cloned().unwrap_or_default();
    let token = CancellationToken::new();
    // held until `f` is done, or dropped with the request
    let _cancel_on_drop = CancelOnDrop(token.clone());
    compute(move || f(&config, token)).await
}

/// Runs `f` on the compute pool.
async fn compute<T: Send + 'static>(
    f: impl FnOnce() -> Result<T, SparqlError> + Send + 'static,
) -> Result<T, SparqlError> {
    blocking_compute(move || {
        // a panic on the compute pool would abort the server
        catch_unwind(AssertUnwindSafe(f)).unwrap_or_else(|panic| {
            let message = panic
                .downcast_ref::<&str>()
                .copied()
                .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
                .unwrap_or("unknown panic");
            error!("RDF operation panicked: {message}");
            Err(SparqlError::Internal(
                "the RDF operation failed unexpectedly".to_owned(),
            ))
        })
    })
    .await
}

/// Cancels the work when the request future is dropped (e.g. on server shutdown). Client
/// disconnects do not drop it; that work runs until `sparql_timeout`.
struct CancelOnDrop(CancellationToken);

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

/// The SPARQL queries running (tests check when a query stops).
#[cfg(test)]
static RUNNING: Mutex<Vec<String>> = Mutex::new(Vec::new());

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
