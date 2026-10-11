//! SPARQL queries over RDF graph views (feature `rdf`), shared by the `sparql` field of
//! `RdfGraph` and the SPARQL 1.1 Protocol endpoint at `/sparql/<graph path>`, and the limits of
//! the server that the RDF fields of `RdfGraph` run within.
mod accept;
pub(crate) mod endpoint;
#[cfg(test)]
mod tests;

use crate::{
    config::concurrency_config::ConcurrencyConfig, model::graph::rdf_graph::GqlRdfGraph,
    rayon::blocking_compute,
};
use raphtory_rdf::{QueryForm, RdfError, RdfView, SparqlDataset, SparqlOptions};
use std::{
    io::{self, Write},
    panic::{catch_unwind, AssertUnwindSafe},
};
use thiserror::Error;
use tracing::error;

/// Why a SPARQL query or an RDF export failed. The messages are the errors of the GraphQL
/// fields.
#[derive(Debug, Error)]
pub(crate) enum SparqlError {
    /// The server sets `disable_lists`.
    #[error(
        "SPARQL queries, RDF exports and SHACL validation are disabled on this server \
         (`disable_lists`)"
    )]
    Disabled,
    /// The query is longer than `max_sparql_query_length`.
    #[error(
        "SPARQL query too long: {length} bytes, the limit is {max} bytes \
         (`max_sparql_query_length`)"
    )]
    TooLong { length: usize, max: usize },
    /// Parsing, evaluating or writing the query failed.
    #[error("{}{}", .0, limit_setting(.0))]
    Query(#[from] RdfError),
    /// The client accepts no format for the results of this form of query.
    #[error("none of the formats the Accept header allows can hold {} results", .form.name())]
    NotAcceptable { form: QueryForm },
    /// The results are larger than `max_sparql_result_bytes`.
    #[error("SPARQL results larger than {max} bytes (`max_sparql_result_bytes`)")]
    TooLarge { max: usize },
    /// The shapes of `validateShacl` are recursive, which a reader could use to overflow the
    /// validator's stack and abort the server.
    #[cfg(feature = "shacl")]
    #[error(
        "recursive SHACL shapes (a shape that refers to itself) cannot be validated on this \
         server"
    )]
    RecursiveShapes,
    /// The operation panicked; the panic is logged.
    #[error("the RDF operation failed unexpectedly")]
    Panicked,
}

/// The server setting that limits a query, for the message of `error`.
fn limit_setting(error: &RdfError) -> &'static str {
    match error {
        RdfError::TooManyPatterns { .. } => " (`max_sparql_triple_patterns`)",
        _ => "",
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

/// A writer that fails once more than `max` bytes would be written to it.
pub(crate) struct Capped<W> {
    inner: W,
    max: Option<usize>,
    written: usize,
    exceeded: bool,
}

impl<W: Write> Write for Capped<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.max.is_some_and(|max| self.written + buf.len() > max) {
            self.exceeded = true;
            return Err(io::Error::other("the output is larger than its limit"));
        }
        let written = self.inner.write(buf)?;
        self.written += written;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// Writes with `write` into a buffer of at most `max_sparql_result_bytes` of `config`.
pub(crate) fn write_capped<T>(
    config: &ConcurrencyConfig,
    write: impl FnOnce(&mut Capped<Vec<u8>>) -> Result<T, RdfError>,
) -> Result<Vec<u8>, SparqlError> {
    let mut out = Capped {
        inner: Vec::new(),
        max: config.max_sparql_result_bytes,
        written: 0,
        exceeded: false,
    };
    let result = write(&mut out);
    match out.max {
        Some(max) if out.exceeded => Err(SparqlError::TooLarge { max }),
        _ => Ok(result.map(|_| out.inner)?),
    }
}

/// Runs `query` on `graph` within the server's limits of `config`. `write` evaluates the query
/// and serializes its results, within the limits of the config it is given.
pub(crate) async fn run_sparql<T: Send + 'static>(
    config: Option<&ConcurrencyConfig>,
    graph: &GqlRdfGraph,
    query: String,
    dataset: Option<SparqlDataset>,
    write: impl FnOnce(&RdfView, &str, &SparqlOptions, &ConcurrencyConfig) -> Result<T, SparqlError>
        + Send
        + 'static,
) -> Result<T, SparqlError> {
    let view = graph.view()?;
    let temporal_functions = graph.temporal_functions();
    run_limited(config, move |config| {
        check_length(config, &query)?;
        let options = SparqlOptions::default()
            .with_temporal_functions(temporal_functions)
            .with_max_triple_patterns(config.max_sparql_triple_patterns)
            .with_dataset(dataset);
        write(&view, &query, &options, config)
    })
    .await
}

/// Runs `f` on the compute pool with the limits of `config` (the defaults if the server sets
/// none), unless the server sets `disable_lists`.
pub(crate) async fn run_limited<T: Send + 'static>(
    config: Option<&ConcurrencyConfig>,
    f: impl FnOnce(&ConcurrencyConfig) -> Result<T, SparqlError> + Send + 'static,
) -> Result<T, SparqlError> {
    check_enabled(config)?;
    let config = config.cloned().unwrap_or_default();
    compute(move || f(&config)).await
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
            Err(SparqlError::Panicked)
        })
    })
    .await
}
