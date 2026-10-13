//! The SPARQL 1.1 Protocol endpoint at `/sparql/<graph path>`, and how the RDF fields of
//! `RdfGraph` run: with the server's limits, on the compute pool.
mod accept;
pub(crate) mod endpoint;
#[cfg(test)]
mod tests;

use crate::{
    config::concurrency_config::ConcurrencyConfig, data::gql_error_with_code,
    rayon::blocking_compute,
};
use raphtory_rdf::{RdfError, SparqlOptions};
use thiserror::Error;

/// The `extensions.code` of an RDF field's error that is not the caller's fault; the SPARQL
/// endpoint answers it with 413 rather than 400.
pub(crate) const CODE_RESULTS_TOO_LARGE: &str = "RESULTS_TOO_LARGE";

/// Why an RDF field failed. The messages are the errors of the GraphQL fields.
#[derive(Debug, Error)]
pub(crate) enum SparqlError {
    #[error("{}{}", .0, limit_setting(.0))]
    Query(#[from] RdfError),
}

impl SparqlError {
    fn into_gql_error(self) -> async_graphql::Error {
        match &self {
            Self::Query(RdfError::ResultsTooLarge { .. }) => {
                gql_error_with_code(self, CODE_RESULTS_TOO_LARGE)
            }
            Self::Query(_) => self.into(),
        }
    }
}

/// The server setting that limits a query, for the message of `error`.
fn limit_setting(error: &RdfError) -> &'static str {
    match error {
        RdfError::QueryTooLong { .. } => " (`max_sparql_query_length`)",
        RdfError::TooManyPatterns { .. } => " (`max_sparql_triple_patterns`)",
        RdfError::ResultsTooLarge { .. } => " (`max_sparql_result_bytes`)",
        _ => "",
    }
}

/// The options of SPARQL queries on a server with `config`.
pub(crate) fn sparql_options(config: &ConcurrencyConfig) -> SparqlOptions {
    SparqlOptions::default()
        .with_max_query_length(config.max_sparql_query_length)
        .with_max_triple_patterns(config.max_sparql_triple_patterns)
        .with_max_result_bytes(config.max_sparql_result_bytes)
}

/// Runs `f` on the compute pool with the SPARQL options of `config` (the defaults if the server
/// sets none). A panic gives `None`.
pub(crate) async fn run_limited<T: Send + 'static>(
    config: Option<&ConcurrencyConfig>,
    f: impl FnOnce(&SparqlOptions) -> Result<T, RdfError> + Send + 'static,
) -> async_graphql::Result<Option<T>> {
    let options = sparql_options(&config.cloned().unwrap_or_default());
    match blocking_compute(move || f(&options)).await {
        Ok(value) => Ok(Some(value)),
        // its message can name data the caller may not read
        Err(RdfError::Panicked(_)) => Ok(None),
        Err(error) => Err(SparqlError::from(error).into_gql_error()),
    }
}
