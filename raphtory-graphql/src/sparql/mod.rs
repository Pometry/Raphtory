//! The SPARQL 1.1 Protocol endpoint at `/sparql/<graph path>`, and how the RDF fields of
//! `RdfGraph` run: with the server's limits, on the compute pool.
pub(crate) mod config;
pub(crate) mod endpoint;
pub(crate) mod format;
pub(crate) mod jobs;

use self::{
    config::{CODE_RESULTS_TOO_LARGE, CODE_TIMEOUT},
    jobs::RdfJobs,
};
use crate::{config::concurrency_config::ConcurrencyConfig, rayon::blocking_compute, GQLError};
use raphtory_rdf::{shacl::ShaclLimit, RdfError};
use thiserror::Error;
use tracing::error;

#[derive(Debug, Error)]
pub enum SparqlError {
    #[error("{}{}", .0, limit_setting(.0))]
    Query(#[from] RdfError),
    // oxigraph 0.5 expands RDF/XML DOCTYPE entities and aborts on deeply nested JSON-LD
    #[error("validateShacl reads shapes in TURTLE or N_TRIPLES only")]
    ShapesFormat,
}

impl SparqlError {
    /// The `extensions.code` of a limit the query hit, so a client can tell it from a bad query.
    pub(crate) fn code(&self) -> Option<&'static str> {
        match self {
            Self::Query(RdfError::ResultsTooLarge { .. }) => Some(CODE_RESULTS_TOO_LARGE),
            Self::Query(
                RdfError::Timeout { .. } | RdfError::ShaclLimit(ShaclLimit::TimedOut { .. }),
            ) => Some(CODE_TIMEOUT),
            Self::Query(_) | Self::ShapesFormat => None,
        }
    }
}

fn limit_setting(error: &RdfError) -> &'static str {
    match error {
        RdfError::QueryTooLong { .. } => " (`max_sparql_query_length`)",
        RdfError::TooManyPatterns { .. } => " (`max_sparql_triple_patterns`)",
        RdfError::ResultsTooLarge { .. } => " (`max_sparql_result_bytes`)",
        RdfError::Timeout { .. } => " (`max_sparql_query_ms`)",
        RdfError::ShaclLimit(ShaclLimit::ShapesTooLong { .. }) => " (`max_shacl_shapes_length`)",
        RdfError::ShaclLimit(ShaclLimit::TooManyResults { .. }) => " (`max_shacl_results`)",
        RdfError::ShaclLimit(ShaclLimit::TimedOut { .. }) => " (`max_shacl_validation_ms`)",
        _ => "",
    }
}

pub(crate) async fn run_limited<T: Send + 'static>(
    config: Option<&ConcurrencyConfig>,
    jobs: Option<&RdfJobs>,
    f: impl FnOnce(&ConcurrencyConfig) -> Result<T, RdfError> + Send + 'static,
) -> Result<Option<T>, GQLError> {
    let config = config.cloned().unwrap_or_default();
    let permit = RdfJobs::permit(jobs).await;
    // moved into the job: a dropped request must not free the slot while the job still runs
    match blocking_compute(move || {
        let _permit = permit;
        f(&config)
    })
    .await?
    {
        Ok(value) => Ok(Some(value)),
        // logged, not returned: the message can name data the caller may not read
        Err(RdfError::Panicked(message)) => {
            error!(error = %message, "SPARQL query panicked");
            Ok(None)
        }
        Err(error) => Err(SparqlError::from(error).into()),
    }
}
