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
use crate::{
    config::concurrency_config::ConcurrencyConfig, data::gql_error_with_code,
    rayon::blocking_compute,
};
use async_graphql::Error as GqlError;
use raphtory_rdf::{shacl::ShaclLimit, RdfError};
use thiserror::Error;
use tracing::error;

#[derive(Debug, Error)]
pub(crate) enum SparqlError {
    #[error("{}{}", .0, limit_setting(.0))]
    Query(#[from] RdfError),
}

impl SparqlError {
    fn into_gql_error(self) -> GqlError {
        match &self {
            Self::Query(RdfError::ResultsTooLarge { .. }) => {
                gql_error_with_code(self, CODE_RESULTS_TOO_LARGE)
            }
            Self::Query(
                RdfError::Timeout { .. } | RdfError::ShaclLimit(ShaclLimit::TimedOut { .. }),
            ) => gql_error_with_code(self, CODE_TIMEOUT),
            Self::Query(_) => self.into(),
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
) -> Result<Option<T>, GqlError> {
    let config = config.cloned().unwrap_or_default();
    let permit = RdfJobs::permit(jobs).await;
    // moved into the job: a dropped request must not free the slot while the job still runs
    match blocking_compute(move || {
        let _permit = permit;
        f(&config)
    })
    .await
    {
        Ok(value) => Ok(Some(value)),
        // logged, not returned: the message can name data the caller may not read
        Err(RdfError::Panicked(message)) => {
            error!(error = %message, "SPARQL query panicked");
            Ok(None)
        }
        Err(error) => Err(SparqlError::from(error).into_gql_error()),
    }
}
