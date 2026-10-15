use crate::config::concurrency_config::ConcurrencyConfig;
use raphtory_rdf::SparqlOptions;

// not the caller's fault: the SPARQL endpoint answers it with 413 rather than 400
pub(crate) const CODE_RESULTS_TOO_LARGE: &str = "RESULTS_TOO_LARGE";

pub(crate) fn sparql_options(config: &ConcurrencyConfig) -> SparqlOptions {
    SparqlOptions::default()
        .with_max_query_length(config.max_sparql_query_length)
        .with_max_triple_patterns(config.max_sparql_triple_patterns)
        .with_max_result_bytes(config.max_sparql_result_bytes)
}
