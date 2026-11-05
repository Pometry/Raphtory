use crate::config::concurrency_config::ConcurrencyConfig;
use raphtory_rdf::{shacl::ShaclOptions, SparqlOptions};
use std::time::Duration;

// not the caller's fault: the SPARQL endpoint answers it with 413 rather than 400
pub(crate) const CODE_RESULTS_TOO_LARGE: &str = "RESULTS_TOO_LARGE";
// answered with 503 as Fuseki does; 408 is about the connection, not the query
pub(crate) const CODE_TIMEOUT: &str = "TIMEOUT";

pub(crate) fn sparql_options(config: &ConcurrencyConfig) -> SparqlOptions {
    SparqlOptions::default()
        .with_max_query_length(config.max_sparql_query_length)
        .with_max_triple_patterns(config.max_sparql_triple_patterns)
        .with_max_result_bytes(config.max_sparql_result_bytes)
        .with_timeout(config.max_sparql_query_ms.map(Duration::from_millis))
}

pub(crate) fn shacl_options(config: &ConcurrencyConfig) -> ShaclOptions {
    ShaclOptions::default()
        .with_reject_recursive_shapes(true)
        .with_max_shapes_length(config.max_shacl_shapes_length)
        .with_max_results(config.max_shacl_results)
        .with_timeout(config.max_shacl_validation_ms.map(Duration::from_millis))
}
