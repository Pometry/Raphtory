use field_types::FieldName;
use serde::{Deserialize, Serialize};

pub const DEFAULT_EXCLUSIVE_WRITES: bool = false;
pub const DEFAULT_DISABLE_BATCHING: bool = false;
pub const DEFAULT_DISABLE_LISTS: bool = false;
/// Default cap on the number of queries accepted in a single batched HTTP request.
/// Chosen to comfortably cover legitimate batching while preventing a single request
/// from amplifying its computational cost without bound (see `max_batch_size`).
pub const DEFAULT_MAX_BATCH_SIZE: usize = 10;
/// Default cap on the length of a SPARQL query, in bytes (see `max_sparql_query_length`).
#[cfg(feature = "rdf")]
pub const DEFAULT_MAX_SPARQL_QUERY_LENGTH: usize = 16 * 1024;
/// Default cap on the number of triple patterns of a SPARQL query (see
/// `max_sparql_triple_patterns`). The cost of planning grows steeply with the number of
/// patterns.
#[cfg(feature = "rdf")]
pub const DEFAULT_MAX_SPARQL_TRIPLE_PATTERNS: usize = 100;
/// Default cap on the size of SPARQL results, in bytes (see `max_sparql_result_bytes`).
#[cfg(feature = "rdf")]
pub const DEFAULT_MAX_SPARQL_RESULT_BYTES: usize = 256 << 20;
/// Default cap on the time a SPARQL query may run, in milliseconds (see `max_sparql_query_ms`).
#[cfg(feature = "rdf")]
pub const DEFAULT_MAX_SPARQL_QUERY_MS: u64 = 30_000;
/// Default cap on the length of the shapes of `validateShacl`, in bytes (see
/// `max_shacl_shapes_length`).
#[cfg(feature = "rdf")]
pub const DEFAULT_MAX_SHACL_SHAPES_LENGTH: usize = 100 << 10;
/// Default cap on the results of a SHACL validation (see `max_shacl_results`).
#[cfg(feature = "rdf")]
pub const DEFAULT_MAX_SHACL_RESULTS: usize = 10_000;
/// Default cap on the time a SHACL validation may run, in milliseconds (see
/// `max_shacl_validation_ms`).
#[cfg(feature = "rdf")]
pub const DEFAULT_MAX_SHACL_VALIDATION_MS: u64 = 30_000;

/// Controls how Raphtory schedules concurrent GraphQL work.
#[derive(Debug, Deserialize, PartialEq, Clone, Serialize, FieldName)]
pub struct ConcurrencyConfig {
    /// Restricts how many expensive graph traversal queries can execute simultaneously.
    /// Covers operations like connected components, edge traversals, and neighbour lookups
    /// (outComponent, inComponent, edges, outEdges, inEdges, neighbours, outNeighbours,
    /// inNeighbours). Once the limit is exceeded, queries are parked on a semaphore and
    /// wait until a slot becomes available before executing. `None` means unlimited.
    /// At most this many SPARQL queries and SHACL validations run at once too, however many
    /// fields or requests ask for them.
    pub heavy_query_limit: Option<usize>,

    /// Ensures only one ingestion/write operation runs at a time and blocks reads until
    /// it completes.
    pub exclusive_writes: bool,

    /// When true, query batching (sending multiple queries in a single HTTP request) is
    /// rejected outright. Batching can otherwise be used to circumvent per-request depth
    /// and complexity limits.
    pub disable_batching: bool,

    /// Caps the number of queries accepted in a single batched HTTP request. Requests
    /// whose batch exceeds this size are rejected. Defaults to `DEFAULT_MAX_BATCH_SIZE`
    /// so deployments are bounded out-of-the-box; set to `None` for unlimited (subject
    /// to `disable_batching`).
    pub max_batch_size: Option<usize>,

    /// When true, completely disables bulk list endpoints (e.g. `list` on a collection).
    /// Essential for large graphs where unbounded list queries could return billions of
    /// results and exhaust server resources. Clients should use `page` instead.
    pub disable_lists: bool,

    /// Maximum page size enforced on paged collection queries. Caps the `limit` argument
    /// of `page` so clients can't circumvent `disable_lists` by requesting huge pages.
    /// `None` means unlimited.
    pub max_page_size: Option<usize>,

    /// Maximum length in bytes of a SPARQL query, of the `sparql` field of `RdfGraph` or the
    /// SPARQL endpoint. Longer queries are rejected before they are parsed.
    /// Defaults to `DEFAULT_MAX_SPARQL_QUERY_LENGTH`; `None` means unlimited.
    #[cfg(feature = "rdf")]
    pub max_sparql_query_length: Option<usize>,

    /// Maximum number of triple patterns of a SPARQL query, counted as
    /// `SparqlOptions::with_max_triple_patterns` counts them; larger queries are rejected before
    /// planning. Defaults to `DEFAULT_MAX_SPARQL_TRIPLE_PATTERNS`; `None` means unlimited.
    #[cfg(feature = "rdf")]
    pub max_sparql_triple_patterns: Option<usize>,

    /// Maximum size in bytes of the serialized results of a SPARQL query, of the `sparql` field
    /// of `RdfGraph` or the SPARQL endpoint. Larger results fail. Defaults to
    /// `DEFAULT_MAX_SPARQL_RESULT_BYTES`; `None` means unlimited.
    #[cfg(feature = "rdf")]
    pub max_sparql_result_bytes: Option<usize>,

    /// Maximum time in milliseconds a SPARQL query, of the `sparql` field of `RdfGraph` or the
    /// SPARQL endpoint, may run; a longer one fails, and the endpoint answers 503. The timeout is
    /// checked while the query is evaluated, not while it is parsed or planned. Defaults to
    /// `DEFAULT_MAX_SPARQL_QUERY_MS`; `None` means unlimited.
    #[cfg(feature = "rdf")]
    pub max_sparql_query_ms: Option<u64>,

    /// Maximum length in bytes of the shapes of `validateShacl`. Longer shapes are rejected
    /// before they are parsed. Defaults to `DEFAULT_MAX_SHACL_SHAPES_LENGTH`; `None` means
    /// unlimited.
    #[cfg(feature = "rdf")]
    pub max_shacl_shapes_length: Option<usize>,

    /// Maximum number of results of a `validateShacl` report, split evenly across the shapes
    /// with targets; more fail. Defaults to `DEFAULT_MAX_SHACL_RESULTS`; `None` means unlimited.
    #[cfg(feature = "rdf")]
    pub max_shacl_results: Option<usize>,

    /// Maximum time in milliseconds a `validateShacl` validation may run; a longer one fails.
    /// Defaults to `DEFAULT_MAX_SHACL_VALIDATION_MS`; `None` means unlimited.
    #[cfg(feature = "rdf")]
    pub max_shacl_validation_ms: Option<u64>,

    /// Maximum graph loads decoding at once. Each in-flight load holds a whole graph in memory,
    /// so this bounds peak memory when many graphs are requested together. `None` = cores / 4,
    /// at least 2.
    pub max_concurrent_loads: Option<usize>,

    /// Threads that cache filtered graph views in the background. `None` = 4 with more than 6
    /// cores, otherwise cores / 2.
    pub caching_threads: Option<usize>,

    /// Maximum compute jobs (query resolvers, algorithms, ...) on the compute pool at once; more
    /// wait for a slot. Bounds how deep jobs can nest on a compute thread's stack, which with
    /// enough concurrent queries otherwise overflows it, so the default stack size grows with
    /// it (see `compute_stack_size`). `None` = 8 times the compute threads. Must be at least 1.
    pub max_concurrent_compute: Option<usize>,

    /// Stack size in bytes of each compute thread. It must fit `max_concurrent_compute` nested
    /// jobs; it is address space reserved per thread, and only the pages a thread touches use
    /// memory. `None` = 256KB per concurrent compute job, at least 64MB.
    pub compute_stack_size: Option<usize>,
}

impl Default for ConcurrencyConfig {
    fn default() -> Self {
        Self {
            heavy_query_limit: None,
            exclusive_writes: DEFAULT_EXCLUSIVE_WRITES,
            disable_batching: DEFAULT_DISABLE_BATCHING,
            max_batch_size: Some(DEFAULT_MAX_BATCH_SIZE),
            disable_lists: DEFAULT_DISABLE_LISTS,
            max_page_size: None,
            #[cfg(feature = "rdf")]
            max_sparql_query_length: Some(DEFAULT_MAX_SPARQL_QUERY_LENGTH),
            #[cfg(feature = "rdf")]
            max_sparql_triple_patterns: Some(DEFAULT_MAX_SPARQL_TRIPLE_PATTERNS),
            #[cfg(feature = "rdf")]
            max_sparql_result_bytes: Some(DEFAULT_MAX_SPARQL_RESULT_BYTES),
            #[cfg(feature = "rdf")]
            max_sparql_query_ms: Some(DEFAULT_MAX_SPARQL_QUERY_MS),
            #[cfg(feature = "rdf")]
            max_shacl_shapes_length: Some(DEFAULT_MAX_SHACL_SHAPES_LENGTH),
            #[cfg(feature = "rdf")]
            max_shacl_results: Some(DEFAULT_MAX_SHACL_RESULTS),
            #[cfg(feature = "rdf")]
            max_shacl_validation_ms: Some(DEFAULT_MAX_SHACL_VALIDATION_MS),
            max_concurrent_loads: None,
            caching_threads: None,
            max_concurrent_compute: None,
            compute_stack_size: None,
        }
    }
}
