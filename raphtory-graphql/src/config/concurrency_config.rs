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
/// Default time limit of a SPARQL query, in seconds (see `sparql_timeout`).
#[cfg(feature = "rdf")]
pub const DEFAULT_SPARQL_TIMEOUT: f64 = 30.0;
/// Default cap on the number of triple patterns of a SPARQL query (see
/// `max_sparql_triple_patterns`). Planning cannot be interrupted and its cost grows steeply with
/// the number of patterns; 100 keeps planning to about a second.
#[cfg(feature = "rdf")]
pub const DEFAULT_MAX_SPARQL_TRIPLE_PATTERNS: usize = 100;

/// Converts `sparql_timeout` seconds to a `Duration`; errors unless finite and at least 0.
/// Values too large for a `Duration` mean a limit that is never reached.
#[cfg(feature = "rdf")]
pub fn sparql_timeout_duration(seconds: f64) -> Result<std::time::Duration, String> {
    raphtory::rdf::timeout_from_secs(seconds).ok_or_else(|| {
        format!("invalid SPARQL timeout {seconds}: expected a finite number of seconds, at least 0")
    })
}

/// Controls how Raphtory schedules concurrent GraphQL work.
#[derive(Debug, Deserialize, PartialEq, Clone, Serialize, FieldName)]
pub struct ConcurrencyConfig {
    /// Restricts how many expensive graph traversal queries can execute simultaneously.
    /// Covers operations like connected components, edge traversals, and neighbour lookups
    /// (outComponent, inComponent, edges, outEdges, inEdges, neighbours, outNeighbours,
    /// inNeighbours). Once the limit is exceeded, queries are parked on a semaphore and
    /// wait until a slot becomes available before executing. `None` means unlimited.
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

    /// When true, completely disables bulk list endpoints (e.g. `list` on a collection, and
    /// SPARQL queries with feature `rdf`).
    /// Essential for large graphs where unbounded list queries could return billions of
    /// results and exhaust server resources. Clients should use `page` instead.
    pub disable_lists: bool,

    /// Maximum page size enforced on paged collection queries. Caps the `limit` argument
    /// of `page` so clients can't circumvent `disable_lists` by requesting huge pages.
    /// `None` means unlimited.
    pub max_page_size: Option<usize>,

    /// Maximum length in bytes of a SPARQL query, of the `sparql` field of `Graph` or the SPARQL
    /// endpoint (feature `rdf`). Longer queries are rejected before they are parsed.
    /// Defaults to `DEFAULT_MAX_SPARQL_QUERY_LENGTH`; `None` means unlimited.
    #[cfg(feature = "rdf")]
    pub max_sparql_query_length: Option<usize>,

    /// Time limit in seconds of a SPARQL query, of the `sparql` field of `Graph` or the SPARQL
    /// endpoint (feature `rdf`). A query that runs longer is stopped and returns an error.
    /// Client disconnects are not detected, so this is also what stops abandoned queries, which
    /// hold a compute thread and a `heavy_query_limit` slot until then. Defaults to
    /// `DEFAULT_SPARQL_TIMEOUT`; `None` means unlimited (keep a limit on a server shared with
    /// clients you do not control).
    #[cfg(feature = "rdf")]
    pub sparql_timeout: Option<f64>,

    /// Maximum number of triple patterns of a SPARQL query, of the `sparql` field of `Graph` or
    /// the SPARQL endpoint (feature `rdf`), counting those of collections, property paths (one
    /// per predicate), `BIND` and the expressions `(... AS ?v)` of `SELECT` and `GROUP BY` (one
    /// each) and `VALUES` (one per block, plus one per variable and per 100 rows).
    /// Larger queries are rejected before they are planned.
    /// Defaults to `DEFAULT_MAX_SPARQL_TRIPLE_PATTERNS`; `None` means unlimited.
    #[cfg(feature = "rdf")]
    pub max_sparql_triple_patterns: Option<usize>,

    /// Maximum graph loads decoding at once. Each in-flight load holds a whole graph in memory,
    /// so this bounds peak memory when many graphs are requested together. `None` = cores / 4,
    /// at least 2.
    pub max_concurrent_loads: Option<usize>,
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
            sparql_timeout: Some(DEFAULT_SPARQL_TIMEOUT),
            #[cfg(feature = "rdf")]
            max_sparql_triple_patterns: Some(DEFAULT_MAX_SPARQL_TRIPLE_PATTERNS),
            max_concurrent_loads: None,
        }
    }
}
