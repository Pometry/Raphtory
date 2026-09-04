use crate::rayon::default_express_threads;
use field_types::FieldName;
use serde::{Deserialize, Serialize};

pub const DEFAULT_EXCLUSIVE_WRITES: bool = false;
pub const DEFAULT_DISABLE_BATCHING: bool = false;
pub const DEFAULT_DISABLE_LISTS: bool = false;
/// Default cap on the number of queries accepted in a single batched HTTP request.
/// Chosen to comfortably cover legitimate batching while preventing a single request
/// from amplifying its computational cost without bound (see `max_batch_size`).
pub const DEFAULT_MAX_BATCH_SIZE: usize = 10;

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

    /// When true, completely disables bulk list endpoints (e.g. `list` on a collection).
    /// Essential for large graphs where unbounded list queries could return billions of
    /// results and exhaust server resources. Clients should use `page` instead.
    pub disable_lists: bool,

    /// Maximum page size enforced on paged collection queries. Caps the `limit` argument
    /// of `page` so clients can't circumvent `disable_lists` by requesting huge pages.
    /// `None` means unlimited.
    pub max_page_size: Option<usize>,

    /// Threads reserved for the express pool (health checks and cheap resolvers), taken out of
    /// the compute pool so they stay responsive while heavy queries saturate it.
    pub express_threads: usize,

    /// Max query closures running on the compute pool at once; the rest queue. Prevents heavy
    /// queries time-sharing every thread, so slots free up quickly. `None` = half the compute
    /// threads: raising it amplifies storage-lock contention faster than it adds parallelism.
    pub max_concurrent_queries: Option<usize>,

    /// Dispatch queued queries newest-first, so a fresh short query jumps a backlog of heavy ones
    /// instead of waiting for it to drain. Old waiters are periodically dispatched first so a
    /// sustained backlog cannot starve them.
    pub newest_first_scheduling: bool,

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
            express_threads: default_express_threads(),
            max_concurrent_queries: None,
            newest_first_scheduling: true,
            max_concurrent_loads: None,
        }
    }
}
