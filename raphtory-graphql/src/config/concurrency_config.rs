use serde::{Deserialize, Serialize};

pub const DEFAULT_EXCLUSIVE_WRITES: bool = false;
pub const DEFAULT_DISABLE_BATCHING: bool = false;

/// Controls how Raphtory schedules concurrent GraphQL work.
#[derive(Debug, Default, Deserialize, PartialEq, Clone, Serialize)]
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
    /// whose batch exceeds this size are rejected. `None` means unlimited (subject to
    /// `disable_batching`).
    pub max_batch_size: Option<usize>,
}
