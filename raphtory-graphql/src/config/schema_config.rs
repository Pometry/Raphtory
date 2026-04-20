use serde::{Deserialize, Serialize};

pub const DEFAULT_DISABLE_INTROSPECTION: bool = false;

/// Controls GraphQL schema-level protections applied when the server builds its schema.
#[derive(Debug, Default, Deserialize, PartialEq, Clone, Serialize)]
pub struct SchemaConfig {
    /// Limits how deeply nested a query can be. For example, a query like
    /// graph → nodes → page → edges → page → destination → edges → page → destination
    /// would have a depth of 9. `None` means unlimited.
    pub max_query_depth: Option<usize>,

    /// Limits the total estimated cost of a query based on the number of fields selected.
    /// Blocks queries that try to fetch too much data in one request. `None` means unlimited.
    pub max_query_complexity: Option<usize>,

    /// Internal safety limit to prevent stack overflows from pathologically structured
    /// queries. `None` falls back to the async-graphql default of 32.
    pub max_recursive_depth: Option<usize>,

    /// Limits the number of GraphQL directives on any single field. Directives are
    /// annotations prefixed with @ that modify how a field is executed (e.g. @skip,
    /// @include, @deprecated). This prevents directive-based abuse. `None` means unlimited.
    pub max_directives_per_field: Option<usize>,

    /// When true, schema introspection is fully disabled, preventing clients from
    /// discovering the API's structure and available fields. Recommended for production
    /// to reduce the attack surface.
    pub disable_introspection: bool,
}
