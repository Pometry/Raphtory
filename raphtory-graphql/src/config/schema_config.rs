use field_types::FieldName;
use serde::{Deserialize, Serialize};

pub const DEFAULT_DISABLE_INTROSPECTION: bool = false;

/// Controls GraphQL schema-level protections applied when the server builds its schema.
#[derive(Debug, Default, Deserialize, PartialEq, Clone, Serialize, FieldName)]
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

    /// When true, the browser UI is not served: non-POST requests to the GraphQL endpoint
    /// return 404, leaving only the API (POST) plus the `/health` and `/version` routes.
    pub disable_ui: bool,

    /// When true, the `rdfGraph` field is left out of the schema. This also turns off the SPARQL
    /// endpoint and SHACL, as both run on `rdfGraph`.
    #[cfg(feature = "rdf")]
    pub disable_rdf: bool,

    /// When true, the SPARQL endpoint `/sparql` is not served: requests to it return 404.
    #[cfg(feature = "rdf")]
    pub disable_sparql_endpoint: bool,

    /// When true, the `validateShacl` field of `rdfGraph` is left out of the schema.
    #[cfg(feature = "rdf")]
    pub disable_shacl: bool,
}
