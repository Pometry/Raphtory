use async_graphql::dynamic::Object;
use dynamic_graphql::internal::Registry;

pub mod document;
mod global_search;
pub mod graph_algorithms;
pub mod mutation;
pub mod mutation_entry_point;
pub mod mutation_plugins;
pub mod query;
pub mod query_entry_point;
pub mod query_plugins;
pub mod similarity_search;
pub mod vector_algorithms;

type RegisterFunction = Box<dyn FnOnce(&str, Registry, Object) -> (Registry, Object) + Send>;
