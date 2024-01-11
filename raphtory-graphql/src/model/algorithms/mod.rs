use async_graphql::dynamic::Object;
use dynamic_graphql::internal::Registry;

pub mod algorithm;
pub mod algorithm_entry_point;
pub mod document;
pub mod graph_algorithms;
pub mod similarity_search;
pub mod vector_algorithms;

type RegisterFunction = fn(&str, Registry, Object) -> (Registry, Object);
