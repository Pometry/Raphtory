use async_graphql::dynamic::Object;
use dynamic_graphql::internal::Registry;

pub mod algorithms;
pub mod document;
pub mod global_search;
pub mod graph_algorithms;
pub mod similarity_search;
pub mod vector_algorithms;

pub type RegisterFunction = Box<dyn FnOnce(&str, Registry, Object) -> (Registry, Object) + Send>;
