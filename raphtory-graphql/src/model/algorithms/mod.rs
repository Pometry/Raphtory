use async_graphql::dynamic::Object;
use dynamic_graphql::internal::Registry;

pub mod algorithms;
pub mod document;
pub mod global_search;

pub type RegisterFunction = Box<dyn FnOnce(&str, Registry, Object) -> (Registry, Object) + Send>;
