use async_graphql::dynamic::Object;
use dynamic_graphql::internal::Registry;

pub mod algorithms;
pub mod entry_point;
pub mod graph_algorithm_plugin;
pub mod mutation_entry_point;
pub mod mutation_plugin;
pub mod operation;
pub mod query_entry_point;
pub mod query_plugin;

pub type RegisterFunction = Box<dyn FnOnce(&str, Registry, Object) -> (Registry, Object) + Send>;
