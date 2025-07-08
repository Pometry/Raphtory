use dynamic_graphql::OneOfInput;

pub(crate) mod collection;
mod document;
pub(crate) mod edge;
mod edges;
pub(crate) mod filtering;
pub(crate) mod graph;
pub(crate) mod index;
pub(crate) mod meta_graph;
pub(crate) mod mutable_graph;
pub(crate) mod namespace;
mod namespaced_item;
pub(crate) mod node;
mod nodes;
mod path_from_node;
pub(crate) mod property;
pub(crate) mod vector_selection;
pub(crate) mod vectorised_graph;
mod windowset;

#[derive(OneOfInput, Clone)]
pub(crate) enum WindowDuration {
    Duration(String),
    Epoch(u64),
}
