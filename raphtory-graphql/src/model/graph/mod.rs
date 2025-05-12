// let self_clone = self.clone();
// tokio::task::spawn_blocking(move || {
//
// })
// .await
// .unwrap();

pub(crate) mod edge;
mod edges;
pub(crate) mod filtering;
pub(crate) mod graph;
pub(crate) mod graphs;
pub(crate) mod meta_graph;
pub(crate) mod mutable_graph;
pub(crate) mod namespace;
pub(crate) mod node;
mod nodes;
mod path_from_node;
pub(crate) mod property;
pub(crate) mod vectorised_graph;
mod windowset;
