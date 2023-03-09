use docbrown_db::graph::Graph as Internal_Graph;
use crate::graph::Graph;
use pyo3::prelude::*;
use docbrown_db::graph;

#[pyfunction]
#[pyo3(signature = (shards=1))]
pub(crate) fn lotr_graph(shards:usize) -> Graph {
    Graph::from_db_graph(docbrown_db::graph_loader::lotr_graph::lotr_graph(shards))
}

#[pyfunction]
#[pyo3(signature = (shards=1))]
pub(crate) fn twitter_graph(shards:usize) -> Graph {
    Graph::from_db_graph(docbrown_db::graph_loader::twitter_graph::twitter_graph(shards))
}