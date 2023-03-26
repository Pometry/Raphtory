use crate::graph::Graph;
use pyo3::prelude::*;

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


#[pyfunction]
#[pyo3(signature = (shards=1,timeout_seconds=600))]
pub(crate) fn reddit_hyperlink_graph(shards:usize,timeout_seconds:u64) -> Graph {
    Graph::from_db_graph(docbrown_db::graph_loader::reddit_hyperlinks::reddit_graph(shards,timeout_seconds))
}