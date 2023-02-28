use std::sync::Arc;

use crate::{graph::Graph, wrappers::VertexIdsIterator};
use docbrown_db::graph_window;
use pyo3::prelude::*;

#[pyclass]
pub struct WindowedGraph {
    pub(crate) windowed_graph: graph_window::WindowedGraph,
}

#[pymethods]
impl WindowedGraph {
    #[new]
    pub fn new(graph: &Graph, t_start: i64, t_end: i64) -> Self {
        Self {
            windowed_graph: graph_window::WindowedGraph::new(
                Arc::new(graph.graph.clone()),
                t_start,
                t_end,
            ),
        }
    }

    pub fn vertex_ids(&self) -> VertexIdsIterator {
        VertexIdsIterator {
            iter: self.windowed_graph.vertex_ids(),
        }
    }
}
