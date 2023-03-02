use std::sync::Arc;

use crate::{graph::Graph, wrappers::*};
use docbrown_db::graph_window;
use pyo3::prelude::*;

#[pyclass]
pub struct WindowedGraph {
    pub(crate) graph_w: graph_window::WindowedGraph,
}

#[pymethods]
impl WindowedGraph {
    #[new]
    pub fn new(graph: &Graph, t_start: i64, t_end: i64) -> Self {
        Self {
            graph_w: graph_window::WindowedGraph::new(
                Arc::new(graph.graph.clone()),
                t_start,
                t_end,
            ),
        }
    }

    pub fn has_vertex(&self, v: u64) -> bool {
        self.graph_w.has_vertex(v)
    }

    pub fn vertex(&self, v: u64) -> Option<WindowedVertex> {
        self.graph_w.vertex(v).map(|wv| wv.into())
    }

    pub fn vertex_ids(&self) -> VertexIdsIterator {
        VertexIdsIterator {
            iter: self.graph_w.vertex_ids(),
        }
    }

    pub fn vertices(&self) -> WindowedVertexIterator {
        WindowedVertexIterator {
            iter: Box::new(self.graph_w.vertices().map(|wv| wv.into())),
        }
    }
}

#[pyclass]
pub struct WindowedVertex {
    #[pyo3(get)]
    pub g_id: u64,
    pub(crate) vertex_w: graph_window::WindowedVertex,
}

impl From<graph_window::WindowedVertex> for WindowedVertex {
    fn from(value: graph_window::WindowedVertex) -> WindowedVertex {
        WindowedVertex {
            g_id: value.g_id,
            vertex_w: value,
        }
    }
}

#[pymethods]
impl WindowedVertex {
    pub fn degree(&self) -> usize {
        self.vertex_w.degree()
    }

    pub fn in_degree(&self) -> usize {
        self.vertex_w.in_degree()
    }

    pub fn out_degree(&self) -> usize {
        self.vertex_w.out_degree()
    }

    pub fn neighbours(&self) -> EdgeIterator {
        EdgeIterator {
            iter: Box::new(self.vertex_w.neighbours().map(|te| te.into())),
        }
    }

    pub fn in_neighbours(&self) -> EdgeIterator {
        EdgeIterator {
            iter: Box::new(self.vertex_w.in_neighbours().map(|te| te.into())),
        }
    }

    pub fn out_neighbours(&self) -> EdgeIterator {
        EdgeIterator {
            iter: Box::new(self.vertex_w.out_neighbours().map(|te| te.into())),
        }
    }
}
