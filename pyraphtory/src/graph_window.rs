use std::{collections::HashMap, sync::Arc};

use crate::wrappers;
use crate::{graph::Graph, wrappers::*};
use docbrown_db::graph_window;
use itertools::Itertools;
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

    pub fn edge(&self, v1: u64, v2: u64) -> Option<WindowedEdge> {
        self.graph_w.edge(v1, v2).map(|we| we.into())
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
    pub fn prop(&self, name: String) -> Vec<(i64, Prop)> {
        self.vertex_w
            .prop(name)
            .into_iter()
            .map(|(t, p)| (t, p.into()))
            .collect_vec()
    }

    pub fn props(&self) -> HashMap<String, Vec<(i64, Prop)>> {
        self.vertex_w
            .props()
            .into_iter()
            .map(|(n, p)| {
                let prop = p
                    .into_iter()
                    .map(|(t, p)| (t, p.into()))
                    .collect::<Vec<(i64, wrappers::Prop)>>();
                (n, prop)
            })
            .into_iter()
            .collect::<HashMap<String, Vec<(i64, Prop)>>>()
    }

    pub fn degree(&self) -> usize {
        self.vertex_w.degree()
    }

    pub fn in_degree(&self) -> usize {
        self.vertex_w.in_degree()
    }

    pub fn out_degree(&self) -> usize {
        self.vertex_w.out_degree()
    }

    pub fn edges(&self) -> WindowedEdgeIterator {
        WindowedEdgeIterator {
            iter: Box::new(self.vertex_w.edges().map(|te| te.into())),
        }
    }

    pub fn in_edges(&self) -> WindowedEdgeIterator {
        WindowedEdgeIterator {
            iter: Box::new(self.vertex_w.in_edges().map(|te| te.into())),
        }
    }

    pub fn out_edges(&self) -> WindowedEdgeIterator {
        WindowedEdgeIterator {
            iter: Box::new(self.vertex_w.out_edges().map(|te| te.into())),
        }
    }

    pub fn neighbours(&self) -> WindowedVertexIterator {
        WindowedVertexIterator {
            iter: Box::new(self.vertex_w.neighbours().map(|tv| tv.into())),
        }
    }

    pub fn in_neighbours(&self) -> WindowedVertexIterator {
        WindowedVertexIterator {
            iter: Box::new(self.vertex_w.in_neighbours().map(|tv| tv.into())),
        }
    }

    pub fn out_neighbours(&self) -> WindowedVertexIterator {
        WindowedVertexIterator {
            iter: Box::new(self.vertex_w.out_neighbours().map(|tv| tv.into())),
        }
    }

    pub fn neighbours_ids(&self) -> VertexIdsIterator {
        VertexIdsIterator {
            iter: Box::new(self.vertex_w.neighbours_ids()),
        }
    }

    pub fn in_neighbours_ids(&self) -> VertexIdsIterator {
        VertexIdsIterator {
            iter: Box::new(self.vertex_w.in_neighbours_ids()),
        }
    }

    pub fn out_neighbours_ids(&self) -> VertexIdsIterator {
        VertexIdsIterator {
            iter: Box::new(self.vertex_w.out_neighbours_ids()),
        }
    }
}

#[pyclass]
pub struct WindowedEdge {
    pub edge_id: usize,
    #[pyo3(get)]
    pub src: u64,
    #[pyo3(get)]
    pub dst: u64,
    #[pyo3(get)]
    pub t: Option<i64>,
    #[pyo3(get)]
    pub is_remote: bool,
    pub(crate) edge_w: graph_window::WindowedEdge,
}

impl From<graph_window::WindowedEdge> for WindowedEdge {
    fn from(value: graph_window::WindowedEdge) -> WindowedEdge {
        WindowedEdge {
            edge_id: value.edge_id,
            src: value.src,
            dst: value.dst,
            t: value.t,
            is_remote: value.is_remote,
            edge_w: value,
        }
    }
}

#[pymethods]
impl WindowedEdge {
    pub fn prop(&self, name: String) -> Vec<(i64, Prop)> {
        self.edge_w
            .prop(name)
            .into_iter()
            .map(|(t, p)| (t, p.into()))
            .collect_vec()
    }
}
