use std::{collections::HashMap, sync::Arc};

use crate::wrappers;
use crate::wrappers::Perspective;
use crate::{graph::Graph, wrappers::*};
use docbrown_core::tgraph::EdgeRef;
use docbrown_db::graph_window;
use docbrown_db::view_api::*;
use itertools::Itertools;
use pyo3::prelude::*;
use pyo3::types::PyIterator;

#[pyclass]
pub struct GraphWindowSet {
    window_set: graph_window::GraphWindowSet,
}

impl From<graph_window::GraphWindowSet> for GraphWindowSet {
    fn from(value: graph_window::GraphWindowSet) -> Self {
        GraphWindowSet::new(value)
    }
}

impl GraphWindowSet {
    pub fn new(window_set: graph_window::GraphWindowSet) -> GraphWindowSet {
        GraphWindowSet { window_set }
    }
}

#[pymethods]
impl GraphWindowSet {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>, py: Python) -> Option<WindowedGraph> {
        let windowed_graph = slf.window_set.next()?;
        Some(windowed_graph.into())
    }
}

#[pyclass]
pub struct WindowedGraph {
    pub(crate) graph_w: graph_window::WindowedGraph,
}

impl From<graph_window::WindowedGraph> for WindowedGraph {
    fn from(value: graph_window::WindowedGraph) -> Self {
        WindowedGraph { graph_w: value }
    }
}

#[pymethods]
impl WindowedGraph {
    #[new]
    pub fn new(graph: &Graph, t_start: i64, t_end: i64) -> Self {
        Self {
            graph_w: graph_window::WindowedGraph::new(graph.graph.clone(), t_start, t_end),
        }
    }

    pub fn earliest_time(&self) -> i64 {
        self.graph_w.t_start
    }

    pub fn latest_time(&self) -> i64 {
        self.graph_w.t_end
    }

    pub fn has_vertex(&self, v: u64) -> bool {
        self.graph_w.has_vertex(v)
    }

    pub fn has_edge(&self, src: u64, dst: u64) -> bool {
        self.graph_w.has_edge(src, dst)
    }

    pub fn vertex(slf: PyRef<'_, Self>, v: u64) -> Option<WindowedVertex> {
        let v = slf.graph_w.vertex(v)?;
        let g: Py<Self> = slf.into();
        Some(WindowedVertex::new(g, v))
    }

    pub fn vertex_ids(&self) -> VertexIdsIterator {
        VertexIdsIterator {
            iter: self.graph_w.vertices().id(),
        }
    }

    pub fn vertices(slf: PyRef<'_, Self>) -> WindowedVertices {
        let g: Py<Self> = slf.into();
        WindowedVertices { graph: g }
    }

    pub fn edge(&self, src: u64, dst: u64) -> Option<WindowedEdge> {
        self.graph_w.edge(src, dst).map(|we| we.into())
    }
}

#[pyclass]
pub struct WindowedVertex {
    #[pyo3(get)]
    pub id: u64,
    pub(crate) graph: Py<WindowedGraph>,
    pub(crate) vertex_w: graph_window::WindowedVertex,
}

impl WindowedVertex {
    fn from(&self, value: graph_window::WindowedVertex) -> WindowedVertex {
        WindowedVertex {
            id: value.id(),
            graph: self.graph.clone(),
            vertex_w: value,
        }
    }

    pub(crate) fn new(
        graph: Py<WindowedGraph>,
        vertex: graph_window::WindowedVertex,
    ) -> WindowedVertex {
        WindowedVertex {
            graph,
            id: vertex.id(),
            vertex_w: vertex,
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

    pub fn neighbours(&self) -> WindowedVertexIterable {
        WindowedVertexIterable {
            graph: self.graph.clone(),
            operations: vec![Operations::Neighbours],
            start_at: Some(self.id),
        }
    }

    pub fn in_neighbours(&self) -> WindowedVertexIterable {
        WindowedVertexIterable {
            graph: self.graph.clone(),
            operations: vec![Operations::InNeighbours],
            start_at: Some(self.id),
        }
    }

    pub fn out_neighbours(&self) -> WindowedVertexIterable {
        WindowedVertexIterable {
            graph: self.graph.clone(),
            operations: vec![Operations::OutNeighbours],
            start_at: Some(self.id),
        }
    }

    pub fn neighbours_ids(&self) -> VertexIdsIterator {
        VertexIdsIterator {
            iter: Box::new(self.vertex_w.neighbours().id()),
        }
    }

    pub fn in_neighbours_ids(&self) -> VertexIdsIterator {
        VertexIdsIterator {
            iter: Box::new(self.vertex_w.in_neighbours().id()),
        }
    }

    pub fn out_neighbours_ids(&self) -> VertexIdsIterator {
        VertexIdsIterator {
            iter: Box::new(self.vertex_w.out_neighbours().id()),
        }
    }

    pub fn __repr__(&self) -> String {
        format!("Vertex({})", self.id)
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
    pub time: Option<i64>,
    pub is_remote: bool,
    pub(crate) edge_w: graph_window::WindowedEdge,
}

impl From<graph_window::WindowedEdge> for WindowedEdge {
    fn from(value: graph_window::WindowedEdge) -> WindowedEdge {
        let value_ref: EdgeRef = value.as_ref();
        // FIXME: temporary hack, shouldn't really be copying all these values
        WindowedEdge {
            edge_id: value_ref.edge_id,
            src: value_ref.src_g_id,
            dst: value_ref.dst_g_id,
            time: value_ref.time,
            is_remote: value_ref.is_remote,
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
