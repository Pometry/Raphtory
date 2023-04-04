use crate::dynamic::DynamicGraph;
use crate::edge::{PyEdge, PyEdgeIter};
use crate::util::extract_vertex_ref;
use crate::vertex::{PyVertex, PyVertices};
use crate::wrappers::{PyPerspective, PyPerspectiveSet};
use docbrown_db::graph_window::GraphWindowSet;
use docbrown_db::perspective::Perspective;
use docbrown_db::view_api::*;
use pyo3::prelude::*;
use pyo3::types::PyIterator;

#[pyclass(name = "GraphView", frozen, subclass)]
pub struct PyGraphView {
    pub(crate) graph: DynamicGraph,
}

impl<G: GraphViewOps> From<G> for PyGraphView {
    fn from(value: G) -> Self {
        PyGraphView {
            graph: DynamicGraph::new(value),
        }
    }
}

#[pyclass(name = "PyGraphWindowSet")]
pub struct PyGraphWindowSet {
    window_set: GraphWindowSet<DynamicGraph>,
}

impl From<GraphWindowSet<DynamicGraph>> for PyGraphWindowSet {
    fn from(value: GraphWindowSet<DynamicGraph>) -> Self {
        Self { window_set: value }
    }
}

#[pymethods]
impl PyGraphWindowSet {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<PyGraphView> {
        slf.window_set.next().map(|g| g.into())
    }
}

#[pymethods]
impl PyGraphView {
    //******  Metrics APIs ******//

    pub fn earliest_time(&self) -> Option<i64> {
        self.graph.earliest_time()
    }

    pub fn latest_time(&self) -> Option<i64> {
        self.graph.latest_time()
    }

    pub fn num_edges(&self) -> usize {
        self.graph.num_edges()
    }

    pub fn num_vertices(&self) -> usize {
        self.graph.num_vertices()
    }

    pub fn has_vertex(&self, id: &PyAny) -> PyResult<bool> {
        let v = extract_vertex_ref(id)?;
        Ok(self.graph.has_vertex(v))
    }

    pub fn has_edge(&self, src: &PyAny, dst: &PyAny) -> PyResult<bool> {
        let src = extract_vertex_ref(src)?;
        let dst = extract_vertex_ref(dst)?;
        Ok(self.graph.has_edge(src, dst))
    }

    //******  Getter APIs ******//

    pub fn vertex(&self, id: &PyAny) -> PyResult<Option<PyVertex>> {
        let v = extract_vertex_ref(id)?;
        Ok(self.graph.vertex(v).map(|v| v.into()))
    }

    pub fn vertices(&self) -> PyVertices {
        self.graph.vertices().into()
    }

    pub fn edge(&self, src: &PyAny, dst: &PyAny) -> PyResult<Option<PyEdge>> {
        let src = extract_vertex_ref(src)?;
        let dst = extract_vertex_ref(dst)?;
        Ok(self.graph.edge(src, dst).map(|we| we.into()))
    }

    pub fn edges(&self) -> PyEdgeIter {
        self.graph.edges().into()
    }

    //******  Perspective APIS  ******//

    pub fn window(&self, t_start: i64, t_end: i64) -> PyGraphView {
        self.graph.window(t_start, t_end).into()
    }

    pub fn at(&self, end: i64) -> PyGraphView {
        self.graph.at(end).into()
    }

    fn through(&self, perspectives: &PyAny) -> PyResult<PyGraphWindowSet> {
        struct PyPerspectiveIterator {
            pub iter: Py<PyIterator>,
        }
        unsafe impl Send for PyPerspectiveIterator {} // iter is used by holding the GIL
        impl Iterator for PyPerspectiveIterator {
            type Item = Perspective;
            fn next(&mut self) -> Option<Self::Item> {
                Python::with_gil(|py| {
                    let item = self.iter.as_ref(py).next()?.ok()?;
                    Some(item.extract::<PyPerspective>().ok()?.into())
                })
            }
        }

        let result = match perspectives.extract::<PyPerspectiveSet>() {
            Ok(perspective_set) => self.graph.through_perspectives(perspective_set.ps),
            Err(_) => {
                let iter = PyPerspectiveIterator {
                    iter: Py::from(perspectives.iter()?),
                };
                self.graph.through_iter(Box::new(iter))
            }
        };
        Ok(result.into())
    }

    pub fn __repr__(&self) -> String {
        let num_edges = self.graph.num_edges();
        let num_vertices = self.graph.num_vertices();
        let earliest_time = self.graph.earliest_time().unwrap_or_default();
        let latest_time = self.graph.latest_time().unwrap_or_default();

        format!(
            "Graph(NumEdges({:?}), NumVertices({:?}), EarliestTime({:?}), LatestTime({:?}))",
            num_edges, num_vertices, earliest_time, latest_time
        )
    }
}
