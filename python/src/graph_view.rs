use crate::dynamic::DynamicGraph;
use crate::edge::{PyEdge, PyEdgeIter};
use crate::util::{extract_vertex_ref, through_impl, window_impl};
use crate::vertex::{PyVertex, PyVertices};
use docbrown::db::graph_window::WindowSet;
use docbrown::db::view_api::*;
use pyo3::prelude::*;

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

#[pyclass(name = "GraphWindowSet")]
pub struct PyGraphWindowSet {
    window_set: WindowSet<DynamicGraph>,
}

impl From<WindowSet<DynamicGraph>> for PyGraphWindowSet {
    fn from(value: WindowSet<DynamicGraph>) -> Self {
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

    pub fn has_edge(&self, src: &PyAny, dst: &PyAny, layer: Option<&str>) -> PyResult<bool> {
        let src = extract_vertex_ref(src)?;
        let dst = extract_vertex_ref(dst)?;
        Ok(self.graph.has_edge(src, dst, layer))
    }

    //******  Getter APIs ******//

    pub fn vertex(&self, id: &PyAny) -> PyResult<Option<PyVertex>> {
        let v = extract_vertex_ref(id)?;
        Ok(self.graph.vertex(v).map(|v| v.into()))
    }

    #[getter]
    pub fn vertices(&self) -> PyVertices {
        self.graph.vertices().into()
    }

    pub fn edge(&self, src: &PyAny, dst: &PyAny, layer: Option<&str>) -> PyResult<Option<PyEdge>> {
        let src = extract_vertex_ref(src)?;
        let dst = extract_vertex_ref(dst)?;
        Ok(self.graph.edge(src, dst, layer).map(|we| we.into()))
    }

    pub fn edges(&self) -> PyEdgeIter {
        self.graph.edges().into()
    }

    //******  Perspective APIS  ******//
    pub fn start(&self) -> Option<i64> {
        self.graph.start()
    }

    pub fn end(&self) -> Option<i64> {
        self.graph.end()
    }

    fn expanding(&self, step: u64, start: Option<i64>, end: Option<i64>) -> PyGraphWindowSet {
        self.graph.expanding(step, start, end).into()
    }

    fn rolling(
        &self,
        window: u64,
        step: Option<u64>,
        start: Option<i64>,
        end: Option<i64>,
    ) -> PyGraphWindowSet {
        self.graph.rolling(window, step, start, end).into()
    }

    pub fn window(&self, t_start: Option<i64>, t_end: Option<i64>) -> PyGraphView {
        window_impl(&self.graph, t_start, t_end).into()
    }

    pub fn at(&self, end: i64) -> PyGraphView {
        self.graph.at(end).into()
    }

    fn through(&self, perspectives: &PyAny) -> PyResult<PyGraphWindowSet> {
        through_impl(&self.graph, perspectives).map(|p| p.into())
    }

    pub fn __repr__(&self) -> String {
        let num_edges = self.graph.num_edges();
        let num_vertices = self.graph.num_vertices();
        let earliest_time = self.graph.earliest_time().unwrap_or_default();
        let latest_time = self.graph.latest_time().unwrap_or_default();

        format!(
            "Graph(number_of_edges={:?}, number_of_vertices={:?}, earliest_time={:?}, latest_time={:?})",
            num_edges, num_vertices, earliest_time, latest_time
        )
    }
}
