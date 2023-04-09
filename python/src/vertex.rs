use crate::dynamic::DynamicGraph;
use crate::edge::{PyEdgeIter, PyNestedEdgeIter};
use crate::util::{adapt_err_value, extract_vertex_ref, through_impl, window_impl};
use crate::wrappers::{
    BoolIter, NestedBoolIter, NestedI64Iter, NestedOptionI64Iter, NestedOptionPropIter,
    NestedPropHistoriesIter, NestedPropHistoryIter, NestedPropsIter, NestedStringIter,
    NestedStringVecIter, NestedU64Iter, NestedUsizeIter, OptionI64Iter, OptionPropIter, Prop,
    PropHistoriesIter, PropHistoryIter, PropsIter, StringIter, StringVecIter, U64Iter, UsizeIter,
};
use docbrown_core::tgraph::VertexRef;
use docbrown_db::graph_window::WindowSet;
use docbrown_db::path::{PathFromGraph, PathFromVertex};
use docbrown_db::vertex::VertexView;
use docbrown_db::vertices::Vertices;
use docbrown_db::view_api::*;
use itertools::Itertools;
use pyo3::exceptions::PyIndexError;
use pyo3::{pyclass, pymethods, PyAny, PyRef, PyRefMut, PyResult};
use std::collections::HashMap;

#[pyclass(name = "Vertex")]
#[derive(Clone)]
pub struct PyVertex {
    vertex: VertexView<DynamicGraph>,
}

impl From<VertexView<DynamicGraph>> for PyVertex {
    fn from(value: VertexView<DynamicGraph>) -> Self {
        PyVertex { vertex: value }
    }
}

impl From<PyVertex> for VertexRef {
    fn from(value: PyVertex) -> Self {
        value.vertex.into()
    }
}

#[pymethods]
impl PyVertex {
    pub fn id(&self) -> u64 {
        self.vertex.id()
    }

    pub fn name(&self) -> String {
        self.vertex.name()
    }

    pub fn earliest_time(&self) -> Option<i64> {
        self.vertex.earliest_time()
    }

    pub fn latest_time(&self) -> Option<i64> {
        self.vertex.latest_time()
    }

    pub fn property(&self, name: String, include_static: Option<bool>) -> Option<Prop> {
        let include_static = include_static.unwrap_or(true);
        self.vertex
            .property(name, include_static)
            .map(|prop| prop.into())
    }

    pub fn property_history(&self, name: String) -> Vec<(i64, Prop)> {
        self.vertex
            .property_history(name)
            .into_iter()
            .map(|(k, v)| (k, v.into()))
            .collect()
    }

    pub fn properties(&self, include_static: Option<bool>) -> HashMap<String, Prop> {
        let include_static = include_static.unwrap_or(true);
        self.vertex
            .properties(include_static)
            .into_iter()
            .map(|(k, v)| (k, v.into()))
            .collect()
    }

    pub fn property_histories(&self) -> HashMap<String, Vec<(i64, Prop)>> {
        self.vertex
            .property_histories()
            .into_iter()
            .map(|(k, v)| (k, v.into_iter().map(|(t, p)| (t, p.into())).collect()))
            .collect()
    }

    pub fn property_names(&self, include_static: Option<bool>) -> Vec<String> {
        let include_static = include_static.unwrap_or(true);
        self.vertex.property_names(include_static)
    }

    pub fn has_property(&self, name: String, include_static: Option<bool>) -> bool {
        let include_static = include_static.unwrap_or(true);
        self.vertex.has_property(name, include_static)
    }

    pub fn has_static_property(&self, name: String) -> bool {
        self.vertex.has_static_property(name)
    }

    pub fn static_property(&self, name: String) -> Option<Prop> {
        self.vertex.static_property(name).map(|prop| prop.into())
    }

    pub fn degree(&self) -> usize {
        self.vertex.degree()
    }

    pub fn in_degree(&self) -> usize {
        self.vertex.in_degree()
    }

    pub fn out_degree(&self) -> usize {
        self.vertex.out_degree()
    }

    pub fn edges(&self) -> PyEdgeIter {
        self.vertex.edges().into()
    }

    pub fn in_edges(&self) -> PyEdgeIter {
        self.vertex.in_edges().into()
    }

    pub fn out_edges(&self) -> PyEdgeIter {
        self.vertex.out_edges().into()
    }

    pub fn neighbours(&self) -> PyPathFromVertex {
        self.vertex.neighbours().into()
    }

    pub fn in_neighbours(&self) -> PyPathFromVertex {
        self.vertex.in_neighbours().into()
    }

    pub fn out_neighbours(&self) -> PyPathFromVertex {
        self.vertex.out_neighbours().into()
    }

    //******  Perspective APIS  ******//
    pub fn start(&self) -> Option<i64> {
        self.vertex.start()
    }

    pub fn end(&self) -> Option<i64> {
        self.vertex.end()
    }

    fn expanding(&self, step: u64, start: Option<i64>, end: Option<i64>) -> PyVertexWindowSet {
        self.vertex.expanding(step, start, end).into()
    }

    fn rolling(
        &self,
        window: u64,
        step: Option<u64>,
        start: Option<i64>,
        end: Option<i64>,
    ) -> PyVertexWindowSet {
        self.vertex.rolling(window, step, start, end).into()
    }

    pub fn window(&self, t_start: Option<i64>, t_end: Option<i64>) -> PyVertex {
        window_impl(&self.vertex, t_start, t_end).into()
    }

    pub fn at(&self, end: i64) -> PyVertex {
        self.vertex.at(end).into()
    }

    pub fn through(&self, perspectives: &PyAny) -> PyResult<PyVertexWindowSet> {
        through_impl(&self.vertex, perspectives).map(|p| p.into())
    }

    //******  Python  ******//
    pub fn __getitem__(&self, name: String) -> Option<Prop> {
        self.property(name, Some(true))
    }

    pub fn __repr__(&self) -> String {
        let properties: String = self
            .properties(Some(true))
            .iter()
            .map(|(k, v)| k.to_string() + " : " + &v.to_string())
            .join(", ");

        if properties.is_empty() {
            format!("Vertex(name={})", self.name().trim_matches('"'))
        } else {
            let property_string: String = "{".to_owned() + &properties + "}";
            format!(
                "Vertex(name={}, properties={})",
                self.name().trim_matches('"'),
                property_string
            )
        }
    }
}

#[pyclass(name = "Vertices")]
pub struct PyVertices {
    pub(crate) vertices: Vertices<DynamicGraph>,
}

impl From<Vertices<DynamicGraph>> for PyVertices {
    fn from(value: Vertices<DynamicGraph>) -> Self {
        Self { vertices: value }
    }
}

#[pymethods]
impl PyVertices {
    fn id(&self) -> U64Iter {
        self.vertices.id().into()
    }

    fn name(&self) -> StringIter {
        self.vertices.name().into()
    }

    fn earliest_time(&self) -> OptionI64Iter {
        self.vertices.earliest_time().into()
    }

    fn latest_time(&self) -> OptionI64Iter {
        self.vertices.latest_time().into()
    }

    fn property(&self, name: String, include_static: Option<bool>) -> OptionPropIter {
        self.vertices
            .property(name, include_static.unwrap_or(true))
            .into()
    }

    fn property_history(&self, name: String) -> PropHistoryIter {
        self.vertices.property_history(name).into()
    }

    fn properties(&self, include_static: Option<bool>) -> PropsIter {
        self.vertices
            .properties(include_static.unwrap_or(true))
            .into()
    }

    fn property_histories(&self) -> PropHistoriesIter {
        self.vertices.property_histories().into()
    }

    fn property_names(&self, include_static: Option<bool>) -> StringVecIter {
        self.vertices
            .property_names(include_static.unwrap_or(true))
            .into()
    }

    fn has_property(&self, name: String, include_static: Option<bool>) -> BoolIter {
        self.vertices
            .has_property(name, include_static.unwrap_or(true))
            .into()
    }

    fn has_static_property(&self, name: String) -> BoolIter {
        self.vertices.has_static_property(name).into()
    }

    fn static_property(&self, name: String) -> OptionPropIter {
        self.vertices.static_property(name).into()
    }

    fn degree(&self) -> UsizeIter {
        self.vertices.degree().into()
    }

    fn in_degree(&self) -> UsizeIter {
        self.vertices.in_degree().into()
    }

    fn out_degree(&self) -> UsizeIter {
        self.vertices.out_degree().into()
    }

    fn edges(&self) -> PyNestedEdgeIter {
        self.vertices.edges().into()
    }

    fn in_edges(&self) -> PyNestedEdgeIter {
        self.vertices.in_edges().into()
    }

    fn out_edges(&self) -> PyNestedEdgeIter {
        self.vertices.out_edges().into()
    }

    fn out_neighbours(&self) -> PyPathFromGraph {
        self.vertices.out_neighbours().into()
    }

    fn in_neighbours(&self) -> PyPathFromGraph {
        self.vertices.in_neighbours().into()
    }

    fn neighbours(&self) -> PyPathFromGraph {
        self.vertices.neighbours().into()
    }

    //******  Perspective APIS  ******//
    pub fn start(&self) -> Option<i64> {
        self.vertices.start()
    }

    pub fn end(&self) -> Option<i64> {
        self.vertices.end()
    }

    fn expanding(&self, step: u64, start: Option<i64>, end: Option<i64>) -> PyVerticesWindowSet {
        self.vertices.expanding(step, start, end).into()
    }

    fn rolling(
        &self,
        window: u64,
        step: Option<u64>,
        start: Option<i64>,
        end: Option<i64>,
    ) -> PyVerticesWindowSet {
        self.vertices.rolling(window, step, start, end).into()
    }

    pub fn window(&self, t_start: Option<i64>, t_end: Option<i64>) -> PyVertices {
        window_impl(&self.vertices, t_start, t_end).into()
    }

    pub fn at(&self, end: i64) -> PyVertices {
        self.vertices.at(end).into()
    }

    pub fn through(&self, perspectives: &PyAny) -> PyResult<PyVerticesWindowSet> {
        through_impl(&self.vertices, perspectives).map(|p| p.into())
    }

    //****** Python *******
    pub fn __iter__(&self) -> PyVertexIterator {
        self.vertices.iter().into()
    }

    pub fn __len__(&self) -> usize {
        self.vertices.len()
    }

    pub fn __bool__(&self) -> bool {
        self.vertices.is_empty()
    }

    pub fn __getitem__(&self, vertex: &PyAny) -> PyResult<PyVertex> {
        let vref = extract_vertex_ref(vertex)?;
        self.vertices.get(vref).map_or_else(
            || Err(PyIndexError::new_err("Vertex does not exist")),
            |v| Ok(v.into()),
        )
    }

    pub fn __call__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    pub fn __repr__(&self) -> String {
        let values = self
            .__iter__()
            .into_iter()
            .take(11)
            .map(|v| v.__repr__())
            .collect_vec();
        if values.len() < 11 {
            "Vertices(".to_string() + &values.join(", ") + ")"
        } else {
            "Vertices(".to_string() + &values[0..10].join(", ") + " ... )"
        }
    }
}

#[pyclass(name = "PathFromGraph")]
pub struct PyPathFromGraph {
    path: PathFromGraph<DynamicGraph>,
}

#[pymethods]
impl PyPathFromGraph {
    fn __iter__(&self) -> PathIterator {
        self.path.iter().into()
    }

    fn id(&self) -> NestedU64Iter {
        self.path.id().into()
    }

    fn name(&self) -> NestedStringIter {
        self.path.name().into()
    }

    fn earliest_time(&self) -> NestedOptionI64Iter {
        self.path.earliest_time().into()
    }

    fn latest_time(&self) -> NestedOptionI64Iter {
        self.path.latest_time().into()
    }

    fn property(&self, name: String, include_static: Option<bool>) -> NestedOptionPropIter {
        self.path
            .property(name, include_static.unwrap_or(true))
            .into()
    }

    fn property_history(&self, name: String) -> NestedPropHistoryIter {
        self.path.property_history(name).into()
    }

    fn properties(&self, include_static: Option<bool>) -> NestedPropsIter {
        self.path.properties(include_static.unwrap_or(true)).into()
    }

    fn property_histories(&self) -> NestedPropHistoriesIter {
        self.path.property_histories().into()
    }

    fn property_names(&self, include_static: Option<bool>) -> NestedStringVecIter {
        self.path
            .property_names(include_static.unwrap_or(true))
            .into()
    }

    fn has_property(&self, name: String, include_static: Option<bool>) -> NestedBoolIter {
        self.path
            .has_property(name, include_static.unwrap_or(true))
            .into()
    }

    fn has_static_property(&self, name: String) -> NestedBoolIter {
        self.path.has_static_property(name).into()
    }

    fn static_property(&self, name: String) -> NestedOptionPropIter {
        self.path.static_property(name).into()
    }

    fn degree(&self) -> NestedUsizeIter {
        self.path.degree().into()
    }

    fn in_degree(&self) -> NestedUsizeIter {
        self.path.in_degree().into()
    }

    fn out_degree(&self) -> NestedUsizeIter {
        self.path.out_degree().into()
    }

    fn edges(&self) -> PyNestedEdgeIter {
        self.path.edges().into()
    }

    fn in_edges(&self) -> PyNestedEdgeIter {
        self.path.in_edges().into()
    }

    fn out_edges(&self) -> PyNestedEdgeIter {
        self.path.out_edges().into()
    }

    fn out_neighbours(&self) -> Self {
        self.path.out_neighbours().into()
    }

    fn in_neighbours(&self) -> Self {
        self.path.in_neighbours().into()
    }

    fn neighbours(&self) -> Self {
        self.path.neighbours().into()
    }

    //******  Perspective APIS  ******//
    pub fn start(&self) -> Option<i64> {
        self.path.start()
    }

    pub fn end(&self) -> Option<i64> {
        self.path.end()
    }

    fn expanding(
        &self,
        step: u64,
        start: Option<i64>,
        end: Option<i64>,
    ) -> PyPathFromGraphWindowSet {
        self.path.expanding(step, start, end).into()
    }

    fn rolling(
        &self,
        window: u64,
        step: Option<u64>,
        start: Option<i64>,
        end: Option<i64>,
    ) -> PyPathFromGraphWindowSet {
        self.path.rolling(window, step, start, end).into()
    }

    pub fn window(&self, t_start: Option<i64>, t_end: Option<i64>) -> Self {
        window_impl(&self.path, t_start, t_end).into()
    }

    pub fn at(&self, end: i64) -> Self {
        self.path.at(end).into()
    }

    pub fn through(&self, perspectives: &PyAny) -> PyResult<PyPathFromGraphWindowSet> {
        through_impl(&self.path, perspectives).map(|p| p.into())
    }

    fn __repr__(&self) -> String {
        let values = self
            .__iter__()
            .into_iter()
            .take(11)
            .map(|v| v.__repr__())
            .collect_vec();
        if values.len() < 11 {
            "WindowedVerticesPath(".to_string() + &values.join(", ") + ")"
        } else {
            "WindowedVerticesPath(".to_string() + &values.join(", ") + " ... )"
        }
    }
}

impl From<PathFromGraph<DynamicGraph>> for PyPathFromGraph {
    fn from(value: PathFromGraph<DynamicGraph>) -> Self {
        Self { path: value }
    }
}

#[pyclass(name = "PathFromVertex")]
pub struct PyPathFromVertex {
    path: PathFromVertex<DynamicGraph>,
}

impl From<PathFromVertex<DynamicGraph>> for PyPathFromVertex {
    fn from(value: PathFromVertex<DynamicGraph>) -> Self {
        Self { path: value }
    }
}

#[pymethods]
impl PyPathFromVertex {
    fn __iter__(&self) -> PyVertexIterator {
        self.path.iter().into()
    }

    fn id(&self) -> U64Iter {
        self.path.id().into()
    }

    fn name(&self) -> StringIter {
        self.path.name().into()
    }

    fn earliest_time(&self) -> OptionI64Iter {
        self.path.earliest_time().into()
    }

    fn latest_time(&self) -> OptionI64Iter {
        self.path.latest_time().into()
    }

    fn property(&self, name: String, include_static: Option<bool>) -> OptionPropIter {
        self.path
            .property(name, include_static.unwrap_or(true))
            .into()
    }

    fn property_history(&self, name: String) -> PropHistoryIter {
        self.path.property_history(name).into()
    }

    fn properties(&self, include_static: Option<bool>) -> PropsIter {
        self.path.properties(include_static.unwrap_or(true)).into()
    }

    fn property_histories(&self) -> PropHistoriesIter {
        self.path.property_histories().into()
    }

    fn property_names(&self, include_static: Option<bool>) -> StringVecIter {
        self.path
            .property_names(include_static.unwrap_or(true))
            .into()
    }

    fn has_property(&self, name: String, include_static: Option<bool>) -> BoolIter {
        self.path
            .has_property(name, include_static.unwrap_or(true))
            .into()
    }

    fn has_static_property(&self, name: String) -> BoolIter {
        self.path.has_static_property(name).into()
    }

    fn static_property(&self, name: String) -> OptionPropIter {
        self.path.static_property(name).into()
    }

    fn in_degree(&self) -> UsizeIter {
        self.path.in_degree().into()
    }

    fn out_degree(&self) -> UsizeIter {
        self.path.out_degree().into()
    }

    fn degree(&self) -> UsizeIter {
        self.path.degree().into()
    }

    fn edges(&self) -> PyEdgeIter {
        self.path.edges().into()
    }

    fn in_edges(&self) -> PyEdgeIter {
        self.path.in_edges().into()
    }

    fn out_edges(&self) -> PyEdgeIter {
        self.path.out_edges().into()
    }

    fn out_neighbours(&self) -> Self {
        self.path.out_neighbours().into()
    }

    fn in_neighbours(&self) -> Self {
        self.path.in_neighbours().into()
    }

    fn neighbours(&self) -> Self {
        self.path.neighbours().into()
    }

    //******  Perspective APIS  ******//
    pub fn start(&self) -> Option<i64> {
        self.path.start()
    }

    pub fn end(&self) -> Option<i64> {
        self.path.end()
    }

    fn expanding(
        &self,
        step: u64,
        start: Option<i64>,
        end: Option<i64>,
    ) -> PyPathFromVertexWindowSet {
        self.path.expanding(step, start, end).into()
    }

    fn rolling(
        &self,
        window: u64,
        step: Option<u64>,
        start: Option<i64>,
        end: Option<i64>,
    ) -> PyPathFromVertexWindowSet {
        self.path.rolling(window, step, start, end).into()
    }

    pub fn window(&self, t_start: Option<i64>, t_end: Option<i64>) -> Self {
        window_impl(&self.path, t_start, t_end).into()
    }

    pub fn at(&self, end: i64) -> Self {
        self.path.at(end).into()
    }

    pub fn through(&self, perspectives: &PyAny) -> PyResult<PyPathFromVertexWindowSet> {
        through_impl(&self.path, perspectives).map(|p| p.into())
    }

    fn __repr__(&self) -> String {
        let values = self
            .__iter__()
            .into_iter()
            .take(11)
            .map(|v| v.__repr__())
            .collect_vec();
        if values.len() < 11 {
            "PathFromVertex(".to_string() + &values.join(", ") + ")"
        } else {
            "PathFromVertex(".to_string() + &values[0..10].join(", ") + " ... )"
        }
    }
}

#[pyclass(name = "VertexIterator")]
pub struct PyVertexIterator {
    iter: Box<dyn Iterator<Item = PyVertex> + Send>,
}

impl From<Box<dyn Iterator<Item = VertexView<DynamicGraph>> + Send>> for PyVertexIterator {
    fn from(value: Box<dyn Iterator<Item = VertexView<DynamicGraph>> + Send>) -> Self {
        Self {
            iter: Box::new(value.map(|v| v.into())),
        }
    }
}

impl IntoIterator for PyVertexIterator {
    type Item = PyVertex;
    type IntoIter = Box<dyn Iterator<Item = PyVertex> + Send>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter
    }
}

#[pymethods]
impl PyVertexIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<PyVertex> {
        slf.iter.next()
    }
}

impl From<Box<dyn Iterator<Item = PyVertex> + Send>> for PyVertexIterator {
    fn from(value: Box<dyn Iterator<Item = PyVertex> + Send>) -> Self {
        Self { iter: value }
    }
}

#[pyclass]
pub struct PathIterator {
    pub(crate) iter: Box<dyn Iterator<Item = PyPathFromVertex> + Send>,
}

impl IntoIterator for PathIterator {
    type Item = PyPathFromVertex;
    type IntoIter = Box<dyn Iterator<Item = PyPathFromVertex> + Send>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter
    }
}

impl From<Box<dyn Iterator<Item = PathFromVertex<DynamicGraph>> + Send>> for PathIterator {
    fn from(value: Box<dyn Iterator<Item = PathFromVertex<DynamicGraph>> + Send>) -> Self {
        Self {
            iter: Box::new(value.map(|path| path.into())),
        }
    }
}

#[pymethods]
impl PathIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<PyPathFromVertex> {
        slf.iter.next()
    }
}

#[pyclass(name = "VertexWindowSet")]
pub struct PyVertexWindowSet {
    window_set: WindowSet<VertexView<DynamicGraph>>,
}

impl From<WindowSet<VertexView<DynamicGraph>>> for PyVertexWindowSet {
    fn from(value: WindowSet<VertexView<DynamicGraph>>) -> Self {
        Self { window_set: value }
    }
}

#[pymethods]
impl PyVertexWindowSet {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<PyVertex> {
        slf.window_set.next().map(|g| g.into())
    }
}

#[pyclass(name = "VerticesWindowSet")]
pub struct PyVerticesWindowSet {
    window_set: WindowSet<Vertices<DynamicGraph>>,
}

impl From<WindowSet<Vertices<DynamicGraph>>> for PyVerticesWindowSet {
    fn from(value: WindowSet<Vertices<DynamicGraph>>) -> Self {
        Self { window_set: value }
    }
}

#[pymethods]
impl PyVerticesWindowSet {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<PyVertices> {
        slf.window_set.next().map(|g| g.into())
    }
}

#[pyclass(name = "PathFromGraphWindowSet")]
pub struct PyPathFromGraphWindowSet {
    window_set: WindowSet<PathFromGraph<DynamicGraph>>,
}

impl From<WindowSet<PathFromGraph<DynamicGraph>>> for PyPathFromGraphWindowSet {
    fn from(value: WindowSet<PathFromGraph<DynamicGraph>>) -> Self {
        Self { window_set: value }
    }
}

#[pymethods]
impl PyPathFromGraphWindowSet {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<PyPathFromGraph> {
        slf.window_set.next().map(|g| g.into())
    }
}

#[pyclass(name = "PathFromVertexWindowSet")]
pub struct PyPathFromVertexWindowSet {
    window_set: WindowSet<PathFromVertex<DynamicGraph>>,
}

impl From<WindowSet<PathFromVertex<DynamicGraph>>> for PyPathFromVertexWindowSet {
    fn from(value: WindowSet<PathFromVertex<DynamicGraph>>) -> Self {
        Self { window_set: value }
    }
}

#[pymethods]
impl PyPathFromVertexWindowSet {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<PyPathFromVertex> {
        slf.window_set.next().map(|g| g.into())
    }
}
