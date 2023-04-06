use crate::dynamic::DynamicGraph;
use crate::vertex::PyVertex;
use crate::wrappers::Prop;
use docbrown_db::edge::EdgeView;
use docbrown_db::view_api::*;
use itertools::Itertools;
use pyo3::{pyclass, pymethods, PyRef, PyRefMut};
use std::collections::HashMap;

#[pyclass(name = "Edge")]
pub struct PyEdge {
    pub(crate) edge: EdgeView<DynamicGraph>,
}

impl From<EdgeView<DynamicGraph>> for PyEdge {
    fn from(value: EdgeView<DynamicGraph>) -> Self {
        Self { edge: value }
    }
}

#[pymethods]
impl PyEdge {
    pub fn __getitem__(&self, name: String) -> Option<Prop> {
        self.property(name, Some(true))
    }

    pub fn has_property(&self, name: String, include_static: Option<bool>) -> bool {
        let include_static = include_static.unwrap_or(true);
        self.edge.has_property(name, include_static)
    }

    pub fn property(&self, name: String, include_static: Option<bool>) -> Option<Prop> {
        let include_static = include_static.unwrap_or(true);
        self.edge
            .property(name, include_static)
            .map(|prop| prop.into())
    }

    pub fn properties(&self, include_static: Option<bool>) -> HashMap<String, Prop> {
        let include_static = include_static.unwrap_or(true);
        self.edge
            .properties(include_static)
            .into_iter()
            .map(|(k, v)| (k, v.into()))
            .collect()
    }

    pub fn property_names(&self, include_static: Option<bool>) -> Vec<String> {
        let include_static = include_static.unwrap_or(true);
        self.edge.property_names(include_static)
    }

    pub fn property_history(&self, name: String) -> Vec<(i64, Prop)> {
        self.edge
            .property_history(name)
            .into_iter()
            .map(|(k, v)| (k, v.into()))
            .collect()
    }

    pub fn property_histories(&self) -> HashMap<String, Vec<(i64, Prop)>> {
        self.edge
            .property_histories()
            .into_iter()
            .map(|(k, v)| (k, v.into_iter().map(|(t, p)| (t, p.into())).collect()))
            .collect()
    }

    pub fn has_static_property(&self, name: String) -> bool {
        self.edge.has_static_property(name)
    }
    pub fn static_property(&self, name: String) -> Option<Prop> {
        self.edge.static_property(name).map(|prop| prop.into())
    }

    pub fn id(&self) -> usize {
        self.edge.id()
    }

    fn src(&self) -> PyVertex {
        self.edge.src().into()
    }

    fn dst(&self) -> PyVertex {
        self.edge.dst().into()
    }

    pub fn __repr__(&self) -> String {
        let properties = "{".to_string()
            + &self
                .properties(Some(true))
                .iter()
                .map(|(k, v)| k.to_string() + " : " + &v.to_string())
                .join(", ")
            + "}";
        let property_string = if properties.is_empty() {
            "Properties({})".to_string()
        } else {
            format!("Properties({})", properties)
        };
        let source = self.edge.src().name();
        let target = self.edge.dst().name();
        format!(
            "Edge(Src({}), Dst({}), {}",
            source.trim_matches('"'),
            target.trim_matches('"'),
            property_string
        )
    }
}

#[pyclass(name = "EdgeIterator")]
pub struct PyEdgeIter {
    iter: Box<dyn Iterator<Item = PyEdge> + Send>,
}

#[pymethods]
impl PyEdgeIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<PyEdge> {
        slf.iter.next()
    }
}

impl From<Box<dyn Iterator<Item = PyEdge> + Send>> for PyEdgeIter {
    fn from(value: Box<dyn Iterator<Item = PyEdge> + Send>) -> Self {
        Self { iter: value }
    }
}

impl From<Box<dyn Iterator<Item = EdgeView<DynamicGraph>> + Send>> for PyEdgeIter {
    fn from(value: Box<dyn Iterator<Item = EdgeView<DynamicGraph>> + Send>) -> Self {
        Self {
            iter: Box::new(value.map(|e| e.into())),
        }
    }
}
