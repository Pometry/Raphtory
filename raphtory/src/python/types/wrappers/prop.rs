use super::document::PyDocument;
use crate::{
    core::{utils::errors::GraphError, DocumentInput, Prop},
    db::graph::views::property_filter::internal::{
        InternalEdgeFilterOps, InternalExplodedEdgeFilterOps, InternalNodePropertyFilterOps,
    },
    prelude::{GraphViewOps, PropertyFilter},
    python::types::repr::Repr,
};
use pyo3::{exceptions::PyTypeError, prelude::*, types::PyBool, IntoPyObjectExt};
use std::{collections::HashSet, ops::Deref, sync::Arc};

impl<'py> IntoPyObject<'py> for Prop {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(match self {
            Prop::Str(s) => s.into_pyobject(py)?.into_any(),
            Prop::Bool(bool) => bool.into_pyobject(py)?.into_bound_py_any(py)?,
            Prop::U8(u8) => u8.into_pyobject(py)?.into_any(),
            Prop::U16(u16) => u16.into_pyobject(py)?.into_any(),
            Prop::I64(i64) => i64.into_pyobject(py)?.into_any(),
            Prop::U64(u64) => u64.into_pyobject(py)?.into_any(),
            Prop::F64(f64) => f64.into_pyobject(py)?.into_any(),
            Prop::DTime(dtime) => dtime.into_pyobject(py)?.into_any(),
            Prop::NDTime(ndtime) => ndtime.into_pyobject(py)?.into_any(),
            Prop::Array(blob) => pyo3_arrow::PyArray::from_array_ref(blob.0)
                .to_pyarrow(py)?
                .into_bound(py),
            Prop::Document(d) => PyDocument::from(d).into_pyobject(py)?.into_any(),
            Prop::I32(v) => v.into_pyobject(py)?.into_any(),
            Prop::U32(v) => v.into_pyobject(py)?.into_any(),
            Prop::F32(v) => v.into_pyobject(py)?.into_any(),
            Prop::List(v) => v.deref().clone().into_pyobject(py)?.into_any(), // Fixme: optimise the clone here?
            Prop::Map(v) => v.deref().clone().into_pyobject(py)?.into_any(),
        })
    }
}

// Manually implemented to make sure we don't end up with f32/i32/u32 from python ints/floats
impl<'source> FromPyObject<'source> for Prop {
    fn extract_bound(ob: &Bound<'source, PyAny>) -> PyResult<Self> {
        if ob.is_instance_of::<PyBool>() {
            return Ok(Prop::Bool(ob.extract()?));
        }
        if let Ok(v) = ob.extract() {
            return Ok(Prop::I64(v));
        }
        if let Ok(v) = ob.extract() {
            return Ok(Prop::F64(v));
        }
        if let Ok(d) = ob.extract() {
            return Ok(Prop::NDTime(d));
        }
        if let Ok(d) = ob.extract() {
            return Ok(Prop::DTime(d));
        }
        if let Ok(s) = ob.extract::<String>() {
            return Ok(Prop::Str(s.into()));
        }
        if let Ok(d) = ob.extract::<PyDocument>() {
            return Ok(Prop::Document(DocumentInput {
                content: d.content,
                life: d.life,
            }));
        }
        if let Ok(list) = ob.extract() {
            return Ok(Prop::List(Arc::new(list)));
        }
        if let Ok(map) = ob.extract() {
            return Ok(Prop::Map(Arc::new(map)));
        }
        Err(PyTypeError::new_err("Not a valid property type"))
    }
}

impl Repr for Prop {
    fn repr(&self) -> String {
        match &self {
            Prop::Str(v) => v.repr(),
            Prop::Bool(v) => v.repr(),
            Prop::I64(v) => v.repr(),
            Prop::U8(v) => v.repr(),
            Prop::U16(v) => v.repr(),
            Prop::U64(v) => v.repr(),
            Prop::F64(v) => v.repr(),
            Prop::DTime(v) => v.repr(),
            Prop::NDTime(v) => v.repr(),
            Prop::Array(v) => format!("{:?}", v),
            Prop::Document(d) => d.content.repr(), // We can't reuse the __repr__ defined for PyDocument because it needs to run python code
            Prop::I32(v) => v.repr(),
            Prop::U32(v) => v.repr(),
            Prop::F32(v) => v.repr(),
            Prop::List(v) => v.repr(),
            Prop::Map(v) => v.repr(),
        }
    }
}

pub type PropValue = Option<Prop>;
pub type PropHistItems = Vec<(i64, Prop)>;

#[pyclass(frozen, name = "PropertyFilter", module = "raphtory")]
#[derive(Clone)]
pub struct PyPropertyFilter(PropertyFilter);

impl InternalEdgeFilterOps for PyPropertyFilter {
    type EdgeFiltered<'graph, G>
        = <PropertyFilter as InternalEdgeFilterOps>::EdgeFiltered<'graph, G>
    where
        G: GraphViewOps<'graph>,
        Self: 'graph;

    fn create_edge_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EdgeFiltered<'graph, G>, GraphError> {
        self.0.create_edge_filter(graph)
    }
}

impl InternalExplodedEdgeFilterOps for PyPropertyFilter {
    type ExplodedEdgeFiltered<'graph, G>
        = <PropertyFilter as InternalExplodedEdgeFilterOps>::ExplodedEdgeFiltered<'graph, G>
    where
        G: GraphViewOps<'graph>,
        Self: 'graph;

    fn create_exploded_edge_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::ExplodedEdgeFiltered<'graph, G>, GraphError> {
        self.0.create_exploded_edge_filter(graph)
    }
}

impl InternalNodePropertyFilterOps for PyPropertyFilter {
    type NodePropertyFiltered<'graph, G>
        = <PropertyFilter as InternalNodePropertyFilterOps>::NodePropertyFiltered<'graph, G>
    where
        Self: 'graph,
        G: GraphViewOps<'graph>;

    fn create_node_property_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::NodePropertyFiltered<'graph, G>, GraphError> {
        self.0.create_node_property_filter(graph)
    }
}

/// A reference to a property used for constructing filters
///
/// Use `==`, `!=`, `<`, `<=`, `>`, `>=` to filter based on
/// property value (these filters always exclude entities that do not
/// have the property) or use one of the methods to construct
/// other kinds of filters.
#[pyclass(frozen, name = "Prop", module = "raphtory")]
#[derive(Clone)]
pub struct PyPropertyRef {
    name: String,
}

#[pymethods]
impl PyPropertyRef {
    #[new]
    fn new(name: String) -> Self {
        PyPropertyRef { name }
    }

    fn __eq__(&self, value: Prop) -> PyPropertyFilter {
        let filter = PropertyFilter::eq(&self.name, value);
        PyPropertyFilter(filter)
    }

    fn __ne__(&self, value: Prop) -> PyPropertyFilter {
        let filter = PropertyFilter::ne(&self.name, value);
        PyPropertyFilter(filter)
    }

    fn __lt__(&self, value: Prop) -> PyPropertyFilter {
        let filter = PropertyFilter::lt(&self.name, value);
        PyPropertyFilter(filter)
    }

    fn __le__(&self, value: Prop) -> PyPropertyFilter {
        let filter = PropertyFilter::le(&self.name, value);
        PyPropertyFilter(filter)
    }

    fn __gt__(&self, value: Prop) -> PyPropertyFilter {
        let filter = PropertyFilter::gt(&self.name, value);
        PyPropertyFilter(filter)
    }

    fn __ge__(&self, value: Prop) -> PyPropertyFilter {
        let filter = PropertyFilter::ge(&self.name, value);
        PyPropertyFilter(filter)
    }

    /// Create a filter that only keeps entities if they have the property
    fn is_some(&self) -> PyPropertyFilter {
        let filter = PropertyFilter::is_some(&self.name);
        PyPropertyFilter(filter)
    }

    /// Create a filter that only keeps entities that do not have the property
    fn is_none(&self) -> PyPropertyFilter {
        let filter = PropertyFilter::is_none(&self.name);
        PyPropertyFilter(filter)
    }

    /// Create a filter that keeps entities if their property value is in the set
    fn any(&self, values: HashSet<Prop>) -> PyPropertyFilter {
        let filter = PropertyFilter::any(&self.name, values);
        PyPropertyFilter(filter)
    }

    /// Create a filter that keeps entities if their property value is not in the set or
    /// if they don't have the property
    fn not_any(&self, values: HashSet<Prop>) -> PyPropertyFilter {
        let filter = PropertyFilter::not_any(&self.name, values);
        PyPropertyFilter(filter)
    }
}
