use crate::{
    core::{prop_array::PropArray, utils::errors::GraphError, Prop},
    db::{
        api::view::{BoxableGraphView, DynamicGraph, IntoDynamic},
        graph::views::filter::{
            internal::{
                InternalEdgeFilterOps, InternalExplodedEdgeFilterOps, InternalNodeFilterOps,
            },
            AndFilter, AsEdgeFilter, AsNodeFilter, PropertyRef,
        },
    },
    prelude::{GraphViewOps, PropertyFilter},
    python::types::{
        repr::Repr,
        wrappers::filter_expr::{PyFilterExpr, PyInnerFilterExpr},
    },
};
use bigdecimal::BigDecimal;
use pyo3::{
    exceptions::PyTypeError,
    prelude::*,
    sync::GILOnceCell,
    types::{PyBool, PyType},
    IntoPyObjectExt,
};
use pyo3_arrow::PyArray;
use std::{collections::HashSet, ops::Deref, str::FromStr, sync::Arc};

static DECIMAL_CLS: GILOnceCell<Py<PyType>> = GILOnceCell::new();

fn get_decimal_cls(py: Python<'_>) -> PyResult<&Bound<'_, PyType>> {
    DECIMAL_CLS.import(py, "decimal", "Decimal")
}

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
            Prop::Array(blob) => {
                if let Some(arr_ref) = blob.into_array_ref() {
                    pyo3_arrow::PyArray::from_array_ref(arr_ref)
                        .to_pyarrow(py)?
                        .into_bound(py)
                } else {
                    py.None().into_bound(py)
                }
            }
            Prop::I32(v) => v.into_pyobject(py)?.into_any(),
            Prop::U32(v) => v.into_pyobject(py)?.into_any(),
            Prop::F32(v) => v.into_pyobject(py)?.into_any(),
            Prop::List(v) => v.deref().clone().into_pyobject(py)?.into_any(), // Fixme: optimise the clone here?
            Prop::Map(v) => v.deref().clone().into_pyobject(py)?.into_any(),
            Prop::Decimal(d) => {
                let decl_cls = get_decimal_cls(py)?;
                decl_cls.call1((d.to_string(),))?
            }
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
        if ob.get_type().name()?.contains("Decimal")? {
            // this sits before f64, otherwise it will be picked up as f64
            let py_str = &ob.str()?;
            let rs_str = &py_str.to_cow()?;

            return Ok(BigDecimal::from_str(&rs_str)
                .map_err(|_| {
                    PyTypeError::new_err(format!("Could not convert {} to Decimal", rs_str))
                })
                .and_then(|bd| {
                    Prop::try_from_bd(bd)
                        .map_err(|_| PyTypeError::new_err(format!("Decimal too large {}", rs_str)))
                })?);
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
        if let Ok(list) = ob.extract() {
            return Ok(Prop::List(Arc::new(list)));
        }
        if let Ok(map) = ob.extract() {
            return Ok(Prop::Map(Arc::new(map)));
        }
        if let Ok(arrow) = ob.extract::<PyArray>() {
            let (arr, _) = arrow.into_inner();
            return Ok(Prop::Array(PropArray::Array(arr)));
        }
        Err(PyTypeError::new_err(format!(
            "Could not convert {:?} to Prop",
            ob
        )))
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
            Prop::I32(v) => v.repr(),
            Prop::U32(v) => v.repr(),
            Prop::F32(v) => v.repr(),
            Prop::List(v) => v.repr(),
            Prop::Map(v) => v.repr(),
            Prop::Decimal(v) => v.repr(),
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

impl InternalNodeFilterOps for PyPropertyFilter {
    type NodeFiltered<'graph, G>
        = <PropertyFilter as InternalNodeFilterOps>::NodeFiltered<'graph, G>
    where
        Self: 'graph,
        G: GraphViewOps<'graph>;

    fn create_node_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::NodeFiltered<'graph, G>, GraphError> {
        self.0.create_node_filter(graph)
    }
}

impl IntoDynamic for Arc<dyn BoxableGraphView> {
    fn into_dynamic(self) -> DynamicGraph {
        DynamicGraph(self)
    }
}

impl InternalNodeFilterOps for PyFilterExpr {
    type NodeFiltered<'graph, G: GraphViewOps<'graph>>
        = Arc<dyn BoxableGraphView + 'graph>
    where
        Self: 'graph;

    fn create_node_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::NodeFiltered<'graph, G>, GraphError> {
        match self.0 {
            PyInnerFilterExpr::Node(i) => i.create_node_filter(graph),
            PyInnerFilterExpr::Property(i) => i.create_node_filter(graph),
            PyInnerFilterExpr::Edge(i) => Err(GraphError::ParsingError),
        }
    }
}

pub trait DynInternalNodeFilterOps: AsNodeFilter {
    fn create_dyn_node_filter<'graph>(
        &self,
        graph: Arc<dyn BoxableGraphView + 'graph>,
    ) -> Result<Arc<dyn BoxableGraphView + 'graph>, GraphError>;
}

impl<T: InternalNodeFilterOps + AsNodeFilter + Clone + 'static> DynInternalNodeFilterOps for T {
    fn create_dyn_node_filter<'graph>(
        &self,
        graph: Arc<dyn BoxableGraphView + 'graph>,
    ) -> Result<Arc<dyn BoxableGraphView + 'graph>, GraphError> {
        Ok(Arc::new(self.clone().create_node_filter(graph)?))
    }
}

impl<T: DynInternalNodeFilterOps + ?Sized + 'static> InternalNodeFilterOps for Arc<T> {
    type NodeFiltered<'graph, G: GraphViewOps<'graph>>
        = Arc<dyn BoxableGraphView + 'graph>
    where
        Self: 'graph;

    fn create_node_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::NodeFiltered<'graph, G>, GraphError> {
        self.create_dyn_node_filter(Arc::new(graph))
    }
}

impl InternalEdgeFilterOps for PyFilterExpr {
    type EdgeFiltered<'graph, G: GraphViewOps<'graph>>
        = Arc<dyn BoxableGraphView + 'graph>
    where
        Self: 'graph;

    fn create_edge_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EdgeFiltered<'graph, G>, GraphError> {
        match self.0 {
            PyInnerFilterExpr::Edge(i) => i.create_edge_filter(graph),
            PyInnerFilterExpr::Property(i) => i.create_edge_filter(graph),
            PyInnerFilterExpr::Node(i) => Err(GraphError::ParsingError),
        }
    }
}

pub trait DynInternalEdgeFilterOps: AsEdgeFilter {
    fn create_dyn_edge_filter<'graph>(
        &self,
        graph: Arc<dyn BoxableGraphView + 'graph>,
    ) -> Result<Arc<dyn BoxableGraphView + 'graph>, GraphError>;
}

impl<T: InternalEdgeFilterOps + AsEdgeFilter + Clone + 'static> DynInternalEdgeFilterOps for T {
    fn create_dyn_edge_filter<'graph>(
        &self,
        graph: Arc<dyn BoxableGraphView + 'graph>,
    ) -> Result<Arc<dyn BoxableGraphView + 'graph>, GraphError> {
        Ok(Arc::new(self.clone().create_edge_filter(graph)?))
    }
}

impl<T: DynInternalEdgeFilterOps + ?Sized + 'static> InternalEdgeFilterOps for Arc<T> {
    type EdgeFiltered<'graph, G: GraphViewOps<'graph>>
        = Arc<dyn BoxableGraphView + 'graph>
    where
        Self: 'graph;

    fn create_edge_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EdgeFiltered<'graph, G>, GraphError> {
        self.create_dyn_edge_filter(Arc::new(graph))
    }
}

/// A reference to a property used for constructing filters
///
/// Use `==`, `!=`, `<`, `<=`, `>`, `>=` to filter based on
/// property value (these filters always exclude entities that do not
/// have the property) or use one of the methods to construct
/// other kinds of filters.
///
/// Arguments:
///     name (str): the name of the property
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
        let filter = PropertyFilter::eq(PropertyRef::Property(self.name.clone()), value);
        PyPropertyFilter(filter)
    }

    fn __ne__(&self, value: Prop) -> PyPropertyFilter {
        let filter = PropertyFilter::ne(PropertyRef::Property(self.name.clone()), value);
        PyPropertyFilter(filter)
    }

    fn __lt__(&self, value: Prop) -> PyPropertyFilter {
        let filter = PropertyFilter::lt(PropertyRef::Property(self.name.clone()), value);
        PyPropertyFilter(filter)
    }

    fn __le__(&self, value: Prop) -> PyPropertyFilter {
        let filter = PropertyFilter::le(PropertyRef::Property(self.name.clone()), value);
        PyPropertyFilter(filter)
    }

    fn __gt__(&self, value: Prop) -> PyPropertyFilter {
        let filter = PropertyFilter::gt(PropertyRef::Property(self.name.clone()), value);
        PyPropertyFilter(filter)
    }

    fn __ge__(&self, value: Prop) -> PyPropertyFilter {
        let filter = PropertyFilter::ge(PropertyRef::Property(self.name.clone()), value);
        PyPropertyFilter(filter)
    }

    /// Create a filter that only keeps entities if they have the property
    ///
    /// Returns:
    ///     PropertyFilter: the property filter
    fn is_some(&self) -> PyPropertyFilter {
        let filter = PropertyFilter::is_some(PropertyRef::Property(self.name.clone()));
        PyPropertyFilter(filter)
    }

    /// Create a filter that only keeps entities that do not have the property
    ///
    /// Returns:
    ///     PropertyFilter: the property filter
    fn is_none(&self) -> PyPropertyFilter {
        let filter = PropertyFilter::is_none(PropertyRef::Property(self.name.clone()));
        PyPropertyFilter(filter)
    }

    /// Create a filter that keeps entities if their property value is in the set
    ///
    /// Arguments:
    ///     values (set[PropValue]): the set of values to match
    ///
    /// Returns:
    ///     PropertyFilter: the property filter
    fn any(&self, values: HashSet<Prop>) -> PyPropertyFilter {
        let filter = PropertyFilter::includes(PropertyRef::Property(self.name.clone()), values);
        PyPropertyFilter(filter)
    }

    /// Create a filter that keeps entities if their property value is not in the set or
    /// if they don't have the property
    ///
    /// Arguments:
    ///     values (set[PropValue]): the set of values to exclude
    ///
    /// Returns:
    ///     PropertyFilter: the property filter
    fn not_any(&self, values: HashSet<Prop>) -> PyPropertyFilter {
        let filter = PropertyFilter::excludes(PropertyRef::Property(self.name.clone()), values);
        PyPropertyFilter(filter)
    }
}
