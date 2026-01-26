use crate::core::{
    entities::properties::prop::{data_type_as_prop_type, Prop, PropType},
    storage::arc_str::ArcStr,
};
use bigdecimal::BigDecimal;
use pyo3::{
    exceptions::PyTypeError,
    prelude::*,
    pybacked::PyBackedStr,
    sync::PyOnceLock,
    types::{PyBool, PyDict, PyType},
    Bound, FromPyObject, IntoPyObject, IntoPyObjectExt, Py, PyAny, PyErr, PyResult, Python,
};
use pyo3_arrow::PyDataType;
use rustc_hash::FxHashMap;
use std::{collections::HashMap, ops::Deref, str::FromStr, sync::Arc};

mod array_ext {
    use pyo3::{intern, prelude::*, types::PyTuple};
    use pyo3_arrow::PyArray;

    pub trait ArrayExportExt: Sized {
        fn into_pyarrow<'py>(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>>;
    }

    impl ArrayExportExt for PyArray {
        fn into_pyarrow<'py>(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
            let pyarrow_mod = py.import(intern!(py, "pyarrow"))?;
            pyarrow_mod
                .getattr(intern!(py, "array"))?
                .call1(PyTuple::new(py, vec![self.into_pyobject(py)?])?)
        }
    }
}

use crate::core::entities::properties::prop::PropArray;
use array_ext::*;
use pyo3_arrow::PyArray;

static DECIMAL_CLS: PyOnceLock<Py<PyType>> = PyOnceLock::new();

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
            Prop::Bool(bool) => bool.into_bound_py_any(py)?,
            Prop::U8(u8) => u8.into_pyobject(py)?.into_any(),
            Prop::U16(u16) => u16.into_pyobject(py)?.into_any(),
            Prop::I64(i64) => i64.into_pyobject(py)?.into_any(),
            Prop::U64(u64) => u64.into_pyobject(py)?.into_any(),
            Prop::F64(f64) => f64.into_pyobject(py)?.into_any(),
            Prop::DTime(dtime) => dtime.into_pyobject(py)?.into_any(),
            Prop::NDTime(ndtime) => ndtime.into_pyobject(py)?.into_any(),
            Prop::I32(v) => v.into_pyobject(py)?.into_any(),
            Prop::U32(v) => v.into_pyobject(py)?.into_any(),
            Prop::F32(v) => v.into_pyobject(py)?.into_any(),
            Prop::List(PropArray::Array(arr_ref)) => {
                PyArray::from_array_ref(arr_ref).into_pyarrow(py)?
            }
            Prop::List(PropArray::Vec(v)) => v.into_pyobject(py)?.into_any(), // Fixme: optimise the clone here?
            Prop::Map(v) => v.deref().clone().into_pyobject(py)?.into_any(),
            Prop::Decimal(d) => {
                let decl_cls = get_decimal_cls(py)?;
                decl_cls.call1((d.to_string(),))?
            }
        })
    }
}

impl<'a, 'py: 'a> IntoPyObject<'py> for &'a Prop {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(match self {
            Prop::Str(s) => s.into_pyobject(py)?.into_any(),
            Prop::Bool(bool) => bool.into_bound_py_any(py)?,
            Prop::U8(u8) => u8.into_pyobject(py)?.into_any(),
            Prop::U16(u16) => u16.into_pyobject(py)?.into_any(),
            Prop::I64(i64) => i64.into_pyobject(py)?.into_any(),
            Prop::U64(u64) => u64.into_pyobject(py)?.into_any(),
            Prop::F64(f64) => f64.into_pyobject(py)?.into_any(),
            Prop::DTime(dtime) => dtime.into_pyobject(py)?.into_any(),
            Prop::NDTime(ndtime) => ndtime.into_pyobject(py)?.into_any(),
            Prop::I32(v) => v.into_pyobject(py)?.into_any(),
            Prop::U32(v) => v.into_pyobject(py)?.into_any(),
            Prop::F32(v) => v.into_pyobject(py)?.into_any(),
            Prop::List(PropArray::Array(arr_ref)) => {
                PyArray::from_array_ref(arr_ref.clone()).into_pyarrow(py)?
            }
            Prop::List(PropArray::Vec(v)) => v.into_pyobject(py)?.into_any(),
            Prop::Map(v) => v.deref().clone().into_pyobject(py)?.into_any(),
            Prop::Decimal(d) => {
                let decl_cls = get_decimal_cls(py)?;
                decl_cls.call1((d.to_string(),))?
            }
        })
    }
}

#[pyclass(name = "Prop", module = "raphtory")]
pub struct PyProp(pub Prop);

#[pymethods]
impl PyProp {
    #[staticmethod]
    pub fn u8(value: u8) -> Self {
        PyProp(Prop::U8(value))
    }

    #[staticmethod]
    pub fn u16(value: u16) -> Self {
        PyProp(Prop::U16(value))
    }

    #[staticmethod]
    pub fn u32(value: u32) -> Self {
        PyProp(Prop::U32(value))
    }

    #[staticmethod]
    pub fn u64(value: u64) -> Self {
        PyProp(Prop::U64(value))
    }

    #[staticmethod]
    pub fn i32(value: i32) -> Self {
        PyProp(Prop::I32(value))
    }

    #[staticmethod]
    pub fn i64(value: i64) -> Self {
        PyProp(Prop::I64(value))
    }

    #[staticmethod]
    pub fn f32(value: f32) -> Self {
        PyProp(Prop::F32(value))
    }

    #[staticmethod]
    pub fn f64(value: f64) -> Self {
        PyProp(Prop::F64(value))
    }

    #[staticmethod]
    pub fn str(value: &str) -> Self {
        PyProp(Prop::str(value))
    }

    #[staticmethod]
    pub fn bool(value: bool) -> Self {
        PyProp(Prop::Bool(value))
    }

    #[staticmethod]
    pub fn list(values: &Bound<'_, PyAny>) -> PyResult<Self> {
        let elems: Vec<Prop> = values.extract()?;
        Ok(PyProp(Prop::list(elems)))
    }

    #[staticmethod]
    pub fn map(dict: Bound<'_, PyDict>) -> PyResult<Self> {
        let items: HashMap<String, Prop> = dict.extract()?;

        let map: FxHashMap<ArcStr, Prop> = items
            .into_iter()
            .map(|(k, v)| (ArcStr::from(k), v))
            .collect();

        Ok(PyProp(Prop::Map(Arc::new(map))))
    }

    pub fn dtype(&self) -> PropType {
        self.0.dtype()
    }

    pub fn __repr__(&self) -> String {
        format!("{}", self.0)
    }
}

// Manually implemented to make sure we don't end up with f32/i32/u32 from python ints/floats
impl<'py> FromPyObject<'_, 'py> for Prop {
    type Error = PyErr;
    fn extract(ob: Borrowed<'_, 'py, PyAny>) -> PyResult<Self> {
        if let Ok(pyref) = ob.extract::<PyRef<PyProp>>() {
            return Ok(pyref.0.clone());
        }

        if ob.is_instance_of::<PyBool>() {
            return Ok(Prop::Bool(ob.extract()?));
        }

        if let Ok(v) = ob.extract() {
            return Ok(Prop::I64(v));
        }

        if let Ok(v) = ob.extract() {
            return Ok(Prop::U64(v));
        }

        if ob.get_type().name()?.contains("Decimal")? {
            // this sits before f64, otherwise it will be picked up as f64
            let py_str = &ob.str()?;
            let rs_str = &py_str.to_cow()?;

            return BigDecimal::from_str(rs_str)
                .map_err(|_| {
                    PyTypeError::new_err(format!("Could not convert {} to Decimal", rs_str))
                })
                .and_then(|bd| {
                    Prop::try_from_bd(bd)
                        .map_err(|_| PyTypeError::new_err(format!("Decimal too large {}", rs_str)))
                });
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
        if let Ok(arrow) = ob.extract::<PyArray>() {
            let (arr, _) = arrow.into_inner();
            return Ok(Prop::List(PropArray::Array(arr)));
        }
        if let Ok(list) = ob.extract::<Vec<Prop>>() {
            return Ok(Prop::List(PropArray::Vec(list.into())));
        }

        if let Ok(map) = ob.extract() {
            return Ok(Prop::Map(Arc::new(map)));
        }

        Err(PyTypeError::new_err(format!(
            "Could not convert {:?} to Prop",
            ob
        )))
    }
}

/// PropType provides access to the types used by Raphtory. They can be used to specify the data type of different properties,
/// which is especially useful if one wishes to cast some input column from one type to another during ingestion.
/// PropType can be used to define the schema in the various load_* functions used for data ingestion
/// (i.e. Graph.load_nodes(...)/Graph.load_edges(...) etc.)
#[pyclass(name = "PropType", frozen, module = "raphtory")]
pub struct PyPropType(pub PropType);

#[pymethods]
impl PyPropType {
    #[staticmethod]
    pub fn u8() -> PropType {
        PropType::U8
    }

    #[staticmethod]
    pub fn u16() -> PropType {
        PropType::U16
    }

    #[staticmethod]
    pub fn u32() -> PropType {
        PropType::U32
    }

    #[staticmethod]
    pub fn u64() -> PropType {
        PropType::U64
    }

    #[staticmethod]
    pub fn i32() -> PropType {
        PropType::I32
    }

    #[staticmethod]
    pub fn i64() -> PropType {
        PropType::I64
    }

    #[staticmethod]
    pub fn f32() -> PropType {
        PropType::F32
    }

    #[staticmethod]
    pub fn f64() -> PropType {
        PropType::F64
    }

    #[staticmethod]
    pub fn str() -> PropType {
        PropType::Str
    }

    #[staticmethod]
    pub fn bool() -> PropType {
        PropType::Bool
    }

    #[staticmethod]
    pub fn naive_datetime() -> PropType {
        PropType::NDTime
    }

    #[staticmethod]
    pub fn datetime() -> PropType {
        PropType::DTime
    }

    #[staticmethod]
    pub fn list(p: PropType) -> PropType {
        PropType::List(Box::new(p))
    }

    #[staticmethod]
    pub fn map(hash_map: HashMap<String, PropType>) -> PropType {
        PropType::Map(Arc::new(hash_map))
    }

    fn __repr__(&self) -> String {
        format!("PropType.{}", self.0)
    }

    fn __str__(&self) -> String {
        format!("{:?}", self.0)
    }

    fn __eq__(&self, other: PropType) -> bool {
        self.0 == other
    }
}

impl<'py> IntoPyObject<'py> for PropType {
    type Target = PyPropType;
    type Output = Bound<'py, Self::Target>;
    type Error = <Self::Target as IntoPyObject<'py>>::Error;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        PyPropType(self).into_pyobject(py)
    }
}

impl<'source> FromPyObject<'_, 'source> for PropType {
    type Error = PyErr;
    fn extract(ob: Borrowed<'_, 'source, PyAny>) -> PyResult<Self> {
        if let Ok(prop_type) = ob.cast::<PyPropType>() {
            Ok(prop_type.get().0.clone())
        } else if let Ok(prop_type_str) = ob.extract::<PyBackedStr>() {
            match prop_type_str.deref().to_ascii_lowercase().as_str() {
                "i64" | "int64" | "int" => Ok(PropType::I64),
                "i32" | "int32" => Ok(PropType::I32),
                "u64" | "uint64" => Ok(PropType::U64),
                "u32" | "uint32" => Ok(PropType::I32),
                "u16" | "uint16" => Ok(PropType::U16),
                "u8" | "uint8" => Ok(PropType::U8),
                "f64" | "float64" | "float" | "double" => Ok(PropType::F64),
                "f32" | "float32" => Ok(PropType::F32),
                "bool" | "boolean" => Ok(PropType::Bool),
                "str" | "string" | "utf8" => Ok(PropType::Str),
                "ndtime" | "naivedatetime" | "datetime" => Ok(PropType::NDTime),
                "dtime" | "datetimetz" => Ok(PropType::DTime),
                other => Err(PyTypeError::new_err(format!(
                    "Unknown type name '{other:?}'"
                ))),
            }
        } else if let Ok(py_datatype) = ob.extract::<PyDataType>() {
            data_type_as_prop_type(&py_datatype.into_inner())
                .map_err(|e| PyTypeError::new_err(format!("Unsupported Arrow DataType {:?}", e.0)))
        } else {
            Err(PyTypeError::new_err(
                "PropType must be a string or an instance of itself.",
            ))
        }
    }
}
