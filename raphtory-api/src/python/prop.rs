use crate::core::entities::properties::prop::{Prop, PropType};
use bigdecimal::BigDecimal;
use pyo3::{
    exceptions::PyTypeError,
    prelude::*,
    pybacked::PyBackedStr,
    sync::GILOnceCell,
    types::{PyBool, PyType},
    Bound, FromPyObject, IntoPyObject, IntoPyObjectExt, Py, PyAny, PyErr, PyResult, Python,
};
use std::{ops::Deref, str::FromStr, sync::Arc};

#[cfg(feature = "arrow")]
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

#[cfg(feature = "arrow")]
use {crate::core::entities::properties::prop::PropArray, array_ext::*, pyo3_arrow::PyArray};

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
            Prop::Bool(bool) => bool.into_bound_py_any(py)?,
            Prop::U8(u8) => u8.into_pyobject(py)?.into_any(),
            Prop::U16(u16) => u16.into_pyobject(py)?.into_any(),
            Prop::I64(i64) => i64.into_pyobject(py)?.into_any(),
            Prop::U64(u64) => u64.into_pyobject(py)?.into_any(),
            Prop::F64(f64) => f64.into_pyobject(py)?.into_any(),
            Prop::DTime(dtime) => dtime.into_pyobject(py)?.into_any(),
            Prop::NDTime(ndtime) => ndtime.into_pyobject(py)?.into_any(),
            #[cfg(feature = "arrow")]
            Prop::Array(blob) => {
                if let Some(arr_ref) = blob.into_array_ref() {
                    PyArray::from_array_ref(arr_ref).into_pyarrow(py)?
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
        #[cfg(feature = "arrow")]
        if let Ok(arrow) = ob.extract::<PyArray>() {
            let (arr, _) = arrow.into_inner();
            return Ok(Prop::Array(PropArray::Array(arr)));
        }
        if let Ok(list) = ob.extract() {
            return Ok(Prop::List(Arc::new(list)));
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

#[pyclass(name = "PropType", frozen, module = "raphtory")]
pub struct PyPropType(pub PropType);

#[pymethods]
impl PyPropType {
    #[classattr]
    pub fn u8() -> PropType {
        PropType::U8
    }

    #[classattr]
    pub fn u16() -> PropType {
        PropType::U16
    }

    #[classattr]
    pub fn u32() -> PropType {
        PropType::U32
    }

    #[classattr]
    pub fn u64() -> PropType {
        PropType::U64
    }

    #[classattr]
    pub fn i32() -> PropType {
        PropType::I32
    }

    #[classattr]
    pub fn i64() -> PropType {
        PropType::I64
    }

    #[classattr]
    pub fn f32() -> PropType {
        PropType::F32
    }

    #[classattr]
    pub fn f64() -> PropType {
        PropType::F64
    }

    #[classattr]
    pub fn str() -> PropType {
        PropType::Str
    }

    #[classattr]
    pub fn bool() -> PropType {
        PropType::Bool
    }

    #[classattr]
    pub fn naive_datetime() -> PropType {
        PropType::NDTime
    }

    #[classattr]
    pub fn datetime() -> PropType {
        PropType::DTime
    }

    fn __repr__(&self) -> String {
        format!("PropType.{}", self.0)
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

impl<'source> FromPyObject<'source> for PropType {
    fn extract_bound(ob: &Bound<'source, PyAny>) -> PyResult<Self> {
        if let Ok(prop_type) = ob.downcast::<PyPropType>() {
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
        } else {
            Err(PyTypeError::new_err(
                "PropType must be a string or an instance of itself.",
            ))
        }
    }
}
