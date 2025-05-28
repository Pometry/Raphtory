use crate::core::entities::properties::prop::Prop;
use bigdecimal::BigDecimal;
use pyo3::{
    exceptions::PyTypeError,
    prelude::*,
    sync::GILOnceCell,
    types::{PyBool, PyType},
    Bound, FromPyObject, IntoPyObject, IntoPyObjectExt, Py, PyAny, PyErr, PyResult, Python,
};
use std::{ops::Deref, str::FromStr, sync::Arc};

#[cfg(feature = "arrow")]
use {crate::core::entities::properties::prop::PropArray, pyo3_arrow::PyArray};

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
            #[cfg(feature = "arrow")]
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
        #[cfg(feature = "arrow")]
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
