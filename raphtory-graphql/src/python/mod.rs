use crate::url_encode::{url_decode_graph, url_encode_graph, UrlDecodeError};
use async_graphql::{dynamic::ValueAccessor, Value as GraphqlValue};
use pyo3::{
    exceptions::{PyTypeError, PyValueError},
    prelude::*,
    types::{PyDict, PyList, PyNone},
    IntoPyObjectExt,
};
use raphtory::{db::api::view::MaterializedGraph, python::utils::errors::adapt_err_value};
use serde_json::{Map, Number, Value as JsonValue};

pub mod client;
pub mod global_plugins;
pub mod pymodule;
pub mod server;

const WAIT_CHECK_INTERVAL_MILLIS: u64 = 200;
const RUNNING_SERVER_CONSUMED_MSG: &str =
    "Running server object has already been used, please create another one from scratch";

pub(crate) fn adapt_graphql_value(value: &ValueAccessor, py: Python) -> PyObject {
    match value.as_value() {
        GraphqlValue::Number(number) => {
            if number.is_f64() {
                number.as_f64().unwrap().into_py_any(py).unwrap()
            } else if number.is_u64() {
                number.as_u64().unwrap().into_py_any(py).unwrap()
            } else {
                number.as_i64().unwrap().into_py_any(py).unwrap()
            }
        }
        GraphqlValue::String(value) => value.into_py_any(py).unwrap(),
        GraphqlValue::Boolean(value) => value.into_py_any(py).unwrap(),
        value => panic!("graphql input value {value} has an unsupported type"),
    }
}

pub(crate) fn translate_from_python(value: Bound<PyAny>) -> PyResult<JsonValue> {
    if let Ok(value) = value.extract::<i64>() {
        Ok(JsonValue::Number(value.into()))
    } else if let Ok(value) = value.extract::<f64>() {
        Ok(JsonValue::Number(Number::from_f64(value).unwrap()))
    } else if let Ok(value) = value.extract::<bool>() {
        Ok(JsonValue::Bool(value))
    } else if let Ok(value) = value.extract::<String>() {
        Ok(JsonValue::String(value))
    } else if let Ok(value) = value.extract::<Vec<Bound<PyAny>>>() {
        let mut vec = Vec::new();
        for item in value {
            vec.push(translate_from_python(item)?);
        }
        Ok(JsonValue::Array(vec))
    } else if let Ok(value) = value.extract::<Bound<PyDict>>() {
        let mut map = Map::new();
        for (key, value) in value.iter() {
            let key = key.extract::<String>()?;
            let value = translate_from_python(value)?;
            map.insert(key, value);
        }
        Ok(JsonValue::Object(map))
    } else {
        Err(PyErr::new::<PyTypeError, _>("Unsupported type"))
    }
}

pub(crate) fn translate_map_to_python(
    py: Python,
    input: impl IntoIterator<Item = (String, serde_json::Value)>,
) -> PyResult<Bound<PyDict>> {
    let dict = PyDict::new(py);
    for (key, value) in input {
        dict.set_item(key, translate_to_python(py, value)?)?;
    }
    Ok(dict)
}

fn translate_to_python(py: Python, value: serde_json::Value) -> PyResult<Bound<PyAny>> {
    match value {
        JsonValue::Number(num) => {
            if num.is_i64() {
                num.as_i64().unwrap().into_bound_py_any(py)
            } else if num.is_f64() {
                num.as_f64().unwrap().into_bound_py_any(py)
            } else {
                Err(PyErr::new::<PyTypeError, _>("Unsupported number type"))
            }
        }
        JsonValue::String(s) => s.into_bound_py_any(py),
        JsonValue::Array(vec) => {
            let list = PyList::empty(py);
            for item in vec {
                list.append(translate_to_python(py, item)?)?;
            }
            Ok(list.into_any())
        }
        JsonValue::Object(map) => Ok(translate_map_to_python(py, map)?.into_any()),
        JsonValue::Bool(b) => b.into_bound_py_any(py),
        JsonValue::Null => Ok(PyNone::get(py).to_owned().into_any()),
    }
}

#[pyfunction]
pub(crate) fn encode_graph(graph: MaterializedGraph) -> PyResult<String> {
    let result = url_encode_graph(graph);
    match result {
        Ok(s) => Ok(s),
        Err(e) => Err(PyValueError::new_err(format!("Error encoding: {:?}", e))),
    }
}

#[pyfunction]
pub(crate) fn decode_graph(graph: &str) -> PyResult<MaterializedGraph> {
    let result = url_decode_graph(graph);
    match result {
        Ok(g) => Ok(g),
        Err(e) => Err(PyValueError::new_err(format!("Error decoding: {:?}", e))),
    }
}

impl From<UrlDecodeError> for PyErr {
    fn from(value: UrlDecodeError) -> Self {
        adapt_err_value(&value)
    }
}
