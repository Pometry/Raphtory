use crate::url_encode::{url_encode_graph, UrlDecodeError};
use async_graphql::{dynamic::ValueAccessor, Value as GraphqlValue};
use pyo3::{
    exceptions::{PyTypeError, PyValueError},
    pyfunction,
    types::PyDict,
    IntoPy, PyErr, PyObject, PyResult, Python, ToPyObject,
};
use raphtory::{db::api::view::MaterializedGraph, python::utils::errors::adapt_err_value};
use serde_json::{Map, Number, Value as JsonValue};
use std::collections::HashMap;

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
                number.as_f64().unwrap().to_object(py)
            } else if number.is_u64() {
                number.as_u64().unwrap().to_object(py)
            } else {
                number.as_i64().unwrap().to_object(py)
            }
        }
        GraphqlValue::String(value) => value.to_object(py),
        GraphqlValue::Boolean(value) => value.to_object(py),
        value => panic!("graphql input value {value} has an unsupported type"),
    }
}

pub(crate) fn translate_from_python(py: Python, value: PyObject) -> PyResult<JsonValue> {
    if let Ok(value) = value.extract::<i64>(py) {
        Ok(JsonValue::Number(value.into()))
    } else if let Ok(value) = value.extract::<f64>(py) {
        Ok(JsonValue::Number(Number::from_f64(value).unwrap()))
    } else if let Ok(value) = value.extract::<bool>(py) {
        Ok(JsonValue::Bool(value))
    } else if let Ok(value) = value.extract::<String>(py) {
        Ok(JsonValue::String(value))
    } else if let Ok(value) = value.extract::<Vec<PyObject>>(py) {
        let mut vec = Vec::new();
        for item in value {
            vec.push(translate_from_python(py, item)?);
        }
        Ok(JsonValue::Array(vec))
    } else if let Ok(value) = value.extract::<&PyDict>(py) {
        let mut map = Map::new();
        for (key, value) in value.iter() {
            let key = key.extract::<String>()?;
            let value = translate_from_python(py, value.into_py(py))?;
            map.insert(key, value);
        }
        Ok(JsonValue::Object(map))
    } else {
        Err(PyErr::new::<PyTypeError, _>("Unsupported type"))
    }
}

pub(crate) fn translate_map_to_python(
    py: Python,
    input: HashMap<String, JsonValue>,
) -> PyResult<HashMap<String, PyObject>> {
    let mut output_dict = HashMap::new();
    for (key, value) in input {
        let py_value = translate_to_python(py, value)?;
        output_dict.insert(key, py_value);
    }

    Ok(output_dict)
}

fn translate_to_python(py: Python, value: serde_json::Value) -> PyResult<PyObject> {
    match value {
        JsonValue::Number(num) => {
            if num.is_i64() {
                Ok(num.as_i64().unwrap().into_py(py))
            } else if num.is_f64() {
                Ok(num.as_f64().unwrap().into_py(py))
            } else {
                Err(PyErr::new::<PyTypeError, _>("Unsupported number type"))
            }
        }
        JsonValue::String(s) => Ok(s.into_py(py)),
        JsonValue::Array(vec) => {
            let mut list = Vec::new();
            for item in vec {
                list.push(translate_to_python(py, item)?);
            }
            Ok(list.into_py(py))
        }
        JsonValue::Object(map) => {
            let dict = PyDict::new(py);
            for (key, value) in map {
                dict.set_item(key, translate_to_python(py, value)?)?;
            }
            Ok(dict.into())
        }
        JsonValue::Bool(b) => Ok(b.into_py(py)),
        JsonValue::Null => Ok(py.None()),
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

impl From<UrlDecodeError> for PyErr {
    fn from(value: UrlDecodeError) -> Self {
        adapt_err_value(&value)
    }
}
