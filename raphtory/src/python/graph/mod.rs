use std::collections::HashMap;
use pyo3::prelude::*;
use pyo3::types::PyDict;

mod algorithm_result;
pub mod edge;
pub mod graph;
pub mod graph_with_deletions;
pub mod pandas;
pub mod vertex;
pub mod views;

pub fn hashmap_to_dict<K, V>(hm: HashMap<K, V>, py: Python) -> PyResult<&PyDict>
    where
        K: IntoPy<PyObject>,
        V: IntoPy<PyObject>,
{
    let dict = PyDict::new(py);
    for (key, value) in hm {
        let py_key = key.into_py(py);
        let py_value = value.into_py(py);

        dict.set_item(py_key, py_value)?;
    }
    Ok(dict)
}

#[pyfunction]
pub fn example_one(py: Python) -> PyResult<&PyDict> {
    let mut h: HashMap<String, f64> = HashMap::new();
    h.insert("hello".to_string(), 1.0);
    hashmap_to_dict(h, py)
}