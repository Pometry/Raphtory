use ordered_float::OrderedFloat;
use pyo3::prelude::*;

/// Create a macro for py_algorithm_result
macro_rules! py_algorithm_result {
    ($name:ident, $rustKey:ty, $rustValue:ty) => {
        #[pyclass]
        pub struct $name($crate::algorithms::algorithm_result::AlgorithmResult<$rustKey, $rustValue>);

        impl pyo3::IntoPy<pyo3::PyObject> for $crate::algorithms::algorithm_result::AlgorithmResult<$rustKey, $rustValue> {
            fn into_py(self, py: Python<'_>) -> pyo3::PyObject {
                $name(self).into_py(py)
            }
        }
    };
}

macro_rules! py_algorithm_result_base {
    ($name:ident, $rustKey:ty, $rustValue:ty) => {
        #[pymethods]
        impl $name {
            fn get_all(&self) -> std::collections::HashMap<$rustKey, $rustValue> {
                self.0.get_all().clone()
            }

            fn get(&self, key: $rustKey) -> Option<$rustValue> {
                self.0.get(&key).cloned()
            }

            #[pyo3(signature = ())]
            pub fn to_df(
                &self,
            ) -> PyResult<PyObject>
            {
                let hashmap = &self.0.result;
                let mut keys = Vec::new();
                let mut values = Vec::new();
                Python::with_gil(|py| {
                    for (key, value) in hashmap.iter() {
                        keys.push(key.to_object(py));
                        values.push(value.to_object(py));
                    }
                    let dict = pyo3::types::PyDict::new(py);
                    dict.set_item("Key", pyo3::types::PyList::new(py, keys.as_slice()))?;
                    dict.set_item("Value", pyo3::types::PyList::new(py, values.as_slice()))?;
                    let pandas = pyo3::types::PyModule::import(py, "pandas")?;
                    let df: &PyAny = pandas.getattr("DataFrame")?.call1((dict,))?;
                    Ok(df.to_object(py))
                })
            }
        }
    }
}

macro_rules! py_algorithm_result_partial_ord {
    ($name:ident, $rustKey:ty, $rustValue:ty) => {
        #[pymethods]
        impl $name {
            #[pyo3(signature = (reverse=true))]
            fn sort_by_value(&self, reverse: bool) -> Vec<($rustKey, $rustValue)> {
                self.0.sort_by_value(reverse)
            }

            #[pyo3(signature = (reverse=true))]
            fn sort_by_key(&self, reverse: bool) -> Vec<($rustKey, $rustValue)> {
                self.0.sort_by_key(reverse)
            }

            #[pyo3(signature = (k, percentage=false, reverse=true))]
            fn top_k(&self, k: usize, percentage: bool, reverse: bool) -> Option<Vec<($rustKey, $rustValue)>> {
                self.0.top_k(k, percentage, reverse)
            }
        }
        py_algorithm_result_base!($name, $rustKey, $rustValue);
    };
}

macro_rules! py_algorithm_result_ord_hash_eq {
    ($name:ident, $rustKey:ty, $rustValue:ty) => {
        #[pymethods]
        impl $name {
            fn group_by(&self) -> std::collections::HashMap<$rustValue, Vec<$rustKey>> {
                self.0.group_by()
            }
        }
        py_algorithm_result_partial_ord!($name, $rustKey, $rustValue);
    };
}

py_algorithm_result!(AlgorithmResultStrU64, String, u64);
py_algorithm_result_ord_hash_eq!(AlgorithmResultStrU64, String, u64);

py_algorithm_result!(AlgorithmResultStrTupleF32F32, String, (f32, f32));
py_algorithm_result_partial_ord!(AlgorithmResultStrTupleF32F32, String, (f32, f32));

py_algorithm_result!(AlgorithmResultStrVecI64Str, String, Vec<(i64, String)>);
py_algorithm_result_ord_hash_eq!(AlgorithmResultStrVecI64Str, String, Vec<(i64, String)>);

py_algorithm_result!(AlgorithmResultU64VecUsize, u64, Vec<usize>);
py_algorithm_result_ord_hash_eq!(AlgorithmResultU64VecUsize, u64, Vec<usize>);

py_algorithm_result!(AlgorithmResultStrF64, String, OrderedFloat<f64>);

#[pymethods]
impl AlgorithmResultStrF64 {
    #[pyo3(signature = ())]
    fn get_all(&self) -> std::collections::HashMap<String, f64> {
        self.0.get_all().into_iter()
            .map(|(key, of)| (key.clone(), of.into_inner()))
            .collect()
    }


    fn get(&self, key: String) -> Option<f64> {
        Some(self.0.get(&key).unwrap().0)
    }

    #[pyo3(signature = (reverse=true))]
    fn sort_by_value(&self, reverse: bool) -> Vec<(String, f64)> {
        self.0.sort_by_value(reverse)
            .into_iter()
            .map(|(key, ordered_float)| (key, ordered_float.into_inner()))
            .collect()
    }

    #[pyo3(signature = (reverse=true))]
    fn sort_by_key(&self, reverse: bool) -> Vec<(String, f64)> {
        self.0.sort_by_key(reverse).into_iter()
            .map(|(key, ordered_float)| (key, ordered_float.into_inner()))
            .collect()
    }

    #[pyo3(signature = (k, percentage=false, reverse=true))]
    fn top_k(&self, k: usize, percentage: bool, reverse: bool) -> Option<Vec<(String, f64)>> {
        self.0.top_k(k, percentage, reverse)
            .map(|vec| {
                vec.into_iter()
                    .map(|(key, ordered_float)| (key, ordered_float.into_inner()))
                    .collect()
            })
    }

    #[pyo3(signature = ())]
    fn to_df(&self) -> PyResult<PyObject> {
        let hashmap: std::collections::HashMap<String, f64> = self.0.get_all().clone().into_iter()
            .map(|(k, v)| (k, v.into()))
            .collect();
        let mut keys = Vec::new();
        let mut values = Vec::new();
        Python::with_gil(|py| {
            for (key, value) in hashmap.iter() {
                keys.push(key.to_object(py));
                values.push(value.to_object(py));
            }
            let dict = pyo3::types::PyDict::new(py);
            dict.set_item("Key", pyo3::types::PyList::new(py, keys.as_slice()))?;
            dict.set_item("Value", pyo3::types::PyList::new(py, values.as_slice()))?;
            let pandas = PyModule::import(py, "pandas")?;
            let df: &PyAny = pandas.getattr("DataFrame")?.call1((dict,))?;
            Ok(df.to_object(py))
        })
    }

    #[pyo3(signature = ())]
    fn group_by(&self) -> std::collections::HashMap<String, Vec<String>> {
        let ordered_map = self.0.group_by();
        let mut f64_map: std::collections::HashMap<String, Vec<String>> = std::collections::HashMap::new();
        for (ordered_float, strings) in ordered_map {
            let f64_value = ordered_float.into_inner();
            f64_map.insert(f64_value.to_string().parse().unwrap(), strings);
        }
        f64_map
     }
}


