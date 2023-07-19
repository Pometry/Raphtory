use itertools::Itertools;
use ordered_float::OrderedFloat;
use pyo3::prelude::*;

/// Create a macro for py_algorithm_result
macro_rules! py_algorithm_result {
    ($name:ident, $rustKey:ty, $rustValue:ty) => {
        #[pyclass]
        pub struct $name(
            $crate::algorithms::algorithm_result::AlgorithmResult<$rustKey, $rustValue>,
        );

        impl pyo3::IntoPy<pyo3::PyObject>
            for $crate::algorithms::algorithm_result::AlgorithmResult<$rustKey, $rustValue>
        {
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
            /// Returns a reference to the entire `result` hashmap.
            fn get_all(&self) -> std::collections::HashMap<$rustKey, $rustValue> {
                self.0.get_all().clone()
            }

            /// Returns the value corresponding to the provided key in the `result` hashmap.
            ///
            /// # Arguments
            ///
            /// * `key`: The key of type `H` for which the value is to be retrieved.
            fn get(&self, key: $rustKey) -> Option<$rustValue> {
                self.0.get(&key).cloned()
            }

            /// Creates a dataframe from the result
            ///
            /// # Returns
            ///
            /// A `pandas.DataFrame` containing the result
            pub fn to_df(&self) -> PyResult<PyObject> {
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
    };
}

macro_rules! py_algorithm_result_partial_ord {
    ($name:ident, $rustKey:ty, $rustValue:ty) => {
        #[pymethods]
        impl $name {
            /// Sorts the `AlgorithmResult` by its values in ascending or descending order.
            ///
            /// # Arguments
            ///
            /// * `reverse`: If `true`, sorts the result in descending order; otherwise, sorts in ascending order.
            ///
            /// # Returns
            ///
            /// A sorted vector of tuples containing keys of type `H` and values of type `Y`.
            #[pyo3(signature = (reverse=true))]
            fn sort_by_value(&self, reverse: bool) -> Vec<($rustKey, $rustValue)> {
                self.0.sort_by_value(reverse)
            }

            /// Sorts the `AlgorithmResult` by its keys in ascending or descending order.
            ///
            /// # Arguments
            ///
            /// * `reverse`: If `true`, sorts the result in descending order; otherwise, sorts in ascending order.
            ///
            /// # Returns
            ///
            /// A sorted vector of tuples containing keys of type `H` and values of type `Y`.
            #[pyo3(signature = (reverse=true))]
            fn sort_by_key(&self, reverse: bool) -> Vec<($rustKey, $rustValue)> {
                self.0.sort_by_key(reverse)
            }

            /// Retrieves the top-k elements from the `AlgorithmResult` based on its values.
            ///
            /// # Arguments
            ///
            /// * `k`: The number of elements to retrieve.
            /// * `percentage`: If `true`, the `k` parameter is treated as a percentage of total elements.
            /// * `reverse`: If `true`, retrieves the elements in descending order; otherwise, in ascending order.
            ///
            /// # Returns
            ///
            /// An `Option` containing a vector of tuples with keys of type `H` and values of type `Y`.
            /// If `percentage` is `true`, the returned vector contains the top `k` percentage of elements.
            /// If `percentage` is `false`, the returned vector contains the top `k` elements.
            /// Returns `None` if the result is empty or if `k` is 0.
            #[pyo3(signature = (k, percentage=false, reverse=true))]
            fn top_k(
                &self,
                k: usize,
                percentage: bool,
                reverse: bool,
            ) -> Vec<($rustKey, $rustValue)> {
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
            /// Groups the `AlgorithmResult` by its values.
            ///
            /// # Returns
            ///
            /// A `HashMap` where keys are unique values from the `AlgorithmResult` and values are vectors
            /// containing keys of type `H` that share the same value.
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
    /// Returns all results as a dict
    #[pyo3(signature = ())]
    fn get_all(&self) -> std::collections::HashMap<String, f64> {
        self.0
            .get_all()
            .into_iter()
            .map(|(key, of)| (key.clone(), of.into_inner()))
            .collect()
    }

    /// Returns the value corresponding to the provided key
    ///
    /// # Arguments
    ///
    /// * `key`: The key of type `H` for which the value is to be retrieved.
    fn get(&self, key: String) -> Option<f64> {
        Some(self.0.get(&key).unwrap().0)
    }

    /// Sorts the `AlgorithmResult` by its values in ascending or descending order.
    ///
    /// # Arguments
    ///
    /// * `reverse`: If `true`, sorts the result in descending order; otherwise, sorts in ascending order.
    ///
    /// # Returns
    ///
    /// A sorted vector of tuples containing keys of type `H` and values of type `Y`.
    #[pyo3(signature = (reverse=true))]
    fn sort_by_value(&self, reverse: bool) -> Vec<(String, f64)> {
        self.0
            .sort_by_value(reverse)
            .into_iter()
            .map(|(key, ordered_float)| (key, ordered_float.into_inner()))
            .collect()
    }

    /// Sorts the `AlgorithmResult` by its keys in ascending or descending order.
    ///
    /// # Arguments
    ///
    /// * `reverse`: If `true`, sorts the result in descending order; otherwise, sorts in ascending order.
    ///
    /// # Returns
    ///
    /// A sorted vector of tuples containing keys of type `H` and values of type `Y`.
    #[pyo3(signature = (reverse=true))]
    fn sort_by_key(&self, reverse: bool) -> Vec<(String, f64)> {
        self.0
            .sort_by_key(reverse)
            .into_iter()
            .map(|(key, ordered_float)| (key, ordered_float.into_inner()))
            .collect()
    }

    /// Retrieves the top-k elements from the `AlgorithmResult` based on its values.
    ///
    /// # Arguments
    ///
    /// * `k`: The number of elements to retrieve.
    /// * `percentage`: If `true`, the `k` parameter is treated as a percentage of total elements.
    /// * `reverse`: If `true`, retrieves the elements in descending order; otherwise, in ascending order.
    ///
    /// # Returns
    ///
    /// An `Option` containing a vector of tuples with keys of type `H` and values of type `Y`.
    /// If `percentage` is `true`, the returned vector contains the top `k` percentage of elements.
    /// If `percentage` is `false`, the returned vector contains the top `k` elements.
    /// Returns `None` if the result is empty or if `k` is 0.
    #[pyo3(signature = (k, percentage=false, reverse=true))]
    fn top_k(&self, k: usize, percentage: bool, reverse: bool) -> Vec<(String, f64)> {
        self.0
            .top_k(k, percentage, reverse)
            .into_iter()
            .map(|vec| (vec.0, vec.1.into_inner()))
            .collect()
    }

    /// Creates a dataframe from the result
    ///
    /// # Returns
    ///
    /// A `pandas.DataFrame` containing the result
    #[pyo3(signature = ())]
    fn to_df(&self) -> PyResult<PyObject> {
        let hashmap: std::collections::HashMap<String, f64> = self
            .0
            .get_all()
            .clone()
            .into_iter()
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

    /// Groups the `AlgorithmResult` by its values.
    ///
    /// # Returns
    ///
    /// A `HashMap` where keys are unique values from the `AlgorithmResult` and values are vectors
    /// containing keys of type `H` that share the same value.
    #[pyo3(signature = ())]
    fn group_by(&self) -> std::collections::HashMap<String, Vec<String>> {
        let ordered_map = self.0.group_by();
        let mut f64_map: std::collections::HashMap<String, Vec<String>> =
            std::collections::HashMap::new();
        for (ordered_float, strings) in ordered_map {
            let f64_value = ordered_float.into_inner();
            f64_map.insert(f64_value.to_string().parse().unwrap(), strings);
        }
        f64_map
    }
}
