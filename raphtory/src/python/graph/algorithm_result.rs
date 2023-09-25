use crate::python::types::repr::Repr;
use ordered_float::OrderedFloat;
use pyo3::prelude::*;

/// Create a macro for py_algorithm_result
macro_rules! py_algorithm_result {
    ($name:ident, $rustKey:ty, $rustValue:ty, $rustSortValue:ty) => {
        #[pyclass]
        pub struct $name(
            $crate::algorithms::algorithm_result::AlgorithmResult<
                $rustKey,
                $rustValue,
                $rustSortValue,
            >,
        );

        impl Repr
            for $crate::algorithms::algorithm_result::AlgorithmResult<
                $rustKey,
                $rustValue,
                $rustSortValue,
            >
        {
            fn repr(&self) -> String {
                let algo_name = &self.algo_repr.algo_name;
                let num_vertices = &self.algo_repr.num_vertices;
                let result_type = &self.algo_repr.result_type;
                format!(
                    "Algorithm Name: {}, Number of Vertices: {}, Result Type: {}",
                    algo_name, num_vertices, result_type
                )
            }
        }

        impl pyo3::IntoPy<pyo3::PyObject>
            for $crate::algorithms::algorithm_result::AlgorithmResult<
                $rustKey,
                $rustValue,
                $rustSortValue,
            >
        {
            fn into_py(self, py: Python<'_>) -> pyo3::PyObject {
                $name(self).into_py(py)
            }
        }
    };

    ($name:ident, $rustKey:ty, $rustValue:ty) => {
        py_algorithm_result!($name, $rustKey, $rustValue, $rustValue);
    };
}

#[macro_export]
macro_rules! py_algorithm_result_base {
    ($name:ident, $rustKey:ty, $rustValue:ty) => {
        #[pymethods]
        impl $name {
            /// Returns a reference to the entire `result` hashmap.
            fn get_all(&self) -> std::collections::HashMap<$rustKey, $rustValue> {
                self.0.get_all().clone()
            }

            /// Returns a formatted string representation of the algorithm.
            fn to_string(&self) -> String {
                self.0.repr()
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

#[macro_export]
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

#[macro_export]
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

py_algorithm_result!(
    AlgorithmResultStrTupleF32F32,
    String,
    (f32, f32),
    (OrderedFloat<f32>, OrderedFloat<f32>)
);
py_algorithm_result_partial_ord!(AlgorithmResultStrTupleF32F32, String, (f32, f32));

py_algorithm_result!(AlgorithmResultStrVecI64Str, String, Vec<(i64, String)>);
py_algorithm_result_ord_hash_eq!(AlgorithmResultStrVecI64Str, String, Vec<(i64, String)>);

py_algorithm_result!(AlgorithmResultU64VecUsize, u64, Vec<usize>);
py_algorithm_result_ord_hash_eq!(AlgorithmResultU64VecUsize, u64, Vec<usize>);

py_algorithm_result!(AlgorithmResultStrF64, String, f64, OrderedFloat<f64>);
py_algorithm_result_partial_ord!(AlgorithmResultStrF64, String, f64);
