use crate::{
    db::api::view::internal::DynamicGraph,
    python::{graph::views::graph_view::PyGraphView, types::repr::Repr},
};
use ordered_float::OrderedFloat;
use pyo3::prelude::*;

macro_rules! py_algorithm_result_new {
    ($objectName:ident, $rustGraph:ty, $rustValue:ty, $rustSortValue:ty) => {
        #[pyclass]
        pub struct $objectName(
            $crate::algorithms::algorithm_result_new::AlgorithmResultNew<
                $rustGraph,
                $rustValue,
                $rustSortValue,
            >,
        );

        impl Repr
            for $crate::algorithms::algorithm_result_new::AlgorithmResultNew<
                $rustGraph,
                $rustValue,
                $rustSortValue,
            >
        {
            fn repr(&self) -> String {
                let algo_name = &self.algo_repr.algo_name;
                let num_vertices = &self.result.len();
                let result_type = &self.algo_repr.result_type;
                format!(
                    "Algorithm Name: {}, Number of Vertices: {}, Result Type: {}",
                    algo_name, num_vertices, result_type
                )
            }
        }

        impl pyo3::IntoPy<pyo3::PyObject>
            for $crate::algorithms::algorithm_result_new::AlgorithmResultNew<
                $rustGraph,
                $rustValue,
                $rustSortValue,
            >
        {
            fn into_py(self, py: Python<'_>) -> pyo3::PyObject {
                $objectName(self).into_py(py)
            }
        }
    };

    ($name:ident, $rustGraph:ty, $rustKey:ty, $rustSortValue:ty) => {
        py_algorithm_result!($name, $rustGraph, $rustKey, $rustSortValue);
    };
}

#[macro_export]
macro_rules! py_algorithm_result_new_base {
    ($objectName:ident, $rustGraph:ty, $rustValue:ty, $rustOrderedValue:ty) => {
        #[pymethods]
        impl $objectName {
            /// Returns a reference to the entire `result` hashmap.
            fn get_all_values(&self) -> std::vec::Vec<$rustValue> {
                self.0.get_all_values().clone()
            }

            /// Returns a formatted string representation of the algorithm.
            fn to_string(&self) -> String {
                self.0.repr()
            }

            // /// Returns the value corresponding to the provided key in the `result` hashmap.
            // ///
            // /// Arguments:
            // ///     key: The key of type `H` for which the value is to be retrieved.
            // fn get(&self, key: $rustOrderedValue) -> Option<$rustOrderedValue> {
            //     self.0.get(&key).cloned()
            // }

            // /// Creates a dataframe from the result
            // ///
            // /// Returns:
            // ///     A `pandas.DataFrame` containing the result
            // pub fn to_df(&self) -> PyResult<PyObject> {
            //     let hashmap = &self.0.result;
            //     let mut keys = Vec::new();
            //     let mut values = Vec::new();
            //     Python::with_gil(|py| {
            //         for (key, value) in hashmap.iter() {
            //             keys.push(key.to_object(py));
            //             values.push(value.to_object(py));
            //         }
            //         let dict = pyo3::types::PyDict::new(py);
            //         dict.set_item("Key", pyo3::types::PyList::new(py, keys.as_slice()))?;
            //         dict.set_item("Value", pyo3::types::PyList::new(py, values.as_slice()))?;
            //         let pandas = pyo3::types::PyModule::import(py, "pandas")?;
            //         let df: &PyAny = pandas.getattr("DataFrame")?.call1((dict,))?;
            //         Ok(df.to_object(py))
            //     })
            // }
        }
    };
}

#[macro_export]
macro_rules! py_algorithm_result_new_partial_ord {
    ($objectName:ident, $rustGraph:ty, $rustValue:ty, $rustOrderedValue:ty) => {
        #[pymethods]
        impl $objectName {
            /// Sorts the `AlgorithmResult` by its values in ascending or descending order.
            ///
            /// Arguments:
            ///     reverse (bool): If `true`, sorts the result in descending order; otherwise, sorts in ascending order.
            ///
            /// Returns:
            ///     A sorted vector of tuples containing keys of type `H` and values of type `Y`.
            #[pyo3(signature = (reverse=true))]
            fn sort_by_value(&self, reverse: bool) -> Vec<(String, Option<$rustValue>)> {
                self.0
                    .sort_by_value(reverse)
                    .iter()
                    .map(|(s, opt_f)| (s.clone(), opt_f.map(|&f| f)))
                    .collect()
            }

            /// Sorts the `AlgorithmResult` by its keys in ascending or descending order.
            ///
            /// Arguments:
            ///     reverse (bool): If `true`, sorts the result in descending order; otherwise, sorts in ascending order.
            ///
            /// Returns:
            ///     A sorted vector of tuples containing keys of type `H` and values of type `Y`.
            #[pyo3(signature = (reverse=true))]
            fn sort_by_key(&self, reverse: bool) -> Vec<(String, Option<$rustValue>)> {
                self.0
                    .sort_by_vertex_id(reverse)
                    .iter()
                    .map(|(s, opt_f)| (s.clone(), opt_f.map(|&f| f)))
                    .collect()
            }

            /// Retrieves the top-k elements from the `AlgorithmResult` based on its values.
            ///
            /// Arguments:
            ///     k (int): The number of elements to retrieve.
            ///     percentage (bool): If `true`, the `k` parameter is treated as a percentage of total elements.
            ///     reverse (bool): If `true`, retrieves the elements in descending order; otherwise, in ascending order.
            ///
            /// Returns:
            ///     An Option containing a vector of tuples with keys of type `H` and values of type `Y`.
            ///     If percentage is true, the returned vector contains the top `k` percentage of elements.
            ///     If percentage is false, the returned vector contains the top `k` elements.
            ///     Returns None if the result is empty or if `k` is 0.
            #[pyo3(signature = (k, percentage=false, reverse=true))]
            fn top_k(
                &self,
                k: usize,
                percentage: bool,
                reverse: bool,
            ) -> Vec<(String, Option<$rustValue>)> {
                self.0
                    .top_k(k, percentage, reverse)
                    .iter()
                    .map(|(s, opt_f)| (s.clone(), opt_f.map(|&f| f)))
                    .collect()
            }
        }
        py_algorithm_result_new_base!($objectName, $rustGraph, $rustValue, $rustOrderedValue);
    };
}

py_algorithm_result_new!(AlgorithmResultStrF64, DynamicGraph, f64, OrderedFloat<f64>);

py_algorithm_result_new_partial_ord!(AlgorithmResultStrF64, DynamicGraph, f64, OrderedFloat<f64>);
