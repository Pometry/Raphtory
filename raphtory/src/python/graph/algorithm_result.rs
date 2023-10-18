use crate::{
    core::entities::vertices::vertex_ref::VertexRef, db::api::view::internal::DynamicGraph,
    python::types::repr::Repr,
};
use ordered_float::OrderedFloat;
use pyo3::prelude::*;

macro_rules! py_algorithm_result {
    ($objectName:ident, $rustGraph:ty, $rustValue:ty, $rustSortValue:ty) => {
        #[pyclass]
        pub struct $objectName(
            $crate::algorithms::algorithm_result::AlgorithmResult<
                $rustGraph,
                $rustValue,
                $rustSortValue,
            >,
        );

        impl Repr
            for $crate::algorithms::algorithm_result::AlgorithmResult<
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
            for $crate::algorithms::algorithm_result::AlgorithmResult<
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
macro_rules! py_algorithm_result_base {
    ($objectName:ident, $rustGraph:ty, $rustValue:ty, $rustOrderedValue:ty) => {
        #[pymethods]
        impl $objectName {
            /// Returns a Dict containing all the vertices (as keys) and their corresponding values (values) or none.
            ///
            /// Returns:
            ///     A dict of vertices and their values
            fn get_all(
                &self,
            ) -> std::collections::HashMap<
                $crate::db::graph::vertex::VertexView<$rustGraph>,
                Option<$rustValue>,
            > {
                self.0.get_all()
            }

            /// Returns a a list of all values
            fn get_all_values(&self) -> std::vec::Vec<$rustValue> {
                self.0.get_all_values().clone()
            }

            /// Returns a formatted string representation of the algorithm.
            fn to_string(&self) -> String {
                self.0.repr()
            }

            /// Returns the value corresponding to the provided key
            ///
            /// Arguments:
            ///     key: The key of type `H` for which the value is to be retrieved.
            fn get(&self, key: VertexRef) -> Option<$rustValue> {
                self.0.get(key).cloned()
            }

            /// Returns a dict with vertex names and values
            ///
            /// Returns:
            ///     a dict with vertex names and values
            fn get_all_with_names(&self) -> std::collections::HashMap<String, Option<$rustValue>> {
                self.0.get_all_with_names()
            }

            /// Sorts by vertex id in ascending or descending order.
            ///
            /// Arguments:
            ///     `reverse`: If `true`, sorts the result in descending order; otherwise, sorts in ascending order.
            ///
            /// Returns:
            ///     A sorted list of tuples containing vertex names and values.
            #[pyo3(signature = (reverse=true))]
            fn sort_by_vertex(
                &self,
                reverse: bool,
            ) -> std::vec::Vec<(
                $crate::db::graph::vertex::VertexView<$rustGraph>,
                Option<$rustValue>,
            )> {
                self.0.sort_by_vertex(reverse)
            }

            /// Creates a dataframe from the result
            ///
            /// Returns:
            ///     A `pandas.DataFrame` containing the result
            fn to_df(&self) -> PyResult<PyObject> {
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
            fn sort_by_value(
                &self,
                reverse: bool,
            ) -> std::vec::Vec<(
                $crate::db::graph::vertex::VertexView<$rustGraph>,
                Option<$rustValue>,
            )> {
                self.0.sort_by_value(reverse)
            }

            /// The function `sort_by_vertex_name` sorts a vector of tuples containing a vertex and an optional
            /// value by the vertex name in either ascending or descending order.
            ///
            /// Arguments:
            ///
            ///      reverse: A boolean value indicating whether the sorting should be done in reverse order or not.
            /// If reverse is true, the sorting will be done in descending order, otherwise it will be done in
            /// ascending order.
            ///
            /// Returns:
            ///     The function sort_by_vertex_name returns a vector of tuples. Each tuple contains a Vertex and value
            #[pyo3(signature = (reverse=true))]
            fn sort_by_vertex_name(
                &self,
                reverse: bool,
            ) -> std::vec::Vec<(
                $crate::db::graph::vertex::VertexView<$rustGraph>,
                Option<$rustValue>,
            )> {
                self.0.sort_by_vertex_name(reverse)
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
            ) -> std::vec::Vec<(
                $crate::db::graph::vertex::VertexView<$rustGraph>,
                Option<$rustValue>,
            )> {
                self.0.top_k(k, percentage, reverse)
            }

            /// Returns a tuple of the min result with its key
            fn min(
                &self,
            ) -> Option<(
                $crate::db::graph::vertex::VertexView<$rustGraph>,
                Option<$rustValue>,
            )> {
                self.0.min().map(|(k, v)| (k, v.map(|val| val)))
            }

            /// Returns a tuple of the max result with its key
            fn max(
                &self,
            ) -> Option<(
                $crate::db::graph::vertex::VertexView<$rustGraph>,
                Option<$rustValue>,
            )> {
                self.0.max().map(|(k, v)| (k, v.map(|val| val)))
            }

            /// Returns a tuple of the median result with its key
            fn median(
                &self,
            ) -> Option<(
                $crate::db::graph::vertex::VertexView<$rustGraph>,
                Option<$rustValue>,
            )> {
                self.0.median().map(|(k, v)| (k, v.map(|val| val)))
            }
        }
        py_algorithm_result_base!($objectName, $rustGraph, $rustValue, $rustOrderedValue);
    };
}

#[macro_export]
macro_rules! py_algorithm_result_new_ord_hash_eq {
    ($objectName:ident, $rustGraph:ty, $rustValue:ty, $rustOrderedValue:ty) => {
        #[pymethods]
        impl $objectName {
            /// Groups the `AlgorithmResult` by its values.
            ///
            /// Returns:
            ///     A `HashMap` where keys are unique values from the `AlgorithmResult` and values are vectors
            ///     containing keys of type `H` that share the same value.
            fn group_by(&self) -> std::collections::HashMap<$rustValue, Vec<String>> {
                self.0.group_by()
            }
        }
        py_algorithm_result_partial_ord!($objectName, $rustGraph, $rustValue, $rustOrderedValue);
    };
}

py_algorithm_result!(AlgorithmResult, DynamicGraph, String, String);
py_algorithm_result_new_ord_hash_eq!(AlgorithmResult, DynamicGraph, String, String);

py_algorithm_result!(AlgorithmResultStrF64, DynamicGraph, f64, OrderedFloat<f64>);
py_algorithm_result_partial_ord!(AlgorithmResultStrF64, DynamicGraph, f64, OrderedFloat<f64>);

py_algorithm_result!(AlgorithmResultStrU64, DynamicGraph, u64, u64);
py_algorithm_result_new_ord_hash_eq!(AlgorithmResultStrU64, DynamicGraph, u64, u64);

py_algorithm_result!(
    AlgorithmResultStrTupleF32F32,
    DynamicGraph,
    (f32, f32),
    (OrderedFloat<f32>, OrderedFloat<f32>)
);
py_algorithm_result_partial_ord!(
    AlgorithmResultStrTupleF32F32,
    DynamicGraph,
    (f32, f32),
    (f32, f32)
);

py_algorithm_result!(
    AlgorithmResultStrVecI64Str,
    DynamicGraph,
    Vec<(i64, String)>,
    Vec<(i64, String)>
);
py_algorithm_result_new_ord_hash_eq!(
    AlgorithmResultStrVecI64Str,
    DynamicGraph,
    Vec<(i64, String)>,
    Vec<(i64, String)>
);

py_algorithm_result!(
    AlgorithmResultU64VecUsize,
    DynamicGraph,
    Vec<usize>,
    Vec<usize>
);
py_algorithm_result_new_ord_hash_eq!(
    AlgorithmResultU64VecUsize,
    DynamicGraph,
    Vec<usize>,
    Vec<usize>
);

py_algorithm_result!(
    AlgorithmResultStrVecStr,
    DynamicGraph,
    Vec<String>,
    Vec<String>
);
py_algorithm_result_new_ord_hash_eq!(
    AlgorithmResultStrVecStr,
    DynamicGraph,
    Vec<String>,
    Vec<String>
);
