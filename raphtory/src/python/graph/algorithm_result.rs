use crate::{
    algorithms::algorithm_result::AlgorithmResult as AlgorithmResultRs,
    core::entities::VID,
    db::api::view::{internal::DynamicGraph, StaticGraphViewOps},
    python::types::repr::{Repr, StructReprBuilder},
};
use ordered_float::OrderedFloat;
use pyo3::prelude::*;
use raphtory_api::core::entities::GID;

impl<G: StaticGraphViewOps, V: Repr + Clone, O> Repr for AlgorithmResultRs<G, V, O> {
    fn repr(&self) -> String {
        let algo_name = &self.algo_repr.algo_name;
        let num_nodes = &self.result.len();
        StructReprBuilder::new("AlgorithmResult")
            .add_field("name", algo_name)
            .add_field("num_nodes", num_nodes)
            .add_field("result", self.get_all_with_names())
            .finish()
    }
}

#[macro_export]
macro_rules! py_algorithm_result {
    ($objectName:ident, $rustGraph:ty, $rustValue:ty, $rustSortValue:ty) => {
        #[pyclass(module = "raphtory", frozen)]
        pub struct $objectName(
            $crate::algorithms::algorithm_result::AlgorithmResult<
                $rustGraph,
                $rustValue,
                $rustSortValue,
            >,
        );

        impl<'py> pyo3::IntoPyObject<'py>
            for $crate::algorithms::algorithm_result::AlgorithmResult<
                $rustGraph,
                $rustValue,
                $rustSortValue,
            >
        {
            type Target = $objectName;
            type Output = <Self::Target as pyo3::IntoPyObject<'py>>::Output;
            type Error = <Self::Target as pyo3::IntoPyObject<'py>>::Error;

            fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
                $objectName(self).into_pyobject(py)
            }
        }
    };
}

#[macro_export]
macro_rules! py_algorithm_result_base {
    ($objectName:ident, $rustGraph:ty, $rustValue:ty, $rustOrderedValue:ty) => {
        #[pymethods]
        impl $objectName {
            /// Returns a Dict containing all the nodes (as keys) and their corresponding values (values) or none.
            ///
            /// Returns:
            ///     dict[Node, Any]: A dict of nodes and their values
            fn get_all(
                &self,
            ) -> std::collections::HashMap<
                $crate::db::graph::node::NodeView<$rustGraph, $rustGraph>,
                $rustValue,
            > {
                self.0.get_all()
            }

            /// Get all values
            ///
            /// Returns:
            ///     list[Any]: the values for each node as a list
            fn get_all_values(&self) -> std::vec::Vec<$rustValue> {
                self.0.get_all_values().clone()
            }

            /// Returns the value corresponding to the provided key
            ///
            /// Arguments:
            ///     key (InputNode): The node for which the value is to be retrieved.
            ///
            /// Returns:
            ///     Optional[Any]: The value for the node or `None` if the value does not exist.
            fn get(&self, key: $crate::python::utils::PyNodeRef) -> Option<$rustValue> {
                self.0.get(key).cloned()
            }

            /// Returns a dict with node names and values
            ///
            /// Returns:
            ///     dict[str, Any]: a dict with node names and values
            fn get_all_with_names(&self) -> std::collections::HashMap<String, $rustValue> {
                self.0.get_all_with_names()
            }

            /// Sorts by node id in ascending or descending order.
            ///
            /// Arguments:
            ///     reverse (bool): If `true`, sorts the result in descending order; otherwise, sorts in ascending order. Defaults to True.
            ///
            /// Returns:
            ///     list[Tuple[Node, Any]]: A sorted list of tuples containing nodes and values.
            #[pyo3(signature = (reverse=true))]
            fn sort_by_node(
                &self,
                reverse: bool,
            ) -> std::vec::Vec<(
                $crate::db::graph::node::NodeView<$rustGraph, $rustGraph>,
                $rustValue,
            )> {
                self.0.sort_by_node(reverse)
            }

            fn __len__(&self) -> usize {
                self.0.len()
            }

            fn __repr__(&self) -> String {
                self.0.repr()
            }

            /// Creates a dataframe from the result
            ///
            /// Returns:
            ///     DataFrame: A `pandas.DataFrame` containing the result
            fn to_df(&self) -> PyResult<PyObject> {
                let hashmap = &self.0.result;
                let mut keys = Vec::new();
                let mut values = Vec::new();
                Python::with_gil(|py| {
                    for (key, value) in hashmap.iter() {
                        let node = $crate::db::api::view::internal::core_ops::CoreGraphOps::node_id(
                            &self.0.graph,
                            VID(*key),
                        );
                        keys.push(node.into_pyobject(py)?.into_any().unbind());
                        values.push(value.into_pyobject(py)?.into_any().unbind());
                    }
                    let dict = pyo3::types::PyDict::new(py);
                    dict.set_item("Node", pyo3::types::PyList::new(py, keys.as_slice())?)?;
                    dict.set_item("Value", pyo3::types::PyList::new(py, values.as_slice())?)?;
                    let pandas = pyo3::types::PyModule::import(py, "pandas")?;
                    let df = pandas.getattr("DataFrame")?.call1((dict,))?;
                    Ok(df.unbind())
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
            ///     reverse (bool): If `true`, sorts the result in descending order, otherwise, sorts in ascending order. Defaults to True.
            ///
            /// Returns:
            ///     list[Tuple[Node, Any]]: A sorted vector of tuples containing Nodes and values.
            #[pyo3(signature = (reverse=true))]
            fn sort_by_value(
                &self,
                reverse: bool,
            ) -> std::vec::Vec<(
                $crate::db::graph::node::NodeView<$rustGraph, $rustGraph>,
                $rustValue,
            )> {
                self.0.sort_by_value(reverse)
            }

            /// The function `sort_by_node_name` sorts a vector of tuples containing a node and an optional
            /// value by the node name in either ascending or descending order.
            ///
            /// Arguments:
            ///     reverse (bool): A boolean value indicating whether the sorting should be done in reverse order or not. Defaults to True.
            ///         If reverse is true, the sorting will be done in descending order, otherwise it will be done in
            ///         ascending order.
            ///
            /// Returns:
            ///     list[Tuple[Node, Any]]: The function sort_by_node_name returns a vector of tuples. Each tuple contains a Node and value
            #[pyo3(signature = (reverse=true))]
            fn sort_by_node_name(
                &self,
                reverse: bool,
            ) -> std::vec::Vec<(
                $crate::db::graph::node::NodeView<$rustGraph, $rustGraph>,
                $rustValue,
            )> {
                self.0.sort_by_node_name(reverse)
            }

            /// Retrieves the top-k elements from the `AlgorithmResult` based on its values.
            ///
            /// Arguments:
            ///     k (int): The number of elements to retrieve.
            ///     percentage (bool): If `True`, the `k` parameter is treated as a percentage of total elements. Defaults to False.
            ///     reverse (bool): If `True`, retrieves the elements in descending order, otherwise, in ascending order. Defaults to True.
            ///
            /// Returns:
            ///     list[Tuple[Node, Any]]: List of tuples with keys of nodes and values of type `Y`.
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
                $crate::db::graph::node::NodeView<$rustGraph, $rustGraph>,
                $rustValue,
            )> {
                self.0.top_k(k, percentage, reverse)
            }

            /// Find node with minimum value
            ///
            /// Returns:
            ///     Tuple[Node, Any]: The node and minimum value.
            fn min(
                &self,
            ) -> Option<(
                $crate::db::graph::node::NodeView<$rustGraph, $rustGraph>,
                $rustValue,
            )> {
                self.0.min()
            }

            /// Find node with maximum value
            ///
            /// Returns:
            ///     Tuple[Node, Any]: The node and maximum value.
            fn max(
                &self,
            ) -> Option<(
                $crate::db::graph::node::NodeView<$rustGraph, $rustGraph>,
                $rustValue,
            )> {
                self.0.max()
            }

            /// Returns a tuple of the median result with its key
            ///
            /// Returns:
            /// Optional[Tuple[Node, Any]]: The node with median value or `None` if there are no nodes.
            fn median(
                &self,
            ) -> Option<(
                $crate::db::graph::node::NodeView<$rustGraph, $rustGraph>,
                $rustValue,
            )> {
                self.0.median()
            }
        }
        $crate::py_algorithm_result_base!($objectName, $rustGraph, $rustValue, $rustOrderedValue);
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
            ///     dict[Any, list[str]]: A mapping where keys are unique values from the `AlgorithmResult` and values are lists of nodes
            ///                           that share the same value.
            fn group_by(&self) -> std::collections::HashMap<$rustValue, Vec<String>> {
                self.0.group_by()
            }
        }
        $crate::py_algorithm_result_partial_ord!(
            $objectName,
            $rustGraph,
            $rustValue,
            $rustOrderedValue
        );
    };
}

py_algorithm_result!(AlgorithmResult, DynamicGraph, String, String);
py_algorithm_result_new_ord_hash_eq!(AlgorithmResult, DynamicGraph, String, String);

py_algorithm_result!(AlgorithmResultF64, DynamicGraph, f64, OrderedFloat<f64>);
py_algorithm_result_partial_ord!(AlgorithmResultF64, DynamicGraph, f64, OrderedFloat<f64>);

py_algorithm_result!(AlgorithmResultU64, DynamicGraph, u64, u64);
py_algorithm_result_new_ord_hash_eq!(AlgorithmResultU64, DynamicGraph, u64, u64);

py_algorithm_result!(AlgorithmResultGID, DynamicGraph, GID, GID);
py_algorithm_result_new_ord_hash_eq!(AlgorithmResultGID, DynamicGraph, GID, GID);

py_algorithm_result!(
    AlgorithmResultTupleF32F32,
    DynamicGraph,
    (f32, f32),
    (OrderedFloat<f32>, OrderedFloat<f32>)
);
py_algorithm_result_partial_ord!(
    AlgorithmResultTupleF32F32,
    DynamicGraph,
    (f32, f32),
    (f32, f32)
);

py_algorithm_result!(
    AlgorithmResultVecI64Str,
    DynamicGraph,
    Vec<(i64, String)>,
    Vec<(i64, String)>
);
py_algorithm_result_new_ord_hash_eq!(
    AlgorithmResultVecI64Str,
    DynamicGraph,
    Vec<(i64, String)>,
    Vec<(i64, String)>
);

py_algorithm_result!(
    AlgorithmResultVecF64,
    DynamicGraph,
    Vec<f64>,
    Vec<OrderedFloat<f64>>
);
py_algorithm_result_partial_ord!(
    AlgorithmResultVecF64,
    DynamicGraph,
    Vec<f64>,
    Vec<OrderedFloat<f64>>
);

py_algorithm_result!(AlgorithmResultUsize, DynamicGraph, usize, usize);
py_algorithm_result_new_ord_hash_eq!(AlgorithmResultUsize, DynamicGraph, usize, usize);

py_algorithm_result!(
    AlgorithmResultVecUsize,
    DynamicGraph,
    Vec<usize>,
    Vec<usize>
);
py_algorithm_result_new_ord_hash_eq!(
    AlgorithmResultVecUsize,
    DynamicGraph,
    Vec<usize>,
    Vec<usize>
);

py_algorithm_result!(AlgorithmResultU64VecU64, DynamicGraph, Vec<u64>, Vec<u64>);
py_algorithm_result_new_ord_hash_eq!(AlgorithmResultU64VecU64, DynamicGraph, Vec<u64>, Vec<u64>);

py_algorithm_result!(AlgorithmResultGIDVecGID, DynamicGraph, Vec<GID>, Vec<GID>);
py_algorithm_result_new_ord_hash_eq!(AlgorithmResultGIDVecGID, DynamicGraph, Vec<GID>, Vec<GID>);

py_algorithm_result!(
    AlgorithmResultVecStr,
    DynamicGraph,
    Vec<String>,
    Vec<String>
);
py_algorithm_result_new_ord_hash_eq!(
    AlgorithmResultVecStr,
    DynamicGraph,
    Vec<String>,
    Vec<String>
);
