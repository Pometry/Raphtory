use crate::{core::entities::nodes::node_ref::NodeRef, prelude::NodeViewOps};
use ordered_float::OrderedFloat;
use std::{
    collections::{hash_map::Iter, HashMap},
    hash::Hash,
    marker::PhantomData,
};

pub trait AsOrd<T: ?Sized + Ord> {
    /// Converts reference of this type into reference of an ordered Type.
    ///
    /// This is the same as AsRef (with the additional constraint that the target type needs to be ordered).
    ///
    /// Importantly, unlike AsRef, this blanket-implements the trivial conversion from a type to itself!
    fn as_ord(&self) -> &T;
}

impl<T: Ord> AsOrd<T> for T {
    fn as_ord(&self) -> &T {
        self
    }
}

impl<T: FloatCore> AsOrd<OrderedFloat<T>> for T {
    fn as_ord(&self) -> &OrderedFloat<T> {
        self.into()
    }
}

impl<T: FloatCore> AsOrd<(OrderedFloat<T>, OrderedFloat<T>)> for (T, T) {
    fn as_ord(&self) -> &(OrderedFloat<T>, OrderedFloat<T>) {
        // Safety: OrderedFloat is #[repr(transparent)] and has no invalid values, i.e. there is no physical difference between OrderedFloat and Float.
        unsafe { &*(self as *const (T, T) as *const (OrderedFloat<T>, OrderedFloat<T>)) }
    }
}

/// An 'AlgorithmRepr' struct that represents the string output in the terminal after running an algorithm.
///
/// It returns the algorithm name, number of nodes in the graph, and the result type.
///
pub struct AlgorithmRepr {
    pub algo_name: String,
    pub result_type: String,
}

/// A generic `AlgorithmResult` struct that represents the result of an algorithm computation.
///
/// The `AlgorithmResult` contains a hashmap, where keys (`H`) are cloneable, hashable, and comparable,
/// and values (`Y`) are cloneable. The keys and values can be of any type that satisfies the specified
/// trait bounds.
///
/// This `AlgorithmResult` is returned for all algorithms that return a HashMap
///
pub struct AlgorithmResult<G, V, O = V> {
    /// The result hashmap that stores keys of type `H` and values of type `Y`.
    pub algo_repr: AlgorithmRepr,
    pub graph: G,
    pub result: HashMap<usize, V>,
    marker: PhantomData<O>,
}

// use pyo3::{prelude::*, types::IntoPyDict};

impl<'graph, G, V, O> AlgorithmResult<G, V, O>
where
    G: GraphViewOps<'graph>,
    V: Clone,
{
    /// Creates a new instance of `AlgorithmResult` with the provided hashmap.
    ///
    /// Arguments:
    ///
    /// * `algo_name`: The name of the algorithm.
    /// * `result_type`: The type of the result.
    /// * `result`: A `Vec` with values of type `V`.
    /// * `graph`: The Raphtory Graph object
    pub fn new(graph: G, algo_name: &str, result_type: &str, result: HashMap<usize, V>) -> Self {
        // let result_type = type_name::<V>();
        Self {
            algo_repr: AlgorithmRepr {
                algo_name: algo_name.to_string(),
                result_type: result_type.to_string(),
            },
            graph,
            result,
            marker: PhantomData,
        }
    }

    /// Returns a reference to the entire `result` vector of values.
    pub fn get_all_values(&self) -> Vec<V> {
        self.result.clone().into_values().collect()
    }

    /// Returns the value corresponding to the provided key in the `result` hashmap.
    ///
    /// Arguments:
    ///     `key`: The key of the node, can be the node object, or name.
    pub fn get<T: Into<NodeRef>>(&self, name: T) -> Option<&V> {
        let v = name.into();
        if self.graph.has_node(v) {
            let internal_id = self.graph.node(v).unwrap().node.0;
            self.result.get(&internal_id)
        } else {
            None
        }
    }

    /// Returns a hashmap with node names and values
    ///
    /// Returns:
    ///     a hashmap with node names and values
    pub fn get_all_with_names(&self) -> HashMap<String, V> {
        self.result
            .iter()
            .map(|(k, v)| (self.graph.node_name(VID(*k)), v.clone()))
            .collect()
    }

    /// Returns a `HashMap` containing `NodeView<G>` keys and `Option<V>` values.
    ///
    /// Returns:
    ///     a `HashMap` containing `NodeView<G>` keys and `Option<V>` values.
    pub fn get_all(&self) -> HashMap<NodeView<G, G>, V> {
        self.result
            .iter()
            .map(|(k, v)| {
                (
                    NodeView::new_internal(self.graph.clone(), VID(*k)),
                    v.clone(),
                )
            })
            .collect()
    }

    /// Sorts the `AlgorithmResult` by its node id in ascending or descending order.
    ///
    /// Arguments:
    ///
    /// * `reverse`: If `true`, sorts the result in descending order; otherwise, sorts in ascending order.
    ///
    /// Returns:
    ///
    /// A sorted vector of tuples containing node names and values.
    pub fn sort_by_node(&self, reverse: bool) -> Vec<(NodeView<G, G>, V)> {
        let mut sorted: Vec<(NodeView<G, G>, V)> = self.get_all().into_iter().collect();
        sorted.sort_by(|(a, _), (b, _)| if reverse { b.cmp(a) } else { a.cmp(b) });
        sorted
    }

    /// Sorts a collection of node views by their names in either ascending or descending order.
    ///
    /// Arguments:
    ///
    /// * `reverse`: A boolean value indicating whether the sorting should be in reverse order or not. If
    /// `reverse` is `true`, the sorting will be in reverse order (descending), otherwise it will be in
    /// ascending order.
    ///
    /// Returns:
    ///
    /// The function `sort_by_node_name` returns a vector of tuples, where each tuple contains a
    /// `NodeView<G>` and an optional `V` value.
    pub fn sort_by_node_name(&self, reverse: bool) -> Vec<(NodeView<G, G>, V)> {
        let mut sorted: Vec<(NodeView<G, G>, V)> = self.get_all().into_iter().collect();
        sorted.sort_by(|(a, _), (b, _)| {
            if reverse {
                b.name().cmp(&a.name())
            } else {
                a.name().cmp(&b.name())
            }
        });
        sorted
    }

    /// The `iter` function returns an iterator over the elements of the `result` field.
    ///
    /// Returns:
    ///
    /// The `iter` method returns an iterator over the elements of the `result` field of the struct. The
    /// iterator yields references to tuples containing a `usize` key and a `V` value.
    pub fn iter(&self) -> Iter<'_, usize, V> {
        self.result.iter()
    }

    /// Sorts the `AlgorithmResult` by its values in ascending or descending order.
    ///
    /// Arguments:
    ///
    /// * `reverse`: If `true`, sorts the result in descending order; otherwise, sorts in ascending order.
    ///
    /// Returns:
    ///
    /// A sorted vector of tuples containing node names and values.
    pub fn sort_by_values<F: FnMut(&V, &V) -> std::cmp::Ordering>(
        &self,
        mut cmp: F,
        reverse: bool,
    ) -> Vec<(NodeView<G, G>, V)> {
        let mut all_as_vec: Vec<(NodeView<G, G>, V)> = self.get_all().into_iter().collect();
        all_as_vec.sort_by(|a, b| {
            let order = cmp(&a.1, &b.1);
            // Reverse the order if `reverse` is true
            if reverse {
                order.reverse()
            } else {
                order
            }
        });
        all_as_vec
    }

    /// Retrieves the top-k elements from the `AlgorithmResult` based on its values.
    ///
    /// Arguments:
    ///
    /// * `k`: The number of elements to retrieve.
    /// * `percentage`: If `true`, the `k` parameter is treated as a percentage of total elements.
    /// * `reverse`: If `true`, retrieves the elements in descending order; otherwise, in ascending order.
    ///
    /// Returns:
    ///
    /// An `a vector of tuples with keys of type `H` and values of type `Y`.
    /// If `percentage` is `true`, the returned vector contains the top `k` percentage of elements.
    /// If `percentage` is `false`, the returned vector contains the top `k` elements.
    /// Returns empty vec if the result is empty or if `k` is 0.
    pub fn top_k_by<F: FnMut(&V, &V) -> std::cmp::Ordering>(
        &self,
        cmp: F,
        k: usize,
        percentage: bool,
        reverse: bool,
    ) -> Vec<(NodeView<G, G>, V)> {
        let k = if percentage {
            let total_count = self.result.len();
            (total_count as f64 * (k as f64 / 100.0)) as usize
        } else {
            k
        };
        self.sort_by_values(cmp, reverse)
            .into_iter()
            .take(k)
            .collect()
    }

    pub fn min_by<F: FnMut(&V, &V) -> std::cmp::Ordering>(
        &self,
        mut cmp: F,
    ) -> Option<(NodeView<G, G>, V)> {
        let min_element = self
            .get_all()
            .into_iter()
            .min_by(|(_, a_value), (_, b_value)| cmp(a_value, b_value));

        // Clone the key and value
        min_element.map(|(k, v)| (k.clone(), v.clone()))
    }

    pub fn max_by<F: FnMut(&V, &V) -> std::cmp::Ordering>(
        &self,
        mut cmp: F,
    ) -> Option<(NodeView<G, G>, V)> {
        let max_element = self
            .get_all()
            .into_iter()
            .max_by(|(_, a_value), (_, b_value)| cmp(a_value, b_value));

        // Clone the key and value
        max_element.map(|(k, v)| (k.clone(), v.clone()))
    }

    pub fn median_by<F: FnMut(&V, &V) -> std::cmp::Ordering>(
        &self,
        mut cmp: F,
    ) -> Option<(NodeView<G, G>, V)> {
        // Assuming self.result is Vec<(String, Option<V>)>
        let mut items: Vec<(NodeView<G, G>, V)> = self.get_all().into_iter().collect();
        let len = items.len();
        if len == 0 {
            return None;
        }

        items.sort_by(|(_, a_value), (_, b_value)| cmp(a_value, b_value));
        let median_index = len / 2;

        Some((items[median_index].0.clone(), items[median_index].1.clone()))
    }

    pub fn len(&self) -> usize {
        self.result.len()
    }
}

impl<'graph, G, V, O> AlgorithmResult<G, V, O>
where
    G: GraphViewOps<'graph>,
    V: Clone,
    O: Ord,
    V: AsOrd<O>,
{
    pub fn group_by(&self) -> HashMap<V, Vec<String>>
    where
        V: Eq + Hash,
    {
        let mut groups: HashMap<V, Vec<String>> = HashMap::new();

        for node in self.graph.nodes() {
            if let Some(value) = self.result.get(&node.node.0) {
                let entry = groups.entry(value.clone()).or_default();
                entry.push(node.name().to_string());
            }
        }
        groups
    }

    /// Sorts the `AlgorithmResult` by its values in ascending or descending order.
    ///
    /// Arguments:
    ///
    /// * `reverse`: If `true`, sorts the result in descending order; otherwise, sorts in ascending order.
    ///
    /// Returns:
    ///
    /// A sorted vector of tuples containing keys of type `H` and values of type `Y`.
    pub fn sort_by_value(&self, reverse: bool) -> Vec<(NodeView<G, G>, V)> {
        self.sort_by_values(|a, b| O::cmp(a.as_ord(), b.as_ord()), reverse)
    }

    /// Retrieves the top-k elements from the `AlgorithmResult` based on its values.
    ///
    /// Arguments:
    ///
    /// * `k`: The number of elements to retrieve.
    /// * `percentage`: If `true`, the `k` parameter is treated as a percentage of total elements.
    /// * `reverse`: If `true`, retrieves the elements in descending order; otherwise, in ascending order.
    ///
    /// Returns:
    ///
    /// An `a vector of tuples with keys of type `H` and values of type `Y`.
    /// If `percentage` is `true`, the returned vector contains the top `k` percentage of elements.
    /// If `percentage` is `false`, the returned vector contains the top `k` elements.
    /// Returns empty vec if the result is empty or if `k` is 0.
    pub fn top_k(&self, k: usize, percentage: bool, reverse: bool) -> Vec<(NodeView<G, G>, V)> {
        self.top_k_by(
            |a, b| O::cmp(a.as_ord(), b.as_ord()),
            k,
            percentage,
            reverse,
        )
    }

    /// Returns a tuple of the min result with its key
    pub fn min(&self) -> Option<(NodeView<G, G>, V)> {
        self.min_by(|a, b| O::cmp(a.as_ord(), b.as_ord()))
    }

    /// Returns a tuple of the max result with its key
    pub fn max(&self) -> Option<(NodeView<G, G>, V)> {
        self.max_by(|a, b| O::cmp(a.as_ord(), b.as_ord()))
    }

    /// Returns a tuple of the median result with its key
    pub fn median(&self) -> Option<(NodeView<G, G>, V)> {
        self.median_by(|a, b| O::cmp(a.as_ord(), b.as_ord()))
    }
}

use crate::{core::entities::VID, db::graph::node::NodeView, prelude::GraphViewOps};
use num_traits::float::FloatCore;
use std::fmt;

impl<'graph, G: GraphViewOps<'graph>, V: fmt::Debug, O> fmt::Display for AlgorithmResult<G, V, O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "AlgorithmResultNew {{")?;
        writeln!(f, "  Algorithm Name: {}", self.algo_repr.algo_name)?;
        writeln!(f, "  Result Type: {}", self.algo_repr.result_type)?;
        writeln!(f, "  Number of Nodes: {}", self.result.len())?;
        writeln!(f, "  Results: [")?;

        for node in self.graph.nodes().iter() {
            let value = self.result.get(&node.node.0);
            writeln!(f, "    {}: {:?}", node.name(), value)?;
        }

        writeln!(f, "  ]")?;
        writeln!(f, "}}")
    }
}

impl<'graph, G: GraphViewOps<'graph>, V: fmt::Debug, O> fmt::Debug for AlgorithmResult<G, V, O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "AlgorithmResultNew {{")?;
        writeln!(f, "  Algorithm Name: {:?}", self.algo_repr.algo_name)?;
        writeln!(f, "  Result Type: {:?}", self.algo_repr.result_type)?;
        writeln!(f, "  Number of Nodes: {:?}", self.result.len())?;
        writeln!(f, "  Results: [")?;

        for node in self.graph.nodes().iter() {
            let value = self.result.get(&node.node.0);
            writeln!(f, "    {:?}: {:?}", node.name(), value)?;
        }

        writeln!(f, "  ]")?;
        writeln!(f, "}}")
    }
}

/// Add tests for all functions
#[cfg(test)]
mod algorithm_result_test {
    use super::*;
    use crate::{
        algorithms::components::weakly_connected_components,
        db::{api::mutation::AdditionOps, graph::graph::Graph},
        prelude::{NO_PROPS, *},
    };
    use ordered_float::OrderedFloat;

    fn create_algo_result_u64() -> AlgorithmResult<Graph, u64> {
        let g = create_graph();
        let mut map: HashMap<usize, u64> = HashMap::new();
        map.insert(g.node("A").unwrap().node.0, 10);
        map.insert(g.node("B").unwrap().node.0, 20);
        map.insert(g.node("C").unwrap().node.0, 30);
        let results_type = std::any::type_name::<u64>();
        AlgorithmResult::new(g, "create_algo_result_u64_test", results_type, map)
    }

    fn create_graph() -> Graph {
        let g = Graph::new();
        g.add_node(0, "A", NO_PROPS, None)
            .expect("Could not add node to graph");
        g.add_node(0, "B", NO_PROPS, None)
            .expect("Could not add node to graph");
        g.add_node(0, "C", NO_PROPS, None)
            .expect("Could not add node to graph");
        g.add_node(0, "D", NO_PROPS, None)
            .expect("Could not add node to graph");
        g
    }

    fn group_by_test() -> AlgorithmResult<Graph, u64> {
        let g = create_graph();
        let mut map: HashMap<usize, u64> = HashMap::new();
        map.insert(g.node("A").unwrap().node.0, 10);
        map.insert(g.node("B").unwrap().node.0, 20);
        map.insert(g.node("C").unwrap().node.0, 30);
        map.insert(g.node("D").unwrap().node.0, 10);
        let results_type = std::any::type_name::<u64>();
        AlgorithmResult::new(g, "group_by_test", results_type, map)
    }

    fn create_algo_result_f64() -> AlgorithmResult<Graph, f64, OrderedFloat<f64>> {
        let g = Graph::new();
        g.add_node(0, "A", NO_PROPS, None)
            .expect("Could not add node to graph");
        g.add_node(0, "B", NO_PROPS, None)
            .expect("Could not add node to graph");
        g.add_node(0, "C", NO_PROPS, None)
            .expect("Could not add node to graph");
        g.add_node(0, "D", NO_PROPS, None)
            .expect("Could not add node to graph");
        let mut map: HashMap<usize, f64> = HashMap::new();
        map.insert(g.node("A").unwrap().node.0, 10.0);
        map.insert(g.node("B").unwrap().node.0, 20.0);
        map.insert(g.node("C").unwrap().node.0, 30.0);
        let results_type = std::any::type_name::<f64>();
        AlgorithmResult::new(g, "create_algo_result_u64_test", results_type, map)
    }

    fn create_algo_result_tuple(
    ) -> AlgorithmResult<Graph, (f32, f32), (OrderedFloat<f32>, OrderedFloat<f32>)> {
        let g = create_graph();
        let mut res: HashMap<usize, (f32, f32)> = HashMap::new();
        res.insert(g.node("A").unwrap().node.0, (10.0, 20.0));
        res.insert(g.node("B").unwrap().node.0, (20.0, 30.0));
        res.insert(g.node("C").unwrap().node.0, (30.0, 40.0));
        let results_type = std::any::type_name::<(f32, f32)>();
        AlgorithmResult::new(g, "create_algo_result_tuple", results_type, res)
    }

    fn create_algo_result_hashmap_vec() -> AlgorithmResult<Graph, Vec<(i32, String)>> {
        let g = create_graph();
        let mut res: HashMap<usize, Vec<(i32, String)>> = HashMap::new();
        res.insert(g.node("A").unwrap().node.0, vec![(11, "H".to_string())]);
        res.insert(g.node("B").unwrap().node.0, vec![]);
        res.insert(
            g.node("C").unwrap().node.0,
            vec![(22, "E".to_string()), (33, "F".to_string())],
        );
        let results_type = std::any::type_name::<(i32, String)>();
        AlgorithmResult::new(g, "create_algo_result_hashmap_vec", results_type, res)
    }

    #[test]
    fn test_min_max_value() {
        let algo_result = create_algo_result_u64();
        let v_a = algo_result.graph.node("A".to_string()).unwrap();
        let v_b = algo_result.graph.node("B".to_string()).unwrap();
        let v_c = algo_result.graph.node("C".to_string()).unwrap();
        assert_eq!(algo_result.min(), Some((v_a.clone(), 10u64)));
        assert_eq!(algo_result.max(), Some((v_c.clone(), 30u64)));
        assert_eq!(algo_result.median(), Some((v_b.clone(), 20u64)));
        let algo_result = create_algo_result_f64();
        assert_eq!(algo_result.min(), Some((v_a, 10.0)));
        assert_eq!(algo_result.max(), Some((v_c, 30.0)));
        assert_eq!(algo_result.median(), Some((v_b, 20.0)));
    }

    #[test]
    fn test_get() {
        let algo_result = create_algo_result_u64();
        let node_c = algo_result.graph.node("C").unwrap();
        let node_d = algo_result.graph.node("D").unwrap();
        assert_eq!(algo_result.get(node_c.clone()), Some(&30u64));
        assert_eq!(algo_result.get(node_d.clone()), None);
        let algo_result = create_algo_result_f64();
        assert_eq!(algo_result.get(node_c.clone()), Some(&30.0f64));
        let algo_result = create_algo_result_tuple();
        assert_eq!(algo_result.get(node_c.clone()).unwrap().0, 30.0f32);
        let algo_result = create_algo_result_hashmap_vec();
        let answer = algo_result.get(node_c.clone()).unwrap().get(0).unwrap().0;
        assert_eq!(answer, 22i32);
    }

    #[test]
    fn test_sort() {
        let algo_result = create_algo_result_u64();
        let sorted = algo_result.sort_by_value(true);
        let v_c = algo_result.graph.node("C").unwrap();
        let v_b = algo_result.graph.node("B").unwrap();
        let v_a = algo_result.graph.node("A").unwrap();
        assert_eq!(sorted[0].0, v_c.clone());
        let sorted = algo_result.sort_by_value(false);
        assert_eq!(sorted[0].0, v_a.clone());

        let algo_result = create_algo_result_f64();
        let sorted = algo_result.sort_by_value(true);
        assert_eq!(sorted[0].0, v_c.clone());
        let sorted = algo_result.sort_by_value(false);
        assert_eq!(sorted[0].0, v_a);
        assert_eq!(sorted[1].0, v_b);

        let algo_result = create_algo_result_tuple();
        assert_eq!(algo_result.sort_by_value(true)[0].0, v_c.clone());

        let algo_result = create_algo_result_hashmap_vec();
        assert_eq!(algo_result.sort_by_value(true)[0].0, v_c);
    }

    #[test]
    fn test_top_k() {
        let algo_result = create_algo_result_u64();
        let v_c = algo_result.graph.node("C").unwrap();
        let v_a = algo_result.graph.node("A").unwrap();
        let v_b = algo_result.graph.node("B").unwrap();

        let top_k = algo_result.top_k(2, false, false);
        assert_eq!(top_k[0].0, v_a.clone());
        let top_k = algo_result.top_k(2, false, true);
        assert_eq!(top_k[0].0, v_c.clone());

        let algo_result = create_algo_result_f64();
        let top_k = algo_result.top_k(2, false, false);
        assert_eq!(top_k[0].0, v_a.clone());
        assert_eq!(top_k[1].0, v_b.clone());
        let top_k = algo_result.top_k(2, false, true);
        assert_eq!(top_k[0].0, v_c);

        let algo_result = create_algo_result_tuple();
        assert_eq!(algo_result.top_k(2, false, false)[0].0, v_a.clone());
        assert_eq!(algo_result.top_k(2, false, false)[1].0, v_b.clone());

        let algo_result = create_algo_result_hashmap_vec();
        assert_eq!(algo_result.top_k(2, false, false)[0].0, v_b);
        assert_eq!(algo_result.top_k(2, false, false)[1].0, v_a);
    }

    #[test]
    fn test_group_by() {
        let algo_result = group_by_test();
        let grouped = algo_result.group_by();
        assert_eq!(grouped.get(&10).unwrap().len(), 2);
        assert!(grouped.get(&10).unwrap().contains(&"A".to_string()));
        assert!(!grouped.get(&10).unwrap().contains(&"B".to_string()));

        let algo_result = create_algo_result_hashmap_vec();
        assert_eq!(
            algo_result
                .group_by()
                .get(&vec![(11, "H".to_string())])
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn test_get_all_with_names() {
        let algo_result = create_algo_result_u64();
        let all = algo_result.get_all_values();
        assert_eq!(all.len(), 3);

        let algo_result = create_algo_result_f64();
        let all = algo_result.get_all_values();
        assert_eq!(all.len(), 3);

        let algo_result = create_algo_result_tuple();
        let algo_results_hashmap = algo_result.get_all_with_names();
        let tuple_result = algo_results_hashmap.get("A").unwrap();
        assert_eq!(tuple_result.0, 10.0);
        assert_eq!(algo_result.get_all_values().len(), 3);

        let algo_result = create_algo_result_hashmap_vec();
        let algo_results_hashmap = algo_result.get_all_with_names();
        let tuple_result = algo_results_hashmap.get("A").unwrap();
        assert_eq!(tuple_result.clone().get(0).unwrap().0, 11);
        assert_eq!(algo_result.get_all_values().len(), 3);
    }

    #[test]
    fn test_sort_by_node() {
        let algo_result = create_algo_result_u64();
        let v_c = algo_result.graph.node("C").unwrap();
        let v_a = algo_result.graph.node("A").unwrap();
        let v_b = algo_result.graph.node("B").unwrap();
        let sorted = algo_result.sort_by_node(true);
        let my_array: Vec<(NodeView<Graph, Graph>, u64)> =
            vec![(v_c, 30u64), (v_b, 20u64), (v_a, 10u64)];
        assert_eq!(my_array, sorted);

        let algo_result = create_algo_result_f64();
        let v_c = algo_result.graph.node("C").unwrap();
        let v_a = algo_result.graph.node("A").unwrap();
        let v_b = algo_result.graph.node("B").unwrap();
        let sorted = algo_result.sort_by_node(true);
        let my_array: Vec<(NodeView<Graph, Graph>, f64)> =
            vec![(v_c, 30.0), (v_b, 20.0), (v_a, 10.0)];
        assert_eq!(my_array, sorted);

        let algo_result = create_algo_result_tuple();
        let v_c = algo_result.graph.node("C").unwrap();
        let v_a = algo_result.graph.node("A").unwrap();
        let v_b = algo_result.graph.node("B").unwrap();

        let sorted = algo_result.sort_by_node(true);
        let my_array: Vec<(NodeView<Graph, Graph>, (f32, f32))> = vec![
            (v_c, (30.0, 40.0)),
            (v_b, (20.0, 30.0)),
            (v_a, (10.0, 20.0)),
        ];
        assert_eq!(my_array, sorted);
        //
        let algo_result = create_algo_result_hashmap_vec();
        let v_c = algo_result.graph.node("C").unwrap();
        let v_a = algo_result.graph.node("A").unwrap();
        let v_b = algo_result.graph.node("B").unwrap();

        let sorted = algo_result.sort_by_node(true);
        let vec_c = vec![(22, "E".to_string()), (33, "F".to_string())];
        let vec_b = vec![];
        let vec_a = vec![(11, "H".to_string())];
        let my_array: Vec<(NodeView<Graph, Graph>, Vec<(i32, String)>)> =
            vec![(v_c, vec_c), (v_b, vec_b), (v_a, vec_a)];
        assert_eq!(my_array, sorted);
    }

    #[test]
    fn test_get_all() {
        let algo_result = create_algo_result_u64();
        let gotten_all = algo_result.get_all();
        let names: Vec<String> = gotten_all.keys().map(|vv| vv.name()).collect();
        println!("{:?}", names)
    }

    #[test]
    fn test_windowed_graph() {
        let g = Graph::new();
        g.add_edge(0, 1, 2, NO_PROPS, Some("ZERO-TWO"))
            .expect("Unable to add edge");
        g.add_edge(1, 1, 3, NO_PROPS, Some("ZERO-TWO"))
            .expect("Unable to add edge");
        g.add_edge(2, 4, 5, NO_PROPS, Some("ZERO-TWO"))
            .expect("Unable to add edge");
        g.add_edge(3, 6, 7, NO_PROPS, Some("THREE-FIVE"))
            .expect("Unable to add edge");
        g.add_edge(4, 8, 9, NO_PROPS, Some("THREE-FIVE"))
            .expect("Unable to add edge");
        let g_layer = g.layers(vec!["ZERO-TWO"]).unwrap();
        let res_window = weakly_connected_components(&g_layer, 20, None);
        let mut expected_result: HashMap<String, u64> = HashMap::new();
        expected_result.insert("8".to_string(), 8);
        expected_result.insert("1".to_string(), 1);
        expected_result.insert("3".to_string(), 1);
        expected_result.insert("2".to_string(), 1);
        expected_result.insert("5".to_string(), 4);
        expected_result.insert("6".to_string(), 6);
        expected_result.insert("7".to_string(), 7);
        expected_result.insert("4".to_string(), 4);
        expected_result.insert("9".to_string(), 9);
        assert_eq!(res_window.get_all_with_names(), expected_result);
    }
}
