use crate::{
    core::entities::vertices::vertex_ref::VertexRef,
    prelude::{Graph, GraphViewOps, VertexViewOps},
};
use itertools::Itertools;
use num_traits::Float;
use ordered_float::OrderedFloat;
use std::{collections::HashMap, hash::Hash, marker::PhantomData};

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

impl<T: Float> AsOrd<OrderedFloat<T>> for T {
    fn as_ord(&self) -> &OrderedFloat<T> {
        self.into()
    }
}

impl<T: Float> AsOrd<(OrderedFloat<T>, OrderedFloat<T>)> for (T, T) {
    fn as_ord(&self) -> &(OrderedFloat<T>, OrderedFloat<T>) {
        // Safety: OrderedFloat is #[repr(transparent)] and has no invalid values, i.e. there is no physical difference between OrderedFloat and Float.
        unsafe { &*(self as *const (T, T) as *const (OrderedFloat<T>, OrderedFloat<T>)) }
    }
}

/// An 'AlgorithmRepr' struct that represents the string output in the terminal after running an algorithm.
///
/// It returns the algorithm name, number of vertices in the graph, and the result type.
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
pub struct AlgorithmResultNew<V, O = V> {
    /// The result hashmap that stores keys of type `H` and values of type `Y`.
    pub algo_repr: AlgorithmRepr,
    pub graph: Graph,
    pub result: Vec<V>,
    marker: PhantomData<O>,
}

use std::any::type_name;
impl<V, O> AlgorithmResultNew<V, O>
where
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
    pub fn new(algo_name: &str, result: Vec<V>, graph: Graph) -> Self {
        let result_type = type_name::<V>();
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

    /// Returns a formatted string representation of the algorithm.
    pub fn repr(&self) -> String {
        let algo_info_str = format!(
            "Algorithm Name: {}, Number of Vertices: {}, Result Type: {}",
            &self.algo_repr.algo_name,
            &self.result.len(),
            &self.algo_repr.result_type
        );
        algo_info_str
    }

    /// Returns a reference to the entire `result` vector of values.
    pub fn get_all(&self) -> &Vec<V> {
        &self.result
    }

    /// Returns the value corresponding to the provided key in the `result` hashmap.
    ///
    /// Arguments:
    ///     `key`: The key of type `H` for which the value is to be retrieved.
    pub fn get(&self, v_ref: VertexRef) -> Option<&V> {
        if self.graph.has_vertex(v_ref) {
            let internal_id = self.graph.vertex(v_ref).unwrap().vertex.0;
            self.result.get(internal_id)
        } else {
            None
        }
    }

    /// Returns a vector of tuples with vertex names and values
    ///
    /// Returns:
    ///     a vector of tuples with vertex names and values
    fn get_with_names_vec(&self) -> Vec<(String, Option<&V>)> {
        self.graph
            .vertices()
            .iter()
            .map(|vertex| (vertex.name(), self.result.get(vertex.vertex.0)))
            .collect_vec()
    }

    pub fn get_with_names(&self) -> HashMap<String, Option<&V>> {
        let mut as_map = HashMap::new();
        for vertex in self.graph.vertices().iter() {
            let name = vertex.name();
            let value = self.result.get(vertex.vertex.0);
            as_map.insert(name.to_string(), value);
        }
        as_map
    }
    /// Sorts the `AlgorithmResult` by its vertex id in ascending or descending order.
    ///
    /// Arguments:
    ///
    /// * `reverse`: If `true`, sorts the result in descending order; otherwise, sorts in ascending order.
    ///
    /// Returns:
    ///
    /// A sorted vector of tuples containing vertex names and values.
    pub fn sort_by_vertex_id(&self, reverse: bool) -> Vec<(String, Option<&V>)> {
        let mut sorted = self.get_with_names_vec();
        sorted.sort_by(|(a, _), (b, _)| if reverse { b.cmp(a) } else { a.cmp(b) });
        sorted
    }

    pub fn iter(&self) -> std::slice::Iter<'_, V> {
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
    /// A sorted vector of tuples containing vertex names and values.
    pub fn sort_by_values<F: FnMut(&V, &V) -> std::cmp::Ordering>(
        &self,
        mut cmp: F,
        reverse: bool,
    ) -> Vec<(String, Option<&V>)> {
        let mut sorted = self.get_with_names_vec();
        sorted.sort_by(|a, b| {
            let order = match (a.1, b.1) {
                (Some(a_value), Some(b_value)) => cmp(a_value, b_value),
                (Some(_), None) => std::cmp::Ordering::Greater, // Put Some(_) values before None
                (None, Some(_)) => std::cmp::Ordering::Less,    // Put Some(_) values before None
                (None, None) => std::cmp::Ordering::Equal,      // Equal if both are None
            };

            // Reverse the order if `reverse` is true
            if reverse {
                order.reverse()
            } else {
                order
            }
        });
        sorted
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
    ) -> Vec<(String, Option<&V>)> {
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
    ) -> Option<(String, Option<&V>)> {
        let res: Vec<(String, Option<&V>)> = self.get_with_names_vec();

        // Filter out None values and find the minimum element
        let min_element = res
            .iter()
            .filter_map(|(k, v)| v.as_ref().map(|v| (k, v)))
            .min_by(|(_, a_value), (_, b_value)| cmp(a_value, b_value));

        // Clone the key and value
        min_element.map(|(k, v)| (k.clone(), Some(*v)))
    }

    pub fn max_by<F: FnMut(&V, &V) -> std::cmp::Ordering>(
        &self,
        mut cmp: F,
    ) -> Option<(String, Option<&V>)> {
        let res: Vec<(String, Option<&V>)> = self.get_with_names_vec();

        // Filter out None values and find the minimum element
        let max_element = res
            .iter()
            .filter_map(|(k, v)| v.as_ref().map(|v| (k, v)))
            .max_by(|(_, a_value), (_, b_value)| cmp(a_value, b_value));

        // Clone the key and value
        max_element.map(|(k, v)| (k.clone(), Some(*v)))
    }

    pub fn median_by<F: FnMut(&V, &V) -> std::cmp::Ordering>(
        &self,
        mut cmp: F,
    ) -> Option<(String, Option<&V>)> {
        // Assuming self.result is Vec<(String, Option<V>)>
        let res: Vec<(String, Option<&V>)> = self.get_with_names_vec();
        let mut items: Vec<_> = res
            .iter()
            .filter_map(|(k, v)| v.as_ref().map(|v| (k, v)))
            .collect();
        let len = items.len();
        if len == 0 {
            return None;
        }

        items.sort_by(|(_, a_value), (_, b_value)| cmp(a_value, b_value));
        let median_index = len / 2;

        Some((items[median_index].0.clone(), Some(items[median_index].1)))
    }
}

impl<V, O> AlgorithmResultNew<V, O>
where
    V: Clone,
    O: Ord,
    V: AsOrd<O>,
{
    pub fn group_by(&self) -> HashMap<&V, Vec<String>>
    where
        V: Eq + Hash,
    {
        let mut groups: HashMap<&V, Vec<String>> = HashMap::new();

        for vertex in self.graph.vertices().iter() {
            if let Some(value) = self.result.get(vertex.vertex.0) {
                let entry = groups.entry(value).or_insert_with(Vec::new);
                entry.push(vertex.name().to_string());
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
    pub fn sort_by_value(&self, reverse: bool) -> Vec<(String, Option<&V>)> {
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
    pub fn top_k(&self, k: usize, percentage: bool, reverse: bool) -> Vec<(String, Option<&V>)> {
        self.top_k_by(
            |a, b| O::cmp(a.as_ord(), b.as_ord()),
            k,
            percentage,
            reverse,
        )
    }

    pub fn min(&self) -> Option<(String, Option<&V>)> {
        self.min_by(|a, b| O::cmp(a.as_ord(), b.as_ord()))
    }

    pub fn max(&self) -> Option<(String, Option<&V>)> {
        self.max_by(|a, b| O::cmp(a.as_ord(), b.as_ord()))
    }

    pub fn median(&self) -> Option<(String, Option<&V>)> {
        self.median_by(|a, b| O::cmp(a.as_ord(), b.as_ord()))
    }
}

use std::fmt;

impl<V: fmt::Debug, O> fmt::Display for AlgorithmResultNew<V, O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "AlgorithmResultNew {{")?;
        writeln!(f, "  Algorithm Name: {}", self.algo_repr.algo_name)?;
        writeln!(f, "  Result Type: {}", self.algo_repr.result_type)?;
        writeln!(f, "  Number of Vertices: {}", self.result.len())?;
        writeln!(f, "  Results: [")?;

        for vertex in self.graph.vertices().iter() {
            let value = self.result.get(vertex.vertex.0);
            writeln!(f, "    {}: {:?}", vertex.name(), value)?;
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
        db::{api::mutation::AdditionOps, graph::graph::Graph},
        prelude::NO_PROPS,
    };
    use ordered_float::OrderedFloat;

    fn create_algo_result_u64() -> AlgorithmResultNew<u64> {
        let g = create_graph();
        let mut map: Vec<u64> = Vec::new();
        map.insert(g.vertex("A").unwrap().vertex.0, 10);
        map.insert(g.vertex("B").unwrap().vertex.0, 20);
        map.insert(g.vertex("C").unwrap().vertex.0, 30);
        AlgorithmResultNew::new("create_algo_result_u64_test", map, g)
    }

    fn create_graph() -> Graph {
        let g = Graph::new();
        g.add_vertex(0, "A", NO_PROPS)
            .expect("Could not add vertex to graph");
        g.add_vertex(0, "B", NO_PROPS)
            .expect("Could not add vertex to graph");
        g.add_vertex(0, "C", NO_PROPS)
            .expect("Could not add vertex to graph");
        g.add_vertex(0, "D", NO_PROPS)
            .expect("Could not add vertex to graph");
        g
    }

    fn group_by_test() -> AlgorithmResultNew<u64> {
        let g = create_graph();
        let mut map: Vec<u64> = Vec::new();
        map.insert(g.vertex("A").unwrap().vertex.0, 10);
        map.insert(g.vertex("B").unwrap().vertex.0, 20);
        map.insert(g.vertex("C").unwrap().vertex.0, 30);
        map.insert(g.vertex("D").unwrap().vertex.0, 10);
        AlgorithmResultNew::new("group_by_test", map, g)
    }

    fn create_algo_result_f64() -> AlgorithmResultNew<f64, OrderedFloat<f64>> {
        let g = Graph::new();
        g.add_vertex(0, "A", NO_PROPS)
            .expect("Could not add vertex to graph");
        g.add_vertex(0, "B", NO_PROPS)
            .expect("Could not add vertex to graph");
        g.add_vertex(0, "C", NO_PROPS)
            .expect("Could not add vertex to graph");
        g.add_vertex(0, "D", NO_PROPS)
            .expect("Could not add vertex to graph");
        let mut map: Vec<f64> = Vec::new();
        map.insert(g.vertex("A").unwrap().vertex.0, 10.0);
        map.insert(g.vertex("B").unwrap().vertex.0, 20.0);
        map.insert(g.vertex("C").unwrap().vertex.0, 30.0);
        AlgorithmResultNew::new("create_algo_result_u64_test", map, g)
    }

    fn create_algo_result_tuple(
    ) -> AlgorithmResultNew<(f32, f32), (OrderedFloat<f32>, OrderedFloat<f32>)> {
        let g = create_graph();
        let mut res: Vec<(f32, f32)> = vec![];
        res.insert(g.vertex("A").unwrap().vertex.0, (10.0, 20.0));
        res.insert(g.vertex("B").unwrap().vertex.0, (20.0, 30.0));
        res.insert(g.vertex("C").unwrap().vertex.0, (30.0, 40.0));
        AlgorithmResultNew::new("create_algo_result_tuple", res, g)
    }

    fn create_algo_result_hashmap_vec() -> AlgorithmResultNew<Vec<(i32, String)>> {
        let g = create_graph();
        let mut res: Vec<Vec<(i32, String)>> = vec![];
        res.insert(g.vertex("A").unwrap().vertex.0, vec![(11, "H".to_string())]);
        res.insert(g.vertex("B").unwrap().vertex.0, vec![]);
        res.insert(
            g.vertex("C").unwrap().vertex.0,
            vec![(22, "E".to_string()), (33, "F".to_string())],
        );
        AlgorithmResultNew::new("create_algo_result_hashmap_vec", res, g)
    }

    #[test]
    fn test_min_max_value() {
        let algo_result = create_algo_result_u64();
        assert_eq!(algo_result.min(), Some(("A".to_string(), Some(&10u64))));
        assert_eq!(algo_result.max(), Some(("C".to_string(), Some(&30u64))));
        assert_eq!(algo_result.median(), Some(("B".to_string(), Some(&20u64))));
        let algo_result = create_algo_result_f64();
        assert_eq!(algo_result.min(), Some(("A".to_string(), Some(&10.0))));
        assert_eq!(algo_result.max(), Some(("C".to_string(), Some(&30.0))));
        assert_eq!(algo_result.median(), Some(("B".to_string(), Some(&20.0))));
    }

    #[test]
    fn test_get() {
        let algo_result = create_algo_result_u64();
        let vertex_c = algo_result.graph.vertex("C").unwrap();
        let vertex_d = algo_result.graph.vertex("D").unwrap();
        assert_eq!(algo_result.get(vertex_c.clone().into()), Some(&30));
        assert_eq!(algo_result.get(vertex_d.into()), None);
        let algo_result = create_algo_result_f64();
        assert_eq!(algo_result.get(vertex_c.clone().into()), Some(&30.0));
        let algo_result = create_algo_result_tuple();
        assert_eq!(algo_result.get(vertex_c.clone().into()).unwrap().0, 30.0);
        let algo_result = create_algo_result_hashmap_vec();
        let answer = algo_result
            .get(vertex_c.clone().into())
            .unwrap()
            .get(0)
            .unwrap()
            .0;
        assert_eq!(answer, 22i32);
        println!("{}", algo_result);
    }

    #[test]
    fn test_sort() {
        let algo_result = create_algo_result_u64();
        let sorted = algo_result.sort_by_value(true);
        assert_eq!(sorted[0].0, "C");
        let sorted = algo_result.sort_by_value(false);
        assert_eq!(sorted[0].0, "D");

        let algo_result = create_algo_result_f64();
        let sorted = algo_result.sort_by_value(true);
        assert_eq!(sorted[0].0, "C");
        let sorted = algo_result.sort_by_value(false);
        assert_eq!(sorted[0].0, "D");
        assert_eq!(sorted[1].0, "A");

        let algo_result = create_algo_result_tuple();
        assert_eq!(algo_result.sort_by_value(true)[0].0, "C");

        let algo_result = create_algo_result_hashmap_vec();
        assert_eq!(algo_result.sort_by_value(true)[0].0, "C");
    }

    #[test]
    fn test_top_k() {
        let algo_result = create_algo_result_u64();
        let top_k = algo_result.top_k(2, false, false);
        assert_eq!(top_k[0].0, "D");
        let top_k = algo_result.top_k(2, false, true);
        assert_eq!(top_k[0].0, "C");

        let algo_result = create_algo_result_f64();
        let top_k = algo_result.top_k(2, false, false);
        assert_eq!(top_k[0].0, "D");
        assert_eq!(top_k[1].0, "A");
        let top_k = algo_result.top_k(2, false, true);
        assert_eq!(top_k[0].0, "C");

        let algo_result = create_algo_result_tuple();
        assert_eq!(algo_result.top_k(2, false, false)[0].0, "D");
        assert_eq!(algo_result.top_k(2, false, false)[1].0, "A");

        let algo_result = create_algo_result_hashmap_vec();
        assert_eq!(algo_result.top_k(2, false, false)[0].0, "D");
        assert_eq!(algo_result.top_k(2, false, false)[1].0, "B");
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
    fn test_get_all() {
        let algo_result = create_algo_result_u64();
        let all = algo_result.get_all();
        assert_eq!(all.len(), 3);

        let algo_result = create_algo_result_f64();
        let all = algo_result.get_all();
        assert_eq!(all.len(), 3);

        let algo_result = create_algo_result_tuple();
        let algo_results_hashmap = algo_result.get_with_names();
        let tuple_result = algo_results_hashmap.get("A").unwrap();
        assert_eq!(tuple_result.unwrap().0, 10.0);
        assert_eq!(algo_result.get_all().len(), 3);

        let algo_result = create_algo_result_hashmap_vec();
        let algo_results_hashmap = algo_result.get_with_names();
        let tuple_result = algo_results_hashmap.get("A").unwrap();
        assert_eq!(tuple_result.unwrap().get(0).unwrap().0, 11);
        assert_eq!(algo_result.get_all().len(), 3);
    }

    #[test]
    fn test_sort_by_key() {
        let algo_result = create_algo_result_u64();
        let sorted = algo_result.sort_by_vertex_id(true);
        let my_array: Vec<(String, Option<&u64>)> = vec![
            ("D".to_string(), None),
            ("C".to_string(), Some(&30u64)),
            ("B".to_string(), Some(&20u64)),
            ("A".to_string(), Some(&10u64)),
        ];
        assert_eq!(my_array, sorted);

        let algo_result = create_algo_result_f64();
        let sorted = algo_result.sort_by_vertex_id(true);
        let my_array: Vec<(String, Option<&f64>)> = vec![
            ("D".to_string(), None),
            ("C".to_string(), Some(&30.0)),
            ("B".to_string(), Some(&20.0)),
            ("A".to_string(), Some(&10.0)),
        ];
        assert_eq!(my_array, sorted);

        let algo_result = create_algo_result_tuple();
        let sorted = algo_result.sort_by_vertex_id(true);
        let my_array: Vec<(String, Option<&(f32, f32)>)> = vec![
            ("D".to_string(), None),
            ("C".to_string(), Some(&(30.0, 40.0))),
            ("B".to_string(), Some(&(20.0, 30.0))),
            ("A".to_string(), Some(&(10.0, 20.0))),
        ];
        assert_eq!(my_array, sorted);
        //
        let algo_result = create_algo_result_hashmap_vec();
        let sorted = algo_result.sort_by_vertex_id(true);
        let vec_c = vec![(22, "E".to_string()), (33, "F".to_string())];
        let vec_b = vec![];
        let vec_a = vec![(11, "H".to_string())];
        let my_array: Vec<(String, Option<&Vec<(i32, String)>>)> = vec![
            ("D".to_string(), None),
            ("C".to_string(), Some(&vec_c)),
            ("B".to_string(), Some(&vec_b)),
            ("A".to_string(), Some(&vec_a)),
        ];
        assert_eq!(my_array, sorted);
    }
}
