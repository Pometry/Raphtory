use crate::{
    core::entities::vertices::vertex_ref::VertexRef,
    prelude::{GraphViewOps, VertexViewOps},
};
use num_traits::Float;
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
pub struct AlgorithmResult<G, V, O = V> {
    /// The result hashmap that stores keys of type `H` and values of type `Y`.
    pub algo_repr: AlgorithmRepr,
    pub graph: G,
    pub result: HashMap<usize, V>,
    marker: PhantomData<O>,
}

// use pyo3::{prelude::*, types::IntoPyDict};

impl<G, V, O> AlgorithmResult<G, V, O>
where
    G: GraphViewOps,
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
    pub fn get_all_values(&self) -> Vec<V> {
        self.result.clone().into_values().collect()
    }

    /// Returns the value corresponding to the provided key in the `result` hashmap.
    ///
    /// Arguments:
    ///     `key`: The key of type `H` for which the value is to be retrieved.
    pub fn get(&self, v_ref: VertexRef) -> Option<&V> {
        if self.graph.has_vertex(v_ref) {
            let internal_id = self.graph.vertex(v_ref).unwrap().vertex.0;
            self.result.get(&internal_id)
        } else {
            None
        }
    }

    /// Returns a hashmap with vertex names and values
    ///
    /// Returns:
    ///     a hashmap with vertex names and values
    pub fn get_all_with_names(&self) -> HashMap<String, Option<V>> {
        self.graph
            .vertices()
            .iter()
            .map(|vertex| {
                let name = vertex.name();
                let value = self.result.get(&vertex.vertex.0).cloned();
                (name.to_string(), value)
            })
            .collect()
    }

    /// Returns a `HashMap` containing `VertexView<G>` keys and `Option<V>` values.
    ///
    /// Returns:
    ///     a `HashMap` containing `VertexView<G>` keys and `Option<V>` values.
    pub fn get_all(&self) -> HashMap<VertexView<G>, Option<V>> {
        self.graph
            .vertices()
            .iter()
            .map(|vertex| (vertex.clone(), self.result.get(&vertex.vertex.0).cloned()))
            .collect()
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
    pub fn sort_by_vertex(&self, reverse: bool) -> Vec<(VertexView<G>, Option<V>)> {
        let mut sorted: Vec<(VertexView<G>, Option<V>)> = self.get_all().into_iter().collect();
        sorted.sort_by(|(a, _), (b, _)| if reverse { b.cmp(a) } else { a.cmp(b) });
        sorted
    }

    /// Sorts a collection of vertex views by their names in either ascending or descending order.
    ///
    /// Arguments:
    ///
    /// * `reverse`: A boolean value indicating whether the sorting should be in reverse order or not. If
    /// `reverse` is `true`, the sorting will be in reverse order (descending), otherwise it will be in
    /// ascending order.
    ///
    /// Returns:
    ///
    /// The function `sort_by_vertex_name` returns a vector of tuples, where each tuple contains a
    /// `VertexView<G>` and an optional `V` value.
    pub fn sort_by_vertex_name(&self, reverse: bool) -> Vec<(VertexView<G>, Option<V>)> {
        let mut sorted: Vec<(VertexView<G>, Option<V>)> = self.get_all().into_iter().collect();
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
    /// A sorted vector of tuples containing vertex names and values.
    pub fn sort_by_values<F: FnMut(&V, &V) -> std::cmp::Ordering>(
        &self,
        mut cmp: F,
        reverse: bool,
    ) -> Vec<(VertexView<G>, Option<V>)> {
        let mut all_as_vec: Vec<(VertexView<G>, Option<V>)> = self.get_all().into_iter().collect();
        all_as_vec.sort_by(|a, b| {
            let order = match (&a.1, &b.1) {
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
    ) -> Vec<(VertexView<G>, Option<V>)> {
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
    ) -> Option<(VertexView<G>, Option<V>)> {
        let min_element = self
            .get_all()
            .into_iter()
            .filter_map(|(k, v)| v.map(|v| (k, v)))
            .min_by(|(_, a_value), (_, b_value)| cmp(a_value, b_value));

        // Clone the key and value
        min_element.map(|(k, v)| (k.clone(), Some(v.clone())))
    }

    pub fn max_by<F: FnMut(&V, &V) -> std::cmp::Ordering>(
        &self,
        mut cmp: F,
    ) -> Option<(VertexView<G>, Option<V>)> {
        let max_element = self
            .get_all()
            .into_iter()
            .filter_map(|(k, v)| v.map(|v| (k, v)))
            .max_by(|(_, a_value), (_, b_value)| cmp(a_value, b_value));

        // Clone the key and value
        max_element.map(|(k, v)| (k.clone(), Some(v.clone())))
    }

    pub fn median_by<F: FnMut(&V, &V) -> std::cmp::Ordering>(
        &self,
        mut cmp: F,
    ) -> Option<(VertexView<G>, Option<V>)> {
        // Assuming self.result is Vec<(String, Option<V>)>
        let mut items: Vec<(VertexView<G>, V)> = self
            .get_all()
            .into_iter()
            .filter_map(|(k, v)| v.map(|v| (k, v)))
            .collect();
        let len = items.len();
        if len == 0 {
            return None;
        }

        items.sort_by(|(_, a_value), (_, b_value)| cmp(a_value, b_value));
        let median_index = len / 2;

        Some((
            items[median_index].0.clone(),
            Some(items[median_index].1.clone()),
        ))
    }
}

impl<G, V, O> AlgorithmResult<G, V, O>
where
    G: GraphViewOps,
    V: Clone,
    O: Ord,
    V: AsOrd<O>,
{
    pub fn group_by(&self) -> HashMap<V, Vec<String>>
    where
        V: Eq + Hash,
    {
        let mut groups: HashMap<V, Vec<String>> = HashMap::new();

        for vertex in self.graph.vertices().iter() {
            if let Some(value) = self.result.get(&vertex.vertex.0) {
                let entry = groups.entry(value.clone()).or_insert_with(Vec::new);
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
    pub fn sort_by_value(&self, reverse: bool) -> Vec<(VertexView<G>, Option<V>)> {
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
    pub fn top_k(
        &self,
        k: usize,
        percentage: bool,
        reverse: bool,
    ) -> Vec<(VertexView<G>, Option<V>)> {
        self.top_k_by(
            |a, b| O::cmp(a.as_ord(), b.as_ord()),
            k,
            percentage,
            reverse,
        )
    }

    /// Returns a tuple of the min result with its key
    pub fn min(&self) -> Option<(VertexView<G>, Option<V>)> {
        self.min_by(|a, b| O::cmp(a.as_ord(), b.as_ord()))
    }

    /// Returns a tuple of the max result with its key
    pub fn max(&self) -> Option<(VertexView<G>, Option<V>)> {
        self.max_by(|a, b| O::cmp(a.as_ord(), b.as_ord()))
    }

    /// Returns a tuple of the median result with its key
    pub fn median(&self) -> Option<(VertexView<G>, Option<V>)> {
        self.median_by(|a, b| O::cmp(a.as_ord(), b.as_ord()))
    }
}

use crate::db::graph::vertex::VertexView;
use std::fmt;

impl<G: GraphViewOps, V: fmt::Debug, O> fmt::Display for AlgorithmResult<G, V, O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "AlgorithmResultNew {{")?;
        writeln!(f, "  Algorithm Name: {}", self.algo_repr.algo_name)?;
        writeln!(f, "  Result Type: {}", self.algo_repr.result_type)?;
        writeln!(f, "  Number of Vertices: {}", self.result.len())?;
        writeln!(f, "  Results: [")?;

        for vertex in self.graph.vertices().iter() {
            let value = self.result.get(&vertex.vertex.0);
            writeln!(f, "    {}: {:?}", vertex.name(), value)?;
        }

        writeln!(f, "  ]")?;
        writeln!(f, "}}")
    }
}

impl<G: GraphViewOps, V: fmt::Debug, O> fmt::Debug for AlgorithmResult<G, V, O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "AlgorithmResultNew {{")?;
        writeln!(f, "  Algorithm Name: {:?}", self.algo_repr.algo_name)?;
        writeln!(f, "  Result Type: {:?}", self.algo_repr.result_type)?;
        writeln!(f, "  Number of Vertices: {:?}", self.result.len())?;
        writeln!(f, "  Results: [")?;

        for vertex in self.graph.vertices().iter() {
            let value = self.result.get(&vertex.vertex.0);
            writeln!(f, "    {:?}: {:?}", vertex.name(), value)?;
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
        algorithms::community_detection::connected_components::weakly_connected_components,
        db::{
            api::{mutation::AdditionOps, view::GraphViewOps},
            graph::graph::Graph,
        },
        prelude::{NO_PROPS, *},
    };
    use ordered_float::OrderedFloat;

    fn create_algo_result_u64() -> AlgorithmResult<Graph, u64> {
        let g = create_graph();
        let mut map: HashMap<usize, u64> = HashMap::new();
        map.insert(g.vertex("A").unwrap().vertex.0, 10);
        map.insert(g.vertex("B").unwrap().vertex.0, 20);
        map.insert(g.vertex("C").unwrap().vertex.0, 30);
        let results_type = std::any::type_name::<u64>();
        AlgorithmResult::new(g, "create_algo_result_u64_test", results_type, map)
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

    fn group_by_test() -> AlgorithmResult<Graph, u64> {
        let g = create_graph();
        let mut map: HashMap<usize, u64> = HashMap::new();
        map.insert(g.vertex("A").unwrap().vertex.0, 10);
        map.insert(g.vertex("B").unwrap().vertex.0, 20);
        map.insert(g.vertex("C").unwrap().vertex.0, 30);
        map.insert(g.vertex("D").unwrap().vertex.0, 10);
        let results_type = std::any::type_name::<u64>();
        AlgorithmResult::new(g, "group_by_test", results_type, map)
    }

    fn create_algo_result_f64() -> AlgorithmResult<Graph, f64, OrderedFloat<f64>> {
        let g = Graph::new();
        g.add_vertex(0, "A", NO_PROPS)
            .expect("Could not add vertex to graph");
        g.add_vertex(0, "B", NO_PROPS)
            .expect("Could not add vertex to graph");
        g.add_vertex(0, "C", NO_PROPS)
            .expect("Could not add vertex to graph");
        g.add_vertex(0, "D", NO_PROPS)
            .expect("Could not add vertex to graph");
        let mut map: HashMap<usize, f64> = HashMap::new();
        map.insert(g.vertex("A").unwrap().vertex.0, 10.0);
        map.insert(g.vertex("B").unwrap().vertex.0, 20.0);
        map.insert(g.vertex("C").unwrap().vertex.0, 30.0);
        let results_type = std::any::type_name::<f64>();
        AlgorithmResult::new(g, "create_algo_result_u64_test", results_type, map)
    }

    fn create_algo_result_tuple(
    ) -> AlgorithmResult<Graph, (f32, f32), (OrderedFloat<f32>, OrderedFloat<f32>)> {
        let g = create_graph();
        let mut res: HashMap<usize, (f32, f32)> = HashMap::new();
        res.insert(g.vertex("A").unwrap().vertex.0, (10.0, 20.0));
        res.insert(g.vertex("B").unwrap().vertex.0, (20.0, 30.0));
        res.insert(g.vertex("C").unwrap().vertex.0, (30.0, 40.0));
        let results_type = std::any::type_name::<(f32, f32)>();
        AlgorithmResult::new(g, "create_algo_result_tuple", results_type, res)
    }

    fn create_algo_result_hashmap_vec() -> AlgorithmResult<Graph, Vec<(i32, String)>> {
        let g = create_graph();
        let mut res: HashMap<usize, Vec<(i32, String)>> = HashMap::new();
        res.insert(g.vertex("A").unwrap().vertex.0, vec![(11, "H".to_string())]);
        res.insert(g.vertex("B").unwrap().vertex.0, vec![]);
        res.insert(
            g.vertex("C").unwrap().vertex.0,
            vec![(22, "E".to_string()), (33, "F".to_string())],
        );
        let results_type = std::any::type_name::<(i32, String)>();
        AlgorithmResult::new(g, "create_algo_result_hashmap_vec", results_type, res)
    }

    #[test]
    fn test_min_max_value() {
        let algo_result = create_algo_result_u64();
        let v_a = algo_result.graph.vertex("A".to_string()).unwrap();
        let v_b = algo_result.graph.vertex("B".to_string()).unwrap();
        let v_c = algo_result.graph.vertex("C".to_string()).unwrap();
        assert_eq!(algo_result.min(), Some((v_a.clone(), Some(10u64))));
        assert_eq!(algo_result.max(), Some((v_c.clone(), Some(30u64))));
        assert_eq!(algo_result.median(), Some((v_b.clone(), Some(20u64))));
        let algo_result = create_algo_result_f64();
        assert_eq!(algo_result.min(), Some((v_a, Some(10.0))));
        assert_eq!(algo_result.max(), Some((v_c, Some(30.0))));
        assert_eq!(algo_result.median(), Some((v_b, Some(20.0))));
    }

    #[test]
    fn test_get() {
        let algo_result = create_algo_result_u64();
        let vertex_c = algo_result.graph.vertex("C").unwrap();
        let vertex_d = algo_result.graph.vertex("D").unwrap();
        assert_eq!(algo_result.get(vertex_c.clone().into()), Some(&30u64));
        assert_eq!(algo_result.get(vertex_d.into()), None);
        let algo_result = create_algo_result_f64();
        assert_eq!(algo_result.get(vertex_c.clone().into()), Some(&30.0f64));
        let algo_result = create_algo_result_tuple();
        assert_eq!(algo_result.get(vertex_c.clone().into()).unwrap().0, 30.0f32);
        let algo_result = create_algo_result_hashmap_vec();
        let answer = algo_result
            .get(vertex_c.clone().into())
            .unwrap()
            .get(0)
            .unwrap()
            .0;
        assert_eq!(answer, 22i32);
    }

    #[test]
    fn test_sort() {
        let algo_result = create_algo_result_u64();
        let sorted = algo_result.sort_by_value(true);
        let v_c = algo_result.graph.vertex("C").unwrap();
        let v_d = algo_result.graph.vertex("D").unwrap();
        let v_a = algo_result.graph.vertex("A").unwrap();
        assert_eq!(sorted[0].0, v_c.clone());
        let sorted = algo_result.sort_by_value(false);
        assert_eq!(sorted[0].0, v_d.clone());

        let algo_result = create_algo_result_f64();
        let sorted = algo_result.sort_by_value(true);
        assert_eq!(sorted[0].0, v_c.clone());
        let sorted = algo_result.sort_by_value(false);
        assert_eq!(sorted[0].0, v_d);
        assert_eq!(sorted[1].0, v_a);

        let algo_result = create_algo_result_tuple();
        assert_eq!(algo_result.sort_by_value(true)[0].0, v_c.clone());

        let algo_result = create_algo_result_hashmap_vec();
        assert_eq!(algo_result.sort_by_value(true)[0].0, v_c);
    }

    #[test]
    fn test_top_k() {
        let algo_result = create_algo_result_u64();
        let v_c = algo_result.graph.vertex("C").unwrap();
        let v_d = algo_result.graph.vertex("D").unwrap();
        let v_a = algo_result.graph.vertex("A").unwrap();
        let v_b = algo_result.graph.vertex("B").unwrap();

        let top_k = algo_result.top_k(2, false, false);
        assert_eq!(top_k[0].0, v_d.clone());
        let top_k = algo_result.top_k(2, false, true);
        assert_eq!(top_k[0].0, v_c.clone());

        let algo_result = create_algo_result_f64();
        let top_k = algo_result.top_k(2, false, false);
        assert_eq!(top_k[0].0, v_d.clone());
        assert_eq!(top_k[1].0, v_a.clone());
        let top_k = algo_result.top_k(2, false, true);
        assert_eq!(top_k[0].0, v_c);

        let algo_result = create_algo_result_tuple();
        assert_eq!(algo_result.top_k(2, false, false)[0].0, v_d.clone());
        assert_eq!(algo_result.top_k(2, false, false)[1].0, v_a);

        let algo_result = create_algo_result_hashmap_vec();
        assert_eq!(algo_result.top_k(2, false, false)[0].0, v_d);
        assert_eq!(algo_result.top_k(2, false, false)[1].0, v_b);
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
        assert_eq!(tuple_result.unwrap().0, 10.0);
        assert_eq!(algo_result.get_all_values().len(), 3);

        let algo_result = create_algo_result_hashmap_vec();
        let algo_results_hashmap = algo_result.get_all_with_names();
        let tuple_result = algo_results_hashmap.get("A").unwrap();
        assert_eq!(tuple_result.clone().unwrap().get(0).unwrap().0, 11);
        assert_eq!(algo_result.get_all_values().len(), 3);
    }

    #[test]
    fn test_sort_by_vertex() {
        let algo_result = create_algo_result_u64();
        let v_c = algo_result.graph.vertex("C").unwrap();
        let v_d = algo_result.graph.vertex("D").unwrap();
        let v_a = algo_result.graph.vertex("A").unwrap();
        let v_b = algo_result.graph.vertex("B").unwrap();
        let sorted = algo_result.sort_by_vertex(true);
        let my_array: Vec<(VertexView<Graph>, Option<u64>)> = vec![
            (v_d, None),
            (v_c, Some(30u64)),
            (v_b, Some(20u64)),
            (v_a, Some(10u64)),
        ];
        assert_eq!(my_array, sorted);

        let algo_result = create_algo_result_f64();
        let v_c = algo_result.graph.vertex("C").unwrap();
        let v_d = algo_result.graph.vertex("D").unwrap();
        let v_a = algo_result.graph.vertex("A").unwrap();
        let v_b = algo_result.graph.vertex("B").unwrap();
        let sorted = algo_result.sort_by_vertex(true);
        let my_array: Vec<(VertexView<Graph>, Option<f64>)> = vec![
            (v_d, None),
            (v_c, Some(30.0)),
            (v_b, Some(20.0)),
            (v_a, Some(10.0)),
        ];
        assert_eq!(my_array, sorted);

        let algo_result = create_algo_result_tuple();
        let v_c = algo_result.graph.vertex("C").unwrap();
        let v_d = algo_result.graph.vertex("D").unwrap();
        let v_a = algo_result.graph.vertex("A").unwrap();
        let v_b = algo_result.graph.vertex("B").unwrap();

        let sorted = algo_result.sort_by_vertex(true);
        let my_array: Vec<(VertexView<Graph>, Option<(f32, f32)>)> = vec![
            (v_d, None),
            (v_c, Some((30.0, 40.0))),
            (v_b, Some((20.0, 30.0))),
            (v_a, Some((10.0, 20.0))),
        ];
        assert_eq!(my_array, sorted);
        //
        let algo_result = create_algo_result_hashmap_vec();
        let v_c = algo_result.graph.vertex("C").unwrap();
        let v_d = algo_result.graph.vertex("D").unwrap();
        let v_a = algo_result.graph.vertex("A").unwrap();
        let v_b = algo_result.graph.vertex("B").unwrap();

        let sorted = algo_result.sort_by_vertex(true);
        let vec_c = vec![(22, "E".to_string()), (33, "F".to_string())];
        let vec_b = vec![];
        let vec_a = vec![(11, "H".to_string())];
        let my_array: Vec<(VertexView<Graph>, Option<Vec<(i32, String)>>)> = vec![
            (v_d, None),
            (v_c, Some(vec_c)),
            (v_b, Some(vec_b)),
            (v_a, Some(vec_a)),
        ];
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
        let g_layer = g.layer(vec!["ZERO-TWO"]).unwrap();
        let res_window = weakly_connected_components(&g_layer, 20, None);
        let mut expected_result: HashMap<String, Option<u64>> = HashMap::new();
        expected_result.insert("8".to_string(), Some(8));
        expected_result.insert("1".to_string(), Some(1));
        expected_result.insert("3".to_string(), Some(1));
        expected_result.insert("2".to_string(), Some(1));
        expected_result.insert("5".to_string(), Some(4));
        expected_result.insert("6".to_string(), Some(6));
        expected_result.insert("7".to_string(), Some(7));
        expected_result.insert("4".to_string(), Some(4));
        expected_result.insert("9".to_string(), Some(9));
        assert_eq!(res_window.get_all_with_names(), expected_result);
    }
}
