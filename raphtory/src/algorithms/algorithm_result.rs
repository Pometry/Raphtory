use crate::{
    core::entities::vertices::vertex_ref::VertexRef, db::graph::vertex::VertexView, prelude::*,
};
use itertools::Itertools;
use ordered_float::OrderedFloat;
use std::{collections::HashMap, fmt, fmt::Debug, hash::Hash};

/// A generic `AlgorithmResult` struct that represents the result of an algorithm computation.
///
/// The `AlgorithmResult` contains a hashmap, where keys (`H`) are cloneable, hashable, and comparable,
/// and values (`Y`) are cloneable. The keys and values can be of any type that satisfies the specified
/// trait bounds.
///
/// This `AlgorithmResult` is returned for all algorithms that return a HashMap
///
pub struct AlgorithmResult<V, G>
where
    V: Clone,
    G: GraphViewOps,
{
    /// The result hashmap that stores keys of type `H` and values of type `Y`.
    pub result: Vec<V>,
    pub graph: G,
}

impl<V, G> AlgorithmResult<V, G>
where
    V: Clone,
    G: GraphViewOps,
{
    /// Creates a new instance of `AlgorithmResult` with the provided hashmap.
    ///
    /// # Arguments
    ///
    /// * `result`: A `HashMap` with keys of type `H` and values of type `Y`.
    pub fn new(result: Vec<V>, graph: G) -> Self {
        Self { result, graph }
    }

    /// Returns a reference to the entire `result` hashmap.
    pub fn get_all_values(&self) -> &Vec<V> {
        &self.result
    }

    pub fn get_all(&self) -> Vec<(VertexView<G>, V)> {
        self.graph
            .vertices()
            .into_iter()
            .map(|vertex| (vertex.clone(), self.get(vertex).unwrap().clone()))
            .collect_vec()
    }

    /// Returns the value corresponding to the provided key in the `result` hashmap.
    ///
    /// # Arguments
    ///
    /// * `key`: The key of type `K` for which the value is to be retrieved.
    pub fn get<K: Into<VertexRef>>(&self, vertex: K) -> Option<&V> {
        let vert = self.graph.local_vertex_ref(vertex.into())?;
        let bb: usize = vert.into();
        self.result.get(bb)
    }
}

pub struct AlgorithmResultIterator<'a, V> {
    iter: std::slice::Iter<'a, V>,
}

impl<'a, V> Iterator for AlgorithmResultIterator<'a, V> {
    type Item = (&'a V);

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

impl<'a, V, G> IntoIterator for &'a AlgorithmResult<V, G>
where
    V: Clone,
    G: GraphViewOps,
{
    type Item = (&'a V);
    type IntoIter = AlgorithmResultIterator<'a, V>;

    fn into_iter(self) -> Self::IntoIter {
        AlgorithmResultIterator {
            iter: self.result.iter(),
        }
    }
}

impl<V, G> AlgorithmResult<V, G>
where
    V: Clone + PartialOrd,
    G: GraphViewOps,
{
    /// Sorts the `AlgorithmResult` by its values in ascending or descending order.
    ///
    /// # Arguments
    ///
    /// * `reverse`: If `true`, sorts the result in descending order; otherwise, sorts in ascending order.
    ///
    /// # Returns
    ///
    /// A sorted vector of tuples containing keys of type `H` and values of type `Y`.
    /// returns a vertex view : value
    pub fn sort_by_value(&self, reverse: bool) -> Vec<(VertexView<G>, V)> {
        let mut sorted: Vec<(VertexView<G>, V)> = self.get_all();
        sorted.sort_by(|(_, a), (_, b)| {
            if reverse {
                b.partial_cmp(a).unwrap()
            } else {
                a.partial_cmp(b).unwrap()
            }
        });
        sorted
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
    pub fn sort_by_key(&self, reverse: bool) -> Vec<(VertexView<G>, V)> {
        let mut sorted: Vec<(VertexView<G>, V)> = self.get_all();
        sorted.sort_by(|(a, _), (b, _)| if reverse { b.cmp(a) } else { a.cmp(b) });
        sorted
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
    /// An `a vector of tuples with keys of type `H` and values of type `Y`.
    /// If `percentage` is `true`, the returned vector contains the top `k` percentage of elements.
    /// If `percentage` is `false`, the returned vector contains the top `k` elements.
    /// Returns empty vec if the result is empty or if `k` is 0.
    pub fn top_k(&self, k: usize, percentage: bool, reverse: bool) -> Vec<(VertexView<G>, V)> {
        if percentage {
            let total_count = self.result.len();
            let k = (total_count as f64 * (k as f64 / 100.0)) as usize;
            let sorted_result = self.sort_by_value(reverse);
            sorted_result.iter().cloned().take(k).collect()
        } else {
            let sorted_result = self.sort_by_value(reverse);
            sorted_result.iter().cloned().take(k).collect()
        }
    }
}

impl<G: GraphViewOps> AlgorithmResult<f64, G> {
    /// Creates a new `AlgorithmResult` with floating-point values converted to `OrderedFloat`.
    ///
    /// # Arguments
    ///
    /// * `hashmap`: A `HashMap` with keys of type `H` and values of type `f64`.
    ///
    /// # Returns
    ///
    /// An `AlgorithmResult` with the `f64` values converted to `OrderedFloat<f64>`.
    pub fn new_with_float(old_result: Vec<f64>, graph: G) -> AlgorithmResult<OrderedFloat<f64>, G> {
        let result: Vec<OrderedFloat<f64>> = old_result
            .into_iter()
            .map(|value| OrderedFloat::from(value))
            .collect();
        AlgorithmResult { result, graph }
    }
}

impl<V, G> AlgorithmResult<V, G>
where
    V: Clone + Ord + Hash + Eq,
    G: GraphViewOps,
{
    /// Groups the `AlgorithmResult` by its values.
    ///
    /// # Returns
    ///
    /// A `HashMap` where keys are unique values from the `AlgorithmResult` and values are vectors
    /// containing keys of type `H` that share the same value.
    pub fn group_by(&self) -> HashMap<V, Vec<VertexView<G>>> {
        let mut grouped: HashMap<V, Vec<VertexView<G>>> = HashMap::new();
        let key_val_list = self.get_all();
        for (key, value) in key_val_list {
            grouped.entry(value.clone()).or_default().push(key.clone());
        }
        grouped
    }
}

impl<V: Debug + Clone, G: GraphViewOps> Debug for AlgorithmResult<V, G> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut map_string = "{".to_string();
        for (key, value) in &self.get_all() {
            map_string.push_str(&format!("{:?}: {:?}, ", key.vertex, value));
        }
        map_string.pop(); // Remove the trailing comma
        map_string.pop(); // Remove the space
        map_string.push('}');
        write!(f, "{}", map_string)
    }
}

/// Add tests for all functions
#[cfg(test)]
mod algorithm_result_test {
    use crate::{
        algorithms::algorithm_result::AlgorithmResult,
        db::{api::view::internal::GraphOps, graph::vertex::VertexView},
        prelude::{AdditionOps, Graph, GraphViewOps, NO_PROPS},
    };

    fn generate_small_graph() -> Graph {
        let mut graph: Graph = Graph::new();
        graph.add_vertex(0, "A", NO_PROPS);
        graph.add_vertex(0, "B", NO_PROPS);
        graph.add_vertex(0, "C", NO_PROPS);
        graph
    }

    fn create_algo_result_u64() -> AlgorithmResult<u64, Graph> {
        let graph: Graph = generate_small_graph();
        let mut vec: Vec<u64> = vec![];
        vec.insert(graph.vertex("A").unwrap().vertex.into(), 10);
        vec.insert(graph.vertex("B").unwrap().vertex.into(), 20);
        vec.insert(graph.vertex("C").unwrap().vertex.into(), 30);
        AlgorithmResult::new(vec, graph.clone())
    }

    // fn group_by_test<G: GraphViewOps>() -> AlgorithmResult<u64, G> {
    //     let mut graph: Graph = generate_small_graph();
    //     graph.add_vertex(0, "D", NO_PROPS)?;
    //     let mut vec: Vec<u64> = vec![];
    //     vec.insert(graph.vertex("A"), 10);
    //     vec.insert(graph.vertex("B"), 20);
    //     vec.insert(graph.vertex("C"), 30);
    //     vec.insert(graph.vertex("D"), 40);
    //     AlgorithmResult::new(vec, graph)
    // }
    //
    // fn create_algo_result_f64<G: GraphViewOps>() -> AlgorithmResult<OrderedFloat<f64>, G>{
    //     let graph: Graph = generate_small_graph();
    //     let mut vec: Vec<f64> = vec![];
    //     vec.insert(graph.vertex("A"), 10.0);
    //     vec.insert(graph.vertex("B"), 20.0);
    //     vec.insert(graph.vertex("C"), 30.0);
    //     AlgorithmResult::new_with_float(vec, graph)
    // }
    //
    // fn create_algo_result_tuple<G: GraphViewOps>() -> AlgorithmResult<(f32, f32), G> {
    //     let graph: Graph = generate_small_graph();
    //     let mut vec: Vec<(f32, f32)> = vec![];
    //     vec.insert(graph.vertex("A"), (10.0, 20.0));
    //     vec.insert(graph.vertex("B"), (20.0, 30.0));
    //     vec.insert(graph.vertex("C"), (30.0, 40.0));
    //     AlgorithmResult::new(vec, graph)
    // }
    //
    // fn create_algo_result_hashmap_vec<G: GraphViewOps>() -> AlgorithmResult<Vec<(i64, String)>, G> {
    //     let graph: Graph = generate_small_graph();
    //     let mut vec: Vec<Vec<(i64, String)>> = vec![];
    //     vec.insert(graph.vertex("A"), vec![(11, "H".to_string())]);
    //     vec.insert(graph.vertex("B"), vec![]);
    //     vec.insert(
    //         graph.vertex("C"),
    //         vec![(22, "E".to_string()), (33, "F".to_string())],
    //     );
    //     AlgorithmResult::new(vec, graph)
    // }

    #[test]
    fn test_get() {
        let algo_result = create_algo_result_u64();
        assert_eq!(algo_result.get("C"), Some(&30));
        assert_eq!(algo_result.get("D"), None);
        // let algo_result = create_algo_result_f64();
        // assert_eq!(algo_result.get(&"C".to_string()).unwrap().0, 30.0);
        // let algo_result = create_algo_result_tuple();
        // assert_eq!(algo_result.get(&"C".to_string()).unwrap().0, 30.0);
        // let algo_result = create_algo_result_hashmap_vec();
        // assert_eq!(algo_result.get(&"C".to_string()).unwrap()[0].0, 22);
    }

    #[test]
    fn test_sort() {
        let algo_result = create_algo_result_u64();
        let sorted = algo_result.sort_by_value(true);
        assert_eq!(sorted[0].0, algo_result.graph.vertex("C").unwrap());
        let sorted = algo_result.sort_by_value(false);
        assert_eq!(sorted[0].0, algo_result.graph.vertex("A").unwrap());

        // let algo_result = create_algo_result_f64();
        // let sorted = algo_result.sort_by_value(true);
        // assert_eq!(sorted[0].0, "C");
        // let sorted = algo_result.sort_by_value(false);
        // assert_eq!(sorted[0].0, "A");
        //
        // let algo_result = create_algo_result_tuple();
        // assert_eq!(algo_result.sort_by_value(true)[0].0, "C");
        //
        // let algo_result = create_algo_result_hashmap_vec();
        // assert_eq!(algo_result.sort_by_value(true)[0].0, "C");
    }

    #[test]
    fn test_top_k() {
        let algo_result = create_algo_result_u64();
        let top_k = algo_result.top_k(2, false, false);
        assert_eq!(top_k[0].0, algo_result.graph.vertex("A").unwrap());
        let top_k = algo_result.top_k(2, false, true);
        assert_eq!(top_k[0].0, algo_result.graph.vertex("C").unwrap());

        // let algo_result = create_algo_result_f64();
        // let top_k = algo_result.top_k(2, false, false);
        // assert_eq!(top_k[0].0, "A");
        // let top_k = algo_result.top_k(2, false, true);
        // assert_eq!(top_k[0].0, "C");
        //
        // let algo_result = create_algo_result_tuple();
        // assert_eq!(algo_result.top_k(2, false, false)[0].0, "A");
        //
        // let algo_result = create_algo_result_hashmap_vec();
        // assert_eq!(algo_result.top_k(2, false, false)[0].0, "B");
    }

    // #[test]
    // fn test_group_by() {
    //     let algo_result = group_by_test();
    //     let grouped = algo_result.group_by();
    //     assert_eq!(grouped.get(&10).unwrap().len(), 2);
    //     assert_eq!(grouped.get(&10).unwrap().contains(&"A".to_string()), true);
    //     assert_eq!(grouped.get(&10).unwrap().contains(&"B".to_string()), false);
    //
    //     let algo_result = create_algo_result_f64();
    //     let grouped = algo_result.group_by();
    //     assert_eq!(grouped.get(&OrderedFloat::from(10.0)).unwrap().len(), 1);
    //     assert_eq!(
    //         grouped
    //             .get(&OrderedFloat::from(10.0))
    //             .unwrap()
    //             .contains(&"A".to_string()),
    //         true
    //     );
    //
    //     let algo_result = create_algo_result_hashmap_vec();
    //     assert_eq!(
    //         algo_result
    //             .group_by()
    //             .get(&vec![(11, "H".to_string())])
    //             .unwrap()
    //             .len(),
    //         1
    //     );
    // }

    #[test]
    fn test_get_all() {
        let algo_result = create_algo_result_u64();
        let all = algo_result.get_all();
        assert_eq!(all.len(), 3);
        // assert_eq!(all.contains(&algo_result.graph.vertex("A").unwrap()), true);

        // let algo_result = create_algo_result_f64();
        // let all = algo_result.get_all();
        // assert_eq!(all.len(), 3);
        // assert_eq!(all.contains_key(&"A".to_string()), true);
        //
        // let algo_result = create_algo_result_tuple();
        // assert_eq!(algo_result.get_all().get("A").unwrap().0, 10.0);
        // assert_eq!(algo_result.get_all().len(), 3);
        //
        // let algo_result = create_algo_result_hashmap_vec();
        // assert_eq!(algo_result.get_all().get("A").unwrap()[0].0, 11);
        // assert_eq!(algo_result.get_all().len(), 3);
    }

    #[test]
    fn test_sort_by_key() {
        let algo_result = create_algo_result_u64();
        let sorted = algo_result.sort_by_key(true);
        let my_array: Vec<(VertexView<Graph>, u64)> = vec![
            (algo_result.graph.vertex("C").unwrap(), 30 as u64),
            (algo_result.graph.vertex("B").unwrap(), 20 as u64),
            (algo_result.graph.vertex("A").unwrap(), 10 as u64),
        ];
        assert_eq!(my_array, sorted);
        //
        // let algo_result = create_algo_result_f64();
        // let sorted = algo_result.sort_by_key(true);
        // let my_array: Vec<(String, OrderedFloat<f64>)> = vec![
        //     ("C".to_string(), OrderedFloat(30.0)),
        //     ("B".to_string(), OrderedFloat(20.0)),
        //     ("A".to_string(), OrderedFloat(10.0)),
        // ];
        // assert_eq!(my_array, sorted);
        // //
        // let algo_result = create_algo_result_tuple();
        // let sorted = algo_result.sort_by_key(true);
        // let my_array: Vec<(String, (f32, f32))> = vec![
        //     ("C".to_string(), (30.0, 40.0)),
        //     ("B".to_string(), (20.0, 30.0)),
        //     ("A".to_string(), (10.0, 20.0)),
        // ];
        // assert_eq!(my_array, sorted);
        // //
        // let algo_result = create_algo_result_hashmap_vec();
        // let sorted = algo_result.sort_by_key(true);
        // let my_array: Vec<(String, Vec<(i64, String)>)> = vec![
        //     (
        //         "C".to_string(),
        //         vec![(22, "E".to_string()), (33, "F".to_string())],
        //     ),
        //     ("B".to_string(), vec![]),
        //     ("A".to_string(), vec![(11, "H".to_string())]),
        // ];
        // assert_eq!(my_array, sorted);
    }
}
