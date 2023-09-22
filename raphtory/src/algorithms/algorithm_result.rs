use itertools::Itertools;
use num_traits::Float;
use ordered_float::OrderedFloat;
use std::{
    borrow::Borrow,
    collections::{hash_map::Iter, HashMap},
    fmt,
    fmt::Debug,
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
    pub num_vertices: usize,
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
pub struct AlgorithmResult<K, V, O = V> {
    /// The result hashmap that stores keys of type `H` and values of type `Y`.
    pub algo_repr: AlgorithmRepr,
    pub result: HashMap<K, V>,
    marker: PhantomData<O>,
}

impl<K, V, O> AlgorithmResult<K, V, O>
where
    K: Clone + Hash + Eq + Ord,
    V: Clone,
{
    /// Creates a new instance of `AlgorithmResult` with the provided hashmap.
    ///
    /// # Arguments
    ///
    /// * `algo_name`: The name of the algorithm.
    /// * `num_vertices`: The number of vertices in the graph.
    /// * `result_type`: The type of the result.
    /// * `result`: A `HashMap` with keys of type `H` and values of type `Y`.
    pub fn new(
        algo_name: &str,
        num_vertices: usize,
        result_type: &str,
        result: HashMap<K, V>,
    ) -> Self {
        Self {
            algo_repr: AlgorithmRepr {
                algo_name: algo_name.to_string(),
                num_vertices: num_vertices,
                result_type: result_type.to_string(),
            },
            result,
            marker: PhantomData,
        }
    }

    /// Returns a formatted string representation of the algorithm.
    pub fn repr(&self) -> String {
        let algo_info_str = format!(
            "Algorithm Name: {}, Number of Vertices: {}, Result Type: {}",
            &self.algo_repr.algo_name, &self.algo_repr.num_vertices, &self.algo_repr.result_type
        );
        algo_info_str
    }

    /// Returns a reference to the entire `result` hashmap.
    pub fn get_all(&self) -> &HashMap<K, V> {
        &self.result
    }

    /// Returns the value corresponding to the provided key in the `result` hashmap.
    ///
    /// # Arguments
    ///
    /// * `key`: The key of type `H` for which the value is to be retrieved.
    pub fn get<Q>(&self, key: &Q) -> Option<&V>
    where
        Q: Hash + Eq + ?Sized,
        K: Borrow<Q>,
    {
        self.result.get(key)
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
    pub fn sort_by_key(&self, reverse: bool) -> Vec<(K, V)> {
        let mut sorted: Vec<(K, V)> = self.result.clone().into_iter().collect();
        sorted.sort_by(|(a, _), (b, _)| if reverse { b.cmp(a) } else { a.cmp(b) });
        sorted
    }

    pub fn iter(&self) -> Iter<'_, K, V> {
        self.result.iter()
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
    pub fn sort_by<F: FnMut(&V, &V) -> std::cmp::Ordering>(
        &self,
        mut cmp: F,
        reverse: bool,
    ) -> Vec<(K, V)> {
        let mut sorted: Vec<(K, V)> = self.result.clone().into_iter().collect();
        sorted.sort_by(|(_, a), (_, b)| if reverse { cmp(b, a) } else { cmp(a, b) });
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
    pub fn top_k_by<F: FnMut(&V, &V) -> std::cmp::Ordering>(
        &self,
        cmp: F,
        k: usize,
        percentage: bool,
        reverse: bool,
    ) -> Vec<(K, V)> {
        let k = if percentage {
            let total_count = self.result.len();
            (total_count as f64 * (k as f64 / 100.0)) as usize
        } else {
            k
        };
        self.sort_by(cmp, reverse).into_iter().take(k).collect()
    }

    pub fn min_by<F: FnMut(&V, &V) -> std::cmp::Ordering>(&self, mut cmp: F) -> Option<(K, V)> {
        self.result
            .iter()
            .min_by(|a, b| cmp(a.1, b.1))
            .map(|(k, v)| (k.clone(), v.clone()))
    }

    pub fn max_by<F: FnMut(&V, &V) -> std::cmp::Ordering>(&self, mut cmp: F) -> Option<(K, V)> {
        self.result
            .iter()
            .max_by(|a, b| cmp(a.1, b.1))
            .map(|(k, v)| (k.clone(), v.clone()))
    }

    pub fn median_by<F: FnMut(&V, &V) -> std::cmp::Ordering>(&self, mut cmp: F) -> Option<(K, V)> {
        let mut items: Vec<_> = self.result.iter().collect();
        let len = items.len();
        if len == 0 {
            return None;
        }
        items.sort_by(|(_, a), (_, b)| cmp(a, b));
        let median_index = len / 2;
        Some((items[median_index].0.clone(), items[median_index].1.clone()))
    }
}

impl<K, V, O> IntoIterator for AlgorithmResult<K, V, O>
where
    K: Clone + Hash + Eq + Ord,
    V: Clone,
    for<'a> &'a O: From<&'a V>,
{
    type Item = (K, V);
    type IntoIter = std::collections::hash_map::IntoIter<K, V>;

    fn into_iter(self) -> Self::IntoIter {
        self.result.into_iter()
    }
}

impl<'a, K, V, O> IntoIterator for &'a AlgorithmResult<K, V, O>
where
    K: Clone + Hash + Ord,
    V: Clone,
{
    type Item = (&'a K, &'a V);
    type IntoIter = Iter<'a, K, V>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<K: Clone + Hash + Eq + Ord, V: Clone, O> FromIterator<(K, V)> for AlgorithmResult<K, V, O> {
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        let result = iter.into_iter().collect();
        Self {
            algo_repr: AlgorithmRepr {
                algo_name: String::new(),
                num_vertices: 0,
                result_type: String::new(),
            },
            result,
            marker: PhantomData,
        }
    }
}

impl<K, V, O> AlgorithmResult<K, V, O>
where
    K: Clone + Hash + Eq + Ord,
    V: Clone,
    O: Ord,
    V: AsOrd<O>,
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
    pub fn sort_by_value(&self, reverse: bool) -> Vec<(K, V)> {
        self.sort_by(|a, b| O::cmp(a.as_ord(), b.as_ord()), reverse)
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
    pub fn top_k(&self, k: usize, percentage: bool, reverse: bool) -> Vec<(K, V)> {
        self.top_k_by(
            |a, b| O::cmp(a.as_ord(), b.as_ord()),
            k,
            percentage,
            reverse,
        )
    }

    pub fn min(&self) -> Option<(K, V)> {
        self.min_by(|a, b| O::cmp(a.as_ord(), b.as_ord()))
    }

    pub fn max(&self) -> Option<(K, V)> {
        self.max_by(|a, b| O::cmp(a.as_ord(), b.as_ord()))
    }

    pub fn median(&self) -> Option<(K, V)> {
        self.median_by(|a, b| O::cmp(a.as_ord(), b.as_ord()))
    }
}

impl<K, V, O> AlgorithmResult<K, V, O>
where
    K: Clone + Hash + Eq + Ord,
    V: Clone + Hash + Eq,
{
    /// Groups the `AlgorithmResult` by its values.
    ///
    /// # Returns
    ///
    /// A `HashMap` where keys are unique values from the `AlgorithmResult` and values are vectors
    /// containing keys of type `H` that share the same value.
    pub fn group_by(&self) -> HashMap<V, Vec<K>> {
        let mut grouped: HashMap<V, Vec<K>> = HashMap::new();
        for (key, value) in &self.result {
            grouped.entry(value.clone()).or_default().push(key.clone());
        }
        grouped
    }
}

impl<V: Debug, K: Debug, O> Debug for AlgorithmResult<K, V, O> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let map_string = self
            .result
            .iter()
            .map(|(key, value)| format!("{:?}: {:?}, ", key, value))
            .join(", ");
        write!(f, "{{{}}}", map_string)
    }
}

/// Add tests for all functions
#[cfg(test)]
mod algorithm_result_test {
    use crate::algorithms::algorithm_result::AlgorithmResult;
    use ordered_float::OrderedFloat;
    use std::collections::HashMap;

    fn create_algo_result_u64() -> AlgorithmResult<String, u64> {
        let mut map: HashMap<String, u64> = HashMap::new();
        map.insert("A".to_string(), 10);
        map.insert("B".to_string(), 20);
        map.insert("C".to_string(), 30);
        AlgorithmResult::new("create_algo_result_u64_test", 0, "", map)
    }

    fn group_by_test() -> AlgorithmResult<String, u64> {
        let mut map: HashMap<String, u64> = HashMap::new();
        map.insert("A".to_string(), 10);
        map.insert("B".to_string(), 20);
        map.insert("C".to_string(), 30);
        map.insert("D".to_string(), 10);
        AlgorithmResult::new("group_by_test", 0, "", map)
    }

    fn create_algo_result_f64() -> AlgorithmResult<String, f64, OrderedFloat<f64>> {
        let mut map: HashMap<String, f64> = HashMap::new();
        map.insert("A".to_string(), 10.0);
        map.insert("B".to_string(), 20.0);
        map.insert("C".to_string(), 30.0);
        AlgorithmResult::new("create_algo_result_f64", 0, "", map)
    }

    fn create_algo_result_tuple(
    ) -> AlgorithmResult<String, (f32, f32), (OrderedFloat<f32>, OrderedFloat<f32>)> {
        let mut map: HashMap<String, (f32, f32)> = HashMap::new();
        map.insert("A".to_string(), (10.0, 20.0));
        map.insert("B".to_string(), (20.0, 30.0));
        map.insert("C".to_string(), (30.0, 40.0));
        AlgorithmResult::new("create_algo_result_tuple", 0, "", map)
    }

    fn create_algo_result_hashmap_vec() -> AlgorithmResult<String, Vec<(i64, String)>> {
        let mut map: HashMap<String, Vec<(i64, String)>> = HashMap::new();
        map.insert("A".to_string(), vec![(11, "H".to_string())]);
        map.insert("B".to_string(), vec![]);
        map.insert(
            "C".to_string(),
            vec![(22, "E".to_string()), (33, "F".to_string())],
        );
        AlgorithmResult::new("create_algo_result_hashmap_vec", 0, "", map)
    }

    #[test]
    fn test_min_max_value() {
        let algo_result = create_algo_result_u64();
        assert_eq!(algo_result.min(), Some(("A".to_string(), 10u64)));
        assert_eq!(algo_result.max(), Some(("C".to_string(), 30u64)));
        assert_eq!(algo_result.median(), Some(("B".to_string(), 20u64)));
        let algo_result = create_algo_result_f64();
        assert_eq!(algo_result.min(), Some(("A".to_string(), 10.0)));
        assert_eq!(algo_result.max(), Some(("C".to_string(), 30.0)));
        assert_eq!(algo_result.median(), Some(("B".to_string(), 20.0)));
    }

    #[test]
    fn test_get() {
        let algo_result = create_algo_result_u64();
        assert_eq!(algo_result.get(&"C".to_string()), Some(&30));
        assert_eq!(algo_result.get(&"D".to_string()), None);
        let algo_result = create_algo_result_f64();
        assert_eq!(algo_result.get(&"C".to_string()), Some(&30.0));
        let algo_result = create_algo_result_tuple();
        assert_eq!(algo_result.get(&"C".to_string()).unwrap().0, 30.0);
        let algo_result = create_algo_result_hashmap_vec();
        assert_eq!(algo_result.get(&"C".to_string()).unwrap()[0].0, 22);
    }

    #[test]
    fn test_sort() {
        let algo_result = create_algo_result_u64();
        let sorted = algo_result.sort_by_value(true);
        assert_eq!(sorted[0].0, "C");
        let sorted = algo_result.sort_by_value(false);
        assert_eq!(sorted[0].0, "A");

        let algo_result = create_algo_result_f64();
        let sorted = algo_result.sort_by_value(true);
        assert_eq!(sorted[0].0, "C");
        let sorted = algo_result.sort_by_value(false);
        assert_eq!(sorted[0].0, "A");

        let algo_result = create_algo_result_tuple();
        assert_eq!(algo_result.sort_by_value(true)[0].0, "C");

        let algo_result = create_algo_result_hashmap_vec();
        assert_eq!(algo_result.sort_by_value(true)[0].0, "C");
    }

    #[test]
    fn test_top_k() {
        let algo_result = create_algo_result_u64();
        let top_k = algo_result.top_k(2, false, false);
        assert_eq!(top_k[0].0, "A");
        let top_k = algo_result.top_k(2, false, true);
        assert_eq!(top_k[0].0, "C");

        let algo_result = create_algo_result_f64();
        let top_k = algo_result.top_k(2, false, false);
        assert_eq!(top_k[0].0, "A");
        let top_k = algo_result.top_k(2, false, true);
        assert_eq!(top_k[0].0, "C");

        let algo_result = create_algo_result_tuple();
        assert_eq!(algo_result.top_k(2, false, false)[0].0, "A");

        let algo_result = create_algo_result_hashmap_vec();
        assert_eq!(algo_result.top_k(2, false, false)[0].0, "B");
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
        assert!(all.contains_key("A"));

        let algo_result = create_algo_result_f64();
        let all = algo_result.get_all();
        assert_eq!(all.len(), 3);
        assert!(all.contains_key("A"));

        let algo_result = create_algo_result_tuple();
        assert_eq!(algo_result.get_all().get("A").unwrap().0, 10.0);
        assert_eq!(algo_result.get_all().len(), 3);

        let algo_result = create_algo_result_hashmap_vec();
        assert_eq!(algo_result.get_all().get("A").unwrap()[0].0, 11);
        assert_eq!(algo_result.get_all().len(), 3);
    }

    #[test]
    fn test_sort_by_key() {
        let algo_result = create_algo_result_u64();
        let sorted = algo_result.sort_by_key(true);
        let my_array: Vec<(String, u64)> = vec![
            ("C".to_string(), 30u64),
            ("B".to_string(), 20u64),
            ("A".to_string(), 10u64),
        ];
        assert_eq!(my_array, sorted);
        //
        let algo_result = create_algo_result_f64();
        let sorted = algo_result.sort_by_key(true);
        let my_array: Vec<(String, f64)> = vec![
            ("C".to_string(), 30.0),
            ("B".to_string(), 20.0),
            ("A".to_string(), 10.0),
        ];
        assert_eq!(my_array, sorted);
        //
        let algo_result = create_algo_result_tuple();
        let sorted = algo_result.sort_by_key(true);
        let my_array: Vec<(String, (f32, f32))> = vec![
            ("C".to_string(), (30.0, 40.0)),
            ("B".to_string(), (20.0, 30.0)),
            ("A".to_string(), (10.0, 20.0)),
        ];
        assert_eq!(my_array, sorted);
        //
        let algo_result = create_algo_result_hashmap_vec();
        let sorted = algo_result.sort_by_key(true);
        let my_array: Vec<(String, Vec<(i64, String)>)> = vec![
            (
                "C".to_string(),
                vec![(22, "E".to_string()), (33, "F".to_string())],
            ),
            ("B".to_string(), vec![]),
            ("A".to_string(), vec![(11, "H".to_string())]),
        ];
        assert_eq!(my_array, sorted);
    }
}
