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
pub struct AlgorithmResult<K, V>
where
    K: Clone + Hash + Eq + Ord,
    V: Clone,
{
    /// The result hashmap that stores keys of type `H` and values of type `Y`.
    pub result: HashMap<K, V>,
}

impl<K, V> AlgorithmResult<K, V>
where
    K: Clone + Hash + Eq + Ord,
    V: Clone,
{
    /// Creates a new instance of `AlgorithmResult` with the provided hashmap.
    ///
    /// # Arguments
    ///
    /// * `result`: A `HashMap` with keys of type `H` and values of type `Y`.
    pub fn new(result: HashMap<K, V>) -> Self {
        Self { result }
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
    pub fn get(&self, key: &K) -> Option<&V> {
        self.result.get(&key)
    }
}

pub struct AlgorithmResultIterator<'a, K, V> {
    iter: std::collections::hash_map::Iter<'a, K, V>,
}

impl<'a, K, V> Iterator for AlgorithmResultIterator<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

impl<'a, K, V> IntoIterator for &'a AlgorithmResult<K, V>
where
    K: Clone + Hash + Ord,
    V: Clone,
{
    type Item = (&'a K, &'a V);
    type IntoIter = AlgorithmResultIterator<'a, K, V>;

    fn into_iter(self) -> Self::IntoIter {
        AlgorithmResultIterator {
            iter: self.result.iter(),
        }
    }
}

impl<K, V> AlgorithmResult<K, V>
where
    K: Clone + Hash + Eq + Ord,
    V: Clone + PartialOrd,
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
        let mut sorted: Vec<(K, V)> = self.result.clone().into_iter().collect();
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
    pub fn sort_by_key(&self, reverse: bool) -> Vec<(K, V)> {
        let mut sorted: Vec<(K, V)> = self.result.clone().into_iter().collect();
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
    pub fn top_k(&self, k: usize, percentage: bool, reverse: bool) -> Vec<(K, V)> {
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

    pub fn min_key(&self) -> Option<&K> {
        self.result.keys().min()
    }

    pub fn max_key(&self) -> Option<&K> {
        self.result.keys().max()
    }
}

impl<K: Clone + Hash + Eq + Ord> AlgorithmResult<K, f64> {
    /// Creates a new `AlgorithmResult` with floating-point values converted to `OrderedFloat`.
    ///
    /// # Arguments
    ///
    /// * `hashmap`: A `HashMap` with keys of type `H` and values of type `f64`.
    ///
    /// # Returns
    ///
    /// An `AlgorithmResult` with the `f64` values converted to `OrderedFloat<f64>`.
    pub fn new_with_float(hashmap: HashMap<K, f64>) -> AlgorithmResult<K, OrderedFloat<f64>> {
        let converted_hashmap: HashMap<K, OrderedFloat<f64>> = hashmap
            .into_iter()
            .map(|(key, value)| (key, OrderedFloat::from(value)))
            .collect();
        AlgorithmResult {
            result: converted_hashmap,
        }
    }
}

impl<K, V> AlgorithmResult<K, V>
where
    K: Clone + Hash + Eq + Ord,
    V: Clone + Ord + Hash + Eq,
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

    pub fn min_value(&self) -> Option<&V> {
        self.result.values().min()
    }

    pub fn max_value(&self) -> Option<&V> {
        self.result.values().max()
    }

    // pub fn group_by(&self) -> AlgorithmResult<V, Vec<K>> {
    //     let mut grouped: HashMap<V, Vec<K>> = HashMap::new();
    //     for (key, value) in &self.result {
    //         grouped.entry(value.clone()).or_default().push(key.clone());
    //     }
    //     AlgorithmResult::new(grouped)
    // }
}

impl<V: Debug + Clone, K: Debug + Clone + Hash + Eq + Ord> Debug for AlgorithmResult<K, V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut map_string = "{".to_string();
        for (key, value) in &self.result {
            map_string.push_str(&format!("{:?}: {:?}, ", key, value));
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
    use crate::algorithms::algorithm_result::AlgorithmResult;
    use ordered_float::OrderedFloat;
    use std::collections::HashMap;

    fn create_algo_result_u64() -> AlgorithmResult<String, u64> {
        let mut map: HashMap<String, u64> = HashMap::new();
        map.insert("A".to_string(), 10);
        map.insert("B".to_string(), 20);
        map.insert("C".to_string(), 30);
        AlgorithmResult::new(map.clone())
    }

    fn group_by_test() -> AlgorithmResult<String, u64> {
        let mut map: HashMap<String, u64> = HashMap::new();
        map.insert("A".to_string(), 10);
        map.insert("B".to_string(), 20);
        map.insert("C".to_string(), 30);
        map.insert("D".to_string(), 10);
        AlgorithmResult::new(map.clone())
    }

    fn create_algo_result_f64() -> AlgorithmResult<String, OrderedFloat<f64>> {
        let mut map: HashMap<String, f64> = HashMap::new();
        map.insert("A".to_string(), 10.0);
        map.insert("B".to_string(), 20.0);
        map.insert("C".to_string(), 30.0);
        AlgorithmResult::new_with_float(map.clone())
    }

    fn create_algo_result_tuple() -> AlgorithmResult<String, (f32, f32)> {
        let mut map: HashMap<String, (f32, f32)> = HashMap::new();
        map.insert("A".to_string(), (10.0, 20.0));
        map.insert("B".to_string(), (20.0, 30.0));
        map.insert("C".to_string(), (30.0, 40.0));
        AlgorithmResult::new(map.clone())
    }

    fn create_algo_result_hashmap_vec() -> AlgorithmResult<String, Vec<(i64, String)>> {
        let mut map: HashMap<String, Vec<(i64, String)>> = HashMap::new();
        map.insert("A".to_string(), vec![(11, "H".to_string())]);
        map.insert("B".to_string(), vec![]);
        map.insert(
            "C".to_string(),
            vec![(22, "E".to_string()), (33, "F".to_string())],
        );
        AlgorithmResult::new(map.clone())
    }

    #[test]
    fn test_get() {
        let algo_result = create_algo_result_u64();
        assert_eq!(algo_result.get(&"C".to_string()), Some(&30));
        assert_eq!(algo_result.get(&"D".to_string()), None);
        let algo_result = create_algo_result_f64();
        assert_eq!(algo_result.get(&"C".to_string()).unwrap().0, 30.0);
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
        assert_eq!(grouped.get(&10).unwrap().contains(&"A".to_string()), true);
        assert_eq!(grouped.get(&10).unwrap().contains(&"B".to_string()), false);

        let algo_result = create_algo_result_f64();
        let grouped = algo_result.group_by();
        assert_eq!(grouped.get(&OrderedFloat::from(10.0)).unwrap().len(), 1);
        assert_eq!(
            grouped
                .get(&OrderedFloat::from(10.0))
                .unwrap()
                .contains(&"A".to_string()),
            true
        );

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
        assert_eq!(all.contains_key(&"A".to_string()), true);

        let algo_result = create_algo_result_f64();
        let all = algo_result.get_all();
        assert_eq!(all.len(), 3);
        assert_eq!(all.contains_key(&"A".to_string()), true);

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
            ("C".to_string(), 30 as u64),
            ("B".to_string(), 20 as u64),
            ("A".to_string(), 10 as u64),
        ];
        assert_eq!(my_array, sorted);
        //
        let algo_result = create_algo_result_f64();
        let sorted = algo_result.sort_by_key(true);
        let my_array: Vec<(String, OrderedFloat<f64>)> = vec![
            ("C".to_string(), OrderedFloat(30.0)),
            ("B".to_string(), OrderedFloat(20.0)),
            ("A".to_string(), OrderedFloat(10.0)),
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
