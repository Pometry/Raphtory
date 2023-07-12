use std::collections::HashMap;
use ordered_float::OrderedFloat;
use std::hash::Hash;

struct AlgorithmResult<T> where T: Clone   {
    result: HashMap<String, T>,
}

impl<T> AlgorithmResult<T>
    where
        T: Clone + PartialOrd  {
    pub fn new(result: HashMap<String, T>) -> Self {
        Self {
            result,
        }
    }

    pub fn get(&self, key: &str) -> Option<&T> {
        self.result.get(key)
    }

    pub fn sort(&self, reverse: bool) -> Vec<(String, T)> {
        let mut sorted: Vec<(String, T)> = self.result.clone().into_iter().collect();
        sorted.sort_by(|(_, a), (_, b)| {
            if reverse {
                b.partial_cmp(a).unwrap()
            } else {
                a.partial_cmp(b).unwrap()
            }
        });
        sorted
    }

    pub fn top_k(&self, k: usize, percentage: bool, reverse: bool) -> Option<Vec<(String, T)>> {
        if percentage {
            let total_count = self.result.len();
            let k = (total_count as f64 * (k as f64 / 100.0)) as usize;
            let sorted_result = self.sort(reverse);
            Some(sorted_result.iter().cloned().take(k).collect())
        } else {
            let sorted_result = self.sort(reverse);
            Some(sorted_result.iter().cloned().take(k).collect())
        }
    }

}

impl AlgorithmResult<f64> {
    pub fn new_with_float(hashmap: HashMap<String, f64>) -> AlgorithmResult<OrderedFloat<f64>> {
        let converted_hashmap: HashMap<String, OrderedFloat<f64>> = hashmap
            .into_iter()
            .map(|(key, value)| (key, OrderedFloat::from(value)))
            .collect();
        AlgorithmResult {
            result: converted_hashmap,
        }
    }
}

impl<T> AlgorithmResult<T> where T: Clone + Ord + Hash + Eq {
    pub fn group_by(
        &self
    ) -> HashMap<T, Vec<String>> {
        let mut grouped: HashMap<T, Vec<String>> = HashMap::new();
        for (key, value) in &self.result {
            grouped.entry(value.clone()).or_default().push(key.clone());
        }
        grouped
    }
}



fn main() {
    let mut map = HashMap::new();
    map.insert("A".to_string(), 10);
    map.insert("B".to_string(), 20);
    map.insert("C".to_string(), 30);
    let algorithm_result = AlgorithmResult::new(map.clone());
    println!("{:?}", algorithm_result.result);
}

/// Add tests for all functions
#[cfg(test)]
mod algorithm_result_test {

    use std::collections::HashMap;
    use ordered_float::OrderedFloat;
    use crate::AlgorithmResult;

    fn create_algo_result_u64() -> AlgorithmResult<u64> {
        let mut map: HashMap<String, u64> = HashMap::new();
        map.insert("A".to_string(), 10);
        map.insert("B".to_string(), 20);
        map.insert("C".to_string(), 30);
        AlgorithmResult::new(map.clone())
    }

    fn group_by_test() -> AlgorithmResult<u64> {
        let mut map: HashMap<String, u64> = HashMap::new();
        map.insert("A".to_string(), 10);
        map.insert("B".to_string(), 20);
        map.insert("C".to_string(), 30);
        map.insert("D".to_string(), 10);
        AlgorithmResult::new(map.clone())
    }

    fn create_algo_result_f64() -> AlgorithmResult<OrderedFloat<f64>> {
        let mut map: HashMap<String, f64> = HashMap::new();
        map.insert("A".to_string(), 10.0);
        map.insert("B".to_string(), 20.0);
        map.insert("C".to_string(), 30.0);
        AlgorithmResult::new_with_float(map.clone())
    }

    #[test]
    fn test_get() {
        let algo_result = create_algo_result_u64();
        assert_eq!(algo_result.get("C"), Some(&30));
        assert_eq!(algo_result.get("D"), None);
        let algo_result = create_algo_result_f64();
        assert_eq!(algo_result.get("C").unwrap().0, 30.0);
    }

    #[test]
    fn test_sort() {
        let algo_result = create_algo_result_u64();
        let sorted = algo_result.sort(true);
        assert_eq!(sorted[0].0, "C");
        let sorted = algo_result.sort(false);
        assert_eq!(sorted[0].0, "A");

        let algo_result = create_algo_result_f64();
        let sorted = algo_result.sort(true);
        assert_eq!(sorted[0].0, "C");
        let sorted = algo_result.sort(false);
        assert_eq!(sorted[0].0, "A");

    }

    #[test]
    fn test_top_k() {
        let algo_result = create_algo_result_u64();
        let top_k = algo_result.top_k(2, false, false);
        assert_eq!(top_k.unwrap()[0].0, "A");
        let top_k = algo_result.top_k(2, false, true);
        assert_eq!(top_k.unwrap()[0].0, "C");

        let algo_result = create_algo_result_f64();
        let top_k = algo_result.top_k(2, false, false);
        assert_eq!(top_k.unwrap()[0].0, "A");
        let top_k = algo_result.top_k(2, false, true);
        assert_eq!(top_k.unwrap()[0].0, "C");
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
        assert_eq!(grouped.get(&OrderedFloat::from(10.0)).unwrap().contains(&"A".to_string()), true);
    }
}

// tests
// String, (f32, f32) HITS : what is a Hashmap
// HashMap String, Vec(i64, String)
//