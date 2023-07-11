use polars::prelude::*;
use std::collections::HashMap;
use polars::export::num::ToPrimitive;

pub struct AlgorithmResult<T> {
    result: HashMap<String, T>,
    is_categorical: bool,
}

pub trait AsValueSeries<T> {
    fn as_series(value_vector: Vec<T>) -> Series;
}

impl AsValueSeries<u64> for u64 {
    fn as_series(value_vector: Vec<u64>) -> Series {
        Series::new("value", value_vector)
    }
}

impl AsValueSeries<u32> for u32 {
    fn as_series(value_vector: Vec<u32>) -> Series {
        Series::new("value", value_vector)
    }
}

impl AsValueSeries<i64> for i64 {
    fn as_series(value_vector: Vec<i64>) -> Series {
        Series::new("value", value_vector)
    }
}

impl AsValueSeries<i32> for i32 {
    fn as_series(value_vector: Vec<i32>) -> Series {
        Series::new("value", value_vector)
    }
}

impl AsValueSeries<f64> for f64 {
    fn as_series(value_vector: Vec<f64>) -> Series {
        Series::new("value", value_vector)
    }
}

impl AsValueSeries<f32> for f32 {
    fn as_series(value_vector: Vec<f32>) -> Series {
        Series::new("value", value_vector)
    }
}

impl AsValueSeries<bool> for bool {
    fn as_series(value_vector: Vec<bool>) -> Series {
        Series::new("value", value_vector)
    }
}

impl<T> AlgorithmResult<T> where T: AsValueSeries<T> + Clone + Ord + Eq + std::hash::Hash + Clone {
    pub fn new(result: HashMap<String, T>, is_categorical: bool) -> Self {
        AlgorithmResult {
            result,
            is_categorical,
        }
    }

    pub fn get(&self, key: &str) -> Option<&T> {
        self.result.get(key)
    }

    pub fn to_polars_df(&self) -> DataFrame {
        let keys: Vec<&str> = self.result.keys().map(|s| s.as_str()).collect();
        let y = self.result.values().cloned().collect();
        let values: Series = T::as_series(y);

        let series_keys = Series::new("key", keys);

        let columns = vec![series_keys, values];
        DataFrame::new(columns).unwrap()
    }

    // to pandas dataframe


    pub fn sort(&self, reverse: bool) -> Vec<(String, T)> {
        let mut sorted: Vec<(String, T)> = self.result.clone().into_iter().collect();
        sorted.sort_by(|(_, a), (_, b)| {
            if reverse {
                b.cmp(a)
            } else {
                a.cmp(b)
            }
        });
        sorted
    }

    pub fn top_k(&self, k: usize, percentage: bool, reverse: bool) -> Option<Vec<(String, T)>> {
        if !self.is_categorical {
            if percentage {
                let total_count = self.result.len();
                let k = (total_count as f64 * (k as f64 / 100.0)) as usize;
                let sorted_result = self.sort(reverse);
                Some(sorted_result.iter().cloned().take(k).collect())
            } else {
                let sorted_result = self.sort(reverse);
                Some(sorted_result.iter().cloned().take(k).collect())
            }
        } else {
            println!("Cannot run top(k) on non-categorical data");
            None
        }
    }

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

/// Add tests for all functions
#[cfg(test)]
mod algorithm_result_test {

    use std::collections::HashMap;
    use crate::algorithms::algorithm_result::AlgorithmResult;

    fn create_algo_result() -> AlgorithmResult<u64> {
        let mut map: HashMap<String, u64> = HashMap::new();
        map.insert("A".to_string(), 10);
        map.insert("B".to_string(), 20);
        map.insert("C".to_string(), 30);
        AlgorithmResult::new(map.clone(), false)
    }

    fn create_categorical_algo_result() -> AlgorithmResult<u64> {
        let mut map: HashMap<String, u64> = HashMap::new();
        map.insert("A".to_string(), 1);
        map.insert("B".to_string(), 1);
        map.insert("C".to_string(), 2);
        AlgorithmResult::new(map.clone(), true)
    }

    #[test]
    fn test_hashmap_internal() {
        let algo_result = create_algo_result();
        let mut map: HashMap<String, u64> = HashMap::new();
        map.insert("A".to_string(), 10);
        map.insert("B".to_string(), 20);
        map.insert("C".to_string(), 30);
        assert_eq!(algo_result.result, map);
    }

    #[test]
    fn test_get() {
        let algo_result = create_algo_result();
        assert_eq!(algo_result.get("C"), Some(&30));
        assert_eq!(algo_result.get("D"), None);
    }

    #[test]
    fn test_sort() {
        let algo_result = create_algo_result();
        let sorted = algo_result.sort(true);
        assert_eq!(sorted[0].0, "C");
        let sorted = algo_result.sort(false);
        assert_eq!(sorted[0].0, "A");
    }

    #[test]
    fn test_to_polars_df() {
        let algo_result = create_algo_result();
        let df = algo_result.to_polars_df();
        assert_eq!(df.shape(), (3, 2));
        assert_eq!(df.get_column_names(), &["key", "value"]);
        assert_eq!(df.get_columns().get(0).unwrap().dtype(), &polars::prelude::DataType::Utf8);
        assert_eq!(df.get_columns().get(1).unwrap().dtype(), &polars::prelude::DataType::UInt64);
    }

    #[test]
    fn test_top_k() {
        let algo_result = create_algo_result();
        let top_k = algo_result.top_k(2, false, false);
        assert_eq!(top_k.unwrap()[0].0, "A");
        let top_k = algo_result.top_k(2, false, true);
        assert_eq!(top_k.unwrap()[0].0, "C");
    }

    #[test]
    fn test_group_by() {
        let algo_result = create_categorical_algo_result();
        let grouped = algo_result.group_by();
        assert_eq!(grouped.get(&1).unwrap().len(), 2);
        assert_eq!(grouped.get(&1).unwrap().contains(&"A".to_string()), true);
        assert_eq!(grouped.get(&1).unwrap().contains(&"B".to_string()), true);
    }
}

