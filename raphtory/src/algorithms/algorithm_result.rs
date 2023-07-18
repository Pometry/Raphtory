use std::collections::HashMap;
use std::fmt;
use std::fmt::Debug;
use ordered_float::OrderedFloat;
use std::hash::Hash;
// use pyo3::prelude::*;
// use pyo3::types::{IntoPyDict, PyDict, PyList};

pub struct AlgorithmResult<H, Y>
    where
        H: Clone + Hash + Eq + Ord,
        Y: Clone
{
    pub result: HashMap<H, Y>,
}

impl<H, Y> AlgorithmResult<H, Y>
    where
        H: Clone + Hash + Eq + Ord,
        Y: Clone,
{
    pub fn new(result: HashMap<H, Y>) -> Self {
        Self {
            result,
        }
    }

    pub fn get_all(&self) -> &HashMap<H, Y> {
        &self.result
    }

    pub fn get(&self, key: &H) -> Option<&Y> {
        self.result.get(&key)
    }

    // pub fn to_df<'a>(
    //     &self,
    //     py: Python<'a>
    // ) -> PyResult<PyObject>
    // where
    //     H: Clone + ToPyObject,
    //     Y: Clone + ToPyObject,
    // {
    //     let hashmap = &self.result;
    //     let mut keys = Vec::new();
    //     let mut values = Vec::new();
    //     for (key, value) in hashmap.iter() {
    //         keys.push(key.to_object(py));
    //         values.push(value.to_object(py));
    //     }
    //     let dict = PyDict::new(py);
    //     dict.set_item("Key", PyList::new(py, keys.as_slice()))?;
    //     dict.set_item("Value", PyList::new(py, values.as_slice()))?;
    //     let pandas = pyo3::types::PyModule::import(py, "pandas")?;
    //     let df: &PyAny = pandas.getattr("DataFrame")?.call1((dict,))?;
    //     Ok(df.to_object(py))
    // }
}

impl<H, Y> AlgorithmResult<H, Y>
    where
        H: Clone + Hash + Eq + Ord,
        Y: Clone + PartialOrd
{
    pub fn sort_by_value(&self, reverse: bool) -> Vec<(H, Y)> {
        let mut sorted: Vec<(H, Y)> = self.result.clone().into_iter().collect();
        sorted.sort_by(|(_, a), (_, b)| {
            if reverse {
                b.partial_cmp(a).unwrap()
            } else {
                a.partial_cmp(b).unwrap()
            }
        });
        sorted
    }

    pub fn sort_by_key(&self, reverse: bool) -> Vec<(H, Y)> {
        let mut sorted: Vec<(H, Y)> = self.result.clone().into_iter().collect();
        sorted.sort_by(|(a, _), (b, _)| {
            if reverse {
                b.cmp(a)
            } else {
                a.cmp(b)
            }
        });
        sorted
    }


    pub fn top_k(&self, k: usize, percentage: bool, reverse: bool) -> Option<Vec<(H, Y)>> {
        if percentage {
            let total_count = self.result.len();
            let k = (total_count as f64 * (k as f64 / 100.0)) as usize;
            let sorted_result = self.sort_by_value(reverse);
            Some(sorted_result.iter().cloned().take(k).collect())
        } else {
            let sorted_result = self.sort_by_value(reverse);
            Some(sorted_result.iter().cloned().take(k).collect())
        }
    }

}

impl<H: Clone + Hash + Eq + Ord> AlgorithmResult<H, f64> {
    pub fn new_with_float(hashmap: HashMap<H, f64>) -> AlgorithmResult<H, OrderedFloat<f64>> {
        let converted_hashmap: HashMap<H, OrderedFloat<f64>> = hashmap
            .into_iter()
            .map(|(key, value)| (key, OrderedFloat::from(value)))
            .collect();
        AlgorithmResult {
            result: converted_hashmap,
        }
    }
}

impl<H, Y> AlgorithmResult<H, Y>
    where
        H: Clone + Hash + Eq + Ord,
        Y: Clone + Ord + Hash + Eq
{
    pub fn group_by(
        &self
    ) -> HashMap<Y, Vec<H>> {
        let mut grouped: HashMap<Y, Vec<H>> = HashMap::new();
        for (key, value) in &self.result {
            grouped.entry(value.clone()).or_default().push(key.clone());
        }
        grouped
    }
}

impl<Y: Debug + Clone, H: Debug + Clone + Hash + Eq + Ord> Debug for AlgorithmResult<H, Y> {
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
    use std::collections::HashMap;
    use ordered_float::OrderedFloat;
    use crate::algorithms::algorithm_result::AlgorithmResult;

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
        map.insert("C".to_string(), vec![(22, "E".to_string()), (33, "F".to_string())]);
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
        assert_eq!(top_k.unwrap()[0].0, "A");
        let top_k = algo_result.top_k(2, false, true);
        assert_eq!(top_k.unwrap()[0].0, "C");

        let algo_result = create_algo_result_f64();
        let top_k = algo_result.top_k(2, false, false);
        assert_eq!(top_k.unwrap()[0].0, "A");
        let top_k = algo_result.top_k(2, false, true);
        assert_eq!(top_k.unwrap()[0].0, "C");

        let algo_result = create_algo_result_tuple();
        assert_eq!(algo_result.top_k(2, false, false).unwrap()[0].0, "A");

        let algo_result = create_algo_result_hashmap_vec();
        assert_eq!(algo_result.top_k(2, false, false).unwrap()[0].0, "B");
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

        let algo_result = create_algo_result_hashmap_vec();
        assert_eq!(algo_result.group_by().get(&vec![(11, "H".to_string())]).unwrap().len(), 1);
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
            ("A".to_string(), 10 as u64)];
        assert_eq!(my_array, sorted);
        //
        let algo_result = create_algo_result_f64();
        let sorted = algo_result.sort_by_key(true);
        let my_array: Vec<(String, OrderedFloat<f64>)> = vec![
            ("C".to_string(), OrderedFloat(30.0)),
            ("B".to_string(), OrderedFloat(20.0)),
            ("A".to_string(), OrderedFloat(10.0))];
        assert_eq!(my_array, sorted);
        //
        let algo_result = create_algo_result_tuple();
        let sorted = algo_result.sort_by_key(true);
        let my_array: Vec<(String, (f32, f32))> = vec![
            ("C".to_string(), (30.0, 40.0)),
            ("B".to_string(), (20.0, 30.0)),
            ("A".to_string(), (10.0, 20.0))];
        assert_eq!(my_array, sorted);
        //
        let algo_result = create_algo_result_hashmap_vec();
        let sorted = algo_result.sort_by_key(true);
        let my_array: Vec<(String, Vec<(i64, String)>)> = vec![
            ("C".to_string(), vec![(22, "E".to_string()), (33, "F".to_string())]),
            ("B".to_string(), vec![]),
            ("A".to_string(), vec![(11, "H".to_string())])];
        assert_eq!(my_array, sorted);
    }
}