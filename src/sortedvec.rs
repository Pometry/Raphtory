use std::ops::Range;

#[derive(Debug, Default, PartialEq)]
pub struct SortedVec<K:Ord, V>(Vec<(K, V)>);

impl<K: Ord + Copy, V> SortedVec<K, V> {
    pub fn new() -> Self {
        SortedVec(vec![])
    }

    pub fn insert(&mut self, k: K, v: V) {
        if let Err(i) = self.0.binary_search_by_key(&k, |&(k, _)| k) {
            self.0.insert(i, (k, v));
        }
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn range(&self, r: Range<K>) -> impl Iterator<Item = (&K, &V)> {
        let i0 = self.0.binary_search_by_key(&r.start, |&(k, _)| k);
        let j0 = self.0.binary_search_by_key(&r.end, |&(k, _)| k);

        let i: usize = if let Ok(i1) = &i0 {
            *i1
        } else {
            i0.unwrap_err()
        };

        let j: usize = if let Ok(i1) = &j0 {
            *i1
        } else {
            j0.unwrap_err()
        };


        self.0[i..j].iter().map(|(k, v)| (k, v))
    }

    pub fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.0.iter().map(|(k, v)| (k, v))
    }
}

#[cfg(test)]
mod sorted_vec_tests {

    use super::SortedVec;


    #[test]
    fn sorted_vec_empty() {
        let vs: SortedVec<i32, String> = SortedVec::new();

        let actual = vs.range(3 .. 185).collect::<Vec<_>>();
        let expected:Vec<(&i32, &String)> = vec![];
        assert_eq!(actual, expected);
    }

    #[test]
    fn sorted_vec_insert_order() {
        let mut vs: SortedVec<i32, String> = SortedVec::new();
        vs.insert(5, "what".to_string());
        vs.insert(7, "where".to_string());
        vs.insert(9, "who".to_string());

        let actual = vs.range(5 .. 8).collect::<Vec<_>>();
        assert_eq!(actual, vec![(&5, &"what".to_string()), (&7, &"where".to_string())]);
    }


    #[test]
    fn sorted_vec_insert_out_of_order() {
        let mut vs: SortedVec<i32, String> = SortedVec::new();
        vs.insert(9, "what".to_string());
        vs.insert(2, "where".to_string());
        vs.insert(-3, "who".to_string());

        let actual = vs.range(-5 .. 8).collect::<Vec<_>>();
        assert_eq!(actual, vec![(&-3, &"who".to_string()), (&2, &"where".to_string())]);
    }
}
