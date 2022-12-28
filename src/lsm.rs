use std::fmt::Debug;

use itertools::Itertools;
use replace_with::replace_with_or_abort;

static MERGE_SORT_SIZE: usize = 64;

#[derive(Debug, PartialEq, Default)]
pub struct LSMSet<K: Ord> {
    cur: Vec<K>,
    sorted: Vec<K>,
}

impl<K: Ord> LSMSet<K> {
    pub fn insert(&mut self, k: K) {
        self.cur.push(k);
        if self.cur.len() > MERGE_SORT_SIZE {
            replace_with_or_abort(self, |mut _self| {
                let new_sorted: Vec<K> = [_self.cur, _self.sorted]
                    .into_iter()
                    .kmerge()
                    .dedup()
                    .collect(); //FIXME: wonky but should do, uses some memory

                LSMSet {
                    cur: vec![],
                    sorted: new_sorted,
                }
            });
        }
    }

    /*
     *
     * find k otherwise find the smallest value that is greater than k
     *
     */
    fn find_local_unsorted<'a, 'b>(k: &'a K, unsorted: &'b Vec<K>) -> Option<&'b K> {
        let mut alt: Option<&K> = None;

        for k0 in unsorted.iter() {
            if k0 == k {
                // awesome
                return Some(k0);
            } else if k0 > k {
                let next_k_alt = alt.get_or_insert(k0);
                *next_k_alt = Ord::min(&next_k_alt, k0);
            }
        }

        alt
    }

    fn find_local<'a, 'b>(k: &'a K, sorted: &'b Vec<K>) -> Option<&'b K> {
        match sorted.binary_search(k) {
            Ok(i) => Some(&sorted[i]),
            Err(j) if j < sorted.len() => Some(&sorted[j]),
            _ => None,
        }
    }

    pub fn find(&self, k: K) -> Option<&K> {
        let a = Self::find_local_unsorted(&k, &self.cur);
        let b = Self::find_local(&k, &self.sorted);

        match (a, b) {
            (Some(a1), Some(b1)) => Some(Ord::min(a1, b1)),
            (a1 @ Some(_), None) => a1,
            (None, a1 @ Some(_)) => a1,
            _ => None,
        }
    }

    pub fn iter(&self) -> Box<dyn Iterator<Item = &K> + '_> {
        Box::new(
            [self.sorted_cur(), self.sorted()]
                .into_iter()
                .kmerge()
                .dedup(),
        )
    }

    fn sorted_cur(&self) -> Box<dyn Iterator<Item = &K> + '_> {
        Box::new(self.cur.iter().sorted())
    }

    fn sorted(&self) -> Box<dyn Iterator<Item = &K> + '_> {
        Box::new(self.sorted.iter())
    }
}

impl<K: Ord, I: Iterator<Item = K>> From<I> for LSMSet<K> {
    fn from(i: I) -> Self {
        let new_sorted = i.sorted().collect_vec();
        LSMSet {
            cur: vec![],
            sorted: new_sorted,
        }
    }
}

#[cfg(test)]
mod lsmset_tests {
    use std::collections::BTreeSet;

    use super::*;

    #[test]
    fn insert() {
        let mut s = LSMSet::default();

        s.insert(4);
        s.insert(9);
        s.insert(1);
    }

    #[test]
    fn insert_find() {
        let mut s = LSMSet::default();

        s.insert((4, 1));
        s.insert((4, 4));
        s.insert((1, 1));
        s.insert((1, 2));
        s.insert((4, 3));

        println!("{s:?}");
        assert_eq!(s.find((4, 1)), Some(&(4, 1)));
        assert_eq!(s.find((1, 2)), Some(&(1, 2)));
        assert_eq!(s.find((1, 3)), Some(&(4, 1)));
        assert_eq!(s.find((1, 2)), Some(&(1, 2)));

        let mut ss = BTreeSet::default();

        ss.insert((4, 1));
        ss.insert((4, 4));
        ss.insert((1, 1));
        ss.insert((1, 2));
        ss.insert((4, 3));

        assert_eq!(ss.range((4, 2)..).next(), Some(&(4, 3)));

        assert_eq!(s.find((4, 2)), Some(&(4, 3)));
        assert_eq!(s.find((1, 3)), Some(&(4, 1)));
    }

    #[test]
    fn iter() {
        let mut s = LSMSet::default();

        s.insert((4, 1));
        s.insert((4, 4));
        s.insert((1, 1));
        s.insert((1, 2));
        s.insert((4, 3));

        let all = s.iter().collect_vec();

        assert_eq!(all, vec![&(1, 1), &(1, 2), &(4, 1), &(4, 3), &(4, 4)])
    }

    #[test]
    fn example() {
        let mut ss = BTreeSet::default();

        ss.insert((1, 2));

        let expected: Option<&(i32, i32)> = None;
        assert_eq!(ss.range((4, 2)..).next(), expected);
    }
}
