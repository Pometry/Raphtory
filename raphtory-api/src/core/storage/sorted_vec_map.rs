use serde::{Deserialize, Serialize};
use std::{borrow::Borrow, mem, ops::Range};

/// Implements a sorted vector map
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct SVM<K: Ord, V>(Vec<(K, V)>);

impl<K: Ord, V> Default for SVM<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Ord, V> FromIterator<(K, V)> for SVM<K, V> {
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        let mut is_sorted = true;
        let iter = iter.into_iter();
        let mut data = Vec::with_capacity(iter.size_hint().0);
        for (key, value) in iter {
            is_sorted = is_sorted && data.last().is_none_or(|(last_key, _)| last_key <= &key);
            data.push((key, value));
        }
        if !is_sorted {
            data.sort_by(|(lk, _), (rk, _)| lk.cmp(rk));
        }
        Self(data)
    }
}

impl<K: Ord, V> SVM<K, V> {
    pub fn new() -> Self {
        Self(Vec::new())
    }

    /// Utility function to binary search for an index using the key.
    #[inline]
    fn find_index<Q>(&self, q: &Q) -> Result<usize, usize>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.0.binary_search_by(|e| e.0.borrow().cmp(q))
    }

    #[inline]
    fn insertion_point<Q>(&self, q: &Q) -> usize
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        match self.find_index(q) {
            Ok(i) | Err(i) => i,
        }
    }

    /// Returns a reference to the value corresponding to the key.
    pub fn get<Q>(&self, q: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        match self.find_index(q) {
            Ok(index) => Some(&self.0[index].1),
            Err(_index) => None,
        }
    }

    /// Inserts a key-value pair into the map.
    ///
    /// If the map did not have this key present, `None` is returned.
    ///
    /// If the map did have this key present, the value is updated and the
    /// old value is returned.  They key is not updated, though; this matters
    /// for types that can be `==` without being identical.
    ///
    /// If the map did not have the key present, and the key is greater than
    /// all of the keys already present, insertion is amortized O(1).  Otherwise,
    /// insertion is O(n).
    pub fn insert(&mut self, k: K, v: V) -> Option<V> {
        let len = self.0.len();
        if len == 0 || self.0[len - 1].0 < k {
            self.0.push((k, v));
            None
        } else {
            let mut v = v;
            match self.find_index(&k) {
                Ok(index) => {
                    mem::swap(&mut self.0[index].1, &mut v);
                    Some(v)
                }
                Err(index) => {
                    self.0.insert(index, (k, v));
                    None
                }
            }
        }
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = (&K, &V)> {
        self.0.iter().map(|(k, v)| (k, v))
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns an iterator over the given range of keys.
    ///
    /// # Panics
    ///
    /// Panics if the range start is after the range end.
    #[inline]
    pub fn range(&self, range: Range<K>) -> impl DoubleEndedIterator<Item = (&K, &V)> {
        let start = self.insertion_point(&range.start);
        let end = self.insertion_point(&range.end);
        self.0[start..end].iter().map(|(k, v)| (k, v))
    }

    #[inline]
    pub fn active(&self, range: Range<K>) -> bool {
        let end = self.insertion_point(&range.end).checked_sub(1);
        match end {
            None => false,
            Some(i) => self.0[i].0 >= range.start,
        }
    }

    pub fn first_key_value(&self) -> Option<(&K, &V)> {
        self.0.first().map(|(k, v)| (k, v))
    }

    pub fn last_key_value(&self) -> Option<(&K, &V)> {
        self.0.last().map(|(k, v)| (k, v))
    }
}

impl<K, V> IntoIterator for SVM<K, V>
where
    K: Ord,
{
    type Item = (K, V);
    type IntoIter = std::vec::IntoIter<Self::Item>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}
