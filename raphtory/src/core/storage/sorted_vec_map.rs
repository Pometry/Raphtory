use serde::{ser::SerializeSeq, Deserialize, Serialize};
use sorted_vector_map::SortedVectorMap;
use std::ops::Range;

// wrapper for SortedVectorMap
#[derive(Debug, PartialEq, Clone)]
pub struct SVM<K: Ord, V>(SortedVectorMap<K, V>);

impl<K: Ord, V> Default for SVM<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: Ord, V> SVM<K, V> {
    pub(crate) fn new() -> Self {
        Self(SortedVectorMap::new())
    }

    pub(crate) fn insert(&mut self, k: K, v: V) -> Option<V> {
        self.0.insert(k, v)
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (&K, &V)> {
        self.0.iter()
    }

    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }

    pub(crate) fn range(&self, range: Range<K>) -> impl Iterator<Item = (&K, &V)> {
        self.0.range(range)
    }

    pub(crate) fn from_iter<I: IntoIterator<Item = (K, V)>>(iter: I) -> Self {
        Self(SortedVectorMap::from_iter(iter))
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

// this implements Serialize for SortedVectorMap
impl<K: Ord + Serialize, V: Serialize> Serialize for SVM<K, V> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut seq = serializer.serialize_seq(Some(self.len()))?;
        for (k, v) in self.iter() {
            seq.serialize_element(&(k, v))?;
        }
        seq.end()
    }
}

// this implements Serialize for SortedVectorMap
impl<'de, K: Ord + Deserialize<'de>, V: Deserialize<'de>> Deserialize<'de> for SVM<K, V> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let vec = Vec::<(K, V)>::deserialize(deserializer)?;
        Ok(SVM::from_iter(vec))
    }
}
