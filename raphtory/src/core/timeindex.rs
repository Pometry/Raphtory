use serde::{Deserialize, Serialize};
use std::collections::btree_set::Iter;
use std::collections::BTreeSet;
use std::ops::Range;

#[repr(transparent)]
#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TimeIndex(BTreeSet<i64>);

impl TimeIndex {
    pub fn one(t: i64) -> Self {
        let mut s = Self::default();
        s.insert(t);
        s
    }

    pub fn active(&self, w: Range<i64>) -> bool {
        self.0.range(w).next().is_some()
    }

    pub fn range(&self, w: Range<i64>) -> std::collections::btree_set::Range<'_, i64> {
        self.0.range(w)
    }

    pub fn first(&self) -> Option<&i64> {
        self.0.first()
    }

    pub fn last(&self) -> Option<&i64> {
        self.0.last()
    }

    pub fn insert(&mut self, t: i64) -> bool {
        self.0.insert(t)
    }

    pub fn iter(&self) -> Iter<'_, i64> {
        self.0.iter()
    }
}
