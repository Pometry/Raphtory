use std::ops::Range;
use std::collections::btree_set::Iter;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Default, Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TimeIndex(BTreeSet<i64>);

impl TimeIndex {
    pub fn one(t: i64) -> Self {
        let mut s = Self::default();
        s.insert(t);
        s
    }

    #[inline(always)]
    pub fn active(&self, w: Range<i64>) -> bool {
        self.0.range(w).next().is_some()
    }

    #[inline(always)]
    pub fn range(&self, w: Range<i64>) -> std::collections::btree_set::Range<'_, i64> {
        self.0.range(w)
    }

    #[inline(always)]
    pub fn first(&self) -> Option<&i64> {
        self.0.first()
    }

    #[inline(always)]
    pub fn last(&self) -> Option<&i64> {
        self.0.last()
    }

    #[inline(always)]
    pub fn insert(&mut self, t: i64) -> bool {
        self.0.insert(t)
    }

    #[inline(always)]
    pub fn iter(&self) -> Iter<'_, i64> {
        self.0.iter()
    }
}
