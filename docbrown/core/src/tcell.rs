use std::{collections::BTreeMap, fmt::Debug, ops::Range};

use serde::{Deserialize, Serialize};

use crate::sorted_vec_map::SVM;

#[derive(Debug, PartialEq, Default, Clone, Serialize, Deserialize)]

// TCells represent a value in time that can be set at multiple times and keeps a history
pub(crate) enum TCell<A: Clone + Default + Debug + PartialEq> {
    #[default]
    Empty,
    TCell1(i64, A),
    TCellCap(SVM<i64, A>),
    TCellN(BTreeMap<i64, A>),
}

const BTREE_CUTOFF: usize = 128;

impl<A: Clone + Default + Debug + PartialEq> TCell<A> {
    pub fn new(t: i64, value: A) -> Self {
        TCell::TCell1(t, value)
    }

    pub fn set(&mut self, t: i64, value: A) {
        match self {
            TCell::Empty => {
                *self = TCell::TCell1(t, value);
            }
            TCell::TCell1(t0, value0) => {
                if t != *t0 {
                    let mut svm = SVM::new();
                    svm.insert(t, value);
                    svm.insert(*t0, value0.clone());
                    *self = TCell::TCellCap(svm)
                }
            }
            TCell::TCellCap(svm) => {
                if svm.len() < BTREE_CUTOFF {
                    svm.insert(t, value.clone());
                } else {
                    let mut btm: BTreeMap<i64, A> = BTreeMap::new();
                    for (k, v) in svm.iter() {
                        btm.insert(*k, v.clone());
                    }
                    btm.insert(t, value.clone());
                    *self = TCell::TCellN(btm)
                }
            }
            TCell::TCellN(btm) => {
                btm.insert(t, value);
            }
        }
    }

    pub fn iter_window_t(&self, r: Range<i64>) -> Box<dyn Iterator<Item = (&i64, &A)> + '_> {
        match self {
            TCell::Empty => Box::new(std::iter::empty()),
            TCell::TCell1(t, value) => {
                if r.contains(t) {
                    Box::new(std::iter::once((t, value)))
                } else {
                    Box::new(std::iter::empty())
                }
            }
            TCell::TCellCap(svm) => Box::new(svm.range(r)),
            TCell::TCellN(btm) => Box::new(btm.range(r)),
        }
    }

    pub fn iter_window(&self, r: Range<i64>) -> Box<dyn Iterator<Item = &A> + '_> {
        match self {
            TCell::Empty => Box::new(std::iter::empty()),
            TCell::TCell1(t, value) => {
                if r.contains(t) {
                    Box::new(std::iter::once(value))
                } else {
                    Box::new(std::iter::empty())
                }
            }
            TCell::TCellCap(svm) => Box::new(svm.range(r).map(|(_, value)| value)),
            TCell::TCellN(btm) => Box::new(btm.range(r).map(|(_, value)| value)),
        }
    }

    pub fn iter(&self) -> Box<dyn Iterator<Item = &A> + '_> {
        match self {
            TCell::Empty => Box::new(std::iter::empty()),
            TCell::TCell1(_, value) => Box::new(std::iter::once(value)),
            TCell::TCellCap(svm) => Box::new(svm.iter().map(|(_, value)| value)),
            TCell::TCellN(btm) => Box::new(btm.values()),
        }
    }

    pub fn iter_t(&self) -> Box<dyn Iterator<Item = (&i64, &A)> + '_> {
        match self {
            TCell::Empty => Box::new(std::iter::empty()),
            TCell::TCell1(t, value) => Box::new(std::iter::once((t, value))),
            TCell::TCellCap(svm) => Box::new(svm.iter()),
            TCell::TCellN(btm) => Box::new(btm.iter()),
        }
    }
}

#[cfg(test)]
mod tcell_tests {

    use super::TCell;

    #[test]
    fn set_tcell_once_from_empty() {
        let mut tcell = TCell::default();
        tcell.set(16, String::from("lobster"));

        let actual = tcell.iter().collect::<Vec<_>>();

        assert_eq!(actual, vec!["lobster"]);
    }

    #[test]
    fn set_tcell_twice_from_empty() {
        let mut tcell = TCell::default();
        tcell.set(16, String::from("lobster"));
        tcell.set(7, String::from("hamster"));

        let actual = tcell.iter().collect::<Vec<_>>();

        assert_eq!(actual, vec!["hamster", "lobster"]); // ordering is important by time
    }

    #[test]
    fn set_tcell_trice_from_empty_range_iter() {
        let mut tcell = TCell::default();
        tcell.set(16, String::from("lobster"));
        tcell.set(7, String::from("hamster"));
        tcell.set(3, String::from("faster"));

        let actual = tcell
            .iter_window(i64::MIN..i64::MAX)
            .collect::<Vec<_>>();
        assert_eq!(actual, vec!["faster", "hamster", "lobster"]); // ordering is important by time

        let actual = tcell.iter_window(4..i64::MAX).collect::<Vec<_>>();
        assert_eq!(actual, vec!["hamster", "lobster"]); // ordering is important by time

        let actual = tcell.iter_window(3..8).collect::<Vec<_>>();
        assert_eq!(actual, vec!["faster", "hamster"]); // ordering is important by time

        let actual = tcell.iter_window(6..i64::MAX).collect::<Vec<_>>();
        assert_eq!(actual, vec!["hamster", "lobster"]); // ordering is important by time

        let actual = tcell.iter_window(8..i64::MAX).collect::<Vec<_>>();
        assert_eq!(actual, vec!["lobster"]); // ordering is important by time

        let actual: Vec<&String> = tcell.iter_window(17..i64::MAX).collect::<Vec<_>>();
        let expected: Vec<&String> = vec![];
        assert_eq!(actual, expected); // ordering is important by time
    }
}
