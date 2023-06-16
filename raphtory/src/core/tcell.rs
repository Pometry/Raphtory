use std::{collections::BTreeMap, fmt::Debug, ops::Range};

use serde::{Deserialize, Serialize};

use crate::core::sorted_vec_map::SVM;

#[derive(Debug, PartialEq, Default, Clone, Serialize, Deserialize)]

// TCells represent a value in time that can be set at multiple times and keeps a history
pub enum TCell<A: Clone + Default + Debug + PartialEq> {
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

    #[allow(dead_code)]
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

    #[allow(dead_code)]
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
    
    pub fn last_before(&self, t: i64)-> Option<&A> {
        match self {
            TCell::Empty => {None}
            TCell::TCell1(t2, v) => {
                (*t2 < t).then_some(v)
            }
            TCell::TCellCap(map) => {map.range(i64::MIN..t).last().map(|(_, v)| v)}
            TCell::TCellN(map) => {map.range(i64::MIN..t).last().map(|(_, v)| v)}
        }
    }
}

#[cfg(test)]
mod tcell_tests {
    use super::TCell;

    #[test]
    fn set_new_value_for_tcell_initialized_as_empty() {
        let mut tcell = TCell::default();
        tcell.set(16, String::from("lobster"));

        assert_eq!(tcell.iter().collect::<Vec<_>>(), vec!["lobster"]);
    }

    #[test]
    fn every_new_update_to_the_same_prop_is_recorded_as_history() {
        let mut tcell = TCell::new(1, "Pometry");
        tcell.set(2, "Pometry Inc.");

        assert_eq!(
            tcell.iter_t().collect::<Vec<_>>(),
            vec![(&1, &"Pometry"), (&2, &"Pometry Inc."),]
        );
    }

    #[test]
    fn new_update_with_the_same_time_to_a_prop_is_ignored() {
        let mut tcell = TCell::new(1, "Pometry");
        tcell.set(1, "Pometry Inc.");

        assert_eq!(tcell.iter_t().collect::<Vec<_>>(), vec![(&1, &"Pometry")]);
    }

    #[test]
    fn updates_to_prop_can_be_iterated() {
        let tcell: TCell<String> = TCell::default();

        let actual = tcell.iter().collect::<Vec<_>>();
        let expected: Vec<&String> = vec![];
        assert_eq!(actual, expected);

        assert_eq!(tcell.iter_t().collect::<Vec<_>>(), vec![]);

        let tcell = TCell::new(3, "Pometry");

        assert_eq!(tcell.iter().collect::<Vec<_>>(), vec![&"Pometry"]);

        assert_eq!(tcell.iter_t().collect::<Vec<_>>(), vec![(&3, &"Pometry")]);

        let mut tcell = TCell::new(2, "Pometry");
        tcell.set(1, "Inc. Pometry");

        assert_eq!(
            // Results are ordered by time
            tcell.iter_t().collect::<Vec<_>>(),
            vec![(&1, &"Inc. Pometry"), (&2, &"Pometry"),]
        );

        assert_eq!(
            // Results are ordered by time
            tcell.iter().collect::<Vec<_>>(),
            vec![&"Inc. Pometry", &"Pometry"]
        );

        let mut tcell: TCell<i64> = TCell::default();
        for n in 1..130 {
            tcell.set(n, n)
        }

        assert_eq!(tcell.iter_t().count(), 129);

        assert_eq!(tcell.iter().count(), 129)
    }

    #[test]
    fn updates_to_prop_can_be_window_iterated() {
        let tcell: TCell<String> = TCell::default();

        let actual = tcell.iter_window(i64::MIN..i64::MAX).collect::<Vec<_>>();
        let expected: Vec<&String> = vec![];
        assert_eq!(actual, expected);

        assert_eq!(
            tcell.iter_window_t(i64::MIN..i64::MAX).collect::<Vec<_>>(),
            vec![]
        );

        let tcell = TCell::new(3, "Pometry");

        assert_eq!(
            tcell.iter_window(3..4).collect::<Vec<_>>(),
            vec![&"Pometry"]
        );

        assert_eq!(
            tcell.iter_window_t(3..4).collect::<Vec<_>>(),
            vec![(&3, &"Pometry")]
        );

        let mut tcell = TCell::new(3, "Pometry");
        tcell.set(1, "Pometry Inc.");
        tcell.set(2, "Raphtory");

        assert_eq!(
            tcell.iter_window_t(2..3).collect::<Vec<_>>(),
            vec![(&2, &"Raphtory")]
        );

        let expected: Vec<&String> = vec![];
        assert_eq!(tcell.iter_window(4..5).collect::<Vec<_>>(), expected);

        assert_eq!(
            tcell.iter_window(1..i64::MAX).collect::<Vec<_>>(),
            vec![&"Pometry Inc.", &"Raphtory", &"Pometry"]
        );

        assert_eq!(
            tcell.iter_window(3..i64::MAX).collect::<Vec<_>>(),
            vec![&"Pometry"]
        );

        assert_eq!(
            tcell.iter_window(2..i64::MAX).collect::<Vec<_>>(),
            vec![&"Raphtory", &"Pometry"]
        );

        let expected: Vec<&String> = vec![];
        assert_eq!(tcell.iter_window(5..i64::MAX).collect::<Vec<_>>(), expected);

        assert_eq!(
            tcell.iter_window(i64::MIN..4).collect::<Vec<_>>(),
            vec![&"Pometry Inc.", &"Raphtory", &"Pometry"]
        );

        let expected: Vec<&String> = vec![];
        assert_eq!(tcell.iter_window(i64::MIN..1).collect::<Vec<_>>(), expected);

        let mut tcell: TCell<i64> = TCell::default();
        for n in 1..130 {
            tcell.set(n, n)
        }

        assert_eq!(tcell.iter_window_t(i64::MIN..i64::MAX).count(), 129);

        assert_eq!(tcell.iter_window(i64::MIN..i64::MAX).count(), 129)
    }
}
