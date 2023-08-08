use crate::core::storage::{
    sorted_vec_map::SVM,
    timeindex::{AsTime, TimeIndexEntry},
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fmt::Debug, ops::Range};

#[derive(Debug, PartialEq, Default, Clone, Serialize, Deserialize)]

// TCells represent a value in time that can be set at multiple times and keeps a history
pub enum TCell<A: Clone + Default + Debug + PartialEq> {
    #[default]
    Empty,
    TCell1(TimeIndexEntry, A),
    TCellCap(SVM<TimeIndexEntry, A>),
    TCellN(BTreeMap<TimeIndexEntry, A>),
}

const BTREE_CUTOFF: usize = 128;

impl<A: Clone + Default + Debug + PartialEq> TCell<A> {
    pub fn new(t: TimeIndexEntry, value: A) -> Self {
        TCell::TCell1(t, value)
    }

    pub fn set(&mut self, t: TimeIndexEntry, value: A) {
        match self {
            TCell::Empty => {
                *self = TCell::TCell1(t, value);
            }
            TCell::TCell1(t0, value0) => {
                if &t != t0 {
                    if let TCell::TCell1(t0, value0) = std::mem::take(self) {
                        let mut svm = SVM::new();
                        svm.insert(t, value);
                        svm.insert(t0, value0);
                        *self = TCell::TCellCap(svm)
                    }
                }
            }
            TCell::TCellCap(svm) => {
                if svm.len() < BTREE_CUTOFF {
                    svm.insert(t, value.clone());
                } else {
                    let svm = std::mem::take(svm);
                    let mut btm: BTreeMap<TimeIndexEntry, A> = BTreeMap::new();
                    for (k, v) in svm.into_iter() {
                        btm.insert(k, v);
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

    pub fn at(&self, ti: &TimeIndexEntry) -> Option<&A> {
        match self {
            TCell::Empty => None,
            TCell::TCell1(t, v) => (t == ti).then_some(v),
            TCell::TCellCap(svm) => svm.get(ti),
            TCell::TCellN(btm) => btm.get(ti),
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
            TCell::TCell1(t, value) => Box::new(std::iter::once((t.t(), value))),
            TCell::TCellCap(svm) => Box::new(svm.iter().map(|(ti, v)| (ti.t(), v))),
            TCell::TCellN(btm) => Box::new(btm.iter().map(|(ti, v)| (ti.t(), v))),
        }
    }

    #[allow(dead_code)]
    pub fn iter_window(&self, r: Range<i64>) -> Box<dyn Iterator<Item = &A> + '_> {
        match self {
            TCell::Empty => Box::new(std::iter::empty()),
            TCell::TCell1(t, value) => {
                if r.contains(t.t()) {
                    Box::new(std::iter::once(value))
                } else {
                    Box::new(std::iter::empty())
                }
            }
            TCell::TCellCap(svm) => {
                Box::new(svm.range(TimeIndexEntry::range(r)).map(|(_, value)| value))
            }
            TCell::TCellN(btm) => {
                Box::new(btm.range(TimeIndexEntry::range(r)).map(|(_, value)| value))
            }
        }
    }

    pub fn iter_window_t(&self, r: Range<i64>) -> Box<dyn Iterator<Item = (&i64, &A)> + '_> {
        match self {
            TCell::Empty => Box::new(std::iter::empty()),
            TCell::TCell1(t, value) => {
                if r.contains(t.t()) {
                    Box::new(std::iter::once((t.t(), value)))
                } else {
                    Box::new(std::iter::empty())
                }
            }
            TCell::TCellCap(svm) => Box::new(
                svm.range(TimeIndexEntry::range(r))
                    .map(|(ti, v)| (ti.t(), v)),
            ),
            TCell::TCellN(btm) => Box::new(
                btm.range(TimeIndexEntry::range(r))
                    .map(|(ti, v)| (ti.t(), v)),
            ),
        }
    }

    pub fn last_before(&self, t: i64) -> Option<(&i64, &A)> {
        match self {
            TCell::Empty => None,
            TCell::TCell1(t2, v) => (t2.t() < &t).then_some((t2.t(), v)),
            TCell::TCellCap(map) => map
                .range(TimeIndexEntry::range(i64::MIN..t))
                .last()
                .map(|(ti, v)| (ti.t(), v)),
            TCell::TCellN(map) => map
                .range(TimeIndexEntry::range(i64::MIN..t))
                .last()
                .map(|(ti, v)| (ti.t(), v)),
        }
    }
}

#[cfg(test)]
mod tcell_tests {
    use super::TCell;
    use crate::{core::storage::timeindex::TimeIndexEntry, db::api::view::TimeIndex};

    #[test]
    fn set_new_value_for_tcell_initialized_as_empty() {
        let mut tcell = TCell::default();
        tcell.set(TimeIndexEntry::start(16), String::from("lobster"));

        assert_eq!(tcell.iter().collect::<Vec<_>>(), vec!["lobster"]);
    }

    #[test]
    fn every_new_update_to_the_same_prop_is_recorded_as_history() {
        let mut tcell = TCell::new(TimeIndexEntry::start(1), "Pometry");
        tcell.set(TimeIndexEntry::start(2), "Pometry Inc.");

        assert_eq!(
            tcell.iter_t().collect::<Vec<_>>(),
            vec![(&1, &"Pometry"), (&2, &"Pometry Inc."),]
        );
    }

    #[test]
    fn new_update_with_the_same_time_to_a_prop_is_ignored() {
        let mut tcell = TCell::new(TimeIndexEntry::start(1), "Pometry");
        tcell.set(TimeIndexEntry::start(1), "Pometry Inc.");

        assert_eq!(tcell.iter_t().collect::<Vec<_>>(), vec![(&1, &"Pometry")]);
    }

    #[test]
    fn updates_to_prop_can_be_iterated() {
        let tcell: TCell<String> = TCell::default();

        let actual = tcell.iter().collect::<Vec<_>>();
        let expected: Vec<&String> = vec![];
        assert_eq!(actual, expected);

        assert_eq!(tcell.iter_t().collect::<Vec<_>>(), vec![]);

        let tcell = TCell::new(TimeIndexEntry::start(3), "Pometry");

        assert_eq!(tcell.iter().collect::<Vec<_>>(), vec![&"Pometry"]);

        assert_eq!(tcell.iter_t().collect::<Vec<_>>(), vec![(&3, &"Pometry")]);

        let mut tcell = TCell::new(TimeIndexEntry::start(2), "Pometry");
        tcell.set(TimeIndexEntry::start(1), "Inc. Pometry");

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
            tcell.set(TimeIndexEntry::start(n), n)
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

        let tcell = TCell::new(TimeIndexEntry::start(3), "Pometry");

        assert_eq!(
            tcell.iter_window(3..4).collect::<Vec<_>>(),
            vec![&"Pometry"]
        );

        assert_eq!(
            tcell.iter_window_t(3..4).collect::<Vec<_>>(),
            vec![(&3, &"Pometry")]
        );

        let mut tcell = TCell::new(TimeIndexEntry::start(3), "Pometry");
        tcell.set(TimeIndexEntry::start(1), "Pometry Inc.");
        tcell.set(TimeIndexEntry::start(2), "Raphtory");

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
            tcell.set(TimeIndexEntry::start(n), n)
        }

        assert_eq!(tcell.iter_window_t(i64::MIN..i64::MAX).count(), 129);

        assert_eq!(tcell.iter_window(i64::MIN..i64::MAX).count(), 129)
    }
}
