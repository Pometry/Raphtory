use crate::core::storage::{
    sorted_vec_map::SVM,
    timeindex::{AsTime, TimeIndexEntry},
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fmt::Debug, ops::Range};

#[derive(Debug, PartialEq, Default, Clone, Serialize, Deserialize)]
// TCells represent a value in time that can be set at multiple times and keeps a history
pub enum TCell<A: Clone + Debug + PartialEq> {
    #[default]
    Empty,
    TCell1(TimeIndexEntry, A),
    TCellCap(SVM<TimeIndexEntry, A>),
    TCellN(BTreeMap<TimeIndexEntry, A>),
}

const BTREE_CUTOFF: usize = 128;

impl<A: Clone + Debug + PartialEq + Send + Sync> TCell<A> {
    pub fn new(t: TimeIndexEntry, value: A) -> Self {
        TCell::TCell1(t, value)
    }

    pub fn set(&mut self, t: TimeIndexEntry, value: A) {
        match self {
            TCell::Empty => {
                *self = TCell::TCell1(t, value);
            }
            TCell::TCell1(t0, v) => {
                if &t != t0 {
                    if let TCell::TCell1(t0, value0) = std::mem::take(self) {
                        let mut svm = SVM::new();
                        svm.insert(t, value);
                        svm.insert(t0, value0);
                        *self = TCell::TCellCap(svm)
                    }
                } else {
                    *v = value
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

    pub fn iter(&self) -> Box<dyn Iterator<Item = (&TimeIndexEntry, &A)> + Send + '_> {
        match self {
            TCell::Empty => Box::new(std::iter::empty()),
            TCell::TCell1(t, value) => Box::new(std::iter::once((t, value))),
            TCell::TCellCap(svm) => Box::new(svm.iter()),
            TCell::TCellN(btm) => Box::new(btm.iter()),
        }
    }

    pub fn iter_t(&self) -> Box<dyn Iterator<Item = (i64, &A)> + Send + '_> {
        match self {
            TCell::Empty => Box::new(std::iter::empty()),
            TCell::TCell1(t, value) => Box::new(std::iter::once((t.t(), value))),
            TCell::TCellCap(svm) => Box::new(svm.iter().map(|(ti, v)| (ti.t(), v))),
            TCell::TCellN(btm) => Box::new(btm.iter().map(|(ti, v)| (ti.t(), v))),
        }
    }

    pub fn iter_window(
        &self,
        r: Range<TimeIndexEntry>,
    ) -> Box<dyn Iterator<Item = (&TimeIndexEntry, &A)> + Send + '_> {
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

    pub fn iter_window_t(&self, r: Range<i64>) -> Box<dyn Iterator<Item = (i64, &A)> + Send + '_> {
        match self {
            TCell::Empty => Box::new(std::iter::empty()),
            TCell::TCell1(t, value) => {
                if r.contains(&t.t()) {
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

    pub fn last_before(&self, t: TimeIndexEntry) -> Option<(TimeIndexEntry, &A)> {
        match self {
            TCell::Empty => None,
            TCell::TCell1(t2, v) => (*t2 < t).then_some((*t2, v)),
            TCell::TCellCap(map) => map
                .range(TimeIndexEntry::MIN..t)
                .last()
                .map(|(ti, v)| (*ti, v)),
            TCell::TCellN(map) => map
                .range(TimeIndexEntry::MIN..t)
                .last()
                .map(|(ti, v)| (*ti, v)),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            TCell::Empty => 0,
            TCell::TCell1(_, _) => 1,
            TCell::TCellCap(v) => v.len(),
            TCell::TCellN(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tcell_tests {
    use super::TCell;
    use crate::core::storage::timeindex::{AsTime, TimeIndexEntry};

    #[test]
    fn set_new_value_for_tcell_initialized_as_empty() {
        let mut tcell = TCell::default();
        tcell.set(TimeIndexEntry::start(16), String::from("lobster"));

        assert_eq!(
            tcell.iter().map(|(_, v)| v).collect::<Vec<_>>(),
            vec!["lobster"]
        );
    }

    #[test]
    fn every_new_update_to_the_same_prop_is_recorded_as_history() {
        let mut tcell = TCell::new(TimeIndexEntry::start(1), "Pometry");
        tcell.set(TimeIndexEntry::start(2), "Pometry Inc.");

        assert_eq!(
            tcell.iter_t().collect::<Vec<_>>(),
            vec![(1, &"Pometry"), (2, &"Pometry Inc."),]
        );
    }

    #[test]
    fn new_update_with_the_same_time_to_a_prop_is_ignored() {
        let mut tcell = TCell::new(TimeIndexEntry::start(1), "Pometry");
        tcell.set(TimeIndexEntry::start(1), "Pometry Inc.");

        assert_eq!(tcell.iter_t().collect::<Vec<_>>(), vec![(1, &"Pometry")]);
    }

    #[test]
    fn updates_to_prop_can_be_iterated() {
        let tcell: TCell<String> = TCell::default();

        let actual = tcell.iter().collect::<Vec<_>>();
        let expected = vec![];
        assert_eq!(actual, expected);

        assert_eq!(tcell.iter_t().collect::<Vec<_>>(), vec![]);

        let tcell = TCell::new(TimeIndexEntry::start(3), "Pometry");

        assert_eq!(
            tcell.iter().collect::<Vec<_>>(),
            vec![(&TimeIndexEntry::start(3), &"Pometry")]
        );

        assert_eq!(tcell.iter_t().collect::<Vec<_>>(), vec![(3, &"Pometry")]);

        let mut tcell = TCell::new(TimeIndexEntry::start(2), "Pometry");
        tcell.set(TimeIndexEntry::start(1), "Inc. Pometry");

        assert_eq!(
            // Results are ordered by time
            tcell.iter_t().collect::<Vec<_>>(),
            vec![(1, &"Inc. Pometry"), (2, &"Pometry"),]
        );

        assert_eq!(
            // Results are ordered by time
            tcell.iter().collect::<Vec<_>>(),
            vec![
                (&TimeIndexEntry::start(1), &"Inc. Pometry"),
                (&TimeIndexEntry::start(2), &"Pometry")
            ]
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

        let actual = tcell
            .iter_window(TimeIndexEntry::MIN..TimeIndexEntry::MAX)
            .collect::<Vec<_>>();
        let expected = vec![];
        assert_eq!(actual, expected);

        assert_eq!(
            tcell.iter_window_t(i64::MIN..i64::MAX).collect::<Vec<_>>(),
            vec![]
        );

        let tcell = TCell::new(TimeIndexEntry::start(3), "Pometry");

        assert_eq!(
            tcell
                .iter_window(TimeIndexEntry::range(3..4))
                .collect::<Vec<_>>(),
            vec![(&TimeIndexEntry(3, 0), &"Pometry")]
        );

        assert_eq!(
            tcell.iter_window_t(3..4).collect::<Vec<_>>(),
            vec![(3, &"Pometry")]
        );

        let mut tcell = TCell::new(TimeIndexEntry::start(3), "Pometry");
        tcell.set(TimeIndexEntry::start(1), "Pometry Inc.");
        tcell.set(TimeIndexEntry::start(2), "Raphtory");

        let one = TimeIndexEntry::start(1);
        let two = TimeIndexEntry::start(2);
        let three = TimeIndexEntry::start(3);

        assert_eq!(
            tcell.iter_window_t(2..3).collect::<Vec<_>>(),
            vec![(2, &"Raphtory")]
        );

        let expected = vec![];
        assert_eq!(
            tcell
                .iter_window(TimeIndexEntry::range(4..5))
                .collect::<Vec<_>>(),
            expected
        );

        assert_eq!(
            tcell
                .iter_window(TimeIndexEntry::range(1..i64::MAX))
                .collect::<Vec<_>>(),
            vec![
                (&one, &"Pometry Inc."),
                (&two, &"Raphtory"),
                (&three, &"Pometry")
            ]
        );

        assert_eq!(
            tcell
                .iter_window(TimeIndexEntry::range(3..i64::MAX))
                .collect::<Vec<_>>(),
            vec![(&three, &"Pometry")]
        );

        assert_eq!(
            tcell
                .iter_window(TimeIndexEntry::range(2..i64::MAX))
                .collect::<Vec<_>>(),
            vec![(&two, &"Raphtory"), (&three, &"Pometry")]
        );

        let expected = vec![];
        assert_eq!(
            tcell
                .iter_window(TimeIndexEntry::range(5..i64::MAX))
                .collect::<Vec<_>>(),
            expected
        );

        assert_eq!(
            tcell
                .iter_window(TimeIndexEntry::range(i64::MIN..4))
                .collect::<Vec<_>>(),
            vec![
                (&one, &"Pometry Inc."),
                (&two, &"Raphtory"),
                (&three, &"Pometry")
            ]
        );

        let expected = vec![];
        assert_eq!(
            tcell
                .iter_window(TimeIndexEntry::range(i64::MIN..1))
                .collect::<Vec<_>>(),
            expected
        );

        let mut tcell: TCell<i64> = TCell::default();
        for n in 1..130 {
            tcell.set(TimeIndexEntry::start(n), n)
        }

        assert_eq!(tcell.iter_window_t(i64::MIN..i64::MAX).count(), 129);

        assert_eq!(
            tcell
                .iter_window(TimeIndexEntry::range(i64::MIN..i64::MAX))
                .count(),
            129
        )
    }
}
