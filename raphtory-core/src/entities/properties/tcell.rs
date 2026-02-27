use crate::storage::timeindex::{AsTime, EventTime, TimeIndexOps, TimeIndexWindow};
use either::Either;
use iter_enum::{DoubleEndedIterator, ExactSizeIterator, Extend, FusedIterator, Iterator};
use raphtory_api::core::storage::{sorted_vec_map::SVM, timeindex::TimeIndexLike};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fmt::Debug, ops::Range};

#[derive(Debug, PartialEq, Default, Clone, Serialize, Deserialize)]
// TCells represent a value in time that can be set at multiple times and keeps a history
pub enum TCell<A> {
    #[default]
    Empty,
    TCell1(EventTime, A),
    TCellCap(SVM<EventTime, A>),
    TCellN(BTreeMap<EventTime, A>),
}

#[derive(Iterator, DoubleEndedIterator, ExactSizeIterator, FusedIterator, Extend)]
enum TCellVariants<Empty, TCell1, TCellCap, TCellN> {
    Empty(Empty),
    TCell1(TCell1),
    TCellCap(TCellCap),
    TCellN(TCellN),
}

const BTREE_CUTOFF: usize = 128;

impl<A: PartialEq> TCell<A> {
    pub fn new(t: EventTime, value: A) -> Self {
        TCell::TCell1(t, value)
    }

    #[inline]
    pub fn set(&mut self, t: EventTime, value: A) {
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
                    svm.insert(t, value);
                } else {
                    let svm = std::mem::take(svm);
                    let mut btm: BTreeMap<EventTime, A> = BTreeMap::new();
                    for (k, v) in svm.into_iter() {
                        btm.insert(k, v);
                    }
                    btm.insert(t, value);
                    *self = TCell::TCellN(btm)
                }
            }
            TCell::TCellN(btm) => {
                btm.insert(t, value);
            }
        }
    }

    pub fn at(&self, ti: &EventTime) -> Option<&A> {
        match self {
            TCell::Empty => None,
            TCell::TCell1(t, v) => (t == ti).then_some(v),
            TCell::TCellCap(svm) => svm.get(ti),
            TCell::TCellN(btm) => btm.get(ti),
        }
    }
}
impl<A: Sync + Send> TCell<A> {
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = (&EventTime, &A)> + Send + Sync {
        match self {
            TCell::Empty => TCellVariants::Empty(std::iter::empty()),
            TCell::TCell1(t, value) => TCellVariants::TCell1(std::iter::once((t, value))),
            TCell::TCellCap(svm) => TCellVariants::TCellCap(svm.iter()),
            TCell::TCellN(btm) => TCellVariants::TCellN(btm.iter()),
        }
    }

    pub fn iter_t(&self) -> impl DoubleEndedIterator<Item = (i64, &A)> + Send + Sync {
        self.iter().map(|(t, a)| (t.t(), a))
    }

    pub fn iter_window(
        &self,
        r: Range<EventTime>,
    ) -> impl DoubleEndedIterator<Item = (&EventTime, &A)> + Send + Sync {
        match self {
            TCell::Empty => TCellVariants::Empty(std::iter::empty()),
            TCell::TCell1(t, value) => TCellVariants::TCell1(if r.contains(t) {
                Either::Left(std::iter::once((t, value)))
            } else {
                Either::Right(std::iter::empty())
            }),
            TCell::TCellCap(svm) => TCellVariants::TCellCap(svm.range(r)),
            TCell::TCellN(btm) => TCellVariants::TCellN(btm.range(r)),
        }
    }

    pub fn iter_window_t(
        &self,
        r: Range<i64>,
    ) -> impl DoubleEndedIterator<Item = (i64, &A)> + Send + Sync + '_ {
        self.iter_window(EventTime::range(r))
            .map(|(t, a)| (t.t(), a))
    }

    pub fn last_before(&self, t: EventTime) -> Option<(EventTime, &A)> {
        match self {
            TCell::Empty => None,
            TCell::TCell1(t2, v) => (*t2 < t).then_some((*t2, v)),
            TCell::TCellCap(map) => map.range(EventTime::MIN..t).last().map(|(ti, v)| (*ti, v)),
            TCell::TCellN(map) => map.range(EventTime::MIN..t).last().map(|(ti, v)| (*ti, v)),
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

impl<'a, A: Send + Sync> TimeIndexOps<'a> for &'a TCell<A> {
    type IndexType = EventTime;
    type RangeType = TimeIndexWindow<'a, Self::IndexType, TCell<A>>;

    #[inline]
    fn active(&self, w: Range<Self::IndexType>) -> bool {
        match self {
            TCell::Empty => false,
            TCell::TCell1(time_index_entry, _) => w.contains(time_index_entry),
            TCell::TCellCap(svm) => svm.range(w).next().is_some(),
            TCell::TCellN(btree_map) => btree_map.range(w).next().is_some(),
        }
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn range(&self, w: Range<Self::IndexType>) -> Self::RangeType {
        let range = match self {
            TCell::Empty => TimeIndexWindow::Empty,
            TCell::TCell1(t, _) => {
                if w.contains(t) {
                    TimeIndexWindow::All(*self)
                } else {
                    TimeIndexWindow::Empty
                }
            }
            _ => {
                if let Some(min_val) = self.first() {
                    if let Some(max_val) = self.last() {
                        if min_val >= w.start && max_val < w.end {
                            TimeIndexWindow::All(*self)
                        } else {
                            TimeIndexWindow::Range {
                                timeindex: *self,
                                range: w,
                            }
                        }
                    } else {
                        TimeIndexWindow::Empty
                    }
                } else {
                    TimeIndexWindow::Empty
                }
            }
        };
        range
    }

    fn first(&self) -> Option<Self::IndexType> {
        match self {
            TCell::Empty => None,
            TCell::TCell1(t, _) => Some(*t),
            TCell::TCellCap(svm) => svm.first_key_value().map(|(ti, _)| *ti),
            TCell::TCellN(btm) => btm.first_key_value().map(|(ti, _)| *ti),
        }
    }

    fn last(&self) -> Option<Self::IndexType> {
        match self {
            TCell::Empty => None,
            TCell::TCell1(t, _) => Some(*t),
            TCell::TCellCap(svm) => svm.last_key_value().map(|(ti, _)| *ti),
            TCell::TCellN(btm) => btm.last_key_value().map(|(ti, _)| *ti),
        }
    }

    #[allow(refining_impl_trait)]
    fn iter(self) -> impl DoubleEndedIterator<Item = Self::IndexType> + Send + Sync + 'a {
        match self {
            TCell::Empty => TCellVariants::Empty(std::iter::empty()),
            TCell::TCell1(t, _) => TCellVariants::TCell1(std::iter::once(*t)),
            TCell::TCellCap(svm) => TCellVariants::TCellCap(svm.iter().map(|(ti, _)| *ti)),
            TCell::TCellN(btm) => TCellVariants::TCellN(btm.keys().copied()),
        }
    }

    fn iter_rev(self) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'a {
        TimeIndexOps::iter(self).rev()
    }

    fn len(&self) -> usize {
        match self {
            TCell::Empty => 0,
            TCell::TCell1(_, _) => 1,
            TCell::TCellCap(svm) => svm.len(),
            TCell::TCellN(btm) => btm.len(),
        }
    }
}

impl<'a, A: Send + Sync> TimeIndexLike<'a> for &'a TCell<A> {
    #[allow(refining_impl_trait)]
    fn range_iter(
        self,
        w: Range<Self::IndexType>,
    ) -> impl DoubleEndedIterator<Item = Self::IndexType> + Send + Sync + 'a {
        self.iter_window(w).map(|(ti, _)| *ti)
    }

    fn range_iter_rev(
        self,
        w: Range<Self::IndexType>,
    ) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'a {
        self.range_iter(w).rev()
    }

    fn range_count(&self, w: Range<Self::IndexType>) -> usize {
        match self {
            TCell::Empty => 0,
            TCell::TCell1(t, _) => {
                if w.contains(t) {
                    1
                } else {
                    0
                }
            }
            TCell::TCellCap(ts) => ts.range(w).count(),
            TCell::TCellN(ts) => ts.range(w).count(),
        }
    }

    fn last_range(&self, w: Range<Self::IndexType>) -> Option<Self::IndexType> {
        self.iter_window(w).next_back().map(|(ti, _)| *ti)
    }
}

#[cfg(test)]
mod tcell_tests {
    use super::TCell;
    use crate::storage::timeindex::{AsTime, EventTime};

    #[test]
    fn set_new_value_for_tcell_initialized_as_empty() {
        let mut tcell = TCell::default();
        tcell.set(EventTime::start(16), String::from("lobster"));

        assert_eq!(
            tcell.iter().map(|(_, v)| v).collect::<Vec<_>>(),
            vec!["lobster"]
        );
    }

    #[test]
    fn every_new_update_to_the_same_prop_is_recorded_as_history() {
        let mut tcell = TCell::new(EventTime::start(1), "Pometry");
        tcell.set(EventTime::start(2), "Pometry Inc.");

        assert_eq!(
            tcell.iter_t().collect::<Vec<_>>(),
            vec![(1, &"Pometry"), (2, &"Pometry Inc."),]
        );
    }

    #[test]
    fn new_update_with_the_same_time_to_a_prop_is_ignored() {
        let mut tcell = TCell::new(EventTime::start(1), "Pometry");
        tcell.set(EventTime::start(1), "Pometry Inc.");

        assert_eq!(
            tcell.iter_t().collect::<Vec<_>>(),
            vec![(1, &"Pometry Inc.")]
        );
    }

    #[test]
    fn updates_to_prop_can_be_iterated() {
        let tcell: TCell<String> = TCell::default();

        let actual = tcell.iter().collect::<Vec<_>>();
        let expected = vec![];
        assert_eq!(actual, expected);

        assert_eq!(tcell.iter_t().collect::<Vec<_>>(), vec![]);

        let tcell = TCell::new(EventTime::start(3), "Pometry");

        assert_eq!(
            tcell.iter().collect::<Vec<_>>(),
            vec![(&EventTime::start(3), &"Pometry")]
        );

        assert_eq!(tcell.iter_t().collect::<Vec<_>>(), vec![(3, &"Pometry")]);

        let mut tcell = TCell::new(EventTime::start(2), "Pometry");
        tcell.set(EventTime::start(1), "Inc. Pometry");

        assert_eq!(
            // Results are ordered by time
            tcell.iter_t().collect::<Vec<_>>(),
            vec![(1, &"Inc. Pometry"), (2, &"Pometry"),]
        );

        assert_eq!(
            // Results are ordered by time
            tcell.iter().collect::<Vec<_>>(),
            vec![
                (&EventTime::start(1), &"Inc. Pometry"),
                (&EventTime::start(2), &"Pometry")
            ]
        );

        let mut tcell: TCell<i64> = TCell::default();
        for n in 1..130 {
            tcell.set(EventTime::start(n), n)
        }

        assert_eq!(tcell.iter_t().count(), 129);

        assert_eq!(tcell.iter().count(), 129)
    }

    #[test]
    fn updates_to_prop_can_be_window_iterated() {
        let tcell: TCell<String> = TCell::default();

        let actual = tcell
            .iter_window(EventTime::MIN..EventTime::MAX)
            .collect::<Vec<_>>();
        let expected = vec![];
        assert_eq!(actual, expected);

        assert_eq!(
            tcell.iter_window_t(i64::MIN..i64::MAX).collect::<Vec<_>>(),
            vec![]
        );

        let tcell = TCell::new(EventTime::start(3), "Pometry");

        assert_eq!(
            tcell
                .iter_window(EventTime::range(3..4))
                .collect::<Vec<_>>(),
            vec![(&EventTime(3, 0), &"Pometry")]
        );

        assert_eq!(
            tcell.iter_window_t(3..4).collect::<Vec<_>>(),
            vec![(3, &"Pometry")]
        );

        let mut tcell = TCell::new(EventTime::start(3), "Pometry");
        tcell.set(EventTime::start(1), "Pometry Inc.");
        tcell.set(EventTime::start(2), "Raphtory");

        let one = EventTime::start(1);
        let two = EventTime::start(2);
        let three = EventTime::start(3);

        assert_eq!(
            tcell.iter_window_t(2..3).collect::<Vec<_>>(),
            vec![(2, &"Raphtory")]
        );

        let expected = vec![];
        assert_eq!(
            tcell
                .iter_window(EventTime::range(4..5))
                .collect::<Vec<_>>(),
            expected
        );

        assert_eq!(
            tcell
                .iter_window(EventTime::range(1..i64::MAX))
                .collect::<Vec<_>>(),
            vec![
                (&one, &"Pometry Inc."),
                (&two, &"Raphtory"),
                (&three, &"Pometry")
            ]
        );

        assert_eq!(
            tcell
                .iter_window(EventTime::range(3..i64::MAX))
                .collect::<Vec<_>>(),
            vec![(&three, &"Pometry")]
        );

        assert_eq!(
            tcell
                .iter_window(EventTime::range(2..i64::MAX))
                .collect::<Vec<_>>(),
            vec![(&two, &"Raphtory"), (&three, &"Pometry")]
        );

        let expected = vec![];
        assert_eq!(
            tcell
                .iter_window(EventTime::range(5..i64::MAX))
                .collect::<Vec<_>>(),
            expected
        );

        assert_eq!(
            tcell
                .iter_window(EventTime::range(i64::MIN..4))
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
                .iter_window(EventTime::range(i64::MIN..1))
                .collect::<Vec<_>>(),
            expected
        );

        let mut tcell: TCell<i64> = TCell::default();
        for n in 1..130 {
            tcell.set(EventTime::start(n), n)
        }

        assert_eq!(tcell.iter_window_t(i64::MIN..i64::MAX).count(), 129);

        assert_eq!(
            tcell
                .iter_window(EventTime::range(i64::MIN..i64::MAX))
                .count(),
            129
        )
    }
}
