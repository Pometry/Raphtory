use std::{collections::BTreeMap, fmt::Debug, ops::Range};

use serde::{Serialize, Deserialize};

use crate::sorted_vec_map::SVM;

#[derive(Debug, PartialEq, Default, Clone, Serialize, Deserialize)]
/**
 * TCells represent a value in time that can
 * be set at multiple times and keeps a history
 **/
pub(crate) enum TCell<A: Clone + Default + Debug + PartialEq> {
    #[default]
    TCellEmpty,
    TCell1(i64, A),
    TCellCap(SVM<i64, A>),
    TCellN(BTreeMap<i64, A>),
}

const BTREE_CUTOFF: usize = 128;

impl<A: Clone + Default + Debug + PartialEq> TCell<A> {
    pub fn new(t: i64, a: A) -> Self {
        TCell::TCell1(t, a)
    }

    pub fn set(&mut self, t: i64, a: A) {
        match self {
            TCell::TCellEmpty => {
                *self = TCell::TCell1(t, a);
            }
            TCell::TCell1(t0, a0) => {
                if t != *t0 {
                    let mut m = SVM::new();
                    m.insert(t, a);
                    m.insert(*t0, a0.clone());
                    *self = TCell::TCellCap(m)
                }
            }
            TCell::TCellCap(m) => {
                if m.len() < BTREE_CUTOFF {
                    m.insert(t, a.clone());
                } else {
                    let mut new_m: BTreeMap<i64, A> = BTreeMap::new();
                    for (k, v) in m.iter() {
                        new_m.insert(*k, v.clone());
                    }
                    new_m.insert(t, a.clone());
                    *self = TCell::TCellN(new_m)
                }
            }
            TCell::TCellN(m) => {
                m.insert(t, a);
            }
        }
    }

    pub fn iter_window_t(&self, r: Range<i64>) -> Box<dyn Iterator<Item = (&i64, &A)> + '_> {
        match self {
            TCell::TCellEmpty => Box::new(std::iter::empty()),
            TCell::TCell1(t, a) => {
                if r.contains(t) {
                    Box::new(std::iter::once((t, a)))
                } else {
                    Box::new(std::iter::empty())
                }
            }
            TCell::TCellCap(m) => Box::new(m.range(r)),
            TCell::TCellN(m) => Box::new(m.range(r)),
        }
    }

    pub fn iter_window(&self, r: Range<i64>) -> Box<dyn Iterator<Item = &A> + '_> {
        match self {
            TCell::TCellEmpty => Box::new(std::iter::empty()),
            TCell::TCell1(t, a) => {
                if r.contains(t) {
                    Box::new(std::iter::once(a))
                } else {
                    Box::new(std::iter::empty())
                }
            }
            TCell::TCellCap(m) => Box::new(m.range(r).map(|(_, a)| a)),
            TCell::TCellN(m) => Box::new(m.range(r).map(|(_, a)| a)),
        }
    }

    pub fn iter(&self) -> Box<dyn Iterator<Item = &A> + '_> {
        match self {
            TCell::TCellEmpty => Box::new(std::iter::empty()),
            TCell::TCell1(_, a) => Box::new(std::iter::once(a)),
            TCell::TCellCap(m) => Box::new(m.iter().map(|(_, v)| v)),
            TCell::TCellN(v) => Box::new(v.values()),
        }
    }

    pub fn iter_t(&self) -> Box<dyn Iterator<Item = (&i64, &A)> + '_> {
        match self {
            TCell::TCellEmpty => Box::new(std::iter::empty()),
            TCell::TCell1(t, a) => Box::new(std::iter::once((t, a))),
            TCell::TCellCap(v) => Box::new(v.iter()),
            TCell::TCellN(v) => Box::new(v.iter()),
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
            .iter_window(std::i64::MIN..std::i64::MAX)
            .collect::<Vec<_>>();
        assert_eq!(actual, vec!["faster", "hamster", "lobster"]); // ordering is important by time

        let actual = tcell.iter_window(4..std::i64::MAX).collect::<Vec<_>>();
        assert_eq!(actual, vec!["hamster", "lobster"]); // ordering is important by time

        let actual = tcell.iter_window(3..8).collect::<Vec<_>>();
        assert_eq!(actual, vec!["faster", "hamster"]); // ordering is important by time

        let actual = tcell.iter_window(6..std::i64::MAX).collect::<Vec<_>>();
        assert_eq!(actual, vec!["hamster", "lobster"]); // ordering is important by time

        let actual = tcell.iter_window(8..std::i64::MAX).collect::<Vec<_>>();
        assert_eq!(actual, vec!["lobster"]); // ordering is important by time

        let actual: Vec<&String> = tcell.iter_window(17..std::i64::MAX).collect::<Vec<_>>();
        let expected: Vec<&String> = vec![];
        assert_eq!(actual, expected); // ordering is important by time
    }
}
