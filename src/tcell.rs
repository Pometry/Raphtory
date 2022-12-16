use std::{collections::BTreeMap, fmt::Debug, ops::Range};

use crate::sortedvec::SortedVec;

#[derive(Debug, PartialEq)]
/**
 * TCells represent a value in time that can
 * be set at multiple times and keeps a history
 **/
pub enum TCell<A: Clone + Default + Debug + PartialEq> {
    TCellEmpty,
    TCell1(u64, A),
    TCellCap(SortedVec<u64, A>),
    TCellN(BTreeMap<u64, A>),
}

const BTREE_CUTOFF: usize = 128;

impl<A: Clone + Default + Debug + PartialEq> TCell<A> {
    pub fn new(t: u64, a: A) -> Self {
        TCell::TCell1(t, a)
    }

    pub fn empty() -> Self {
        TCell::TCellEmpty
    }

    pub fn is_empty(&self) -> bool {
        match self {
            TCell::TCellEmpty => true,
            _ => false
        }
    }

    pub fn len(&self) -> usize {
        match self {
            TCell::TCell1(_, _) => 1,
            TCell::TCellCap(m) => m.len(),
            TCell::TCellN(m) => m.len(),
            _ => 0,
        }
    }

    pub fn set(&mut self, t: u64, a: A) {
        match self {
            TCell::TCellEmpty => {
                *self = TCell::TCell1(t, a);
            }
            TCell::TCell1(t0, a0) => {
                if t != *t0 {
                    let mut m = SortedVec::new();
                    m.insert(t, a);
                    m.insert(*t0, a0.clone());
                    *self = TCell::TCellCap(m)
                }
            }
            TCell::TCellCap(m) => {
                if m.len() < BTREE_CUTOFF {
                    m.insert(t, a.clone());
                } else {
                    let mut new_m: BTreeMap<u64, A> = BTreeMap::new();
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

    pub fn iter_window_t(&self, r: Range<u64>) -> Box<dyn Iterator<Item = (&u64, &A)> + '_> {
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

    pub fn iter_window(&self, r: Range<u64>) -> Box<dyn Iterator<Item = &A> + '_> {
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

    pub fn iter_t(&self) -> Box<dyn Iterator<Item = (&u64, &A)> + '_> {
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
        let mut tcell = TCell::empty();
        tcell.set(16, String::from("lobster"));

        let actual = tcell.iter().collect::<Vec<_>>();

        assert_eq!(actual, vec!["lobster"]);
    }

    #[test]
    fn set_tcell_twice_from_empty() {
        let mut tcell = TCell::empty();
        tcell.set(16, String::from("lobster"));
        tcell.set(7, String::from("hamster"));

        let actual = tcell.iter().collect::<Vec<_>>();

        assert_eq!(actual, vec!["hamster", "lobster"]); // ordering is important by time
    }

    #[test]
    fn set_tcell_trice_from_empty_range_iter() {
        let mut tcell = TCell::empty();
        tcell.set(16, String::from("lobster"));
        tcell.set(7, String::from("hamster"));
        tcell.set(3, String::from("faster"));

        let actual = tcell
            .iter_window(std::u64::MIN..std::u64::MAX)
            .collect::<Vec<_>>();
        assert_eq!(actual, vec!["faster", "hamster", "lobster"]); // ordering is important by time

        let actual = tcell.iter_window(4..std::u64::MAX).collect::<Vec<_>>();
        assert_eq!(actual, vec!["hamster", "lobster"]); // ordering is important by time

        let actual = tcell.iter_window(3..8).collect::<Vec<_>>();
        assert_eq!(actual, vec!["faster", "hamster"]); // ordering is important by time

        let actual = tcell.iter_window(6..std::u64::MAX).collect::<Vec<_>>();
        assert_eq!(actual, vec!["hamster", "lobster"]); // ordering is important by time

        let actual = tcell.iter_window(8..std::u64::MAX).collect::<Vec<_>>();
        assert_eq!(actual, vec!["lobster"]); // ordering is important by time

        let actual: Vec<&String> = tcell.iter_window(17..std::u64::MAX).collect::<Vec<_>>();
        let expected: Vec<&String> = vec![];
        assert_eq!(actual, expected); // ordering is important by time
    }

}
