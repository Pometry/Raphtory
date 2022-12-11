use std::{collections::BTreeMap, ops::Range};

#[derive(Debug, PartialEq)]
/**
 * TCells represent a value in time that can
 * be set at multiple times and keeps a history
 **/
pub enum TCell<A> {
    TCellEmpty,
    TCellN(BTreeMap<u64, A>),
}

impl<A> TCell<A> {
    pub fn empty() -> Self {
        TCell::TCellEmpty
    }

    pub fn set(&mut self, t: u64, a: A) {
        match self {
            TCell::TCellEmpty => {
                let mut m = BTreeMap::default();
                m.insert(t, a);
                *self = TCell::TCellN(m);
            }
            TCell::TCellN(m) => {
                m.insert(t, a);
            }
        }
    }

    pub fn iter_window_t(
        &self,
        r: Range<u64>,
    ) -> Box<dyn Iterator<Item = (&u64, &A)> + '_> {
        match self {
            TCell::TCellEmpty => Box::new(std::iter::empty()),
            TCell::TCellN(m) => Box::new(m.range(r)),
        }
    }

    pub fn iter_window(&self, r: Range<u64>) -> Box<dyn Iterator<Item = &A> + '_> {
        match self {
            TCell::TCellEmpty => Box::new(std::iter::empty()),
            TCell::TCellN(m) => Box::new(m.range(r).map(|(_, a)| a)),
        }
    }

    pub fn iter(&self) -> Box<dyn Iterator<Item = &A> + '_> {
        match self {
            TCell::TCellEmpty => Box::new(std::iter::empty()),
            TCell::TCellN(v) => Box::new(v.values()),
        }
    }

    pub fn iter_t(&self) -> Box<dyn Iterator<Item = (&u64, &A)> + '_> {
        match self {
            TCell::TCellEmpty => Box::new(std::iter::empty()),
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
