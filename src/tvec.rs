use std::{collections::BTreeMap, ops::Range};

use itertools::Itertools;
use roaring::RoaringTreemap;

use crate::tcell::TCell;

pub trait TVec<A> {
    /**
     * Append the item at the end of the TVec
     *  */
    fn push(&mut self, t: u64, a: A);

    /**
     * Append the item at the end of the TVec
     *  */
    fn insert(&mut self, t: u64, a: A, i: usize);

    /**
     *  Iterate all the items irrespective of time
     *  */
    fn iter(&self) -> Box<dyn Iterator<Item = &A> + '_>;

    /**
     *  Iterate the items in the time window
     *  */
    fn iter_window(&self, r: Range<u64>) -> Box<dyn Iterator<Item = &A> + '_>;

    /**
     *  Iterate the items in the time window and return the time with them
     *  */
    fn iter_window_t(&self, r: Range<u64>) -> Box<dyn Iterator<Item = (&u64, &A)> + '_>;

    /**
     *  get me the oldest value at i
     *  */
    fn get_oldest_at(&self, i: usize) -> Option<&A>;

    /**
     *  get me the youngest value at i
     *  */
    fn get_youngest_at(&self, i: usize) -> Option<&A>;

    /**
     * get all the items at index i
     *  */
    fn get_window(&self, i: usize, r: Range<u64>) -> Box<dyn Iterator<Item = (&A, u64)> + '_>;

    /**
     *  sort the values and rebuild the index
     *  */
    fn sort(&mut self)
    where
        A: Ord;
}

#[derive(Debug, Default, PartialEq)]
pub struct DefaultTVec<A> {
    vs: Vec<TCell<A>>,
    t_index: BTreeMap<u64, RoaringTreemap>,
}

impl<A> DefaultTVec<A> {
    pub fn new(t: u64, a: A) -> Self {
        let mut m = RoaringTreemap::new();
        m.insert(0u64);
        DefaultTVec {
            vs: vec![TCell::new(t, a)],
            t_index: BTreeMap::from_iter(vec![(t, m)]),
        }
    }
}

impl<A> TVec<A> for DefaultTVec<A> {
    fn push(&mut self, t: u64, a: A) {
        let i = self.vs.len();
        // select a cell to insert the timed value at
        let mut cell = TCell::empty();
        cell.set(t, a);
        self.vs.push(cell);

        // add index
        self.t_index
            .entry(t)
            .and_modify(|set| {
                set.push(i.try_into().unwrap()); //FIXME: not happy here with unwrap
            })
            .or_insert_with(|| {
                let mut bs = RoaringTreemap::default();
                bs.push(i.try_into().unwrap()); //FIXME: not happy here with unwrap
                bs
            });
    }

    fn insert(&mut self, t: u64, a: A, i: usize) {
        let _ = &self.vs[i].set(t, a);
        // mark the index with the new time
        //
        self.t_index
            .entry(t)
            .and_modify(|set| {
                set.push(i.try_into().unwrap()); //FIXME: not happy here with unwrap
            })
            .or_insert_with(|| {
                let mut bs = RoaringTreemap::default();
                bs.push(i.try_into().unwrap()); //FIXME: not happy here with unwrap
                bs
            });
    }

    fn iter(&self) -> Box<dyn Iterator<Item = &A> + '_> {
        Box::new(self.vs.iter().flat_map(|cell| cell.iter()))
    }

    fn iter_window(&self, r: Range<u64>) -> Box<dyn Iterator<Item = &A> + '_> {
        // let iter = self
        //     .t_index
        //     .range(r.clone())
        //     .map(|(_, vs)| vs)
        //     .fold(RoaringTreemap::default(), |mut into, rtm| { into.extend(rtm); into }).iter() // this can be fixed if we create a custom iterator that owns the RoaringTreeMap
        //     .flat_map(move |id| {
        //         let i: usize = id.try_into().unwrap();
        //         self.vs[i].iter_window(r.clone()) // this might be stupid
        //     });
        let iter = self
            .t_index
            .range(r.clone())
            .flat_map(|(_, vs)| vs.iter())
            .unique() // problematic as we store the entire thing in memory
            .flat_map(move |id| {
                let i: usize = id.try_into().unwrap();
                self.vs[i].iter_window(r.clone()) // this might be stupid
            });
        Box::new(iter)
    }

    fn iter_window_t(&self, r: Range<u64>) -> Box<dyn Iterator<Item = (&u64, &A)> + '_> {
        let iter = self
            .t_index
            .range(r.clone())
            .flat_map(|(_, vs)| vs.iter())
            .unique() // problematic as we store the entire thing in memory
            .flat_map(move |id| {
                let i: usize = id.try_into().unwrap();
                self.vs[i].iter_window_t(r.clone()) // this might be stupid
            });
        Box::new(iter)
    }

    fn get_oldest_at(&self, i: usize) -> Option<&A> {
        todo!()
    }

    fn get_youngest_at(&self, i: usize) -> Option<&A> {
        todo!()
    }

    fn get_window(&self, i: usize, r: Range<u64>) -> Box<dyn Iterator<Item = (&A, u64)> + '_> {
        todo!()
    }

    fn sort(&mut self)
    where
        A: Ord,
    {
        todo!()
    }
}

#[cfg(test)]
mod tvec_tests {
    use super::*;

    #[test]
    fn push() {
        let mut tvec = DefaultTVec::default();

        tvec.push(4, 12);
        tvec.push(9, 3);
        tvec.push(1, 2);

        assert_eq!(tvec.iter().collect::<Vec<_>>(), vec![&12, &3, &2]);
    }

    #[test]
    fn timed_iter() {
        let mut tvec = DefaultTVec::default();

        tvec.push(4, 12);
        tvec.push(9, 3);
        tvec.push(1, 2);

        assert_eq!(tvec.iter_window(0..5).collect::<Vec<_>>(), vec![&2, &12]);
    }

    #[test]
    fn insert() {
        let mut tvec = DefaultTVec::default();

        tvec.push(4, 12); // t: 4 i:0
        tvec.push(9, 3); // t: 9 i:1
        tvec.push(1, 2); // t: 1 i:2

        // at a different t:3 override the index 2
        tvec.insert(3, 19, 2);

        assert_eq!(
            tvec.iter_window(0..5).collect::<Vec<_>>(),
            vec![&2, &19, &12]
        );
    }

    #[test]
    fn insert_iter_time() {
        let mut tvec = DefaultTVec::default();

        tvec.push(4, String::from("one")); // t: 4 i:0
        tvec.push(9, String::from("two")); // t: 9 i:1
        tvec.push(1, String::from("three")); // t: 1 i:2

        // at a different t:3 override the index 2
        tvec.insert(3, String::from("four"), 2);

        assert_eq!(
            tvec.iter_window_t(0..5).collect::<Vec<_>>(),
            vec![
                (&1u64, &String::from("three")),
                (&3u64, &String::from("four")),
                (&4u64, &String::from("one")),
            ]
        );
    }
}
