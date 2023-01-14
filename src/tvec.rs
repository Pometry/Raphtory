// use std::{borrow::Borrow, collections::BTreeMap, fmt::Debug, ops::Range};

// use itertools::Itertools;

// use crate::{bitset::BitSet, tcell::TCell};

// pub trait TVec<A> {
//     /**
//      * Append the item at the end of the TVec
//      *  */
//     fn push(&mut self, t: u64, a: A);

//     /**
//      * Append the item at the end of the TVec
//      *  */
//     fn insert(&mut self, t: u64, a: A, i: usize);

//     /**
//      *  Iterate all the items irrespective of time
//      *  */
//     fn iter(&self) -> Box<dyn Iterator<Item = &A> + '_>;

//     /**
//      *  Iterate the items in the time window
//      *  */
//     fn iter_window(&self, r: Range<u64>) -> Box<dyn Iterator<Item = &A> + '_>;

//     /**
//      *  Iterate the items in the time window and return the time with them
//      *  */
//     fn iter_window_t(&self, r: Range<u64>) -> Box<dyn Iterator<Item = (&u64, &A)> + '_>;
// }

// #[derive(Debug, Default, PartialEq)]
// pub enum DefaultTVec<A: Clone + Default + Debug + PartialEq + PartialOrd> {
//     #[default]
//     Empty,
//     One(TCell<A>),
//     Vec {
//         vs: Vec<TCell<A>>,
//         t_index: BTreeMap<u64, BitSet>,
//     },
// }

// impl<A: Clone + Default + Debug + PartialEq + PartialOrd> DefaultTVec<A> {
//     pub fn new(t: u64, a: A) -> Self {
//         DefaultTVec::One(TCell::new(t, a))
//     }

//     fn len(&self) -> usize {
//         self.iter().count()
//     }

//     fn len_t(&self, r: Range<u64>) -> usize {
//         self.iter_window(r).count()
//     }

//     pub fn push(&mut self, t: u64, a: A) {
//         if let entry @ DefaultTVec::Empty = self {
//             *entry = DefaultTVec::One(TCell::new(t, a));
//         } else if let DefaultTVec::One(tcell) = self.borrow() {
//             let mut new_entry = DefaultTVec::Vec {
//                 vs: vec![],
//                 t_index: BTreeMap::new(),
//             };

//             for (t0, a0) in tcell.iter_t() {
//                 new_entry.push(*t0, a0.clone());
//             }
//             new_entry.push(t, a);
//             *self = new_entry;
//         } else if let DefaultTVec::Vec { vs, t_index } = self {
//             let i = vs.len();
//             // select a cell to insert the timed value at
//             let cell = TCell::new(t, a);
//             vs.push(cell);

//             // add index
//             t_index
//                 .entry(t)
//                 .and_modify(|set| {
//                     set.push(i);
//                 })
//                 .or_insert_with(|| BitSet::one(i));
//         }
//     }

//     pub fn insert(&mut self, t: u64, a: A, i: usize) {
//         if let DefaultTVec::Empty = self {
//             panic!("insertion index (is {i}) should be < len (is 0)");
//         } else if let DefaultTVec::One(tcell) = self {
//             if i == 0 {
//                 tcell.set(t, a);
//             } else {
//                 panic!("insertion index (is {i}) should be < len (is 1)");
//             }
//         } else if let DefaultTVec::Vec { vs, t_index } = self {
//             vs[i].set(t, a);
//             // add index
//             t_index
//                 .entry(t)
//                 .and_modify(|set| {
//                     set.push(i);
//                 })
//                 .or_insert_with(|| BitSet::one(i));
//         }
//     }

//     pub fn iter(&self) -> Box<dyn Iterator<Item = &A> + '_> {
//         if let DefaultTVec::One(tcell) = self {
//             tcell.iter()
//         } else if let DefaultTVec::Vec { vs, .. } = self {
//             Box::new(vs.iter().flat_map(|cell| cell.iter()))
//         } else {
//             Box::new(std::iter::empty())
//         }
//     }

//     pub fn iter_window(&self, r: Range<u64>) -> Box<dyn Iterator<Item = &A> + '_> {
//         if let DefaultTVec::One(tcell) = self {
//             tcell.iter_window(r)
//         } else if let DefaultTVec::Vec { vs, t_index } = self {
//             let iter = t_index
//                 .range(r.clone())
//                 .map(|(_, vs)| vs.iter()) // TODO: a modified version of kmerge that does not output duplicates should be a better option here since vs.iter() should be ordered
//                 .kmerge()
//                 .dedup()
//                 .map(move |id| {
//                     vs[id].iter_window(r.clone()) // this might be stupid
//                 }).kmerge().dedup();
//             Box::new(iter)
//         } else {
//             Box::new(std::iter::empty())
//         }
//     }

//     pub fn iter_window_t(&self, r: Range<u64>) -> Box<dyn Iterator<Item = (&u64, &A)> + '_> {
//         if let DefaultTVec::One(tcell) = self {
//             tcell.iter_window_t(r)
//         } else if let DefaultTVec::Vec { vs, t_index } = self {
//             let iter = t_index
//                 .range(r.clone())
//                 .map(|(_, vs)| vs.iter()) // TODO: a modified version of kmerge that does not output duplicates should be a better option here since vs.iter() should be ordered
//                 .kmerge()
//                 .dedup()
//                 .map(move |id| {
//                     vs[id].iter_window_t(r.clone()) // this might be stupid
//                 }).kmerge().dedup();
//             Box::new(iter)
//         } else {
//             Box::new(std::iter::empty())
//         }
//     }
// }

// #[cfg(test)]
// mod tvec_tests {
//     use super::*;

//     #[test]
//     fn push() {
//         let mut tvec = DefaultTVec::default();

//         tvec.push(4, 12); // i:0 t: 4
//         tvec.push(9, 3); // i:1 t: 3
//         tvec.push(1, 2); // i: 2 t: 2

//         assert_eq!(tvec.iter().collect::<Vec<_>>(), vec![&12, &3, &2]);
//     }

//     #[test]
//     fn timed_iter() {
//         let mut tvec = DefaultTVec::default();

//         tvec.push(4, 12);
//         tvec.push(9, 3);
//         tvec.push(1, 2);

//         assert_eq!(tvec.iter_window(0..5).collect::<Vec<_>>(), vec![&2, &12]);
//     }

//     #[test]
//     fn insert() {
//         let mut tvec = DefaultTVec::default();

//         tvec.push(4, 12); // t: 4 i:0
//         tvec.push(9, 3); // t: 9 i:1
//         tvec.push(1, 5); // t: 1 i:2

//         // at a different t:3 override the index 2
//         tvec.insert(3, 19, 2);

//         println!("{tvec:?}");

//         assert_eq!(
//             tvec.iter_window(0..5).collect::<Vec<_>>(),
//             vec![&5, &12, &19]
//         );
//     }

//     #[test]
//     fn insert_iter_time() {
//         let mut tvec = DefaultTVec::default();

//         tvec.push(4, String::from("one")); // t: 4 i:0
//         tvec.push(9, String::from("two")); // t: 9 i:1
//         tvec.push(1, String::from("three")); // t: 1 i:2

//         // at a different t:3 override the index 2
//         tvec.insert(3, String::from("four"), 2);

//         assert_eq!(
//             tvec.iter_window_t(0..5).collect::<Vec<_>>(),
//             vec![
//                 (&1u64, &String::from("three")),
//                 (&3u64, &String::from("four")),
//                 (&4u64, &String::from("one")),
//             ]
//         );

//         // from time 3 onwards you cannot see the item "three"
//         assert_eq!(
//             tvec.iter_window_t(3..100).collect::<Vec<_>>(),
//             vec![
//                 (&3u64, &String::from("four")),
//                 (&4u64, &String::from("one")),
//                 (&9u64, &String::from("two")),
//             ]
//         );
//     }

//     #[test]
//     fn push_and_count() {
//         let mut tvec = DefaultTVec::default();

//         tvec.push(4, String::from("one")); // t: 4 i:0
//         tvec.push(9, String::from("two")); // t: 9 i:1
//         tvec.push(1, String::from("three")); // t: 1 i:2

//         assert_eq!(tvec.len(), 3);
//     }

//     #[test]
//     fn insert_and_count() {
//         let mut tvec = DefaultTVec::default();

//         tvec.push(4, String::from("one")); // t: 4 i:0
//         tvec.push(9, String::from("two")); // t: 9 i:1
//         tvec.push(1, String::from("three")); // t: 1 i:2
//                                              //
//         tvec.insert(19, String::from("four"), 0); // t: 19 i:0

//         // len includes all versions
//         assert_eq!(tvec.len(), 4);
//     }

//     #[test]
//     fn push_value_same_time() {}
// }
