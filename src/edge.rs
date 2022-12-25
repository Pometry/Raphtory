// use std::{rc::Rc, cell::RefCell, collections::HashMap};

// use crate::props::TProp;

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord)]
pub(crate) struct Edge {
    pub(crate) v: usize,              // physical id of th vertex
    pub(crate) e_meta: Option<usize>, // physical id of the edge metadata
}

impl Edge {
    pub fn new(v: usize, e_meta: usize) -> Self {
        Edge {
            v,
            e_meta: Some(e_meta),
        }
    }
}

// impl Edge {
//     pub fn physical_id(&self) -> usize {
//         match self {
//             Edge::NoProps(pid) => *pid,
//             Edge::Prop(pid, _) => *pid,
//         }
//     }

// }

// impl Ord for Edge {
//     fn cmp(&self, other: &Self) -> std::cmp::Ordering {
//         match (self, other) {
//             (Self::NoProps(l0), Self::NoProps(r0)) => usize::cmp(l0, r0),
//             (Self::Prop(l0, _), Self::Prop(r0, _)) => usize::cmp(l0, r0),
//             (Self::Prop(l0, _), Self::NoProps(r0)) => usize::cmp(l0, r0),
//             (Self::NoProps(l0), Self::Prop(r0, _)) => usize::cmp(l0, r0),
//         }
//     }
// }

// impl PartialOrd for Edge {
//     fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
//         match (self, other) {
//             (Self::NoProps(l0), Self::NoProps(r0)) => Some(usize::cmp(l0, r0)),
//             (Self::Prop(l0, _), Self::Prop(r0, _)) => Some(usize::cmp(l0, r0)),
//             (Self::Prop(l0, _), Self::NoProps(r0)) => Some(usize::cmp(l0, r0)),
//             (Self::NoProps(l0), Self::Prop(r0, _)) => Some(usize::cmp(l0, r0)),
//         }
//     }
// }

// impl PartialEq for Edge {
//     fn eq(&self, other: &Self) -> bool {
//         match (self, other) {
//             (Self::NoProps(l0), Self::NoProps(r0)) => l0 == r0,
//             (Self::Prop(l0, _), Self::Prop(r0, _)) => l0 == r0,
//             _ => false,
//         }
//     }
// }

// impl Eq for Edge {}

// #[cfg(test)]
// mod edge_tests {
//     use std::{cell::RefCell, collections::BTreeSet};

//     use super::*;

//     #[test]
//     fn upsert_edge_in_btreeset() {
//         // let mut bs = BTreeSet::from([
//         //     RefCell::new(Edge::NoProps(12)),
//         //     RefCell::new(Edge::Prop(
//         //         17,
//         //         Rc::new(TProp::U64(crate::tvec::DefaultTVec::new(5, 96u64))),
//         //     )),
//         // ]);
//         // println!("WHAA! {bs:?}");

//         // if let Some(refcell) = bs.get(&RefCell::new(Edge::NoProps(12))) {
//         //     *refcell.borrow_mut() = Edge::Prop(
//         //         12,
//         //         Rc::new(TProp::U64(crate::tvec::DefaultTVec::new(5, 96u64))),
//         //     )
//         // }

//         // println!("WHAA! {bs:?}");

//         // if let Some(refcell) = bs.get(&RefCell::new(Edge::NoProps(12))) {
//         //     if let Edge::Prop(_, props) = refcell.borrow_mut() {
//         //         *props = Rc::new(TProp::U64(crate::tvec::DefaultTVec::new(17, 96u64)))
//         //     }
//         // }

//         // println!("WHAA! {bs:?}");
//     }
// }
