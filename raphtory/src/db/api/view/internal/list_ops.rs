use crate::{
    core::entities::{EID, VID},
    db::api::{state::Index, view::Base},
};
use enum_dispatch::enum_dispatch;
use rayon::{iter::Either, prelude::*};
use std::hash::Hash;

#[enum_dispatch]
pub trait ListOps {
    fn node_list(&self) -> NodeList;

    fn edge_list(&self) -> EdgeList;
}

pub trait InheritListOps: Base {}

impl<G: InheritListOps> ListOps for G
where
    <G as Base>::Base: ListOps,
{
    fn node_list(&self) -> NodeList {
        self.base().node_list()
    }

    fn edge_list(&self) -> EdgeList {
        self.base().edge_list()
    }
}

#[derive(Debug)]
pub enum List<I> {
    All { len: usize },
    List { elems: Index<I> },
}

pub type NodeList = List<VID>;
pub type EdgeList = List<EID>;

impl<I> Clone for List<I> {
    fn clone(&self) -> Self {
        match self {
            List::All { len } => List::All { len: *len },
            List::List { elems } => List::List {
                elems: elems.clone(),
            },
        }
    }
}

impl<I: Copy + Eq + Hash + Into<usize> + From<usize> + Send + Sync> List<I> {
    pub fn intersection(&self, other: &List<I>) -> List<I> {
        match (self, other) {
            (List::All { len: a }, List::All { len: b }) => {
                let len = *a.min(b);
                List::All { len }
            }
            (List::List { .. }, List::All { .. }) => self.clone(),
            (List::All { .. }, List::List { .. }) => other.clone(),
            (List::List { elems: a }, List::List { elems: b }) => {
                let elems = a.intersection(b);
                List::List { elems }
            }
        }
    }

    pub fn par_iter(&self) -> impl IndexedParallelIterator<Item = I> + '_ {
        match self {
            List::All { len } => Either::Left((0..*len).into_par_iter().map(From::from)),
            List::List { elems } => Either::Right(elems.par_iter()),
        }
    }

    pub fn into_par_iter(self) -> impl IndexedParallelIterator<Item = I> {
        match self {
            List::All { len } => Either::Left((0..len).into_par_iter().map(From::from)),
            List::List { elems } => Either::Right(elems.into_par_iter()),
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = I> + '_ {
        match self {
            List::All { len } => Either::Left((0..*len).map(From::from)),
            List::List { elems } => Either::Right(elems.iter()),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            List::All { len } => *len,
            List::List { elems } => elems.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<I: Copy + Eq + Hash + Into<usize> + From<usize> + Send + Sync + 'static> IntoIterator
    for List<I>
{
    type Item = I;
    type IntoIter = Box<dyn Iterator<Item = Self::Item> + Send + Sync>;

    fn into_iter(self) -> Self::IntoIter {
        match self {
            List::All { len } => Box::new((0..len).map(From::from)),
            List::List { elems } => Box::new(elems.into_iter()),
        }
    }
}
