use std::{
    cmp::{max, min},
    ops::Range,
};

use itertools::Itertools;
use raphtory::{
    core::{
        entities::{nodes::node_store::PropTimestamps, properties::tprop::TPropCell},
        storage::timeindex::{TimeIndexEntry, TimeIndexIntoOps, TimeIndexOps, TimeIndexWindow},
    },
    db::api::storage::graph::tprop_storage_ops::{TPropOps, TPropRef},
    prelude::Prop,
};
use raphtory_api::iter::BoxedLIter;

use crate::LocalPOS;

pub trait EdgePageView {
    fn additions(&self, edge_pos: LocalPOS) -> TimeCell;
    fn t_prop(&self, edge_pos: LocalPOS, prop_id: usize) -> TProp;
}

#[derive(Debug, Clone, Copy, Default)]
pub enum TProp<'a> {
    #[default]
    Empty,
    Mem(TPropCell<'a>),
    Disk(TPropRef<'a>),
}

impl<'a> TPropOps<'a> for TProp<'a> {
    fn last_before(&self, t: TimeIndexEntry) -> Option<(TimeIndexEntry, Prop)> {
        match self {
            TProp::Mem(cell) => cell.last_before(t),
            TProp::Disk(cell) => cell.last_before(t),
            TProp::Empty => None,
        }
    }

    fn iter_inner(
        self,
        range: Option<Range<TimeIndexEntry>>,
    ) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'a {
        let iter: BoxedLIter<_> = match self {
            TProp::Mem(cell) => Box::new(cell.iter_inner(range)),
            TProp::Disk(cell) => Box::new(cell.iter_inner(range)),
            TProp::Empty => Box::new(std::iter::empty()),
        };
        iter
    }

    fn iter_inner_rev(
        self,
        range: Option<Range<TimeIndexEntry>>,
    ) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'a {
        let iter: BoxedLIter<_> = match self {
            TProp::Mem(cell) => Box::new(cell.iter_inner_rev(range)),
            TProp::Disk(cell) => Box::new(cell.iter_inner_rev(range)),
            TProp::Empty => Box::new(std::iter::empty()),
        };
        iter
    }

    fn at(self, ti: &TimeIndexEntry) -> Option<Prop> {
        match self {
            TProp::Mem(cell) => cell.at(ti),
            TProp::Disk(cell) => cell.at(ti),
            TProp::Empty => None,
        }
    }
}

