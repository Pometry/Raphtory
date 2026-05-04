use crate::{
    entities::properties::tcell::TCell,
    storage::{timeindex::EventTime, PropColumn},
};
use either::Either;

use raphtory_api::core::entities::properties::{
    prop::{Prop, PropType},
    tprop::TPropOps,
};
use std::ops::Range;
use thiserror::Error;

#[derive(Error, Debug)]
#[error("Property type mismatch, expected {expected:?}, received {actual:?}")]
pub struct IllegalPropType {
    pub(crate) expected: PropType,
    pub(crate) actual: PropType,
}

#[derive(Copy, Clone, Debug, Default)]
pub struct TPropCell<'a> {
    t_cell: Option<&'a TCell<Option<usize>>>,
    log: Option<&'a PropColumn>,
}

impl<'a> TPropCell<'a> {
    pub fn new(t_cell: &'a TCell<Option<usize>>, log: Option<&'a PropColumn>) -> Self {
        Self {
            t_cell: Some(t_cell),
            log,
        }
    }

    fn iter_window_inner(
        self,
        r: Range<EventTime>,
    ) -> impl DoubleEndedIterator<Item = (EventTime, Prop)> + Send + 'a {
        self.t_cell.into_iter().flat_map(move |t_cell| {
            t_cell
                .iter_window(r.clone())
                .filter_map(move |(t, &id)| self.log?.get(id?).map(|prop| (*t, prop)))
        })
    }

    fn iter_inner(self) -> impl DoubleEndedIterator<Item = (EventTime, Prop)> + Send + 'a {
        self.t_cell.into_iter().flat_map(move |t_cell| {
            t_cell
                .iter()
                .filter_map(move |(t, &id)| self.log?.get(id?).map(|prop| (*t, prop)))
        })
    }
}

impl<'a> TPropOps<'a> for TPropCell<'a> {
    fn last(&self) -> Option<(EventTime, Prop)> {
        self.t_cell?
            .iter()
            .rev()
            .find_map(|(t, pos)| self.log?.get(pos.as_ref().copied()?).map(|prop| (*t, prop)))
    }

    fn last_window(&self, w: Range<EventTime>) -> Option<(EventTime, Prop)> {
        self.t_cell?
            .iter_window(w)
            .rev()
            .find_map(|(t, pos)| self.log?.get(pos.as_ref().copied()?).map(|prop| (*t, prop)))
    }

    fn iter_inner(
        self,
        range: Option<Range<EventTime>>,
    ) -> impl Iterator<Item = (EventTime, Prop)> + Send + Sync + 'a {
        match range {
            Some(w) => {
                let iter = self.iter_window_inner(w);
                Either::Right(iter)
            }
            None => {
                let iter = self.iter_inner();
                Either::Left(iter)
            }
        }
    }

    fn iter_inner_rev(
        self,
        range: Option<Range<EventTime>>,
    ) -> impl Iterator<Item = (EventTime, Prop)> + Send + Sync + 'a {
        match range {
            Some(w) => {
                let iter = self.iter_window_inner(w).rev();
                Either::Right(iter)
            }
            None => {
                let iter = self.iter_inner().rev();
                Either::Left(iter)
            }
        }
    }

    fn at(&self, ti: &EventTime) -> Option<Prop> {
        self.t_cell?.at(ti).and_then(|&id| self.log?.get(id?))
    }
}
