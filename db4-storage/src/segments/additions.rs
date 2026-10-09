use iter_enum::Iterator;
use raphtory_api_macros::box_on_debug_lifetime;
use raphtory_core::{
    entities::{ELID, properties::tcell::TCell},
    storage::timeindex::{EventTime, TimeIndexOps, TimeIndexWindow},
};
use std::ops::Range;

use crate::{generic_time_ops::EdgeEventOps, utils::Iter4};

#[derive(Clone, Debug)]
pub enum MemTimeCell<'a> {
    Edges(&'a TCell<ELID>),
    Deletions(&'a TCell<()>),
    Props(&'a TCell<Option<usize>>),
    WEdges(TimeIndexWindow<'a, EventTime, TCell<ELID>>),
    WDeletions(TimeIndexWindow<'a, EventTime, TCell<()>>),
    WProps(TimeIndexWindow<'a, EventTime, TCell<Option<usize>>>),
}

#[derive(Clone, Debug, Iterator)]
pub enum MemTimeCellVariants<Edges, Deletions, Props, WEdges, WDeletions, WProps> {
    Edges(Edges),
    Deletions(Deletions),
    Props(Props),
    WEdges(WEdges),
    WDeletions(WDeletions),
    WProps(WProps),
}

macro_rules! for_all_variants {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            MemTimeCell::Edges($pattern) => MemTimeCellVariants::Edges($result),
            MemTimeCell::Deletions($pattern) => MemTimeCellVariants::Deletions($result),
            MemTimeCell::Props($pattern) => MemTimeCellVariants::Props($result),
            MemTimeCell::WEdges($pattern) => MemTimeCellVariants::WEdges($result),
            MemTimeCell::WDeletions($pattern) => MemTimeCellVariants::WDeletions($result),
            MemTimeCell::WProps($pattern) => MemTimeCellVariants::WProps($result),
        }
    };
}

macro_rules! for_all {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            MemTimeCell::Edges($pattern) => $result,
            MemTimeCell::Deletions($pattern) => $result,
            MemTimeCell::Props($pattern) => $result,
            MemTimeCell::WEdges($pattern) => $result,
            MemTimeCell::WDeletions($pattern) => $result,
            MemTimeCell::WProps($pattern) => $result,
        }
    };
}

impl<'a> From<&'a TCell<ELID>> for MemTimeCell<'a> {
    fn from(edges: &'a TCell<ELID>) -> Self {
        MemTimeCell::Edges(edges)
    }
}

impl<'a> From<&'a TCell<Option<usize>>> for MemTimeCell<'a> {
    fn from(props: &'a TCell<Option<usize>>) -> Self {
        MemTimeCell::Props(props)
    }
}

impl<'a> From<&'a TCell<()>> for MemTimeCell<'a> {
    fn from(value: &'a TCell<()>) -> Self {
        MemTimeCell::Deletions(value)
    }
}

impl<'a> From<TimeIndexWindow<'a, EventTime, TCell<ELID>>> for MemTimeCell<'a> {
    fn from(value: TimeIndexWindow<'a, EventTime, TCell<ELID>>) -> Self {
        MemTimeCell::WEdges(value)
    }
}

impl<'a> From<TimeIndexWindow<'a, EventTime, TCell<Option<usize>>>> for MemTimeCell<'a> {
    fn from(value: TimeIndexWindow<'a, EventTime, TCell<Option<usize>>>) -> Self {
        MemTimeCell::WProps(value)
    }
}

impl<'a> From<TimeIndexWindow<'a, EventTime, TCell<()>>> for MemTimeCell<'a> {
    fn from(value: TimeIndexWindow<'a, EventTime, TCell<()>>) -> Self {
        MemTimeCell::WDeletions(value)
    }
}

impl<'a> EdgeEventOps<'a> for MemTimeCell<'a> {
    #[box_on_debug_lifetime]
    fn edge_events(self) -> impl Iterator<Item = (EventTime, ELID)> + Send + Sync + 'a {
        match self {
            MemTimeCell::Edges(edges) => Iter4::I(edges.iter().map(|(k, v)| (*k, *v))),
            MemTimeCell::WEdges(TimeIndexWindow::All(ti)) => {
                Iter4::J(ti.iter().map(|(k, v)| (*k, *v)))
            }
            MemTimeCell::WEdges(TimeIndexWindow::Range { timeindex, range }) => {
                Iter4::K(timeindex.iter_window(range).map(|(k, v)| (*k, *v)))
            }
            _ => Iter4::L(std::iter::empty()),
        }
    }

    #[box_on_debug_lifetime]
    fn edge_events_rev(self) -> impl Iterator<Item = (EventTime, ELID)> + Send + Sync + 'a {
        match self {
            MemTimeCell::Edges(edges) => Iter4::I(edges.iter().map(|(k, v)| (*k, *v)).rev()),
            MemTimeCell::WEdges(TimeIndexWindow::All(ti)) => {
                Iter4::J(ti.iter().map(|(k, v)| (*k, *v)).rev())
            }
            MemTimeCell::WEdges(TimeIndexWindow::Range { timeindex, range }) => {
                Iter4::K(timeindex.iter_window(range).map(|(k, v)| (*k, *v)).rev())
            }
            _ => Iter4::L(std::iter::empty()),
        }
    }
}

impl<'a> TimeIndexOps<'a> for MemTimeCell<'a> {
    type IndexType = EventTime;

    type RangeType = Self;

    #[inline]
    fn active(&self, w: Range<Self::IndexType>) -> bool {
        for_all!(self, a => TimeIndexOps::active(a, w))
    }

    fn range(&self, w: Range<Self::IndexType>) -> Self::RangeType {
        for_all!(self, a => TimeIndexOps::range(a, w).into())
    }

    #[box_on_debug_lifetime]
    fn iter(self) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'a {
        for_all_variants!(self, a => TimeIndexOps::iter(a))
    }

    #[box_on_debug_lifetime]
    fn iter_rev(self) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'a {
        for_all_variants!(self, a => TimeIndexOps::iter_rev(a))
    }

    fn len(&self) -> usize {
        for_all!(self, a => TimeIndexOps::len(a))
    }

    fn is_empty(&self) -> bool {
        for_all!(self, a => TimeIndexOps::is_empty(a))
    }
}
