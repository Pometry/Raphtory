use std::ops::Range;

use iter_enum::{DoubleEndedIterator, ExactSizeIterator, FusedIterator, Iterator};
use raphtory::core::{
    entities::nodes::node_store::PropTimestamps,
    storage::timeindex::{TimeIndexEntry, TimeIndexOps, TimeIndexWindow},
};

#[derive(Clone, Debug)]
pub enum MemAdditions<'a> {
    Props(&'a PropTimestamps),
    Window(TimeIndexWindow<'a, TimeIndexEntry, PropTimestamps>),
}

impl<'a> TimeIndexOps<'a> for MemAdditions<'a> {
    type IndexType = TimeIndexEntry;

    type RangeType = Self;

    fn active(&self, w: Range<Self::IndexType>) -> bool {
        match self {
            MemAdditions::Props(props) => props.active(w),
            MemAdditions::Window(window) => window.active(w),
        }
    }

    fn range(&self, w: Range<Self::IndexType>) -> Self::RangeType {
        match self {
            MemAdditions::Props(props) => MemAdditions::Window(props.range(w)),
            MemAdditions::Window(window) => MemAdditions::Window(window.range(w)),
        }
    }

    fn iter(self) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'a {
        match self {
            MemAdditions::Props(props) => Iter2::I1(props.iter()),
            MemAdditions::Window(window) => Iter2::I2(window.iter()),
        }
    }

    fn iter_rev(self) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'a {
        match self {
            MemAdditions::Props(props) => Iter2::I1(props.iter_rev()),
            MemAdditions::Window(window) => Iter2::I2(window.iter_rev()),
        }
    }

    fn len(&self) -> usize {
        match self {
            MemAdditions::Props(props) => props.len(),
            MemAdditions::Window(window) => window.len(),
        }
    }
}

#[derive(Clone, Debug, Iterator, DoubleEndedIterator, ExactSizeIterator, FusedIterator)]
pub enum Iter2<I1, I2> {
    I1(I1),
    I2(I2),
}
