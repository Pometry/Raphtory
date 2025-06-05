use std::ops::Range;

use raphtory::core::{
    entities::nodes::node_store::PropTimestamps,
    storage::timeindex::{TimeIndexEntry, TimeIndexOps, TimeIndexWindow},
};
use raphtory_api::iter::BoxedLIter;

pub enum MemAdditions<'a> {
    Props(&'a PropTimestamps),
    Window(TimeIndexWindow<'a, TimeIndexEntry, PropTimestamps>),
}

impl<'a> TimeIndexOps for MemAdditions<'a> {
    type IndexType = TimeIndexEntry;
    type RangeType<'b>
        = Self
    where
        Self: 'b;

    fn active(&self, w: Range<Self::IndexType>) -> bool {
        match self {
            MemAdditions::Props(props) => props.active(w),
            MemAdditions::Window(window) => window.active(w),
        }
    }

    fn range(&self, w: Range<Self::IndexType>) -> Self::RangeType<'_> {
        match self {
            MemAdditions::Props(props) => MemAdditions::Window(props.range(w)),
            MemAdditions::Window(window) => MemAdditions::Window(window.range_internal(w)),
        }
    }

    fn first(&self) -> Option<Self::IndexType> {
        match self {
            MemAdditions::Props(props) => props.first(),
            MemAdditions::Window(window) => window.first(),
        }
    }

    fn last(&self) -> Option<Self::IndexType> {
        match self {
            MemAdditions::Props(props) => props.last(),
            MemAdditions::Window(window) => window.last(),
        }
    }

    fn iter(&self) -> BoxedLIter<Self::IndexType> {
        match self {
            MemAdditions::Props(props) => props.iter(),
            MemAdditions::Window(window) => window.iter(),
        }
    }

    fn len(&self) -> usize {
        match self {
            MemAdditions::Props(props) => props.len(),
            MemAdditions::Window(window) => window.len(),
        }
    }
}
