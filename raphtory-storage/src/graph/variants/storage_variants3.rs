use iter_enum::{
    DoubleEndedIterator, ExactSizeIterator, FusedIterator, IndexedParallelIterator, Iterator,
    ParallelIterator,
};
use raphtory_api::core::{
    entities::properties::{prop::Prop, tprop::TPropOps},
    storage::timeindex::TimeIndexEntry,
};
use std::ops::Range;

#[derive(
    Copy,
    Clone,
    Debug,
    Iterator,
    DoubleEndedIterator,
    ExactSizeIterator,
    FusedIterator,
    ParallelIterator,
    IndexedParallelIterator,
)]
pub enum StorageVariants3<Mem, Unlocked> {
    Mem(Mem),
    Unlocked(Unlocked),
}

macro_rules! SelfType {
    ($Mem:ident, $Unlocked:ident, $Disk:ident) => {
        StorageVariants3<$Mem, $Unlocked>
    };
}

macro_rules! for_all {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            StorageVariants3::Mem($pattern) => $result,
            StorageVariants3::Unlocked($pattern) => $result,
        }
    };
}

macro_rules! for_all_iter {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            StorageVariants3::Mem($pattern) => StorageVariants3::Mem($result),
            StorageVariants3::Unlocked($pattern) => StorageVariants3::Unlocked($result),
        }
    };
}

impl<'a, Mem: TPropOps<'a> + 'a, Unlocked: TPropOps<'a> + 'a> TPropOps<'a>
    for SelfType!(Mem, Unlocked, Disk)
{
    fn last_before(&self, t: TimeIndexEntry) -> Option<(TimeIndexEntry, Prop)> {
        for_all!(self, props => props.last_before(t))
    }

    fn iter_inner(
        self,
        range: Option<Range<TimeIndexEntry>>,
    ) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'a {
        for_all_iter!(self, props => props.iter_inner(range))
    }

    fn iter_inner_rev(
        self,
        range: Option<Range<TimeIndexEntry>>,
    ) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'a {
        for_all_iter!(self, props => props.iter_inner_rev(range))
    }

    fn at(&self, ti: &TimeIndexEntry) -> Option<Prop> {
        for_all!(self, props => props.at(ti))
    }
}
