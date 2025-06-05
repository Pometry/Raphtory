use iter_enum::{
    DoubleEndedIterator, ExactSizeIterator, FusedIterator, IndexedParallelIterator, Iterator,
    ParallelExtend, ParallelIterator,
};
use raphtory_api::core::{
    entities::properties::{prop::Prop, tprop::TPropOps},
    storage::timeindex::{TimeIndexEntry, TimeIndexOps},
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
    ParallelExtend,
)]
pub enum StorageVariants2<Mem, #[cfg(feature = "storage")] Disk> {
    Mem(Mem),
    #[cfg(feature = "storage")]
    Disk(Disk),
}

#[cfg(feature = "storage")]
macro_rules! SelfType {
    ($Mem:ident, $Disk:ident) => {
        StorageVariants2<$Mem, $Disk>
    };
}

#[cfg(not(feature = "storage"))]
macro_rules! SelfType {
    ($Mem:ident, $Disk:ident) => {
        StorageVariants2<$Mem>
    };
}

macro_rules! for_all {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            StorageVariants2::Mem($pattern) => $result,
            #[cfg(feature = "storage")]
            StorageVariants2::Disk($pattern) => $result,
        }
    };
}

#[cfg(feature = "storage")]
macro_rules! for_all_iter {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            StorageVariants2::Mem($pattern) => StorageVariants2::Mem($result),
            StorageVariants2::Disk($pattern) => StorageVariants2::Disk($result),
        }
    };
}

#[cfg(not(feature = "storage"))]
macro_rules! for_all_iter {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            StorageVariants2::Mem($pattern) => $result,
        }
    };
}

impl<'a, Mem: TPropOps<'a> + 'a, #[cfg(feature = "storage")] Disk: TPropOps<'a> + 'a> TPropOps<'a>
    for SelfType!(Mem, Disk)
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

impl<
        'a,
        Mem: TimeIndexOps<'a>,
        #[cfg(feature = "storage")] Disk: TimeIndexOps<'a, IndexType = Mem::IndexType>,
    > TimeIndexOps<'a> for SelfType!(Mem, Disk)
{
    type IndexType = Mem::IndexType;

    #[cfg(not(feature = "storage"))]
    type RangeType = Mem::RangeType;

    #[cfg(feature = "storage")]
    type RangeType = StorageVariants2<Mem::RangeType, Disk::RangeType>;

    fn active(&self, w: Range<Self::IndexType>) -> bool {
        for_all!(self, props => props.active(w))
    }

    fn range(&self, w: Range<Self::IndexType>) -> Self::RangeType {
        for_all_iter!(self, props => props.range(w))
    }

    fn first(&self) -> Option<Self::IndexType> {
        for_all!(self, props => props.first())
    }

    fn last(&self) -> Option<Self::IndexType> {
        for_all!(self, props => props.last())
    }

    fn iter(self) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'a {
        for_all_iter!(self, props => props.iter())
    }

    fn iter_rev(self) -> impl Iterator<Item = Self::IndexType> + Send + Sync + 'a {
        for_all_iter!(self, props => props.iter_rev())
    }

    fn len(&self) -> usize {
        for_all!(self, props => props.len())
    }
}
