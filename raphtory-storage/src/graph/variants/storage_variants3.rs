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
pub enum StorageVariants<Mem, Unlocked, #[cfg(feature = "storage")] Disk> {
    Mem(Mem),
    Unlocked(Unlocked),
    #[cfg(feature = "storage")]
    Disk(Disk),
}

#[cfg(feature = "storage")]
macro_rules! SelfType {
    ($Mem:ident, $Unlocked:ident, $Disk:ident) => {
        StorageVariants<$Mem, $Unlocked, $Disk>
    };
}

#[cfg(not(feature = "storage"))]
macro_rules! SelfType {
    ($Mem:ident, $Unlocked:ident, $Disk:ident) => {
        StorageVariants<$Mem, $Unlocked>
    };
}

macro_rules! for_all {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            StorageVariants::Mem($pattern) => $result,
            StorageVariants::Unlocked($pattern) => $result,
            #[cfg(feature = "storage")]
            StorageVariants::Disk($pattern) => $result,
        }
    };
}

macro_rules! for_all_iter {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            StorageVariants::Mem($pattern) => StorageVariants::Mem($result),
            StorageVariants::Unlocked($pattern) => StorageVariants::Unlocked($result),
            #[cfg(feature = "storage")]
            StorageVariants::Disk($pattern) => StorageVariants::Disk($result),
        }
    };
}

impl<
        'a,
        Mem: TPropOps<'a> + 'a,
        Unlocked: TPropOps<'a> + 'a,
        #[cfg(feature = "storage")] Disk: TPropOps<'a> + 'a,
    > TPropOps<'a> for SelfType!(Mem, Unlocked, Disk)
{
    fn last_before(&self, t: TimeIndexEntry) -> Option<(TimeIndexEntry, Prop)> {
        for_all!(self, props => props.last_before(t))
    }

    fn iter(self) -> impl DoubleEndedIterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'a {
        for_all_iter!(self, props => props.iter())
    }

    fn iter_window(
        self,
        r: Range<TimeIndexEntry>,
    ) -> impl DoubleEndedIterator<Item = (TimeIndexEntry, Prop)> + Send + Sync + 'a {
        for_all_iter!(self, props => props.iter_window(r))
    }

    fn at(&self, ti: &TimeIndexEntry) -> Option<Prop> {
        for_all!(self, props => props.at(ti))
    }
}
