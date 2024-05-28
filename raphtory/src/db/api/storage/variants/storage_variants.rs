use crate::{core::Prop, db::api::storage::tprop_storage_ops::TPropOps};
use rayon::iter::{
    plumbing::{Consumer, ProducerCallback, UnindexedConsumer},
    IndexedParallelIterator, ParallelIterator,
};
use std::{cmp::Ordering, ops::Range};
use raphtory_api::core::storage::timeindex::TimeIndexEntry;

#[derive(Copy, Clone, Debug)]
pub enum StorageVariants<Mem, #[cfg(feature = "arrow")] Arrow> {
    Mem(Mem),
    #[cfg(feature = "arrow")]
    Arrow(Arrow),
}

#[cfg(feature = "arrow")]
macro_rules! SelfType {
    ($Mem:ident, $Arrow:ident) => {
        StorageVariants<$Mem, $Arrow>
    };
}

#[cfg(not(feature = "arrow"))]
macro_rules! SelfType {
    ($Mem:ident, $Arrow:ident) => {
        StorageVariants<$Mem>
    };
}

macro_rules! for_all {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            StorageVariants::Mem($pattern) => $result,
            #[cfg(feature = "arrow")]
            StorageVariants::Arrow($pattern) => $result,
        }
    };
}

#[cfg(feature = "arrow")]
macro_rules! for_all_iter {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            StorageVariants::Mem($pattern) => StorageVariants::Mem($result),
            StorageVariants::Arrow($pattern) => StorageVariants::Arrow($result),
        }
    };
}

#[cfg(not(feature = "arrow"))]
macro_rules! for_all_iter {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            StorageVariants::Mem($pattern) => $result,
        }
    };
}

impl<V, Mem: Iterator<Item = V>, #[cfg(feature = "arrow")] Arrow: Iterator<Item = V>> Iterator
    for SelfType!(Mem, Arrow)
{
    type Item = V;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        for_all!(self, iter => iter.next())
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        for_all!(self, iter => iter.size_hint())
    }

    #[inline]
    fn count(self) -> usize
    where
        Self: Sized,
    {
        for_all!(self, iter => iter.count())
    }

    #[inline]
    fn last(self) -> Option<Self::Item>
    where
        Self: Sized,
    {
        for_all!(self, iter => iter.last())
    }

    #[inline]
    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        for_all!(self, iter => iter.nth(n))
    }

    #[inline]
    fn fold<B, F>(self, init: B, f: F) -> B
    where
        Self: Sized,
        F: FnMut(B, Self::Item) -> B,
    {
        for_all!(self, iter => iter.fold(init, f))
    }

    #[inline]
    fn find<P>(&mut self, predicate: P) -> Option<Self::Item>
    where
        Self: Sized,
        P: FnMut(&Self::Item) -> bool,
    {
        for_all!(self, iter => iter.find(predicate))
    }

    fn find_map<B, F>(&mut self, f: F) -> Option<B>
    where
        Self: Sized,
        F: FnMut(Self::Item) -> Option<B>,
    {
        for_all!(self, iter => iter.find_map(f))
    }

    fn position<P>(&mut self, predicate: P) -> Option<usize>
    where
        Self: Sized,
        P: FnMut(Self::Item) -> bool,
    {
        for_all!(self, iter => iter.position(predicate))
    }

    fn max(self) -> Option<Self::Item>
    where
        Self: Sized,
        Self::Item: Ord,
    {
        for_all!(self, iter => iter.max())
    }

    fn min(self) -> Option<Self::Item>
    where
        Self: Sized,
        Self::Item: Ord,
    {
        for_all!(self, iter => iter.min())
    }

    fn max_by_key<B: Ord, F>(self, f: F) -> Option<Self::Item>
    where
        Self: Sized,
        F: FnMut(&Self::Item) -> B,
    {
        for_all!(self, iter => iter.max_by_key(f))
    }

    fn max_by<F>(self, compare: F) -> Option<Self::Item>
    where
        Self: Sized,
        F: FnMut(&Self::Item, &Self::Item) -> Ordering,
    {
        for_all!(self, iter => iter.max_by(compare))
    }

    fn min_by_key<B: Ord, F>(self, f: F) -> Option<Self::Item>
    where
        Self: Sized,
        F: FnMut(&Self::Item) -> B,
    {
        for_all!(self, iter => iter.min_by_key(f))
    }

    fn min_by<F>(self, compare: F) -> Option<Self::Item>
    where
        Self: Sized,
        F: FnMut(&Self::Item, &Self::Item) -> Ordering,
    {
        for_all!(self, iter => iter.min_by(compare))
    }
}

impl<
        V,
        Mem: DoubleEndedIterator<Item = V>,
        #[cfg(feature = "arrow")] Arrow: DoubleEndedIterator<Item = V>,
    > DoubleEndedIterator for SelfType!(Mem, Arrow)
{
    fn next_back(&mut self) -> Option<Self::Item> {
        for_all!(self, iter => iter.next_back())
    }

    fn nth_back(&mut self, n: usize) -> Option<Self::Item> {
        for_all!(self, iter => iter.nth_back(n))
    }

    fn rfold<B, F>(self, init: B, f: F) -> B
    where
        Self: Sized,
        F: FnMut(B, Self::Item) -> B,
    {
        for_all!(self, iter => iter.rfold(init, f))
    }

    fn rfind<P>(&mut self, predicate: P) -> Option<Self::Item>
    where
        Self: Sized,
        P: FnMut(&Self::Item) -> bool,
    {
        for_all!(self, iter => iter.rfind(predicate))
    }
}

impl<
        V,
        Mem: ExactSizeIterator<Item = V>,
        #[cfg(feature = "arrow")] Arrow: ExactSizeIterator<Item = V>,
    > ExactSizeIterator for SelfType!(Mem, Arrow)
{
    fn len(&self) -> usize {
        for_all!(self, iter => iter.len())
    }
}

impl<
        V: Send,
        Mem: ParallelIterator<Item = V>,
        #[cfg(feature = "arrow")] Arrow: ParallelIterator<Item = V>,
    > ParallelIterator for SelfType!(Mem, Arrow)
{
    type Item = V;

    fn drive_unindexed<C>(self, consumer: C) -> C::Result
    where
        C: UnindexedConsumer<Self::Item>,
    {
        for_all!(self, iter => iter.drive_unindexed(consumer))
    }

    fn opt_len(&self) -> Option<usize> {
        for_all!(self, iter => iter.opt_len())
    }
}

impl<
        V: Send,
        Mem: IndexedParallelIterator<Item = V>,
        #[cfg(feature = "arrow")] Arrow: IndexedParallelIterator<Item = V>,
    > IndexedParallelIterator for SelfType!(Mem, Arrow)
{
    fn len(&self) -> usize {
        for_all!(self, iter => iter.len())
    }

    fn drive<C: Consumer<Self::Item>>(self, consumer: C) -> C::Result {
        for_all!(self, iter => iter.drive(consumer))
    }

    fn with_producer<CB: ProducerCallback<Self::Item>>(self, callback: CB) -> CB::Output {
        for_all!(self, iter => iter.with_producer(callback))
    }
}

impl<'a, Mem: TPropOps<'a> + 'a, #[cfg(feature = "arrow")] Arrow: TPropOps<'a> + 'a> TPropOps<'a>
    for SelfType!(Mem, Arrow)
{
    fn last_before(self, t: i64) -> Option<(TimeIndexEntry, Prop)> {
        for_all!(self, props => props.last_before(t))
    }

    fn iter(self) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + 'a {
        for_all_iter!(self, props => props.iter())
    }

    fn iter_window(
        self,
        r: Range<TimeIndexEntry>,
    ) -> impl Iterator<Item = (TimeIndexEntry, Prop)> + Send + 'a {
        for_all_iter!(self, props => props.iter_window(r))
    }

    fn at(self, ti: &TimeIndexEntry) -> Option<Prop> {
        for_all!(self, props => props.at(ti))
    }

    fn len(self) -> usize {
        for_all!(self, props=> props.len())
    }

    fn is_empty(self) -> bool {
        for_all!(self, props => props.is_empty())
    }
}
