use rayon::iter::{
    plumbing::{Consumer, ProducerCallback, UnindexedConsumer},
    IndexedParallelIterator, ParallelIterator,
};
use std::cmp::Ordering;

macro_rules! for_all {
    ($value:expr, $pattern:pat => $result:expr) => {
        match $value {
            FilterVariants::Neither($pattern) => $result,
            FilterVariants::Nodes($pattern) => $result,
            FilterVariants::Edges($pattern) => $result,
            FilterVariants::Both($pattern) => $result,
        }
    };
}

pub enum FilterVariants<Neither, Nodes, Edges, Both> {
    Neither(Neither),
    Nodes(Nodes),
    Edges(Edges),
    Both(Both),
}

impl<
        V,
        Neither: Iterator<Item = V>,
        Nodes: Iterator<Item = V>,
        Edges: Iterator<Item = V>,
        Both: Iterator<Item = V>,
    > Iterator for FilterVariants<Neither, Nodes, Edges, Both>
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
        Neither: DoubleEndedIterator<Item = V>,
        Nodes: DoubleEndedIterator<Item = V>,
        Edges: DoubleEndedIterator<Item = V>,
        Both: DoubleEndedIterator<Item = V>,
    > DoubleEndedIterator for FilterVariants<Neither, Nodes, Edges, Both>
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
        Neither: ExactSizeIterator<Item = V>,
        Nodes: ExactSizeIterator<Item = V>,
        Edges: ExactSizeIterator<Item = V>,
        Both: ExactSizeIterator<Item = V>,
    > ExactSizeIterator for FilterVariants<Neither, Nodes, Edges, Both>
{
    fn len(&self) -> usize {
        for_all!(self, iter => iter.len())
    }
}

impl<
        V: Send,
        Neither: ParallelIterator<Item = V>,
        Nodes: ParallelIterator<Item = V>,
        Edges: ParallelIterator<Item = V>,
        Both: ParallelIterator<Item = V>,
    > ParallelIterator for FilterVariants<Neither, Nodes, Edges, Both>
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
        Neither: IndexedParallelIterator<Item = V>,
        Nodes: IndexedParallelIterator<Item = V>,
        Edges: IndexedParallelIterator<Item = V>,
        Both: IndexedParallelIterator<Item = V>,
    > IndexedParallelIterator for FilterVariants<Neither, Nodes, Edges, Both>
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
