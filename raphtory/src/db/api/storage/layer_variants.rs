use rayon::{
    iter::plumbing::{Consumer, ProducerCallback, UnindexedConsumer},
    prelude::{IndexedParallelIterator, ParallelIterator},
};
use std::cmp::Ordering;

macro_rules! for_all {
    ($value:expr, $pattern:pat => $result:expr, $none:expr) => {
        match $value {
            LayerVariants::None => $none,
            LayerVariants::All($pattern) => $result,
            LayerVariants::One($pattern) => $result,
            LayerVariants::Multiple($pattern) => $result,
        }
    };
}

pub enum LayerVariants<All, One, Multiple> {
    None,
    All(All),
    One(One),
    Multiple(Multiple),
}

impl<V, All: Iterator<Item = V>, One: Iterator<Item = V>, Multiple: Iterator<Item = V>> Iterator
    for LayerVariants<All, One, Multiple>
{
    type Item = V;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        for_all!(self, iter => iter.next(), None)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        for_all!(self, iter => iter.size_hint(), (0, Some(0)))
    }

    #[inline]
    fn count(self) -> usize
    where
        Self: Sized,
    {
        for_all!(self, iter => iter.count(), 0)
    }

    #[inline]
    fn last(self) -> Option<Self::Item>
    where
        Self: Sized,
    {
        for_all!(self, iter => iter.last(), None)
    }

    #[inline]
    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        for_all!(self, iter => iter.nth(n), None)
    }

    #[inline]
    fn fold<B, F>(self, init: B, f: F) -> B
    where
        Self: Sized,
        F: FnMut(B, Self::Item) -> B,
    {
        for_all!(self, iter => iter.fold(init, f), init)
    }

    #[inline]
    fn find<P>(&mut self, predicate: P) -> Option<Self::Item>
    where
        Self: Sized,
        P: FnMut(&Self::Item) -> bool,
    {
        for_all!(self, iter => iter.find(predicate), None)
    }

    fn find_map<B, F>(&mut self, f: F) -> Option<B>
    where
        Self: Sized,
        F: FnMut(Self::Item) -> Option<B>,
    {
        for_all!(self, iter => iter.find_map(f), None)
    }

    fn position<P>(&mut self, predicate: P) -> Option<usize>
    where
        Self: Sized,
        P: FnMut(Self::Item) -> bool,
    {
        for_all!(self, iter => iter.position(predicate), None)
    }

    fn max(self) -> Option<Self::Item>
    where
        Self: Sized,
        Self::Item: Ord,
    {
        for_all!(self, iter => iter.max(), None)
    }

    fn min(self) -> Option<Self::Item>
    where
        Self: Sized,
        Self::Item: Ord,
    {
        for_all!(self, iter => iter.min(), None)
    }

    fn max_by_key<B: Ord, F>(self, f: F) -> Option<Self::Item>
    where
        Self: Sized,
        F: FnMut(&Self::Item) -> B,
    {
        for_all!(self, iter => iter.max_by_key(f), None)
    }

    fn max_by<F>(self, compare: F) -> Option<Self::Item>
    where
        Self: Sized,
        F: FnMut(&Self::Item, &Self::Item) -> Ordering,
    {
        for_all!(self, iter => iter.max_by(compare), None)
    }

    fn min_by_key<B: Ord, F>(self, f: F) -> Option<Self::Item>
    where
        Self: Sized,
        F: FnMut(&Self::Item) -> B,
    {
        for_all!(self, iter => iter.min_by_key(f), None)
    }

    fn min_by<F>(self, compare: F) -> Option<Self::Item>
    where
        Self: Sized,
        F: FnMut(&Self::Item, &Self::Item) -> Ordering,
    {
        for_all!(self, iter => iter.min_by(compare), None)
    }
}

impl<
        V,
        All: DoubleEndedIterator<Item = V>,
        One: DoubleEndedIterator<Item = V>,
        Multiple: DoubleEndedIterator<Item = V>,
    > DoubleEndedIterator for LayerVariants<All, One, Multiple>
{
    fn next_back(&mut self) -> Option<Self::Item> {
        for_all!(self, iter => iter.next_back(), None)
    }

    fn nth_back(&mut self, n: usize) -> Option<Self::Item> {
        for_all!(self, iter => iter.nth_back(n), None)
    }

    fn rfold<B, F>(self, init: B, f: F) -> B
    where
        Self: Sized,
        F: FnMut(B, Self::Item) -> B,
    {
        for_all!(self, iter => iter.rfold(init, f), init)
    }

    fn rfind<P>(&mut self, predicate: P) -> Option<Self::Item>
    where
        Self: Sized,
        P: FnMut(&Self::Item) -> bool,
    {
        for_all!(self, iter => iter.rfind(predicate), None)
    }
}

impl<
        V,
        All: ExactSizeIterator<Item = V>,
        One: ExactSizeIterator<Item = V>,
        Multiple: ExactSizeIterator<Item = V>,
    > ExactSizeIterator for LayerVariants<All, One, Multiple>
{
    fn len(&self) -> usize {
        for_all!(self, iter => iter.len(), 0)
    }
}

impl<
        V: Send,
        All: ParallelIterator<Item = V>,
        One: ParallelIterator<Item = V>,
        Multiple: ParallelIterator<Item = V>,
    > ParallelIterator for LayerVariants<All, One, Multiple>
{
    type Item = V;

    fn drive_unindexed<C>(self, consumer: C) -> C::Result
    where
        C: UnindexedConsumer<Self::Item>,
    {
        for_all!(self, iter => iter.drive_unindexed(consumer), rayon::iter::empty().drive_unindexed(consumer))
    }

    fn opt_len(&self) -> Option<usize> {
        for_all!(self, iter => iter.opt_len(), Some(0))
    }
}

impl<
        V: Send,
        All: IndexedParallelIterator<Item = V>,
        One: IndexedParallelIterator<Item = V>,
        Multiple: IndexedParallelIterator<Item = V>,
    > IndexedParallelIterator for LayerVariants<All, One, Multiple>
{
    fn len(&self) -> usize {
        for_all!(self, iter => iter.len(), 0)
    }

    fn drive<C: Consumer<Self::Item>>(self, consumer: C) -> C::Result {
        for_all!(self, iter => iter.drive(consumer), rayon::iter::empty().drive(consumer))
    }

    fn with_producer<CB: ProducerCallback<Self::Item>>(self, callback: CB) -> CB::Output {
        for_all!(self, iter => iter.with_producer(callback), rayon::iter::empty().with_producer(callback))
    }
}
