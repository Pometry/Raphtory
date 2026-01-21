use crate::merge_impl::{KMergeBy, MergeBy, MergePredicate};
use std::iter::FusedIterator;

pub enum FastMerge<I: Iterator, F: MergePredicate<I::Item>> {
    Zero,
    One(I),
    Two(MergeBy<I, I, F>),
    Three(MergeBy<MergeBy<I, I, F>, I, F>),
    Four(MergeBy<MergeBy<I, I, F>, MergeBy<I, I, F>, F>),
    Many(KMergeBy<I, F>),
}

impl<I: Iterator, P: MergePredicate<I::Item> + Clone> FastMerge<I, P> {
    pub(crate) fn new(iters: impl Iterator<Item = I>, predicate: P) -> Self {
        let mut merged = Self::Zero;
        for iter in iters.map(|iter| iter.into_iter()) {
            match merged {
                FastMerge::Zero => {
                    merged = Self::One(iter);
                }
                FastMerge::One(old_iter) => {
                    merged = Self::Two(MergeBy::new(old_iter, iter, predicate.clone()))
                }
                FastMerge::Two(old_iter) => {
                    merged = Self::Three(MergeBy::new(old_iter, iter, predicate.clone()))
                }
                FastMerge::Three(old_iter) => {
                    let (left, right, _) = old_iter.into_inner();
                    merged = Self::Four(MergeBy::new(
                        left,
                        MergeBy::new(right, iter, predicate.clone()),
                        predicate.clone(),
                    ));
                }
                FastMerge::Four(old_iter) => {
                    let (left, right, _) = old_iter.into_inner();
                    let (left_left, left_right, _) = left.into_inner();
                    let (right_left, right_right, _) = right.into_inner();
                    let mut kmerge = KMergeBy::new(iter.size_hint().0 + 5, predicate.clone());
                    kmerge.push(left_left);
                    kmerge.push(left_right);
                    kmerge.push(right_left);
                    kmerge.push(right_right);
                    kmerge.push(iter);
                    merged = Self::Many(kmerge);
                }
                FastMerge::Many(mut kmerge) => {
                    kmerge.push(iter);
                    merged = Self::Many(kmerge);
                }
            }
        }
        match &mut merged {
            FastMerge::Many(kmerge) => {
                kmerge.heapify();
            }
            _ => {}
        }
        merged
    }
}

impl<I: Iterator, P: MergePredicate<I::Item>> Iterator for FastMerge<I, P> {
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            FastMerge::Zero => None,
            FastMerge::One(iter) => iter.next(),
            FastMerge::Two(iter) => iter.next(),
            FastMerge::Three(iter) => iter.next(),
            FastMerge::Four(iter) => iter.next(),
            FastMerge::Many(iter) => iter.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            FastMerge::Zero => (0, Some(0)),
            FastMerge::One(iter) => iter.size_hint(),
            FastMerge::Two(iter) => iter.size_hint(),
            FastMerge::Three(iter) => iter.size_hint(),
            FastMerge::Four(iter) => iter.size_hint(),
            FastMerge::Many(iter) => iter.size_hint(),
        }
    }

    fn count(self) -> usize
    where
        Self: Sized,
    {
        match self {
            FastMerge::Zero => 0,
            FastMerge::One(iter) => iter.count(),
            FastMerge::Two(iter) => iter.count(),
            FastMerge::Three(iter) => iter.count(),
            FastMerge::Four(iter) => iter.count(),
            FastMerge::Many(iter) => iter.count(),
        }
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        match self {
            FastMerge::Zero => None,
            FastMerge::One(iter) => iter.nth(n),
            FastMerge::Two(iter) => iter.nth(n),
            FastMerge::Three(iter) => iter.nth(n),
            FastMerge::Four(iter) => iter.nth(n),
            FastMerge::Many(iter) => iter.nth(n),
        }
    }

    fn fold<B, F>(self, init: B, f: F) -> B
    where
        Self: Sized,
        F: FnMut(B, Self::Item) -> B,
    {
        match self {
            FastMerge::Zero => init,
            FastMerge::One(iter) => iter.fold(init, f),
            FastMerge::Two(iter) => iter.fold(init, f),
            FastMerge::Three(iter) => iter.fold(init, f),
            FastMerge::Four(iter) => iter.fold(init, f),
            FastMerge::Many(iter) => iter.fold(init, f),
        }
    }
}

impl<I: Iterator<Item: Iterator>, F: MergePredicate<I::Item>> FusedIterator for FastMerge<I, F> {}
