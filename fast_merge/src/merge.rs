use crate::merge_impl::{KMergeBy, MergeBy, MergePredicate};
use std::iter::FusedIterator;

pub enum FastMerge<I: Iterator, F: MergePredicate<I::Item>> {
    Zero,
    One(I),
    Two(MergeBy<I, I, F>),
    Three(MergeBy<MergeBy<I, I, F>, I, F>),
    Four(MergeBy<MergeBy<I, I, F>, MergeBy<I, I, F>, F>),
    Five(MergeBy<MergeBy<MergeBy<I, I, F>, MergeBy<I, I, F>, F>, I, F>),
    Six(MergeBy<MergeBy<MergeBy<I, I, F>, MergeBy<I, I, F>, F>, MergeBy<I, I, F>, F>),
    Seven(
        MergeBy<MergeBy<MergeBy<I, I, F>, MergeBy<I, I, F>, F>, MergeBy<MergeBy<I, I, F>, I, F>, F>,
    ),
    Eight(
        MergeBy<
            MergeBy<MergeBy<I, I, F>, MergeBy<I, I, F>, F>,
            MergeBy<MergeBy<I, I, F>, MergeBy<I, I, F>, F>,
            F,
        >,
    ),
    Many(KMergeBy<I, F>),
}

impl<I: Iterator, P: MergePredicate<I::Item> + Clone> FastMerge<I, P> {
    pub(crate) fn new(mut iters: impl Iterator<Item = I>, predicate: P) -> Self {
        let (lower, _) = iters.size_hint();
        if lower > 8 {
            let mut kmerge = KMergeBy::new(lower, predicate);
            for iter in iters {
                kmerge.push(iter);
            }
            kmerge.heapify();
            return Self::Many(kmerge);
        }
        match iters.next() {
            None => return Self::Zero,
            Some(iter1) => match iters.next() {
                None => Self::One(iter1),
                Some(iter2) => match iters.next() {
                    None => Self::Two(MergeBy::new(iter1, iter2, predicate)),
                    Some(iter3) => match iters.next() {
                        None => Self::Three(MergeBy::new(
                            MergeBy::new(iter1, iter2, predicate.clone()),
                            iter3,
                            predicate,
                        )),
                        Some(iter4) => match iters.next() {
                            None => Self::Four(MergeBy::new(
                                MergeBy::new(iter1, iter2, predicate.clone()),
                                MergeBy::new(iter3, iter4, predicate.clone()),
                                predicate,
                            )),
                            Some(iter5) => match iters.next() {
                                None => Self::Five(MergeBy::new(
                                    MergeBy::new(
                                        MergeBy::new(iter1, iter2, predicate.clone()),
                                        MergeBy::new(iter3, iter4, predicate.clone()),
                                        predicate.clone(),
                                    ),
                                    iter5,
                                    predicate,
                                )),
                                Some(iter6) => match iters.next() {
                                    None => Self::Six(MergeBy::new(
                                        MergeBy::new(
                                            MergeBy::new(iter1, iter2, predicate.clone()),
                                            MergeBy::new(iter3, iter4, predicate.clone()),
                                            predicate.clone(),
                                        ),
                                        MergeBy::new(iter5, iter6, predicate.clone()),
                                        predicate,
                                    )),
                                    Some(iter7) => match iters.next() {
                                        None => Self::Seven(MergeBy::new(
                                            MergeBy::new(
                                                MergeBy::new(iter1, iter2, predicate.clone()),
                                                MergeBy::new(iter3, iter4, predicate.clone()),
                                                predicate.clone(),
                                            ),
                                            MergeBy::new(
                                                MergeBy::new(iter5, iter6, predicate.clone()),
                                                iter7,
                                                predicate.clone(),
                                            ),
                                            predicate.clone(),
                                        )),
                                        Some(iter8) => match iters.next() {
                                            None => Self::Eight(MergeBy::new(
                                                MergeBy::new(
                                                    MergeBy::new(iter1, iter2, predicate.clone()),
                                                    MergeBy::new(iter3, iter4, predicate.clone()),
                                                    predicate.clone(),
                                                ),
                                                MergeBy::new(
                                                    MergeBy::new(iter5, iter6, predicate.clone()),
                                                    MergeBy::new(iter7, iter8, predicate.clone()),
                                                    predicate.clone(),
                                                ),
                                                predicate.clone(),
                                            )),
                                            Some(iter9) => {
                                                let mut kmerge = KMergeBy::new(9, predicate);
                                                kmerge.push(iter1);
                                                kmerge.push(iter2);
                                                kmerge.push(iter3);
                                                kmerge.push(iter4);
                                                kmerge.push(iter5);
                                                kmerge.push(iter6);
                                                kmerge.push(iter7);
                                                kmerge.push(iter8);
                                                kmerge.push(iter9);
                                                for iter in iters {
                                                    kmerge.push(iter);
                                                }
                                                kmerge.heapify();
                                                Self::Many(kmerge)
                                            }
                                        },
                                    },
                                },
                            },
                        },
                    },
                },
            },
        }
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
            FastMerge::Five(iter) => iter.next(),
            FastMerge::Six(iter) => iter.next(),
            FastMerge::Seven(iter) => iter.next(),
            FastMerge::Eight(iter) => iter.next(),
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
            FastMerge::Five(iter) => iter.size_hint(),
            FastMerge::Six(iter) => iter.size_hint(),
            FastMerge::Seven(iter) => iter.size_hint(),
            FastMerge::Eight(iter) => iter.size_hint(),
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
            FastMerge::Five(iter) => iter.count(),
            FastMerge::Six(iter) => iter.count(),
            FastMerge::Seven(iter) => iter.count(),
            FastMerge::Eight(iter) => iter.count(),
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
            FastMerge::Five(iter) => iter.nth(n),
            FastMerge::Six(iter) => iter.nth(n),
            FastMerge::Seven(iter) => iter.nth(n),
            FastMerge::Eight(iter) => iter.nth(n),
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
            FastMerge::Five(iter) => iter.fold(init, f),
            FastMerge::Six(iter) => iter.fold(init, f),
            FastMerge::Seven(iter) => iter.fold(init, f),
            FastMerge::Eight(iter) => iter.fold(init, f),
            FastMerge::Many(iter) => iter.fold(init, f),
        }
    }
}

impl<I: Iterator<Item: Iterator>, F: MergePredicate<I::Item>> FusedIterator for FastMerge<I, F> {}
