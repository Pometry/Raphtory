use crate::merge_impl::{KMergeBy, MergeBy, MergeByGe, MergeByLt, MergeByRev, MergePredicate};
use std::iter::{FusedIterator, Rev};

pub trait FastMergeExt: Iterator {
    /// Return an iterator adaptor that flattens an iterator of iterators by
    /// merging them according to the given closure. Uses tree merge for up to 8 iterators.
    ///
    /// The closure `first` is called with two elements *a*, *b* and should
    /// return `true` if *a* is ordered before *b*.
    ///
    /// If all base iterators are sorted according to `first`, the result is
    /// sorted.
    ///
    /// Iterator element type is `Self::Item`.
    fn fast_merge_by<
        F: FnMut(&<Self::Item as IntoIterator>::Item, &<Self::Item as IntoIterator>::Item) -> bool,
    >(
        self,
        cmp_fn: F,
    ) -> FastMerge<<Self::Item as IntoIterator>::IntoIter, F>
    where
        Self: Sized,
        Self::Item: IntoIterator,
    {
        FastMerge::new(self.map(|i| i.into_iter()), cmp_fn)
    }

    /// Return an iterator adaptor that flattens an iterator of iterators by
    /// merging them in ascending order. Uses tree merge for up to 8 iterators.
    ///
    /// If all base iterators are sorted (ascending), the result is sorted.
    ///
    /// Iterator element type is `Self::Item`.
    fn fast_merge(self) -> FastMerge<<Self::Item as IntoIterator>::IntoIter, MergeByLt>
    where
        Self: Sized,
        Self::Item: IntoIterator,
        <Self::Item as IntoIterator>::Item: Ord,
    {
        FastMerge::new(self.map(|i| i.into_iter()), MergeByLt)
    }

    /// Return an iterator adaptor that flattens an iterator of iterators by
    /// merging them in reverse according to the given closure. Uses tree merge for up to 8 iterators.
    ///
    /// The closure `first` is called with two elements *a*, *b* and should
    /// return `true` if *a* is ordered before *b*.
    ///
    /// If all base iterators are sorted ascending according to `first`, the result is
    /// sorted descending according to `first`.
    ///
    /// Iterator element type is `Self::Item`.
    fn fast_merge_by_rev<
        F: FnMut(&<Self::Item as IntoIterator>::Item, &<Self::Item as IntoIterator>::Item) -> bool,
    >(
        self,
        first: F,
    ) -> FastMerge<Rev<<Self::Item as IntoIterator>::IntoIter>, MergeByRev<F>>
    where
        Self: Sized,
        Self::Item: IntoIterator,
        <Self::Item as IntoIterator>::IntoIter: DoubleEndedIterator,
    {
        FastMerge::new(self.map(|iter| iter.into_iter().rev()), MergeByRev(first))
    }

    /// Return an iterator adaptor that flattens an iterator of iterators by
    /// merging and reversing them. Uses tree merge for up to 8 iterators. Uses tree merge for up to 8 iterators.
    ///
    /// If all base iterators are sorted ascending, the result is sorted descending.
    ///
    /// Iterator element type is `Self::Item`.
    fn fast_merge_rev(self) -> FastMerge<Rev<<Self::Item as IntoIterator>::IntoIter>, MergeByGe>
    where
        Self: Sized,
        Self::Item: IntoIterator,
        <Self::Item as IntoIterator>::Item: Ord,
        <Self::Item as IntoIterator>::IntoIter: DoubleEndedIterator,
    {
        FastMerge::new(self.map(|iter| iter.into_iter().rev()), MergeByGe)
    }
}

impl<I: Iterator<Item: IntoIterator>> FastMergeExt for I {}

#[must_use = "this iterator adaptor is not lazy but does nearly nothing unless consumed"]
pub enum FastMerge<I: Iterator, F: MergePredicate<I::Item>> {
    Zero,
    One(I),
    Two(MergeBy<I, F>),
    Many(KMergeBy<I, F>),
}

impl<I: Iterator, P: MergePredicate<I::Item>> FastMerge<I, P> {
    pub(crate) fn new(mut iters: impl Iterator<Item = I>, predicate: P) -> Self {
        let (lower, _) = iters.size_hint();
        if lower > 2 {
            let mut kmerge = KMergeBy::new(lower, predicate);
            for iter in iters {
                kmerge.push(iter);
            }
            kmerge.heapify();
            return Self::Many(kmerge);
        }
        match iters.next() {
            None => Self::Zero,
            Some(iter1) => match iters.next() {
                None => Self::One(iter1),
                Some(iter2) => match iters.next() {
                    None => Self::Two(MergeBy::new(iter1, iter2, predicate)),
                    Some(iter3) => {
                        let mut kmerge = KMergeBy::new(3, predicate);
                        kmerge.push(iter1);
                        kmerge.push(iter2);
                        kmerge.push(iter3);
                        for iter in iters {
                            kmerge.push(iter);
                        }
                        kmerge.heapify();
                        Self::Many(kmerge)
                    }
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
            FastMerge::Many(iter) => iter.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            FastMerge::Zero => (0, Some(0)),
            FastMerge::One(iter) => iter.size_hint(),
            FastMerge::Two(iter) => iter.size_hint(),
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
            FastMerge::Many(iter) => iter.count(),
        }
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        match self {
            FastMerge::Zero => None,
            FastMerge::One(iter) => iter.nth(n),
            FastMerge::Two(iter) => iter.nth(n),
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
            FastMerge::Many(iter) => iter.fold(init, f),
        }
    }
}

impl<I: Iterator<Item: Iterator>, F: MergePredicate<I::Item>> FusedIterator for FastMerge<I, F> {}

#[cfg(test)]
mod tests {
    use crate::merge::FastMergeExt;
    use proptest::{arbitrary::any, prelude::*, proptest};

    #[test]
    fn test_merge_proptest() {
        proptest!(|(input in any::<Vec<Vec<usize>>>().prop_map(|mut input| {
                        for inner in input.iter_mut() {
                inner.sort();
            }
            input
        }))| {
            let mut expected: Vec<_> = input.iter().flatten().copied().collect();
            expected.sort();
            let actual: Vec<_> = input.into_iter().fast_merge().collect();

            assert_eq!(actual, expected);

        })
    }

    #[test]
    fn test_reverse_proptest() {
        proptest!(|(input in any::<Vec<Vec<usize>>>().prop_map(|mut input| {
                        for inner in input.iter_mut() {
                inner.sort();
            }
            input
        }))| {
            let mut expected: Vec<_> = input.iter().flatten().copied().collect();
            expected.sort();
            expected.reverse();
            let actual: Vec<_> = input.into_iter().fast_merge_rev().collect();

            assert_eq!(actual, expected);

        })
    }

    #[test]
    fn test_custom_merge_fn_proptest() {
        proptest!(|(input in any::<Vec<Vec<usize>>>().prop_map(|mut input| {
                        for inner in input.iter_mut() {
                inner.sort();
            }
            input
        }))| {
            let mut expected: Vec<_> = input.iter().flatten().copied().collect();
            expected.sort();
            let actual: Vec<_> = input.into_iter().fast_merge_by(|a, b| a < b).collect();

            assert_eq!(actual, expected);

        })
    }

    #[test]
    fn test_custom_merge_fn_rev_proptest() {
        proptest!(|(input in any::<Vec<Vec<usize>>>().prop_map(|mut input| {
                        for inner in input.iter_mut() {
                inner.sort();
            }
            input
        }))| {
            let mut expected: Vec<_> = input.iter().flatten().copied().collect();
            expected.sort();
            expected.reverse();
            let actual: Vec<_> = input.into_iter().fast_merge_by_rev(|a, b| a < b).collect();

            assert_eq!(actual, expected);

        })
    }

    #[test]
    fn test_mostly_empty() {
        let input = [vec![], vec![], vec![], vec![], vec![0usize]];
        let res: Vec<_> = input.into_iter().fast_merge().collect();
        assert_eq!(res, [0]);
    }
}
