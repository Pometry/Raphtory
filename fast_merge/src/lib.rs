use crate::{
    merge::FastMerge,
    merge_impl::{MergeByGe, MergeByLt, MergeByRev},
};
use std::iter::Rev;

pub(crate) mod merge;
pub(crate) mod merge_impl;

pub trait FastMergeExt: Iterator<Item: IntoIterator> + Sized {
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
        F: FnMut(&<Self::Item as IntoIterator>::Item, &<Self::Item as IntoIterator>::Item) -> bool
            + Clone,
    >(
        self,
        cmp_fn: F,
    ) -> FastMerge<<Self::Item as IntoIterator>::IntoIter, F> {
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
        F: FnMut(&<Self::Item as IntoIterator>::Item, &<Self::Item as IntoIterator>::Item) -> bool
            + Clone,
    >(
        self,
        first: F,
    ) -> FastMerge<Rev<<Self::Item as IntoIterator>::IntoIter>, MergeByRev<F>>
    where
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
        <Self::Item as IntoIterator>::Item: Ord,
        <Self::Item as IntoIterator>::IntoIter: DoubleEndedIterator,
    {
        FastMerge::new(self.map(|iter| iter.into_iter().rev()), MergeByGe)
    }
}

impl<I: Iterator<Item: IntoIterator>> FastMergeExt for I {}

#[cfg(test)]
mod tests {
    use crate::FastMergeExt;
    use proptest::{arbitrary::any, prelude::*, proptest};

    #[test]
    fn test_merge() {
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
    fn test_reverse() {
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
    fn test_custom_merge_fn() {
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
    fn test_custom_merge_fn_rev() {
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
