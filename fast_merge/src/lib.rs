use crate::{
    merge::FastMerge,
    merge_impl::{MergeByGe, MergeByLt, MergeByRev, MergePredicate},
};
use std::iter::Rev;

pub(crate) mod merge;
pub(crate) mod merge_impl;

pub trait FastMergeExt: Iterator<Item: IntoIterator> + Sized {
    fn fast_merge_by<
        F: FnMut(&<Self::Item as IntoIterator>::Item, &<Self::Item as IntoIterator>::Item) -> bool
            + Clone,
    >(
        self,
        cmp_fn: F,
    ) -> FastMerge<<Self::Item as IntoIterator>::IntoIter, F> {
        FastMerge::new(self.map(|i| i.into_iter()), cmp_fn)
    }

    fn fast_merge(self) -> FastMerge<<Self::Item as IntoIterator>::IntoIter, MergeByLt>
    where
        <Self::Item as IntoIterator>::Item: Ord,
    {
        FastMerge::new(self.map(|i| i.into_iter()), MergeByLt)
    }

    fn fast_merge_by_rev<
        F: FnMut(&<Self::Item as IntoIterator>::Item, &<Self::Item as IntoIterator>::Item) -> bool
            + Clone,
    >(
        self,
        cmp_fn: F,
    ) -> FastMerge<Rev<<Self::Item as IntoIterator>::IntoIter>, MergeByRev<F>>
    where
        <Self::Item as IntoIterator>::IntoIter: DoubleEndedIterator,
    {
        FastMerge::new(self.map(|iter| iter.into_iter().rev()), MergeByRev(cmp_fn))
    }

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
    use itertools::Itertools;
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
