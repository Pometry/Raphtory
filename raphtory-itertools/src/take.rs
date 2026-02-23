use std::{cmp, iter::FusedIterator};

/// An iterator that only iterates over the first `n` iterations of `iter`.
///
/// This `struct` is created by the [`takeable`] method on [`Iterator`]. See its
/// documentation for more.
///
/// [`take`]: Iterator::take
/// [`Iterator`]: trait.Iterator.html
#[derive(Clone, Debug)]
#[must_use = "iterators are lazy and do nothing unless consumed"]
pub struct ReTake<I> {
    iter: I,
    n: usize,
}

pub trait TakeExt: Sized {
    fn take_updatable(self, n: usize) -> ReTake<Self>;
}

impl<I: Iterator> TakeExt for I {
    fn take_updatable(self, n: usize) -> ReTake<Self> {
        ReTake { iter: self, n }
    }
}

impl<I: Iterator> ReTake<I> {
    /// Take the first n elements of the iterator by updating the current take
    pub fn take_inplace(&mut self, n: usize) {
        self.n = self.n.min(n);
    }

    /// Advance the iterator by n elements
    pub fn advance_by(&mut self, n: usize) {
        if let Some(to_skip) = n.min(self.n).checked_sub(1) {
            self.iter.nth(to_skip);
        }
        self.n = self.n.saturating_sub(n);
    }
}

impl<I> Iterator for ReTake<I>
where
    I: Iterator,
{
    type Item = <I as Iterator>::Item;

    #[inline]
    fn next(&mut self) -> Option<<I as Iterator>::Item> {
        if self.n != 0 {
            self.n -= 1;
            self.iter.next()
        } else {
            None
        }
    }

    #[inline]
    fn nth(&mut self, n: usize) -> Option<I::Item> {
        if self.n > n {
            self.n -= n + 1;
            self.iter.nth(n)
        } else {
            if self.n > 0 {
                self.iter.nth(self.n - 1);
                self.n = 0;
            }
            None
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        if self.n == 0 {
            return (0, Some(0));
        }

        let (lower, upper) = self.iter.size_hint();

        let lower = cmp::min(lower, self.n);

        let upper = match upper {
            Some(x) if x < self.n => Some(x),
            _ => Some(self.n),
        };

        (lower, upper)
    }
}

impl<I> DoubleEndedIterator for ReTake<I>
where
    I: DoubleEndedIterator + ExactSizeIterator,
{
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.n == 0 {
            None
        } else {
            let n = self.n;
            self.n -= 1;
            self.iter.nth_back(self.iter.len().saturating_sub(n))
        }
    }

    #[inline]
    fn nth_back(&mut self, n: usize) -> Option<Self::Item> {
        let len = self.iter.len();
        if self.n > n {
            let m = len.saturating_sub(self.n) + n;
            self.n -= n + 1;
            self.iter.nth_back(m)
        } else {
            if len > 0 {
                self.iter.nth_back(len - 1);
            }
            None
        }
    }

    #[inline]
    fn rfold<Acc, Fold>(mut self, init: Acc, fold: Fold) -> Acc
    where
        Self: Sized,
        Fold: FnMut(Acc, Self::Item) -> Acc,
    {
        if self.n == 0 {
            init
        } else {
            let len = self.iter.len();
            if len > self.n && self.iter.nth_back(len - self.n - 1).is_none() {
                init
            } else {
                self.iter.rfold(init, fold)
            }
        }
    }
}

impl<I> ExactSizeIterator for ReTake<I> where I: ExactSizeIterator {}

impl<I> FusedIterator for ReTake<I> where I: FusedIterator {}

trait SpecTake: Iterator {
    fn spec_fold<B, F>(self, init: B, f: F) -> B
    where
        Self: Sized,
        F: FnMut(B, Self::Item) -> B;

    fn spec_for_each<F: FnMut(Self::Item)>(self, f: F);
}
