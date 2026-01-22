// Notice: Adapted from Itertools: https://github.com/rust-itertools/itertools
//
// Copyright (c) 2015
//
// Permission is hereby granted, free of charge, to any
// person obtaining a copy of this software and associated
// documentation files (the "Software"), to deal in the
// Software without restriction, including without
// limitation the rights to use, copy, modify, merge,
// publish, distribute, sublicense, and/or sell copies of
// the Software, and to permit persons to whom the Software
// is furnished to do so, subject to the following
// conditions:
//
// The above copyright notice and this permission notice
// shall be included in all copies or substantial portions
// of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF
// ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED
// TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
// PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT
// SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
// CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR
// IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
// DEALINGS IN THE SOFTWARE.

use itertools::{put_back, Either, Itertools, PutBack};
use std::{
    fmt,
    fmt::Formatter,
    iter::{Fuse, FusedIterator},
    mem::replace,
};

/// `SizeHint` is the return type of `Iterator::size_hint()`.
pub type SizeHint = (usize, Option<usize>);

/// Add `SizeHint` correctly.
#[inline]
pub fn size_hint_add(a: SizeHint, b: SizeHint) -> SizeHint {
    let min = a.0.saturating_add(b.0);
    let max = match (a.1, b.1) {
        (Some(x), Some(y)) => x.checked_add(y),
        _ => None,
    };

    (min, max)
}

/// Add `x` correctly to a `SizeHint`.
#[inline]
pub fn size_hint_add_scalar(sh: SizeHint, x: usize) -> SizeHint {
    let (mut low, mut hi) = sh;
    low = low.saturating_add(x);
    hi = hi.and_then(|elt| elt.checked_add(x));
    (low, hi)
}

/// Head element and Tail iterator pair
///
/// `PartialEq`, `Eq`, `PartialOrd` and `Ord` are implemented by comparing sequences based on
/// first items (which are guaranteed to exist).
///
/// The meanings of `PartialOrd` and `Ord` are reversed so as to turn the heap used in
/// `KMerge` into a min-heap.
#[derive(Debug)]
struct HeadTail<I>
where
    I: Iterator,
{
    head: I::Item,
    tail: I,
}

impl<I> HeadTail<I>
where
    I: Iterator,
{
    /// Constructs a `HeadTail` from an `Iterator`. Returns `None` if the `Iterator` is empty.
    fn new(mut it: I) -> Option<Self> {
        let head = it.next();
        head.map(|h| Self { head: h, tail: it })
    }

    /// Get the next element and update `head`, returning the old head in `Some`.
    ///
    /// Returns `None` when the tail is exhausted (only `head` then remains).
    fn next(&mut self) -> Option<I::Item> {
        if let Some(next) = self.tail.next() {
            Some(replace(&mut self.head, next))
        } else {
            None
        }
    }

    /// Hints at the size of the sequence, same as the `Iterator` method.
    fn size_hint(&self) -> (usize, Option<usize>) {
        size_hint_add_scalar(self.tail.size_hint(), 1)
    }
}

/// Make `data` a heap (min-heap w.r.t the sorting).
fn heapify<T, S>(data: &mut [T], mut less_than: S)
where
    S: FnMut(&T, &T) -> bool,
{
    for i in (0..data.len() / 2).rev() {
        sift_down(data, i, &mut less_than);
    }
}

/// Sift down element at `index` (`heap` is a min-heap wrt the ordering)
fn sift_down<T, S>(heap: &mut [T], index: usize, mut less_than: S)
where
    S: FnMut(&T, &T) -> bool,
{
    debug_assert!(index <= heap.len());
    let mut pos = index;
    let mut child = 2 * pos + 1;
    // Require the right child to be present
    // This allows to find the index of the smallest child without a branch
    // that wouldn't be predicted if present
    while child + 1 < heap.len() {
        // pick the smaller of the two children
        // use arithmetic to avoid an unpredictable branch
        child += less_than(&heap[child + 1], &heap[child]) as usize;

        // sift down is done if we are already in order
        if !less_than(&heap[child], &heap[pos]) {
            return;
        }
        heap.swap(pos, child);
        pos = child;
        child = 2 * pos + 1;
    }
    // Check if the last (left) child was an only child
    // if it is then it has to be compared with the parent
    if child + 1 == heap.len() && less_than(&heap[child], &heap[pos]) {
        heap.swap(pos, child);
    }
}

/// An iterator adaptor that merges an abitrary number of base iterators in ascending order.
/// If all base iterators are sorted (ascending), the result is sorted.
///
/// Iterator element type is `I::Item`.
///
/// See [`.kmerge()`](crate::Itertools::kmerge) for more information.
pub type KMerge<I> = KMergeBy<I, MergeByLt>;

pub trait MergePredicate<T> {
    fn merge_pred(&mut self, a: &T, b: &T) -> bool;
}

#[derive(Clone, Debug)]
pub struct MergeByLt;

impl<T: PartialOrd> MergePredicate<T> for MergeByLt {
    fn merge_pred(&mut self, a: &T, b: &T) -> bool {
        a < b
    }
}

#[derive(Clone, Debug)]
pub struct MergeByGe;

impl<T: PartialOrd> MergePredicate<T> for MergeByGe {
    fn merge_pred(&mut self, a: &T, b: &T) -> bool {
        a >= b
    }
}

impl<T, F: FnMut(&T, &T) -> bool> MergePredicate<T> for F {
    fn merge_pred(&mut self, a: &T, b: &T) -> bool {
        self(a, b)
    }
}

#[derive(Clone, Debug)]
pub struct MergeByRev<F>(pub F);

impl<T, F: MergePredicate<T>> MergePredicate<T> for MergeByRev<F> {
    fn merge_pred(&mut self, a: &T, b: &T) -> bool {
        self.0.merge_pred(b, a)
    }
}

/// An iterator adaptor that merges an abitrary number of base iterators
/// according to an ordering function.
///
/// Iterator element type is `I::Item`.
///
/// See [`.kmerge_by()`](crate::Itertools::kmerge_by) for more
/// information.
#[must_use = "this iterator adaptor is not lazy but does nearly nothing unless consumed"]
pub struct KMergeBy<I, F>
where
    I: Iterator,
{
    heap: Vec<HeadTail<I>>,
    cmp_fn: F,
}

impl<I: Iterator, F: MergePredicate<I::Item>> KMergeBy<I, F> {
    pub(crate) fn new(capacity: usize, cmp_fn: F) -> Self {
        let heap = Vec::with_capacity(capacity);
        Self { heap, cmp_fn }
    }

    /// Push a new iterator into this kmerge.
    /// Does not preserve the heap property and should only be used when constructing the iterator!
    /// Call `self.heapify()` when done!
    pub(crate) fn push(&mut self, iter: I) {
        if let Some(new) = HeadTail::new(iter) {
            self.heap.push(new);
        }
    }

    /// Call when done constructing the iterator to finalize the heap
    pub(crate) fn heapify(&mut self) {
        heapify(&mut self.heap, |a, b| {
            self.cmp_fn.merge_pred(&a.head, &b.head)
        });
    }
}

impl<I, F> fmt::Debug for KMergeBy<I, F>
where
    I: Iterator + fmt::Debug,
    I::Item: fmt::Debug,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("KMergeBy")
            .field("heap", &self.heap)
            .finish()
    }
}

impl<I, F> Iterator for KMergeBy<I, F>
where
    I: Iterator,
    F: MergePredicate<I::Item>,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        if self.heap.is_empty() {
            return None;
        }
        let result = if let Some(next) = self.heap[0].next() {
            next
        } else {
            self.heap.swap_remove(0).head
        };
        let less_than = &mut self.cmp_fn;
        sift_down(&mut self.heap, 0, |a, b| {
            less_than.merge_pred(&a.head, &b.head)
        });
        Some(result)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.heap
            .iter()
            .map(|i| i.size_hint())
            .reduce(size_hint_add)
            .unwrap_or((0, Some(0)))
    }
}

impl<I, F> FusedIterator for KMergeBy<I, F>
where
    I: Iterator,
    F: MergePredicate<I::Item>,
{
}

/// An iterator adaptor that merges the two base iterators in ascending order.
/// If both base iterators are sorted (ascending), the result is sorted.
///
/// Iterator element type is `I::Item`.
///
/// See [`.merge_by()`](crate::Itertools::merge_by) for more information.
#[must_use = "iterator adaptors are lazy and do nothing unless consumed"]
pub struct MergeBy<I: Iterator, J: Iterator, F> {
    pub(crate) left: PutBack<I>,
    pub(crate) right: PutBack<J>,
    pub(crate) cmp_fn: F,
}

impl<I: Iterator, J: Iterator, F> MergeBy<I, J, F> {
    pub(crate) fn new(left: I, right: J, cmp_fn: F) -> Self {
        let left = put_back(left);
        let right = put_back(right);
        Self {
            left,
            right,
            cmp_fn,
        }
    }

    /// Take the iterators back out.
    ///
    /// Warning: discards the head in the `PutBack` and should only be used before actually iterating over the struct!
    pub(crate) fn into_inner(self) -> (I, J, F) {
        let (_, left) = self.left.into_parts();
        let (_, right) = self.right.into_parts();
        (left, right, self.cmp_fn)
    }
}

impl<I, J, F> Iterator for MergeBy<I, J, F>
where
    I: Iterator,
    J: Iterator<Item = I::Item>,
    F: MergePredicate<I::Item>,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        match (self.left.next(), self.right.next()) {
            (None, None) => None,
            (Some(left), None) => Some(left),
            (None, Some(right)) => Some(right),
            (Some(left), Some(right)) => {
                if self.cmp_fn.merge_pred(&left, &right) {
                    self.right.put_back(right);
                    Some(left)
                } else {
                    self.left.put_back(left);
                    Some(right)
                }
            }
        }
    }

    fn fold<B, G>(mut self, init: B, mut f: G) -> B
    where
        Self: Sized,
        G: FnMut(B, Self::Item) -> B,
    {
        let mut acc = init;
        let mut left = self.left.next();
        let mut right = self.right.next();

        loop {
            match (left, right) {
                (Some(l), Some(r)) => {
                    if self.cmp_fn.merge_pred(&l, &r) {
                        acc = f(acc, l);
                        left = self.left.next();
                        right = Some(r);
                    } else {
                        acc = f(acc, r);
                        left = Some(l);
                        right = self.right.next();
                    }
                }
                (Some(l), None) => {
                    self.left.put_back(l);
                    acc = self.left.fold(acc, |acc, x| f(acc, x));
                    break;
                }
                (None, Some(r)) => {
                    self.right.put_back(r);
                    acc = self.right.fold(acc, |acc, x| f(acc, x));
                    break;
                }
                (None, None) => {
                    break;
                }
            }
        }

        acc
    }

    fn size_hint(&self) -> SizeHint {
        size_hint_add(self.left.size_hint(), self.right.size_hint())
    }

    fn nth(&mut self, mut n: usize) -> Option<Self::Item> {
        loop {
            if n == 0 {
                break self.next();
            }
            n -= 1;
            match (self.left.next(), self.right.next()) {
                (None, None) => break None,
                (Some(_left), None) => break self.left.nth(n),
                (None, Some(_right)) => break self.right.nth(n),
                (Some(left), Some(right)) => {
                    if self.cmp_fn.merge_pred(&left, &right) {
                        self.right.put_back(right);
                    } else {
                        self.left.put_back(left);
                    }
                }
            }
        }
    }
}

impl<I, J, F> FusedIterator for MergeBy<I, J, F>
where
    I: FusedIterator,
    J: FusedIterator<Item = I::Item>,
    F: MergePredicate<I::Item>,
{
}
