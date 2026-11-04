//! A set of time ranges, closed under intersection, union and complement.
//!
//! One window is a `Range<EventTime>`; the `TimeSemantics::Window` variant
//! carries exactly one. Combining windows with `&` stays within one range,
//! since two intervals intersect to an interval, but `|` and `~` do not: the
//! union of two disjoint windows is two ranges, and the complement of a window
//! is the two ranges either side of it. This type holds those.
//!
//! The ranges are kept **sorted, pairwise disjoint and non-adjacent**, with no
//! empty range among them, and the only way to build one is through a
//! constructor that establishes that. Every consumer relies on it: iterating a
//! history per range and chaining the pieces yields a sorted, duplicate-free
//! stream only because no event can fall in two ranges and the ranges come in
//! order.
//!
//! Adjacent ranges are merged as well as overlapping ones — `[0,5) ∪ [5,10)` is
//! `[0,10)` — so each set has exactly one representation, and the number of
//! ranges says which `TimeSemantics` variant it is: none is the empty window,
//! one is `Window`, more is `MultiWindow`.
use itertools::Itertools;
use raphtory_api::core::storage::timeindex::EventTime;
use std::{
    borrow::Borrow,
    fmt::{self, Debug, Formatter},
    ops::Range,
    sync::{Arc, LazyLock},
};

static ALL: LazyLock<Arc<[Range<EventTime>]>> =
    LazyLock::new(|| Arc::from([EventTime::MIN..EventTime::MAX]));
static NONE: LazyLock<Arc<[Range<EventTime>]>> =
    LazyLock::new(|| Arc::from([] as [Range<EventTime>; 0]));

/// Sorted, pairwise disjoint, non-adjacent, non-empty half-open ranges.
///
/// The ranges live behind an `Arc` and are seen through a `clip` window, so
/// cloning a set is a refcount bump and windowing one ([`Self::clipped_to`]) is
/// two binary searches: neither allocates. `span` holds the indices of the
/// ranges that overlap `clip`; each is yielded intersected with it.
#[derive(Clone)]
pub struct TimeRanges {
    ranges: Arc<[Range<EventTime>]>,
    span: Range<usize>,
    clip: Range<EventTime>,
}

impl TimeRanges {
    /// The set containing the given ranges, normalised.
    ///
    /// Empty ranges are dropped; the rest are sorted and merged wherever they
    /// overlap or touch.
    pub fn new(mut ranges: Vec<Range<EventTime>>) -> Self {
        ranges.retain(|r| r.start < r.end);
        ranges.sort_by_key(|r| r.start);
        // `<=` merges touching ranges too, so `[a,b) ∪ [b,c)` is `[a,c)`.
        ranges.dedup_by(|range, last| {
            let merges = range.start <= last.end;
            if merges {
                last.end = last.end.max(range.end);
            }
            merges
        });
        Self::normalised(ranges.into())
    }

    /// `ranges` must already be sorted, disjoint, non-adjacent and non-empty.
    fn normalised(ranges: Arc<[Range<EventTime>]>) -> Self {
        let span = 0..ranges.len();
        Self {
            ranges,
            span,
            clip: EventTime::MIN..EventTime::MAX,
        }
    }

    /// The set containing one range (or nothing, if the range is empty).
    pub fn single(range: Range<EventTime>) -> Self {
        Self::new(vec![range])
    }

    /// Every time.
    pub fn all() -> Self {
        Self::normalised(ALL.clone())
    }

    /// No time.
    pub fn empty() -> Self {
        Self::normalised(NONE.clone())
    }

    pub fn is_empty(&self) -> bool {
        self.span.is_empty()
    }

    /// Whether this is [`TimeRanges::all`]: one range from the minimum to the
    /// maximum time.
    pub fn is_all(&self) -> bool {
        self.len() == 1 && self.first() == Some(EventTime::MIN..EventTime::MAX)
    }

    /// The number of ranges, which decides the `TimeSemantics` variant.
    pub fn len(&self) -> usize {
        self.span.len()
    }

    /// The `i`th stored range, seen through the clip.
    fn clipped(&self, i: usize) -> Range<EventTime> {
        let range = &self.ranges[i];
        range.start.max(self.clip.start)..range.end.min(self.clip.end)
    }

    /// The ranges in order. Double-ended, so it can be walked from either end.
    pub fn iter(&self) -> RangeIter<&Self> {
        RangeIter {
            ranges: self,
            span: self.span.clone(),
        }
    }

    pub fn first(&self) -> Option<Range<EventTime>> {
        self.iter().next()
    }

    pub fn last(&self) -> Option<Range<EventTime>> {
        self.iter().next_back()
    }

    /// The earliest time in the set, if any.
    pub fn start(&self) -> Option<EventTime> {
        self.first().map(|r| r.start)
    }

    /// The exclusive end of the set, if any.
    pub fn end(&self) -> Option<EventTime> {
        self.last().map(|r| r.end)
    }

    /// This set seen through `window`. Shares the ranges: two binary searches
    /// find the ones that overlap the narrowed clip, and nothing is copied.
    pub fn clipped_to(&self, window: &Range<EventTime>) -> Self {
        let clip = self.clip.start.max(window.start)..self.clip.end.min(window.end);
        let ranges = &self.ranges[self.span.clone()];
        let lo = self.span.start + ranges.partition_point(|r| r.end <= clip.start);
        // An empty or reversed clip keeps nothing.
        let hi = if clip.start < clip.end {
            self.span.start + ranges.partition_point(|r| r.start < clip.end)
        } else {
            lo
        };
        Self {
            ranges: self.ranges.clone(),
            span: lo..hi.max(lo),
            clip,
        }
    }

    /// Times in both sets. A two-pointer sweep over two sorted, disjoint lists;
    /// the output is sorted and disjoint by construction, so it needs no
    /// re-normalising.
    pub fn intersect(&self, other: &Self) -> Self {
        let mut out = Vec::with_capacity(self.len().min(other.len()));
        let (mut a, mut b) = (self.iter().peekable(), other.iter().peekable());
        while let (Some(x), Some(y)) = (a.peek(), b.peek()) {
            let start = x.start.max(y.start);
            let end = x.end.min(y.end);
            if start < end {
                out.push(start..end);
            }
            // Advance whichever range ends first; the other may still overlap
            // the next one.
            if x.end < y.end {
                a.next();
            } else {
                b.next();
            }
        }
        Self::normalised(out.into())
    }

    /// Times in either set. A merge of two sorted, disjoint lists: each range is
    /// taken in start order and either extends the last one written or begins a
    /// new one, so the output needs no sorting and no re-normalising.
    pub fn union(&self, other: &Self) -> Self {
        let mut out = Vec::with_capacity(self.len() + other.len());
        for range in self
            .iter()
            .merge_by(other.iter(), |x, y| x.start <= y.start)
        {
            push_merged(&mut out, range);
        }
        Self::normalised(out.into())
    }

    /// Times in neither range: the gaps, plus whatever lies before the first
    /// range and after the last.
    pub fn complement(&self) -> Self {
        let mut out = Vec::with_capacity(self.len() + 1);
        let mut cursor = EventTime::MIN;
        for r in self.iter() {
            if cursor < r.start {
                out.push(cursor..r.start);
            }
            cursor = r.end;
        }
        if cursor < EventTime::MAX {
            out.push(cursor..EventTime::MAX);
        }
        Self::normalised(out.into())
    }
}

/// Appends `range`, which starts no earlier than the last range in `out`,
/// extending that range instead when the two overlap or touch.
fn push_merged(out: &mut Vec<Range<EventTime>>, range: Range<EventTime>) {
    match out.last_mut() {
        // `<=` merges touching ranges too, so `[a,b) ∪ [b,c)` is `[a,c)`.
        Some(last) if range.start <= last.end => last.end = last.end.max(range.end),
        _ => out.push(range),
    }
}

/// The ranges the set keeps, as a caller sees them.
impl Debug for TimeRanges {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.iter()).finish()
    }
}

/// Two sets are equal when they hold the same times, however each is stored.
impl PartialEq for TimeRanges {
    fn eq(&self, other: &Self) -> bool {
        self.iter().eq(other.iter())
    }
}

impl Eq for TimeRanges {}

/// The ranges of a set, in order, from either end. Over `&TimeRanges` it
/// borrows the set; over `TimeRanges` it owns a refcount on the shared ranges,
/// for streams that must outlive the set they came from. Either way no range
/// is copied until it is yielded.
#[derive(Clone, Debug)]
pub struct RangeIter<R = TimeRanges> {
    ranges: R,
    span: Range<usize>,
}

impl<R: Borrow<TimeRanges>> Iterator for RangeIter<R> {
    type Item = Range<EventTime>;

    fn next(&mut self) -> Option<Self::Item> {
        self.span.next().map(|i| self.ranges.borrow().clipped(i))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.span.size_hint()
    }
}

impl<R: Borrow<TimeRanges>> DoubleEndedIterator for RangeIter<R> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.span
            .next_back()
            .map(|i| self.ranges.borrow().clipped(i))
    }
}

impl<R: Borrow<TimeRanges>> ExactSizeIterator for RangeIter<R> {}

impl IntoIterator for TimeRanges {
    type Item = Range<EventTime>;
    type IntoIter = RangeIter;

    fn into_iter(self) -> Self::IntoIter {
        let span = self.span.clone();
        RangeIter { ranges: self, span }
    }
}

impl<'a> IntoIterator for &'a TimeRanges {
    type Item = Range<EventTime>;
    type IntoIter = RangeIter<&'a TimeRanges>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
