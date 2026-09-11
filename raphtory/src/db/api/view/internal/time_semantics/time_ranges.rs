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
//! order. Establishing the invariant once, when the set is built, is what lets
//! the per-node and per-edge methods stay a `flat_map`, an `any`, or a sum.
//!
//! Adjacent ranges are merged as well as overlapping ones — `[0,5) ∪ [5,10)` is
//! `[0,10)` — so each set has exactly one representation, and the number of
//! ranges says which `TimeSemantics` variant it is: none is the empty window,
//! one is `Window`, more is `MultiWindow`.

use raphtory_api::core::storage::timeindex::EventTime;
use std::ops::Range;

/// Sorted, pairwise disjoint, non-adjacent, non-empty half-open ranges.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TimeRanges(Vec<Range<EventTime>>);

impl TimeRanges {
    /// The set containing the given ranges, normalised.
    ///
    /// Empty ranges are dropped; the rest are sorted and merged wherever they
    /// overlap or touch.
    pub fn new(mut ranges: Vec<Range<EventTime>>) -> Self {
        ranges.retain(|r| r.start < r.end);
        ranges.sort_by_key(|r| r.start);
        let mut out: Vec<Range<EventTime>> = Vec::with_capacity(ranges.len());
        for r in ranges {
            match out.last_mut() {
                // `<=` merges touching ranges too, so `[a,b) ∪ [b,c)` is `[a,c)`.
                Some(last) if r.start <= last.end => last.end = last.end.max(r.end),
                _ => out.push(r),
            }
        }
        TimeRanges(out)
    }

    /// The set containing one range (or nothing, if the range is empty).
    pub fn single(range: Range<EventTime>) -> Self {
        Self::new(vec![range])
    }

    /// Every time.
    pub fn all() -> Self {
        TimeRanges(vec![EventTime::MIN..EventTime::MAX])
    }

    /// No time.
    pub fn empty() -> Self {
        TimeRanges(Vec::new())
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Whether this is [`TimeRanges::all`]: a single range from the minimum to
    /// the maximum time.
    pub fn is_all(&self) -> bool {
        matches!(self.0.as_slice(), [r] if r.start == EventTime::MIN && r.end == EventTime::MAX)
    }

    /// The number of ranges, which decides the `TimeSemantics` variant.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn as_slice(&self) -> &[Range<EventTime>] {
        &self.0
    }

    /// The ranges in order. A slice iterator, so it can be walked from either
    /// end and cloned into closures that outlive the borrow.
    pub fn iter(&self) -> std::slice::Iter<'_, Range<EventTime>> {
        self.0.iter()
    }

    /// The ranges, owned, for iterators that must outlive `self`.
    pub fn into_vec(self) -> Vec<Range<EventTime>> {
        self.0
    }

    /// Whether `t` falls in any range. Binary search, since the ranges are
    /// sorted and disjoint.
    pub fn contains(&self, t: EventTime) -> bool {
        // Index of the first range whose start is after `t`; the candidate is
        // the one before it.
        let idx = self.0.partition_point(|r| r.start <= t);
        idx > 0 && t < self.0[idx - 1].end
    }

    /// The earliest time in the set, if any.
    pub fn start(&self) -> Option<EventTime> {
        self.0.first().map(|r| r.start)
    }

    /// The exclusive end of the set, if any.
    pub fn end(&self) -> Option<EventTime> {
        self.0.last().map(|r| r.end)
    }

    /// Times in both sets. A two-pointer sweep over two sorted, disjoint lists;
    /// the output is sorted and disjoint by construction, so it needs no
    /// re-normalising.
    pub fn intersect(&self, other: &Self) -> Self {
        let (a, b) = (&self.0, &other.0);
        let (mut i, mut j) = (0, 0);
        let mut out = Vec::with_capacity(a.len().min(b.len()));
        while i < a.len() && j < b.len() {
            let start = a[i].start.max(b[j].start);
            let end = a[i].end.min(b[j].end);
            if start < end {
                out.push(start..end);
            }
            // Advance whichever range ends first; the other may still overlap
            // the next one.
            if a[i].end < b[j].end {
                i += 1;
            } else {
                j += 1;
            }
        }
        TimeRanges(out)
    }

    /// Times in either set. A merge of two sorted, disjoint lists: each range is
    /// taken in start order and either extends the last one written or begins a
    /// new one, so the output needs no sorting and no re-normalising.
    pub fn union(&self, other: &Self) -> Self {
        let (a, b) = (&self.0, &other.0);
        let (mut i, mut j) = (0, 0);
        let mut out: Vec<Range<EventTime>> = Vec::with_capacity(a.len() + b.len());
        while i < a.len() || j < b.len() {
            // Whichever of the two heads starts first; both lists are sorted, so
            // nothing still to come can start before it.
            let next = if j == b.len() || (i < a.len() && a[i].start <= b[j].start) {
                i += 1;
                a[i - 1].clone()
            } else {
                j += 1;
                b[j - 1].clone()
            };
            match out.last_mut() {
                // `<=` merges touching ranges too, so `[a,b) ∪ [b,c)` is `[a,c)`.
                Some(last) if next.start <= last.end => last.end = last.end.max(next.end),
                _ => out.push(next),
            }
        }
        TimeRanges(out)
    }

    /// Times in neither range: the gaps, plus whatever lies before the first
    /// range and after the last.
    pub fn complement(&self) -> Self {
        let mut out = Vec::with_capacity(self.0.len() + 1);
        let mut cursor = EventTime::MIN;
        for r in &self.0 {
            if cursor < r.start {
                out.push(cursor..r.start);
            }
            cursor = r.end;
        }
        if cursor < EventTime::MAX {
            out.push(cursor..EventTime::MAX);
        }
        TimeRanges(out)
    }

    /// Each range intersected with `window`, for the `_window` family of time
    /// semantics methods, which receive a caller's range on top of the set's.
    pub fn clipped_to(&self, window: &Range<EventTime>) -> Self {
        self.intersect(&Self::single(window.clone()))
    }
}

impl<'a> IntoIterator for &'a TimeRanges {
    type Item = &'a Range<EventTime>;
    type IntoIter = std::slice::Iter<'a, Range<EventTime>>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl From<Range<EventTime>> for TimeRanges {
    fn from(range: Range<EventTime>) -> Self {
        Self::single(range)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use raphtory_api::core::storage::timeindex::AsTime;

    fn ranges(v: &[(i64, i64)]) -> TimeRanges {
        TimeRanges::new(v.iter().map(|&(a, b)| EventTime::range(a..b)).collect())
    }

    #[test]
    fn empty_ranges_are_dropped() {
        assert_eq!(ranges(&[(5, 5), (3, 1)]), TimeRanges::empty());
    }

    #[test]
    fn overlapping_and_adjacent_ranges_merge_to_one_representation() {
        assert_eq!(ranges(&[(0, 5), (3, 8)]), ranges(&[(0, 8)]));
        assert_eq!(ranges(&[(0, 5), (5, 10)]), ranges(&[(0, 10)]));
        assert_eq!(ranges(&[(6, 10), (0, 5)]), ranges(&[(0, 5), (6, 10)]));
    }

    #[test]
    fn two_windows_intersect_to_their_overlap() {
        // The `window(0,5) & window(3,8)` case: only the overlap survives.
        assert_eq!(
            ranges(&[(0, 5)]).intersect(&ranges(&[(3, 8)])),
            ranges(&[(3, 5)])
        );
        // Disjoint windows intersect to nothing.
        assert!(ranges(&[(0, 5)]).intersect(&ranges(&[(6, 10)])).is_empty());
    }

    #[test]
    fn disjoint_windows_union_to_two_ranges_not_their_hull() {
        // `window(0,5) | window(6,10)`: t=5 is in the gap and must stay out.
        let u = ranges(&[(0, 5)]).union(&ranges(&[(6, 10)]));
        assert_eq!(u.len(), 2);
        assert!(u.contains(EventTime::start(4)));
        assert!(!u.contains(EventTime::start(5)));
        assert!(u.contains(EventTime::start(6)));
    }

    #[test]
    fn complement_of_a_window_is_the_two_sides() {
        // `~window(3,5)`: everything before 3 and from 5 on.
        let c = ranges(&[(3, 5)]).complement();
        assert_eq!(c.len(), 2);
        assert!(c.contains(EventTime::start(1)));
        assert!(!c.contains(EventTime::start(3)));
        assert!(!c.contains(EventTime::start(4)));
        assert!(c.contains(EventTime::start(5)));
        assert!(c.contains(EventTime::start(7)));
    }

    #[test]
    fn all_and_empty_are_each_others_complement() {
        assert_eq!(TimeRanges::all().complement(), TimeRanges::empty());
        assert_eq!(TimeRanges::empty().complement(), TimeRanges::all());
        assert!(TimeRanges::all().is_all());
        assert!(!ranges(&[(0, 5)]).is_all());
    }

    #[test]
    fn length_selects_the_variant() {
        // `TimeSemantics::restrict` picks its variant off this slice: none is
        // the empty window, one is `Window`, more is `MultiWindow`.
        assert!(TimeRanges::empty().as_slice().is_empty());
        assert_eq!(ranges(&[(0, 5)]).as_slice(), [EventTime::range(0..5)]);
        assert_eq!(ranges(&[(0, 5), (6, 10)]).len(), 2);
    }

    // A brute-force model over a small domain: a set of the points each range
    // covers. Every operation must agree with it at every point.
    const DOMAIN: i64 = 24;

    fn points(s: &TimeRanges) -> Vec<bool> {
        (0..DOMAIN)
            .map(|v| s.contains(EventTime::start(v)))
            .collect()
    }

    fn arb_ranges() -> impl Strategy<Value = TimeRanges> {
        prop::collection::vec((0..DOMAIN, 0..DOMAIN), 0..6).prop_map(|v| {
            TimeRanges::new(v.into_iter().map(|(a, b)| EventTime::range(a..b)).collect())
        })
    }

    proptest! {
        #[test]
        fn normalised_form_is_sorted_disjoint_non_adjacent_and_non_empty(s in arb_ranges()) {
            for w in s.iter() {
                prop_assert!(w.start < w.end);
            }
            for pair in s.as_slice().windows(2) {
                // Strictly before, with a gap: touching ranges would have merged.
                prop_assert!(pair[0].end < pair[1].start);
            }
        }

        #[test]
        fn normalising_twice_changes_nothing(s in arb_ranges()) {
            prop_assert_eq!(TimeRanges::new(s.as_slice().to_vec()), s);
        }

        #[test]
        fn intersect_matches_the_point_model(a in arb_ranges(), b in arb_ranges()) {
            let got = points(&a.intersect(&b));
            let want: Vec<bool> = points(&a).iter().zip(points(&b)).map(|(x, y)| *x && y).collect();
            prop_assert_eq!(got, want);
        }

        #[test]
        fn union_matches_the_point_model(a in arb_ranges(), b in arb_ranges()) {
            let got = points(&a.union(&b));
            let want: Vec<bool> = points(&a).iter().zip(points(&b)).map(|(x, y)| *x || y).collect();
            prop_assert_eq!(got, want);
        }

        #[test]
        fn complement_matches_the_point_model(a in arb_ranges()) {
            let got = points(&a.complement());
            let want: Vec<bool> = points(&a).iter().map(|x| !x).collect();
            prop_assert_eq!(got, want);
            // Involution: the complement of the complement is the set itself.
            prop_assert_eq!(a.complement().complement(), a);
        }

        #[test]
        fn intersection_with_all_and_union_with_empty_are_identities(a in arb_ranges()) {
            prop_assert_eq!(a.intersect(&TimeRanges::all()), a.clone());
            prop_assert_eq!(a.union(&TimeRanges::empty()), a);
        }

        #[test]
        fn de_morgan_holds(a in arb_ranges(), b in arb_ranges()) {
            prop_assert_eq!(a.union(&b).complement(), a.complement().intersect(&b.complement()));
            prop_assert_eq!(a.intersect(&b).complement(), a.complement().union(&b.complement()));
        }
    }
}
