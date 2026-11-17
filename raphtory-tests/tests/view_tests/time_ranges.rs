//! The range set behind multi-window views: normalisation, the set operations,
//! and a clipped set behaving exactly like the intersection it stands for.
// A set of one range is written as a list of one range, like every other set.
#![allow(clippy::single_range_in_vec_init)]
use proptest::prelude::*;
use raphtory::db::api::view::internal::TimeRanges;
use raphtory_api::core::storage::timeindex::{AsTime, EventTime};
use std::ops::Range;

fn ranges(v: &[Range<i64>]) -> TimeRanges {
    TimeRanges::new(v.iter().cloned().map(EventTime::range).collect())
}

/// Whether `t` falls in one of the ranges.
fn covers(set: &TimeRanges, t: i64) -> bool {
    set.iter().any(|range| range.contains(&EventTime::start(t)))
}

#[test]
fn empty_ranges_are_dropped() {
    #[allow(clippy::reversed_empty_ranges)]
    let set = ranges(&[5..5, 3..1]);
    assert_eq!(set, TimeRanges::empty());
}

#[test]
fn overlapping_and_adjacent_ranges_merge_to_one_representation() {
    assert_eq!(ranges(&[0..5, 3..8]), ranges(&[0..8]));
    assert_eq!(ranges(&[0..5, 5..10]), ranges(&[0..10]));
    assert_eq!(ranges(&[6..10, 0..5]), ranges(&[0..5, 6..10]));
}

#[test]
fn two_windows_intersect_to_their_overlap() {
    assert_eq!(ranges(&[0..5]).intersect(&ranges(&[3..8])), ranges(&[3..5]));
    assert!(ranges(&[0..5]).intersect(&ranges(&[6..10])).is_empty());
}

#[test]
fn disjoint_windows_union_to_two_ranges_not_their_hull() {
    // t=5 is in the gap and must stay out.
    let union = ranges(&[0..5]).union(&ranges(&[6..10]));
    assert_eq!(union.len(), 2);
    assert!(covers(&union, 4));
    assert!(!covers(&union, 5));
    assert!(covers(&union, 6));
}

#[test]
fn complement_of_a_window_is_the_two_sides() {
    let outside = ranges(&[3..5]).complement();
    assert_eq!(outside.len(), 2);
    assert!(covers(&outside, 1));
    assert!(!covers(&outside, 3));
    assert!(!covers(&outside, 4));
    assert!(covers(&outside, 5));
}

#[test]
fn all_and_empty_are_each_others_complement() {
    assert_eq!(TimeRanges::all().complement(), TimeRanges::empty());
    assert_eq!(TimeRanges::empty().complement(), TimeRanges::all());
    assert!(TimeRanges::all().is_all());
    assert!(!ranges(&[0..5]).is_all());
}

#[test]
fn a_clip_trims_the_ranges_at_its_edges_and_drops_the_rest() {
    // {[0,5), [6,10), [12,20)} seen through [3,13) is {[3,5), [6,10), [12,13)}.
    let set = ranges(&[0..5, 6..10, 12..20]);
    let clipped = set.clipped_to(&EventTime::range(3..13));
    assert_eq!(clipped, ranges(&[3..5, 6..10, 12..13]));
    assert_eq!(clipped.len(), 3);
    // [5,6) falls in a gap: nothing is left.
    assert!(set.clipped_to(&EventTime::range(5..6)).is_empty());
    // Clips narrow: a second clip sees only what the first kept.
    let twice = clipped.clipped_to(&EventTime::range(0..7));
    assert_eq!(twice, ranges(&[3..5, 6..7]));
}

// A brute-force model over a small domain: the points each set covers. Every
// operation must agree with it at every point.
const DOMAIN: i64 = 24;

fn points(set: &TimeRanges) -> Vec<bool> {
    (0..DOMAIN).map(|t| covers(set, t)).collect()
}

fn arb_ranges() -> impl Strategy<Value = TimeRanges> {
    prop::collection::vec((0..DOMAIN, 0..DOMAIN), 0..6)
        .prop_map(|v| TimeRanges::new(v.into_iter().map(|(a, b)| EventTime::range(a..b)).collect()))
}

/// A set, possibly seen through a clip, so every property also covers the
/// shared, clipped form.
fn arb_maybe_clipped() -> impl Strategy<Value = TimeRanges> {
    (arb_ranges(), prop::option::of((0..DOMAIN, 0..DOMAIN))).prop_map(|(set, clip)| match clip {
        Some((a, b)) => set.clipped_to(&EventTime::range(a..b)),
        None => set,
    })
}

proptest! {
    #[test]
    fn normalised_form_is_sorted_disjoint_non_adjacent_and_non_empty(set in arb_maybe_clipped()) {
        for range in set.iter() {
            prop_assert!(range.start < range.end);
        }
        let all: Vec<_> = set.iter().collect();
        for pair in all.windows(2) {
            // Strictly before, with a gap: touching ranges would have merged.
            prop_assert!(pair[0].end < pair[1].start);
        }
        prop_assert_eq!(all.len(), set.len());
    }

    #[test]
    fn normalising_twice_changes_nothing(set in arb_maybe_clipped()) {
        prop_assert_eq!(TimeRanges::new(set.iter().collect()), set);
    }

    #[test]
    fn a_clip_is_the_intersection_with_the_window(set in arb_ranges(), a in 0..DOMAIN, b in 0..DOMAIN) {
        let window = EventTime::range(a..b);
        let clipped = set.clipped_to(&window);
        prop_assert_eq!(&clipped, &set.intersect(&TimeRanges::single(window)));
        prop_assert_eq!(clipped.start(), clipped.iter().next().map(|r| r.start));
        prop_assert_eq!(clipped.end(), clipped.iter().next_back().map(|r| r.end));
    }

    #[test]
    fn intersect_matches_the_point_model(a in arb_maybe_clipped(), b in arb_maybe_clipped()) {
        let got = points(&a.intersect(&b));
        let want: Vec<bool> = points(&a).iter().zip(points(&b)).map(|(x, y)| *x && y).collect();
        prop_assert_eq!(got, want);
    }

    #[test]
    fn union_matches_the_point_model(a in arb_maybe_clipped(), b in arb_maybe_clipped()) {
        let got = points(&a.union(&b));
        let want: Vec<bool> = points(&a).iter().zip(points(&b)).map(|(x, y)| *x || y).collect();
        prop_assert_eq!(got, want);
    }

    #[test]
    fn complement_matches_the_point_model(a in arb_maybe_clipped()) {
        let got = points(&a.complement());
        let want: Vec<bool> = points(&a).iter().map(|x| !x).collect();
        prop_assert_eq!(got, want);
        prop_assert_eq!(a.complement().complement(), a);
    }

    #[test]
    fn intersection_with_all_and_union_with_empty_are_identities(a in arb_maybe_clipped()) {
        prop_assert_eq!(a.intersect(&TimeRanges::all()), a.clone());
        prop_assert_eq!(a.union(&TimeRanges::empty()), a);
    }

    #[test]
    fn de_morgan_holds(a in arb_maybe_clipped(), b in arb_maybe_clipped()) {
        prop_assert_eq!(a.union(&b).complement(), a.complement().intersect(&b.complement()));
        prop_assert_eq!(a.intersect(&b).complement(), a.complement().union(&b.complement()));
    }
}
