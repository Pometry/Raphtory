//! `PropertyFilter::matches` on a condition given as wire data, value by value.

use raphtory::{
    db::graph::views::filter::model::{
        node_filter::NodeFilter,
        property_filter::{PropertyFilter, PropertyFilterValue, PropertyRef},
        FilterOperator,
    },
    prelude::IntoProp,
};
use raphtory_api::core::{
    entities::properties::prop::{prop_hashable::HashableProp, Prop},
    storage::arc_str::ArcStr,
};
use std::sync::Arc;

/// A property condition as wire data, evaluated through
/// [`PropertyFilter::matches`].
fn prop_filter(
    operator: FilterOperator,
    prop_value: PropertyFilterValue,
) -> PropertyFilter<NodeFilter> {
    PropertyFilter {
        prop_ref: PropertyRef::Property("prop".to_string()),
        prop_value,
        operator,
        ops: vec![],
        entity: NodeFilter,
    }
}

fn single(value: impl IntoProp) -> PropertyFilterValue {
    PropertyFilterValue::Single(value.into_prop())
}

fn set(values: impl IntoIterator<Item = Prop>) -> PropertyFilterValue {
    PropertyFilterValue::Set(Arc::new(
        values.into_iter().map(HashableProp::from).collect(),
    ))
}

#[test]
fn test_fuzzy_search_property() {
    let filter = prop_filter(
        FilterOperator::FuzzySearch {
            levenshtein_distance: 2,
            prefix_match: false,
        },
        single("pomet"),
    );
    assert!(filter.matches(Some(&Prop::Str(ArcStr::from("pometry")))));
}

#[test]
fn test_fuzzy_search_property_prefix_match() {
    let filter = prop_filter(
        FilterOperator::FuzzySearch {
            levenshtein_distance: 2,
            prefix_match: false,
        },
        single("pome"),
    );
    assert!(!filter.matches(Some(&Prop::Str(ArcStr::from("pometry")))));

    let filter = prop_filter(
        FilterOperator::FuzzySearch {
            levenshtein_distance: 2,
            prefix_match: true,
        },
        single("pome"),
    );
    assert!(filter.matches(Some(&Prop::Str(ArcStr::from("pometry")))));
}

#[test]
fn test_contains_match() {
    let filter = prop_filter(FilterOperator::Contains, single("shivam"));
    let res = filter.matches(Some(&Prop::Str(ArcStr::from("shivam_kapoor"))));
    assert!(res);
    let res = filter.matches(None);
    assert!(!res);

    let filter = prop_filter(FilterOperator::Contains, single("am_ka"));
    let res = filter.matches(Some(&Prop::Str(ArcStr::from("shivam_kapoor"))));
    assert!(res);
}

#[test]
fn test_contains_not_match() {
    let filter = prop_filter(FilterOperator::NotContains, single("shivam"));
    let res = filter.matches(Some(&Prop::Str(ArcStr::from("shivam_kapoor"))));
    assert!(!res);
    let res = filter.matches(None);
    assert!(!res);
}

#[test]
fn test_is_in_match() {
    let filter = prop_filter(FilterOperator::IsIn, set(["shivam".into_prop()]));
    let res = filter.matches(Some(&Prop::Str(ArcStr::from("shivam"))));
    assert!(res);
    let res = filter.matches(None);
    assert!(!res);
}

#[test]
fn test_is_not_in_match() {
    let filter = prop_filter(FilterOperator::IsNotIn, set(["shivam".into_prop()]));
    let res = filter.matches(Some(&Prop::Str(ArcStr::from("shivam"))));
    assert!(!res);
    let res = filter.matches(None);
    assert!(!res);
}
