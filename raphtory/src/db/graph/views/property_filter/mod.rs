use crate::core::{
    entities::properties::props::Meta, sort_comparable_props, utils::errors::GraphError, Prop,
};
use itertools::Itertools;
use raphtory_api::core::storage::arc_str::ArcStr;
use std::{collections::HashSet, fmt, sync::Arc};
use strsim::levenshtein;

pub mod edge_property_filter;
pub mod exploded_edge_property_filter;
pub(crate) mod internal;
pub mod node_property_filter;

#[derive(Debug, Clone, Copy)]
pub enum FilterOperator {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    In,
    NotIn,
    IsSome,
    IsNone,
    FuzzySearch {
        levenshtein_distance: usize,
        prefix_match: bool,
    },
}

impl fmt::Display for FilterOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let operator = match self {
            FilterOperator::Eq => "==",
            FilterOperator::Ne => "!=",
            FilterOperator::Lt => "<",
            FilterOperator::Le => "<=",
            FilterOperator::Gt => ">",
            FilterOperator::Ge => ">=",
            FilterOperator::In => "IN",
            FilterOperator::NotIn => "NOT_IN",
            FilterOperator::IsSome => "IS_SOME",
            FilterOperator::IsNone => "IS_NONE",
            FilterOperator::FuzzySearch {
                levenshtein_distance,
                prefix_match,
            } => {
                return write!(f, "FUZZY_SEARCH({},{})", levenshtein_distance, prefix_match);
            }
        };
        write!(f, "{}", operator)
    }
}

impl FilterOperator {
    pub fn is_strictly_numeric_operation(&self) -> bool {
        matches!(
            self,
            FilterOperator::Lt | FilterOperator::Le | FilterOperator::Gt | FilterOperator::Ge
        )
    }

    fn operation<T>(&self) -> impl Fn(&T, &T) -> bool
    where
        T: ?Sized + PartialEq + PartialOrd,
    {
        match self {
            FilterOperator::Eq => T::eq,
            FilterOperator::Ne => T::ne,
            FilterOperator::Lt => T::lt,
            FilterOperator::Le => T::le,
            FilterOperator::Gt => T::gt,
            FilterOperator::Ge => T::ge,
            _ => panic!("Operation not supported for this operator"),
        }
    }

    pub fn fuzzy_search(
        &self,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> impl Fn(&str, &str) -> bool {
        move |left: &str, right: &str| {
            let levenshtein_match = levenshtein(left, right) <= levenshtein_distance;
            let prefix_match = prefix_match && right.starts_with(left);
            levenshtein_match || prefix_match
        }
    }

    fn collection_operation<T>(&self) -> impl Fn(&HashSet<T>, &T) -> bool
    where
        T: Eq + std::hash::Hash,
    {
        match self {
            FilterOperator::In => |set: &HashSet<T>, value: &T| set.contains(value),
            FilterOperator::NotIn => |set: &HashSet<T>, value: &T| !set.contains(value),
            _ => panic!("Collection operation not supported for this operator"),
        }
    }

    pub fn apply_to_property(&self, left: &PropertyFilterValue, right: Option<&Prop>) -> bool {
        match left {
            PropertyFilterValue::None => match self {
                FilterOperator::IsSome => right.is_some(),
                FilterOperator::IsNone => right.is_none(),
                _ => unreachable!(),
            },
            PropertyFilterValue::Single(l) => match self {
                FilterOperator::Eq
                | FilterOperator::Ne
                | FilterOperator::Lt
                | FilterOperator::Le
                | FilterOperator::Gt
                | FilterOperator::Ge => right.map_or(false, |r| self.operation()(r, l)),
                FilterOperator::FuzzySearch {
                    levenshtein_distance,
                    prefix_match,
                } => right.map_or(false, |r| match (l, r) {
                    (Prop::Str(l), Prop::Str(r)) => {
                        let fuzzy_fn = self.fuzzy_search(*levenshtein_distance, *prefix_match);
                        fuzzy_fn(l, r)
                    }
                    _ => unreachable!(),
                }),
                _ => unreachable!(),
            },
            PropertyFilterValue::Set(l) => match self {
                FilterOperator::In | FilterOperator::NotIn => {
                    right.map_or(false, |r| self.collection_operation()(l, r))
                }
                _ => unreachable!(),
            },
        }
    }

    pub fn apply(&self, left: &FilterValue, right: Option<&str>) -> bool {
        match left {
            FilterValue::Single(l) => match self {
                FilterOperator::Eq | FilterOperator::Ne => {
                    right.map_or(false, |r| self.operation()(r, l))
                }
                FilterOperator::FuzzySearch {
                    levenshtein_distance,
                    prefix_match,
                } => right.map_or(false, |r| {
                    let fuzzy_fn = self.fuzzy_search(*levenshtein_distance, *prefix_match);
                    fuzzy_fn(l, r)
                }),
                _ => unreachable!(),
            },
            FilterValue::Set(l) => match self {
                FilterOperator::In | FilterOperator::NotIn => {
                    right.map_or(false, |r| self.collection_operation()(l, &r.to_string()))
                }
                _ => unreachable!(),
            },
        }
    }
}

#[derive(Debug, Clone)]
pub enum PropertyFilterValue {
    None,
    Single(Prop),
    Set(Arc<HashSet<Prop>>),
}

#[derive(Debug, Clone)]
pub struct BasePropertyFilter {
    pub prop_name: String,
    pub prop_value: PropertyFilterValue,
    pub operator: FilterOperator,
}

impl fmt::Display for BasePropertyFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.prop_value {
            PropertyFilterValue::None => {
                write!(f, "{} {}", self.prop_name, self.operator)
            }
            PropertyFilterValue::Single(value) => {
                write!(f, "{} {} {}", self.prop_name, self.operator, value)
            }
            PropertyFilterValue::Set(values) => {
                let sorted_values = sort_comparable_props(values.iter().collect_vec());
                let values_str = sorted_values
                    .iter()
                    .map(|v| format!("{}", v))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "{} {} [{}]", self.prop_name, self.operator, values_str)
            }
        }
    }
}

impl BasePropertyFilter {
    pub fn eq(prop_name: impl Into<String>, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_name: prop_name.into(),
            prop_value: PropertyFilterValue::Single(prop_value.into()),
            operator: FilterOperator::Eq,
        }
    }

    pub fn ne(prop_name: impl Into<String>, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_name: prop_name.into(),
            prop_value: PropertyFilterValue::Single(prop_value.into()),
            operator: FilterOperator::Ne,
        }
    }

    pub fn le(prop_name: impl Into<String>, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_name: prop_name.into(),
            prop_value: PropertyFilterValue::Single(prop_value.into()),
            operator: FilterOperator::Le,
        }
    }

    pub fn ge(prop_name: impl Into<String>, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_name: prop_name.into(),
            prop_value: PropertyFilterValue::Single(prop_value.into()),
            operator: FilterOperator::Ge,
        }
    }

    pub fn lt(prop_name: impl Into<String>, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_name: prop_name.into(),
            prop_value: PropertyFilterValue::Single(prop_value.into()),
            operator: FilterOperator::Lt,
        }
    }

    pub fn gt(prop_name: impl Into<String>, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_name: prop_name.into(),
            prop_value: PropertyFilterValue::Single(prop_value.into()),
            operator: FilterOperator::Gt,
        }
    }

    pub fn any(prop_name: impl Into<String>, prop_values: impl IntoIterator<Item = Prop>) -> Self {
        Self {
            prop_name: prop_name.into(),
            prop_value: PropertyFilterValue::Set(Arc::new(prop_values.into_iter().collect())),
            operator: FilterOperator::In,
        }
    }

    pub fn not_any(
        prop_name: impl Into<String>,
        prop_values: impl IntoIterator<Item = Prop>,
    ) -> Self {
        Self {
            prop_name: prop_name.into(),
            prop_value: PropertyFilterValue::Set(Arc::new(prop_values.into_iter().collect())),
            operator: FilterOperator::NotIn,
        }
    }

    pub fn is_none(prop_name: impl Into<String>) -> Self {
        Self {
            prop_name: prop_name.into(),
            prop_value: PropertyFilterValue::None,
            operator: FilterOperator::IsNone,
        }
    }

    pub fn is_some(prop_name: impl Into<String>) -> Self {
        Self {
            prop_name: prop_name.into(),
            prop_value: PropertyFilterValue::None,
            operator: FilterOperator::IsSome,
        }
    }

    pub fn fuzzy_search(
        prop_name: impl Into<String>,
        prop_value: impl Into<String>,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> Self {
        Self {
            prop_name: prop_name.into(),
            prop_value: PropertyFilterValue::Single(Prop::Str(ArcStr::from(prop_value.into()))),
            operator: FilterOperator::FuzzySearch {
                levenshtein_distance,
                prefix_match,
            },
        }
    }

    pub fn resolve_temporal_prop_ids(&self, meta: &Meta) -> Result<Option<usize>, GraphError> {
        if let PropertyFilterValue::Single(value) = &self.prop_value {
            Ok(meta
                .temporal_prop_meta()
                .get_and_validate(&self.prop_name, value.dtype())?)
        } else {
            Ok(meta.temporal_prop_meta().get_id(&self.prop_name))
        }
    }

    pub fn resolve_constant_prop_ids(&self, meta: &Meta) -> Result<Option<usize>, GraphError> {
        if let PropertyFilterValue::Single(value) = &self.prop_value {
            Ok(meta
                .const_prop_meta()
                .get_and_validate(&self.prop_name, value.dtype())?)
        } else {
            Ok(meta.const_prop_meta().get_id(&self.prop_name))
        }
    }

    pub fn matches(&self, other: Option<&Prop>) -> bool {
        let value = &self.prop_value;
        self.operator.apply_to_property(value, other)
    }
}

pub type PropertyFilter = BasePropertyFilter;
pub type ConstPropertyFilter = BasePropertyFilter;
pub type TemporalPropertyFilter = BasePropertyFilter;

#[derive(Debug, Clone)]
pub enum FilterValue {
    Single(String),
    Set(Arc<HashSet<String>>),
}

#[derive(Debug, Clone)]
pub struct Filter {
    pub field_name: String,
    pub field_value: FilterValue,
    pub operator: FilterOperator,
}

impl fmt::Display for Filter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.field_value {
            FilterValue::Single(value) => {
                write!(f, "{} {} {}", self.field_name, self.operator, value)
            }
            FilterValue::Set(values) => {
                let mut sorted_values: Vec<_> = values.iter().collect();
                sorted_values.sort();
                let values_str = sorted_values
                    .iter()
                    .map(|v| format!("{}", v))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "{} {} [{}]", self.field_name, self.operator, values_str)
            }
        }
    }
}

impl Filter {
    pub fn eq(field_name: impl Into<String>, field_value: impl Into<String>) -> Self {
        Self {
            field_name: field_name.into(),
            field_value: FilterValue::Single(field_value.into()),
            operator: FilterOperator::Eq,
        }
    }

    pub fn ne(field_name: impl Into<String>, field_value: impl Into<String>) -> Self {
        Self {
            field_name: field_name.into(),
            field_value: FilterValue::Single(field_value.into()),
            operator: FilterOperator::Ne,
        }
    }

    pub fn any(
        field_name: impl Into<String>,
        field_values: impl IntoIterator<Item = String>,
    ) -> Self {
        Self {
            field_name: field_name.into(),
            field_value: FilterValue::Set(Arc::new(field_values.into_iter().collect())),
            operator: FilterOperator::In,
        }
    }

    pub fn not_any(
        field_name: impl Into<String>,
        field_values: impl IntoIterator<Item = String>,
    ) -> Self {
        Self {
            field_name: field_name.into(),
            field_value: FilterValue::Set(Arc::new(field_values.into_iter().collect())),
            operator: FilterOperator::NotIn,
        }
    }

    pub fn fuzzy_search(
        field_name: impl Into<String>,
        field_value: impl Into<String>,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> Self {
        Self {
            field_name: field_name.into(),
            field_value: FilterValue::Single(field_value.into()),
            operator: FilterOperator::FuzzySearch {
                levenshtein_distance,
                prefix_match,
            },
        }
    }

    pub fn matches(&self, node_value: Option<&str>) -> bool {
        self.operator.apply(&self.field_value, node_value)
    }
}

#[derive(Debug, Clone)]
pub enum CompositeNodeFilter {
    Node(Filter),
    Property(PropertyFilter),
    And(Vec<CompositeNodeFilter>),
    Or(Vec<CompositeNodeFilter>),
}

impl fmt::Display for CompositeNodeFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompositeNodeFilter::Property(filter) => write!(f, "NODE_PROPERTY({})", filter),
            CompositeNodeFilter::Node(filter) => write!(f, "NODE({})", filter),
            CompositeNodeFilter::And(filters) => {
                let formatted = filters
                    .iter()
                    .map(|filter| format!("({})", filter))
                    .collect::<Vec<String>>()
                    .join(" AND ");
                write!(f, "{}", formatted)
            }
            CompositeNodeFilter::Or(filters) => {
                let formatted = filters
                    .iter()
                    .map(|filter| format!("({})", filter))
                    .collect::<Vec<String>>()
                    .join(" OR ");
                write!(f, "{}", formatted)
            }
        }
    }
}

#[derive(Debug, Clone)]
pub enum CompositeEdgeFilter {
    Edge(Filter),
    Property(PropertyFilter),
    And(Vec<CompositeEdgeFilter>),
    Or(Vec<CompositeEdgeFilter>),
}

impl fmt::Display for CompositeEdgeFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompositeEdgeFilter::Property(filter) => write!(f, "EDGE_PROPERTY({})", filter),
            CompositeEdgeFilter::Edge(filter) => write!(f, "EDGE({})", filter),
            CompositeEdgeFilter::And(filters) => {
                let formatted = filters
                    .iter()
                    .map(|filter| format!("({})", filter))
                    .collect::<Vec<String>>()
                    .join(" AND ");
                write!(f, "{}", formatted)
            }
            CompositeEdgeFilter::Or(filters) => {
                let formatted = filters
                    .iter()
                    .map(|filter| format!("({})", filter))
                    .collect::<Vec<String>>()
                    .join(" OR ");
                write!(f, "{}", formatted)
            }
        }
    }
}

// TODO: Add tests for const and temporal properties
#[cfg(test)]
mod test_composite_filters {
    use crate::{
        core::Prop,
        db::graph::views::property_filter::{CompositeEdgeFilter, CompositeNodeFilter, Filter},
        prelude::PropertyFilter,
    };
    use raphtory_api::core::storage::arc_str::ArcStr;

    #[test]
    fn test_composite_node_filter() {
        assert_eq!(
            "NODE_PROPERTY(p2 == 2)",
            CompositeNodeFilter::Property(PropertyFilter::eq("p2", 2u64)).to_string()
        );

        assert_eq!(
            "((NODE(node_type NOT_IN [fire_nation, water_tribe])) AND (NODE_PROPERTY(p2 == 2)) AND (NODE_PROPERTY(p1 == 1)) AND ((NODE_PROPERTY(p3 <= 5)) OR (NODE_PROPERTY(p4 IN [2, 10])))) OR (NODE(node_name == pometry)) OR (NODE_PROPERTY(p5 == 9))",
            CompositeNodeFilter::Or(vec![
                CompositeNodeFilter::And(vec![
                    CompositeNodeFilter::Node(Filter::not_any(
                        "node_type",
                        vec!["fire_nation".into(), "water_tribe".into()]
                    )),
                    CompositeNodeFilter::Property(PropertyFilter::eq("p2", 2u64)),
                    CompositeNodeFilter::Property(PropertyFilter::eq("p1", 1u64)),
                    CompositeNodeFilter::Or(vec![
                        CompositeNodeFilter::Property(PropertyFilter::le("p3", 5u64)),
                        CompositeNodeFilter::Property(PropertyFilter::any("p4", vec![Prop::U64(10), Prop::U64(2)]))
                    ]),
                ]),
                CompositeNodeFilter::Node(Filter::eq("node_name", "pometry")),
                CompositeNodeFilter::Property(PropertyFilter::eq("p5", 9u64)),
            ])
                .to_string()
        );

        assert_eq!(
            "(NODE(name FUZZY_SEARCH(1,true) shivam)) AND (NODE_PROPERTY(nation FUZZY_SEARCH(1,false) air_nomad))",
            CompositeNodeFilter::And(vec![
                CompositeNodeFilter::Node(Filter::fuzzy_search("name", "shivam", 1, true)),
                CompositeNodeFilter::Property(PropertyFilter::fuzzy_search(
                    "nation",
                    "air_nomad",
                    1,
                    false,
                )),
            ])
            .to_string()
        );
    }

    #[test]
    fn test_composite_edge_filter() {
        assert_eq!(
            "EDGE_PROPERTY(p2 == 2)",
            CompositeEdgeFilter::Property(PropertyFilter::eq("p2", 2u64)).to_string()
        );

        assert_eq!(
            "((EDGE(edge_type NOT_IN [fire_nation, water_tribe])) AND (EDGE_PROPERTY(p2 == 2)) AND (EDGE_PROPERTY(p1 == 1)) AND ((EDGE_PROPERTY(p3 <= 5)) OR (EDGE_PROPERTY(p4 IN [2, 10])))) OR (EDGE(from == pometry)) OR (EDGE_PROPERTY(p5 == 9))",
            CompositeEdgeFilter::Or(vec![
                CompositeEdgeFilter::And(vec![
                    CompositeEdgeFilter::Edge(Filter::not_any(
                        "edge_type",
                        vec!["fire_nation".into(), "water_tribe".into()]
                    )),
                    CompositeEdgeFilter::Property(PropertyFilter::eq("p2", 2u64)),
                    CompositeEdgeFilter::Property(PropertyFilter::eq("p1", 1u64)),
                    CompositeEdgeFilter::Or(vec![
                        CompositeEdgeFilter::Property(PropertyFilter::le("p3", 5u64)),
                        CompositeEdgeFilter::Property(PropertyFilter::any("p4", vec![Prop::U64(10), Prop::U64(2)]))
                    ]),
                ]),
                CompositeEdgeFilter::Edge(Filter::eq("from", "pometry")),
                CompositeEdgeFilter::Property(PropertyFilter::eq("p5", 9u64)),
            ])
                .to_string()
        );

        assert_eq!(
            "(EDGE(name FUZZY_SEARCH(1,true) shivam)) AND (EDGE_PROPERTY(nation FUZZY_SEARCH(1,false) air_nomad))",
            CompositeEdgeFilter::And(vec![
                CompositeEdgeFilter::Edge(Filter::fuzzy_search("name", "shivam", 1, true)),
                CompositeEdgeFilter::Property(PropertyFilter::fuzzy_search(
                    "nation",
                    "air_nomad",
                    1,
                    false,
                )),
            ])
            .to_string()
        );
    }

    #[test]
    fn test_fuzzy_search() {
        let filter = Filter::fuzzy_search("name", "pomet", 2, false);
        assert!(filter.matches(Some("pometry")));

        let filter = Filter::fuzzy_search("name", "shivam_kapoor", 2, false);
        assert!(filter.matches(Some("shivam_kapoor2")));

        let filter = Filter::fuzzy_search("name", "shivam kapoor", 2, false);
        assert!(filter.matches(Some("shivam_kapoor2")));

        let filter = Filter::fuzzy_search("name", "shivam kapoor", 2, false);
        assert!(filter.matches(Some("shivam_kapoor2")));

        let filter = Filter::fuzzy_search("name", "shivam kapoor", 2, false);
        assert!(!filter.matches(Some("shivam1_kapoor2")));
    }

    #[test]
    fn test_fuzzy_search_prefix_match() {
        let filter = Filter::fuzzy_search("name", "pome", 2, false);
        assert!(!filter.matches(Some("pometry")));

        let filter = Filter::fuzzy_search("name", "pome", 2, true);
        assert!(filter.matches(Some("pometry")));
    }

    #[test]
    fn test_fuzzy_search_property() {
        let filter = PropertyFilter::fuzzy_search("prop", "pomet", 2, false);
        assert!(filter.matches(Some(&Prop::Str(ArcStr::from("pometry")))));
    }

    #[test]
    fn test_fuzzy_search_property_prefix_match() {
        let filter = PropertyFilter::fuzzy_search("prop", "pome", 2, false);
        assert!(!filter.matches(Some(&Prop::Str(ArcStr::from("pometry")))));

        let filter = PropertyFilter::fuzzy_search("prop", "pome", 2, true);
        assert!(filter.matches(Some(&Prop::Str(ArcStr::from("pometry")))));
    }
}
