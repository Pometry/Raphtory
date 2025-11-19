pub(crate) use crate::db::graph::views::filter::model::and_filter::AndFilter;
use crate::{
    db::graph::views::filter::model::{
        edge_filter::{CompositeEdgeFilter, EdgeFilter, EndpointWrapper},
        exploded_edge_filter::{CompositeExplodedEdgeFilter, ExplodedEdgeFilter},
        filter_operator::FilterOperator,
        node_filter::{CompositeNodeFilter, NodeFilter, NodeNameFilter, NodeTypeFilter},
        not_filter::NotFilter,
        or_filter::OrFilter,
        property_filter::{MetadataFilterBuilder, PropertyFilter, PropertyFilterBuilder},
    },
    errors::GraphError,
};
use raphtory_api::core::{
    entities::{GidRef, GID},
    storage::timeindex::TimeIndexEntry,
};
use raphtory_core::utils::time::IntoTime;
use std::{collections::HashSet, fmt, fmt::Display, ops::Deref, sync::Arc};

pub mod and_filter;
pub mod edge_filter;
pub mod exploded_edge_filter;
pub mod filter_operator;
pub mod node_filter;
pub mod not_filter;
pub mod or_filter;
pub mod property_filter;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Windowed<M> {
    pub start: TimeIndexEntry,
    pub end: TimeIndexEntry,
    pub marker: M,
}

impl<M> Windowed<M> {
    #[inline]
    pub fn new(start: TimeIndexEntry, end: TimeIndexEntry, marker: M) -> Self {
        Self { start, end, marker }
    }

    #[inline]
    pub fn from_times<S: IntoTime, E: IntoTime>(start: S, end: E, marker: M) -> Self {
        let s = TimeIndexEntry::start(start.into_time());
        let e = TimeIndexEntry::end(end.into_time());
        Self::new(s, e, marker)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FilterValue {
    Single(String),
    Set(Arc<HashSet<String>>),
    ID(GID),
    IDSet(Arc<HashSet<GID>>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Filter {
    pub field_name: String,
    pub field_value: FilterValue,
    pub operator: FilterOperator,
}

impl Display for Filter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.field_value {
            FilterValue::Single(value) => {
                write!(f, "{} {} {}", self.field_name, self.operator, value)
            }
            FilterValue::Set(values) => {
                let mut sorted: Vec<&String> = values.iter().collect();
                sorted.sort();
                let values_str = sorted
                    .iter()
                    .map(|s| s.as_str())
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "{} {} [{}]", self.field_name, self.operator, values_str)
            }
            FilterValue::ID(id) => {
                write!(f, "{} {} {}", self.field_name, self.operator, id)
            }
            FilterValue::IDSet(values) => {
                let mut sorted: Vec<&GID> = values.iter().collect();
                sorted.sort();
                let values_str = sorted
                    .iter()
                    .map(ToString::to_string)
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

    pub fn is_in(
        field_name: impl Into<String>,
        field_values: impl IntoIterator<Item = String>,
    ) -> Self {
        Self {
            field_name: field_name.into(),
            field_value: FilterValue::Set(Arc::new(field_values.into_iter().collect())),
            operator: FilterOperator::IsIn,
        }
    }

    pub fn is_not_in(
        field_name: impl Into<String>,
        field_values: impl IntoIterator<Item = String>,
    ) -> Self {
        Self {
            field_name: field_name.into(),
            field_value: FilterValue::Set(Arc::new(field_values.into_iter().collect())),
            operator: FilterOperator::IsNotIn,
        }
    }

    pub fn starts_with(field_name: impl Into<String>, field_value: impl Into<String>) -> Self {
        Self {
            field_name: field_name.into(),
            field_value: FilterValue::Single(field_value.into()),
            operator: FilterOperator::StartsWith,
        }
    }

    pub fn ends_with(field_name: impl Into<String>, field_value: impl Into<String>) -> Self {
        Self {
            field_name: field_name.into(),
            field_value: FilterValue::Single(field_value.into()),
            operator: FilterOperator::EndsWith,
        }
    }

    pub fn contains(field_name: impl Into<String>, field_value: impl Into<String>) -> Self {
        Self {
            field_name: field_name.into(),
            field_value: FilterValue::Single(field_value.into()),
            operator: FilterOperator::Contains,
        }
    }

    pub fn not_contains(field_name: impl Into<String>, field_value: impl Into<String>) -> Self {
        Self {
            field_name: field_name.into(),
            field_value: FilterValue::Single(field_value.into()),
            operator: FilterOperator::NotContains,
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

    pub fn eq_id(field_name: impl Into<String>, field_value: impl Into<GID>) -> Self {
        Self {
            field_name: field_name.into(),
            field_value: FilterValue::ID(field_value.into()),
            operator: FilterOperator::Eq,
        }
    }

    pub fn ne_id(field_name: impl Into<String>, field_value: impl Into<GID>) -> Self {
        Self {
            field_name: field_name.into(),
            field_value: FilterValue::ID(field_value.into()),
            operator: FilterOperator::Ne,
        }
    }

    pub fn is_in_id<I, V>(field_name: impl Into<String>, field_values: I) -> Self
    where
        I: IntoIterator<Item = V>,
        V: Into<GID>,
    {
        let set: HashSet<GID> = field_values.into_iter().map(|x| x.into()).collect();
        Self {
            field_name: field_name.into(),
            field_value: FilterValue::IDSet(Arc::new(set)),
            operator: FilterOperator::IsIn,
        }
    }

    pub fn is_not_in_id<I, V>(field_name: impl Into<String>, field_values: I) -> Self
    where
        I: IntoIterator<Item = V>,
        V: Into<GID>,
    {
        let set: HashSet<GID> = field_values.into_iter().map(|x| x.into()).collect();
        Self {
            field_name: field_name.into(),
            field_value: FilterValue::IDSet(Arc::new(set)),
            operator: FilterOperator::IsNotIn,
        }
    }

    pub fn lt<V: Into<GID>>(field_name: impl Into<String>, field_value: V) -> Self {
        Filter {
            field_name: field_name.into(),
            field_value: FilterValue::ID(field_value.into()),
            operator: FilterOperator::Lt,
        }
        .into()
    }

    pub fn le<V: Into<GID>>(field_name: impl Into<String>, field_value: V) -> Self {
        Filter {
            field_name: field_name.into(),
            field_value: FilterValue::ID(field_value.into()),
            operator: FilterOperator::Le,
        }
        .into()
    }

    pub fn gt<V: Into<GID>>(field_name: impl Into<String>, field_value: V) -> Self {
        Filter {
            field_name: field_name.into(),
            field_value: FilterValue::ID(field_value.into()),
            operator: FilterOperator::Gt,
        }
        .into()
    }

    pub fn ge<V: Into<GID>>(field_name: impl Into<String>, field_value: V) -> Self {
        Filter {
            field_name: field_name.into(),
            field_value: FilterValue::ID(field_value.into()),
            operator: FilterOperator::Ge,
        }
        .into()
    }

    pub fn matches(&self, node_value: Option<&str>) -> bool {
        self.operator.apply(&self.field_value, node_value)
    }

    pub fn id_matches(&self, node_value: GidRef<'_>) -> bool {
        self.operator.apply_id(&self.field_value, node_value)
    }
}

// Fluent Composite Filter Builder APIs
pub trait TryAsCompositeFilter: Send + Sync {
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError>;

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError>;

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError>;
}

impl<T: TryAsCompositeFilter + ?Sized> TryAsCompositeFilter for Arc<T> {
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        self.deref().try_as_composite_node_filter()
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        self.deref().try_as_composite_edge_filter()
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        self.deref().try_as_composite_exploded_edge_filter()
    }
}

pub trait ComposableFilter: Sized {
    fn and<F>(self, other: F) -> AndFilter<Self, F> {
        AndFilter {
            left: self,
            right: other,
        }
    }

    fn or<F>(self, other: F) -> OrFilter<Self, F> {
        OrFilter {
            left: self,
            right: other,
        }
    }

    fn not(self) -> NotFilter<Self> {
        NotFilter(self)
    }
}

impl<M> ComposableFilter for PropertyFilter<M> {}
impl ComposableFilter for NodeNameFilter {}
impl ComposableFilter for NodeTypeFilter {}
impl<T> ComposableFilter for EndpointWrapper<T> where T: TryAsCompositeFilter + Clone {}
impl<L, R> ComposableFilter for AndFilter<L, R> {}
impl<L, R> ComposableFilter for OrFilter<L, R> {}
impl<T> ComposableFilter for NotFilter<T> {}

trait EntityMarker: Clone + Send + Sync {}

impl EntityMarker for NodeFilter {}

impl EntityMarker for EdgeFilter {}

impl EntityMarker for ExplodedEdgeFilter {}

impl<M: EntityMarker + Clone + Send + Sync + 'static> EntityMarker for Windowed<M> {}

pub trait PropertyFilterFactory: Sized {
    fn property(&self, name: impl Into<String>) -> PropertyFilterBuilder<Self>;

    fn metadata(&self, name: impl Into<String>) -> MetadataFilterBuilder<Self>;
}

impl<M: EntityMarker> PropertyFilterFactory for M {
    fn property(&self, name: impl Into<String>) -> PropertyFilterBuilder<Self> {
        PropertyFilterBuilder::new(name, self.clone())
    }

    fn metadata(&self, name: impl Into<String>) -> MetadataFilterBuilder<Self> {
        MetadataFilterBuilder::new(name, self.clone())
    }
}
