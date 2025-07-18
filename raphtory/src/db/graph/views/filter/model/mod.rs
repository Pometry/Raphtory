pub(crate) use crate::db::graph::views::filter::model::and_filter::AndFilter;
use crate::{
    db::graph::views::filter::model::{
        edge_filter::{CompositeEdgeFilter, EdgeFieldFilter},
        filter_operator::FilterOperator,
        node_filter::{CompositeNodeFilter, NodeNameFilter, NodeTypeFilter},
        not_filter::NotFilter,
        or_filter::OrFilter,
        property_filter::{PropertyFilter, PropertyFilterBuilder},
    },
    errors::GraphError,
    prelude::{GraphViewOps, NodeViewOps},
};
use raphtory_storage::graph::edges::{edge_ref::EdgeStorageRef, edge_storage_ops::EdgeStorageOps};
use std::{collections::HashSet, fmt, fmt::Display, ops::Deref, sync::Arc};

pub mod and_filter;
pub mod edge_filter;
pub mod filter_operator;
pub mod node_filter;
pub mod not_filter;
pub mod or_filter;
pub mod property_filter;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FilterValue {
    Single(String),
    Set(Arc<HashSet<String>>),
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
                let mut sorted_values: Vec<_> = values.iter().collect();
                sorted_values.sort();
                let values_str = sorted_values
                    .iter()
                    .map(|v| v.to_string())
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
            operator: FilterOperator::In,
        }
    }

    pub fn is_not_in(
        field_name: impl Into<String>,
        field_values: impl IntoIterator<Item = String>,
    ) -> Self {
        Self {
            field_name: field_name.into(),
            field_value: FilterValue::Set(Arc::new(field_values.into_iter().collect())),
            operator: FilterOperator::NotIn,
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

    pub fn matches(&self, node_value: Option<&str>) -> bool {
        self.operator.apply(&self.field_value, node_value)
    }

    pub fn matches_edge<'graph, G: GraphViewOps<'graph>>(
        &self,
        graph: &G,
        edge: EdgeStorageRef,
    ) -> bool {
        match self.field_name.as_str() {
            "src" => self.matches(graph.node(edge.src()).map(|n| n.name()).as_deref()),
            "dst" => self.matches(graph.node(edge.dst()).map(|n| n.name()).as_deref()),
            _ => false,
        }
    }
}

// Fluent Composite Filter Builder APIs
pub trait AsNodeFilter: Send + Sync {
    fn as_node_filter(&self) -> CompositeNodeFilter;
}

impl<T: AsNodeFilter + ?Sized> AsNodeFilter for Arc<T> {
    fn as_node_filter(&self) -> CompositeNodeFilter {
        self.deref().as_node_filter()
    }
}

pub trait TryAsNodeFilter: Send + Sync {
    fn try_as_node_filter(&self) -> Result<CompositeNodeFilter, GraphError>;
}

impl<T: TryAsNodeFilter + ?Sized> TryAsNodeFilter for Arc<T> {
    fn try_as_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        self.deref().try_as_node_filter()
    }
}

pub trait AsEdgeFilter: Send + Sync {
    fn as_edge_filter(&self) -> CompositeEdgeFilter;
}

impl<T: AsEdgeFilter + ?Sized> AsEdgeFilter for Arc<T> {
    fn as_edge_filter(&self) -> CompositeEdgeFilter {
        self.deref().as_edge_filter()
    }
}

pub trait TryAsEdgeFilter: Send + Sync {
    fn try_as_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError>;
}

impl<T: TryAsEdgeFilter + ?Sized> TryAsEdgeFilter for Arc<T> {
    fn try_as_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        self.deref().try_as_edge_filter()
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
impl ComposableFilter for EdgeFieldFilter {}
impl<L, R> ComposableFilter for AndFilter<L, R> {}
impl<L, R> ComposableFilter for OrFilter<L, R> {}
impl<T> ComposableFilter for NotFilter<T> {}

pub trait PropertyFilterFactory<M> {
    fn property(name: impl Into<String>) -> PropertyFilterBuilder<M>;
}
