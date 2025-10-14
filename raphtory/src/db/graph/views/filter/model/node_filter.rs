use crate::{
    db::{
        api::view::BoxableGraphView,
        graph::views::filter::{
            internal::CreateFilter,
            model::{
                edge_filter::{CompositeEdgeFilter, CompositeExplodedEdgeFilter},
                filter_operator::FilterOperator,
                property_filter::PropertyFilter,
                AndFilter, Filter, FilterValue, NotFilter, OrFilter, PropertyFilterFactory,
                TryAsCompositeFilter,
            },
        },
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use raphtory_api::core::entities::{GidType, GID};
use std::{fmt, fmt::Display, ops::Deref, sync::Arc};

#[derive(Debug, Clone)]
pub struct NodeIdFilter(pub Filter);

impl Display for NodeIdFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<Filter> for NodeIdFilter {
    fn from(filter: Filter) -> Self {
        NodeIdFilter(filter)
    }
}

#[derive(Debug, Clone)]
pub struct NodeNameFilter(pub Filter);

impl Display for NodeNameFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<Filter> for NodeNameFilter {
    fn from(filter: Filter) -> Self {
        NodeNameFilter(filter)
    }
}

#[derive(Debug, Clone)]
pub struct NodeTypeFilter(pub Filter);

impl Display for NodeTypeFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<Filter> for NodeTypeFilter {
    fn from(filter: Filter) -> Self {
        NodeTypeFilter(filter)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompositeNodeFilter {
    Node(Filter),
    Property(PropertyFilter<NodeFilter>),
    And(Box<CompositeNodeFilter>, Box<CompositeNodeFilter>),
    Or(Box<CompositeNodeFilter>, Box<CompositeNodeFilter>),
    Not(Box<CompositeNodeFilter>),
}

impl Display for CompositeNodeFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompositeNodeFilter::Property(filter) => write!(f, "{}", filter),
            CompositeNodeFilter::Node(filter) => write!(f, "{}", filter),
            CompositeNodeFilter::And(left, right) => write!(f, "({} AND {})", left, right),
            CompositeNodeFilter::Or(left, right) => write!(f, "({} OR {})", left, right),
            CompositeNodeFilter::Not(filter) => write!(f, "NOT({})", filter),
        }
    }
}

impl CreateFilter for CompositeNodeFilter {
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> = Arc<dyn BoxableGraphView + 'graph>;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        match self {
            CompositeNodeFilter::Node(i) => match i.field_name.as_str() {
                "node_id" => Ok(Arc::new(NodeIdFilter(i).create_filter(graph)?)),
                "node_name" => Ok(Arc::new(NodeNameFilter(i).create_filter(graph)?)),
                "node_type" => Ok(Arc::new(NodeTypeFilter(i).create_filter(graph)?)),
                _ => {
                    unreachable!()
                }
            },
            CompositeNodeFilter::Property(i) => Ok(Arc::new(i.create_filter(graph)?)),
            CompositeNodeFilter::And(l, r) => Ok(Arc::new(
                AndFilter {
                    left: l.deref().clone(),
                    right: r.deref().clone(),
                }
                .create_filter(graph)?,
            )),
            CompositeNodeFilter::Or(l, r) => Ok(Arc::new(
                OrFilter {
                    left: l.deref().clone(),
                    right: r.deref().clone(),
                }
                .create_filter(graph)?,
            )),
            CompositeNodeFilter::Not(filter) => {
                let base = filter.deref().clone();
                Ok(Arc::new(NotFilter(base).create_filter(graph)?))
            }
        }
    }
}

impl TryAsCompositeFilter for CompositeNodeFilter {
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Ok(self.clone())
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }
}

pub trait InternalNodeFilterBuilderOps: Send + Sync {
    type NodeFilterType: From<Filter> + CreateFilter + TryAsCompositeFilter + Clone + 'static;

    fn field_name(&self) -> &'static str;
}

impl<T: InternalNodeFilterBuilderOps> InternalNodeFilterBuilderOps for Arc<T> {
    type NodeFilterType = T::NodeFilterType;

    fn field_name(&self) -> &'static str {
        self.deref().field_name()
    }
}

pub trait NodeFilterBuilderOps: InternalNodeFilterBuilderOps {
    fn eq(&self, value: impl Into<String>) -> Self::NodeFilterType {
        Filter::eq(self.field_name(), value).into()
    }

    fn ne(&self, value: impl Into<String>) -> Self::NodeFilterType {
        Filter::ne(self.field_name(), value).into()
    }

    fn is_in(&self, values: impl IntoIterator<Item = String>) -> Self::NodeFilterType {
        Filter::is_in(self.field_name(), values).into()
    }

    fn is_not_in(&self, values: impl IntoIterator<Item = String>) -> Self::NodeFilterType {
        Filter::is_not_in(self.field_name(), values).into()
    }

    fn starts_with(&self, value: impl Into<String>) -> Self::NodeFilterType {
        Filter::starts_with(self.field_name(), value).into()
    }

    fn ends_with(&self, value: impl Into<String>) -> Self::NodeFilterType {
        Filter::ends_with(self.field_name(), value).into()
    }

    fn contains(&self, value: impl Into<String>) -> Self::NodeFilterType {
        Filter::contains(self.field_name(), value).into()
    }

    fn not_contains(&self, value: impl Into<String>) -> Self::NodeFilterType {
        Filter::not_contains(self.field_name(), value.into()).into()
    }

    fn fuzzy_search(
        &self,
        value: impl Into<String>,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> Self::NodeFilterType {
        Filter::fuzzy_search(self.field_name(), value, levenshtein_distance, prefix_match).into()
    }
}

impl<T: InternalNodeFilterBuilderOps + ?Sized> NodeFilterBuilderOps for T {}

pub struct NodeIdFilterBuilder;

impl NodeIdFilterBuilder {
    #[inline]
    fn field_name(&self) -> &'static str {
        "node_id"
    }

    pub fn eq<T: Into<GID>>(&self, value: T) -> NodeIdFilter {
        Filter::eq_id(self.field_name(), value).into()
    }

    pub fn ne<T: Into<GID>>(&self, value: T) -> NodeIdFilter {
        Filter::ne_id(self.field_name(), value).into()
    }

    pub fn is_in<I, T>(&self, values: I) -> NodeIdFilter
    where
        I: IntoIterator<Item = T>,
        T: Into<GID>,
    {
        Filter::is_in_id(self.field_name(), values).into()
    }

    pub fn is_not_in<I, T>(&self, values: I) -> NodeIdFilter
    where
        I: IntoIterator<Item = T>,
        T: Into<GID>,
    {
        Filter::is_not_in_id(self.field_name(), values).into()
    }

    pub fn lt<V: Into<GID>>(&self, value: V) -> NodeIdFilter {
        Filter::lt(self.field_name(), value).into()
    }

    pub fn le<V: Into<GID>>(&self, value: V) -> NodeIdFilter {
        Filter::le(self.field_name(), value).into()
    }

    pub fn gt<V: Into<GID>>(&self, value: V) -> NodeIdFilter {
        Filter::gt(self.field_name(), value).into()
    }

    pub fn ge<V: Into<GID>>(&self, value: V) -> NodeIdFilter {
        Filter::ge(self.field_name(), value).into()
    }

    pub fn starts_with<S: Into<String>>(&self, s: S) -> NodeIdFilter {
        Filter::starts_with(self.field_name(), s.into()).into()
    }

    pub fn ends_with<S: Into<String>>(&self, s: S) -> NodeIdFilter {
        Filter::ends_with(self.field_name(), s.into()).into()
    }

    pub fn contains<S: Into<String>>(&self, s: S) -> NodeIdFilter {
        Filter::contains(self.field_name(), s.into()).into()
    }

    pub fn not_contains<S: Into<String>>(&self, s: S) -> NodeIdFilter {
        Filter::not_contains(self.field_name(), s.into()).into()
    }

    pub fn fuzzy_search<S: Into<String>>(
        &self,
        s: S,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> NodeIdFilter {
        Filter::fuzzy_search(self.field_name(), s, levenshtein_distance, prefix_match).into()
    }
}

pub struct NodeNameFilterBuilder;

impl InternalNodeFilterBuilderOps for NodeNameFilterBuilder {
    type NodeFilterType = NodeNameFilter;

    fn field_name(&self) -> &'static str {
        "node_name"
    }
}

pub struct NodeTypeFilterBuilder;

impl InternalNodeFilterBuilderOps for NodeTypeFilterBuilder {
    type NodeFilterType = NodeTypeFilter;

    fn field_name(&self) -> &'static str {
        "node_type"
    }
}

#[derive(Clone, Debug, Copy, PartialEq, Eq)]
pub struct NodeFilter;

impl NodeFilter {
    pub fn id() -> NodeIdFilterBuilder {
        NodeIdFilterBuilder
    }

    pub fn name() -> NodeNameFilterBuilder {
        NodeNameFilterBuilder
    }

    pub fn node_type() -> NodeTypeFilterBuilder {
        NodeTypeFilterBuilder
    }

    pub fn validate(id_dtype: Option<GidType>, filter: &Filter) -> Result<(), GraphError> {
        use FilterOperator::*;
        use GidType::*;

        let Some(kind) = id_dtype else {
            return Ok(());
        };

        fn filter_value_kind(fv: &FilterValue) -> &'static str {
            match fv {
                FilterValue::ID(GID::U64(_)) => "U64",
                FilterValue::ID(GID::Str(_)) => "Str",
                FilterValue::IDSet(set) => {
                    if set.iter().all(|g| matches!(g, GID::U64(_))) {
                        "U64"
                    } else if set.iter().all(|g| matches!(g, GID::Str(_))) {
                        "Str"
                    } else {
                        "heterogeneous id set"
                    }
                }
                FilterValue::Single(_) => "Str",
                FilterValue::Set(_) => "Str",
            }
        }

        let value_matches_kind = |fv: &FilterValue, expect: GidType| -> bool {
            match (fv, expect) {
                (FilterValue::ID(GID::U64(_)), U64) => true,
                (FilterValue::IDSet(set), U64) => set.iter().all(|g| matches!(g, GID::U64(_))),

                (FilterValue::ID(GID::Str(_)), Str) => true,
                (FilterValue::IDSet(set), Str) => set.iter().all(|g| matches!(g, GID::Str(_))),
                (FilterValue::Single(_), Str) => true,
                (FilterValue::Set(_), Str) => true,

                _ => false,
            }
        };

        let op_allowed = match kind {
            U64 => matches!(
                filter.operator,
                Eq | Ne | Lt | Le | Gt | Ge | IsIn | IsNotIn
            ),
            Str => matches!(
                filter.operator,
                Eq | Ne
                    | StartsWith
                    | EndsWith
                    | Contains
                    | NotContains
                    | FuzzySearch { .. }
                    | IsIn
                    | IsNotIn
            ),
        };

        if !op_allowed {
            return Err(GraphError::InvalidGqlFilter(format!(
                "Operator {} not allowed for {:?} ID",
                filter.operator, kind
            )));
        }

        if !value_matches_kind(&filter.field_value, kind) {
            return Err(GraphError::InvalidGqlFilter(format!(
                "Filter value type does not match node ID type. Expected {:?} but got {:?}",
                kind,
                filter_value_kind(&filter.field_value)
            )));
        }

        match filter.operator {
            IsIn | IsNotIn => {
                if !matches!(
                    filter.field_value,
                    FilterValue::IDSet(_) | FilterValue::Set(_)
                ) {
                    return Err(GraphError::InvalidGqlFilter(
                        "IN/NOT_IN on ID expects a set of IDs".into(),
                    ));
                }
            }
            StartsWith | EndsWith | Contains | NotContains | FuzzySearch { .. } => {
                if !matches!(
                    filter.field_value,
                    FilterValue::ID(GID::Str(_)) | FilterValue::Single(_)
                ) {
                    return Err(GraphError::InvalidGqlFilter(
                        "String operators on ID expect a single string ID".into(),
                    ));
                }
            }
            Lt | Le | Gt | Ge => {
                if !matches!(filter.field_value, FilterValue::ID(GID::U64(_))) {
                    return Err(GraphError::InvalidGqlFilter(
                        "Numeric operators on ID expect a single numeric (u64) ID".into(),
                    ));
                }
            }
            IsSome | IsNone => {
                return Err(GraphError::InvalidGqlFilter(
                    "IsSome/IsNone are not supported as filter operators".into(),
                ));
            }
            Eq | Ne => {
                // Eq/Ne already type-checked above
            }
        }

        Ok(())
    }
}

impl PropertyFilterFactory<NodeFilter> for NodeFilter {}

impl TryAsCompositeFilter for NodeIdFilter {
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Ok(CompositeNodeFilter::Node(self.0.clone()))
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }
}

impl TryAsCompositeFilter for NodeNameFilter {
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Ok(CompositeNodeFilter::Node(self.0.clone()))
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }
}

impl TryAsCompositeFilter for NodeTypeFilter {
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Ok(CompositeNodeFilter::Node(self.0.clone()))
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }
}
