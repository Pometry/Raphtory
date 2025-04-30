use crate::{
    core::Prop,
    db::{
        api::{
            storage::graph::{
                edges::{edge_ref::EdgeStorageRef, edge_storage_ops::EdgeStorageOps},
                nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps},
            },
            view::internal::NodeFilterOps,
        },
        graph::views::filter::{
            internal::InternalNodeFilterOps,
            model::{
                edge_filter::{CompositeEdgeFilter, EdgeFieldFilter},
                filter_operator::FilterOperator,
                node_filter::{CompositeNodeFilter, NodeNameFilter, NodeTypeFilter},
                property_filter::{PropertyFilter, PropertyRef, Temporal},
            },
        },
    },
    prelude::{GraphViewOps, NodeViewOps},
};
use raphtory_api::core::entities::LayerIds;
use std::{collections::HashSet, fmt, fmt::Display, ops::Deref, sync::Arc};

pub mod edge_filter;
pub mod filter_operator;
pub mod node_filter;
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

    pub fn matches_node<'graph, G: GraphViewOps<'graph>>(
        &self,
        graph: &G,
        node_types_filter: &Arc<[bool]>,
        layer_ids: &LayerIds,
        node: NodeStorageRef,
    ) -> bool {
        match self.field_name.as_str() {
            "node_name" => self.matches(Some(&node.id().to_str())),
            "node_type" => {
                node_types_filter
                    .get(node.node_type_id())
                    .copied()
                    .unwrap_or(false)
                    && graph.filter_node(node, layer_ids)
            }
            _ => false,
        }
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

pub trait AsEdgeFilter: Send + Sync {
    fn as_edge_filter(&self) -> CompositeEdgeFilter;
}

impl<T: AsEdgeFilter + ?Sized> AsEdgeFilter for Arc<T> {
    fn as_edge_filter(&self) -> CompositeEdgeFilter {
        self.deref().as_edge_filter()
    }
}

impl AsNodeFilter for CompositeNodeFilter {
    fn as_node_filter(&self) -> CompositeNodeFilter {
        self.clone()
    }
}

impl AsEdgeFilter for CompositeEdgeFilter {
    fn as_edge_filter(&self) -> CompositeEdgeFilter {
        self.clone()
    }
}

impl AsNodeFilter for PropertyFilter {
    fn as_node_filter(&self) -> CompositeNodeFilter {
        CompositeNodeFilter::Property(self.clone())
    }
}

impl AsEdgeFilter for PropertyFilter {
    fn as_edge_filter(&self) -> CompositeEdgeFilter {
        CompositeEdgeFilter::Property(self.clone())
    }
}

impl AsNodeFilter for NodeNameFilter {
    fn as_node_filter(&self) -> CompositeNodeFilter {
        CompositeNodeFilter::Node(self.0.clone())
    }
}

impl AsNodeFilter for NodeTypeFilter {
    fn as_node_filter(&self) -> CompositeNodeFilter {
        CompositeNodeFilter::Node(self.0.clone())
    }
}

impl AsEdgeFilter for EdgeFieldFilter {
    fn as_edge_filter(&self) -> CompositeEdgeFilter {
        CompositeEdgeFilter::Edge(self.0.clone())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AndFilter<L, R> {
    pub(crate) left: L,
    pub(crate) right: R,
}

impl<L: Display, R: Display> Display for AndFilter<L, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({} AND {})", self.left, self.right)
    }
}

impl<L: AsNodeFilter, R: AsNodeFilter> AsNodeFilter for AndFilter<L, R> {
    fn as_node_filter(&self) -> CompositeNodeFilter {
        CompositeNodeFilter::And(
            Box::new(self.left.as_node_filter()),
            Box::new(self.right.as_node_filter()),
        )
    }
}

impl<L: AsEdgeFilter, R: AsEdgeFilter> AsEdgeFilter for AndFilter<L, R> {
    fn as_edge_filter(&self) -> CompositeEdgeFilter {
        CompositeEdgeFilter::And(
            Box::new(self.left.as_edge_filter()),
            Box::new(self.right.as_edge_filter()),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrFilter<L, R> {
    pub(crate) left: L,
    pub(crate) right: R,
}

impl<L: Display, R: Display> Display for OrFilter<L, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({} OR {})", self.left, self.right)
    }
}

impl<L: AsNodeFilter, R: AsNodeFilter> AsNodeFilter for OrFilter<L, R> {
    fn as_node_filter(&self) -> CompositeNodeFilter {
        CompositeNodeFilter::Or(
            Box::new(self.left.as_node_filter()),
            Box::new(self.right.as_node_filter()),
        )
    }
}

impl<L: AsEdgeFilter, R: AsEdgeFilter> AsEdgeFilter for OrFilter<L, R> {
    fn as_edge_filter(&self) -> CompositeEdgeFilter {
        CompositeEdgeFilter::Or(
            Box::new(self.left.as_edge_filter()),
            Box::new(self.right.as_edge_filter()),
        )
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
}

impl ComposableFilter for PropertyFilter {}
impl ComposableFilter for NodeNameFilter {}
impl ComposableFilter for NodeTypeFilter {}
impl ComposableFilter for EdgeFieldFilter {}
impl<L, R> ComposableFilter for AndFilter<L, R> {}
impl<L, R> ComposableFilter for OrFilter<L, R> {}

pub trait InternalPropertyFilterOps: Send + Sync {
    fn property_ref(&self) -> PropertyRef;
}

impl<T: InternalPropertyFilterOps> InternalPropertyFilterOps for Arc<T> {
    fn property_ref(&self) -> PropertyRef {
        self.deref().property_ref()
    }
}

pub trait PropertyFilterOps {
    fn eq(&self, value: impl Into<Prop>) -> PropertyFilter;

    fn ne(&self, value: impl Into<Prop>) -> PropertyFilter;

    fn le(&self, value: impl Into<Prop>) -> PropertyFilter;

    fn ge(&self, value: impl Into<Prop>) -> PropertyFilter;

    fn lt(&self, value: impl Into<Prop>) -> PropertyFilter;

    fn gt(&self, value: impl Into<Prop>) -> PropertyFilter;

    fn is_in(&self, values: impl IntoIterator<Item = Prop>) -> PropertyFilter;

    fn is_not_in(&self, values: impl IntoIterator<Item = Prop>) -> PropertyFilter;

    fn is_none(&self) -> PropertyFilter;

    fn is_some(&self) -> PropertyFilter;

    fn contains(&self, value: impl Into<Prop>) -> PropertyFilter;

    fn not_contains(&self, value: impl Into<Prop>) -> PropertyFilter;

    fn fuzzy_search(
        &self,
        prop_value: impl Into<String>,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> PropertyFilter;
}

impl<T: ?Sized + InternalPropertyFilterOps> PropertyFilterOps for T {
    fn eq(&self, value: impl Into<Prop>) -> PropertyFilter {
        PropertyFilter::eq(self.property_ref(), value.into())
    }

    fn ne(&self, value: impl Into<Prop>) -> PropertyFilter {
        PropertyFilter::ne(self.property_ref(), value.into())
    }

    fn le(&self, value: impl Into<Prop>) -> PropertyFilter {
        PropertyFilter::le(self.property_ref(), value.into())
    }

    fn ge(&self, value: impl Into<Prop>) -> PropertyFilter {
        PropertyFilter::ge(self.property_ref(), value.into())
    }

    fn lt(&self, value: impl Into<Prop>) -> PropertyFilter {
        PropertyFilter::lt(self.property_ref(), value.into())
    }

    fn gt(&self, value: impl Into<Prop>) -> PropertyFilter {
        PropertyFilter::gt(self.property_ref(), value.into())
    }

    fn is_in(&self, values: impl IntoIterator<Item = Prop>) -> PropertyFilter {
        PropertyFilter::is_in(self.property_ref(), values.into_iter())
    }

    fn is_not_in(&self, values: impl IntoIterator<Item = Prop>) -> PropertyFilter {
        PropertyFilter::is_not_in(self.property_ref(), values.into_iter())
    }

    fn is_none(&self) -> PropertyFilter {
        PropertyFilter::is_none(self.property_ref())
    }

    fn is_some(&self) -> PropertyFilter {
        PropertyFilter::is_some(self.property_ref())
    }

    fn contains(&self, value: impl Into<Prop>) -> PropertyFilter {
        PropertyFilter::contains(self.property_ref(), value.into())
    }

    fn not_contains(&self, value: impl Into<Prop>) -> PropertyFilter {
        PropertyFilter::not_contains(self.property_ref(), value.into())
    }

    fn fuzzy_search(
        &self,
        prop_value: impl Into<String>,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> PropertyFilter {
        PropertyFilter::fuzzy_search(
            self.property_ref(),
            prop_value.into(),
            levenshtein_distance,
            prefix_match,
        )
    }
}

#[derive(Clone)]
pub struct PropertyFilterBuilder(pub String);

impl PropertyFilterBuilder {
    pub fn constant(self) -> ConstPropertyFilterBuilder {
        ConstPropertyFilterBuilder(self.0)
    }

    pub fn temporal(self) -> TemporalPropertyFilterBuilder {
        TemporalPropertyFilterBuilder(self.0)
    }
}

impl InternalPropertyFilterOps for PropertyFilterBuilder {
    fn property_ref(&self) -> PropertyRef {
        PropertyRef::Property(self.0.clone())
    }
}

#[derive(Clone)]
pub struct ConstPropertyFilterBuilder(pub String);

impl InternalPropertyFilterOps for ConstPropertyFilterBuilder {
    fn property_ref(&self) -> PropertyRef {
        PropertyRef::ConstantProperty(self.0.clone())
    }
}

#[derive(Clone)]
pub struct AnyTemporalPropertyFilterBuilder(pub String);

impl InternalPropertyFilterOps for AnyTemporalPropertyFilterBuilder {
    fn property_ref(&self) -> PropertyRef {
        PropertyRef::TemporalProperty(self.0.clone(), Temporal::Any)
    }
}

#[derive(Clone)]
pub struct LatestTemporalPropertyFilterBuilder(pub String);

impl InternalPropertyFilterOps for LatestTemporalPropertyFilterBuilder {
    fn property_ref(&self) -> PropertyRef {
        PropertyRef::TemporalProperty(self.0.clone(), Temporal::Latest)
    }
}

#[derive(Clone)]
pub struct TemporalPropertyFilterBuilder(pub String);

impl TemporalPropertyFilterBuilder {
    pub fn any(self) -> AnyTemporalPropertyFilterBuilder {
        AnyTemporalPropertyFilterBuilder(self.0)
    }

    pub fn latest(self) -> LatestTemporalPropertyFilterBuilder {
        LatestTemporalPropertyFilterBuilder(self.0)
    }
}

impl PropertyFilter {
    pub fn property(name: impl AsRef<str>) -> PropertyFilterBuilder {
        PropertyFilterBuilder(name.as_ref().to_string())
    }
}

pub trait InternalNodeFilterBuilderOps: Send + Sync {
    type NodeFilterType: From<Filter> + InternalNodeFilterOps + AsNodeFilter + Clone + 'static;

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

#[derive(Clone)]
pub struct NodeFilter;

impl NodeFilter {
    pub fn name() -> NodeNameFilterBuilder {
        NodeNameFilterBuilder
    }

    pub fn node_type() -> NodeTypeFilterBuilder {
        NodeTypeFilterBuilder
    }
}

pub trait InternalEdgeFilterBuilderOps: Send + Sync {
    fn field_name(&self) -> &'static str;
}

impl<T: InternalEdgeFilterBuilderOps> InternalEdgeFilterBuilderOps for Arc<T> {
    fn field_name(&self) -> &'static str {
        self.deref().field_name()
    }
}

pub trait EdgeFilterOps {
    fn eq(&self, value: impl Into<String>) -> EdgeFieldFilter;

    fn ne(&self, value: impl Into<String>) -> EdgeFieldFilter;

    fn is_in(&self, values: impl IntoIterator<Item = String>) -> EdgeFieldFilter;

    fn is_not_in(&self, values: impl IntoIterator<Item = String>) -> EdgeFieldFilter;

    fn contains(&self, value: impl Into<String>) -> EdgeFieldFilter;

    fn not_contains(&self, value: impl Into<String>) -> EdgeFieldFilter;

    fn fuzzy_search(
        &self,
        value: impl Into<String>,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> EdgeFieldFilter;
}

impl<T: ?Sized + InternalEdgeFilterBuilderOps> EdgeFilterOps for T {
    fn eq(&self, value: impl Into<String>) -> EdgeFieldFilter {
        EdgeFieldFilter(Filter::eq(self.field_name(), value))
    }

    fn ne(&self, value: impl Into<String>) -> EdgeFieldFilter {
        EdgeFieldFilter(Filter::ne(self.field_name(), value))
    }

    fn is_in(&self, values: impl IntoIterator<Item = String>) -> EdgeFieldFilter {
        EdgeFieldFilter(Filter::is_in(self.field_name(), values))
    }

    fn is_not_in(&self, values: impl IntoIterator<Item = String>) -> EdgeFieldFilter {
        EdgeFieldFilter(Filter::is_not_in(self.field_name(), values))
    }

    fn contains(&self, value: impl Into<String>) -> EdgeFieldFilter {
        EdgeFieldFilter(Filter::contains(self.field_name(), value.into()))
    }

    fn not_contains(&self, value: impl Into<String>) -> EdgeFieldFilter {
        EdgeFieldFilter(Filter::not_contains(self.field_name(), value.into()))
    }

    fn fuzzy_search(
        &self,
        value: impl Into<String>,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> EdgeFieldFilter {
        EdgeFieldFilter(Filter::fuzzy_search(
            self.field_name(),
            value,
            levenshtein_distance,
            prefix_match,
        ))
    }
}

pub struct EdgeSourceFilterBuilder;

impl InternalEdgeFilterBuilderOps for EdgeSourceFilterBuilder {
    fn field_name(&self) -> &'static str {
        "src"
    }
}

pub struct EdgeDestinationFilterBuilder;

impl InternalEdgeFilterBuilderOps for EdgeDestinationFilterBuilder {
    fn field_name(&self) -> &'static str {
        "dst"
    }
}

#[derive(Clone)]
pub struct EdgeFilter;

#[derive(Clone)]
pub enum EdgeEndpointFilter {
    Src,
    Dst,
}

impl EdgeEndpointFilter {
    pub fn name(&self) -> Arc<dyn InternalEdgeFilterBuilderOps> {
        match self {
            EdgeEndpointFilter::Src => Arc::new(EdgeSourceFilterBuilder),
            EdgeEndpointFilter::Dst => Arc::new(EdgeDestinationFilterBuilder),
        }
    }
}

impl EdgeFilter {
    pub fn src() -> EdgeEndpointFilter {
        EdgeEndpointFilter::Src
    }
    pub fn dst() -> EdgeEndpointFilter {
        EdgeEndpointFilter::Dst
    }
}
