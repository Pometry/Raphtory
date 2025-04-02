use crate::{
    core::{
        entities::properties::props::Meta, sort_comparable_props, utils::errors::GraphError, Prop,
    },
    db::{
        api::storage::graph::nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps},
        graph::node::NodeView,
    },
    prelude::{GraphViewOps, NodeViewOps},
};
use itertools::Itertools;
use raphtory_api::core::storage::arc_str::{ArcStr, OptionAsStr};
use std::{
    collections::HashSet,
    fmt,
    fmt::{Debug, Display, Formatter},
    ops::Deref,
    sync::Arc,
};
use strsim::levenshtein;

pub mod edge_and_filtered_graph;
pub mod edge_field_filtered_graph;
pub mod edge_or_filtered_graph;
pub mod edge_property_filtered_graph;
pub mod exploded_edge_property_filter;
pub(crate) mod internal;
pub mod node_and_filtered_graph;
pub mod node_field_filtered_graph;
pub mod node_or_filtered_graph;
pub mod node_property_filtered_graph;

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

impl Display for FilterOperator {
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
pub enum Temporal {
    Any,
    Latest,
}

#[derive(Debug, Clone)]
pub enum PropertyRef {
    Property(String),
    ConstantProperty(String),
    TemporalProperty(String, Temporal),
}

impl Display for PropertyRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PropertyRef::TemporalProperty(name, temporal) => {
                write!(f, "TemporalProperty({}, {:?})", name, temporal)
            }
            PropertyRef::ConstantProperty(name) => write!(f, "ConstantProperty({})", name),
            PropertyRef::Property(name) => write!(f, "Property({})", name),
        }
    }
}

impl PropertyRef {
    pub fn name(&self) -> &str {
        match self {
            PropertyRef::Property(name)
            | PropertyRef::ConstantProperty(name)
            | PropertyRef::TemporalProperty(name, _) => name,
        }
    }
}

#[derive(Debug, Clone)]
pub enum PropertyFilterValue {
    None,
    Single(Prop),
    Set(Arc<HashSet<Prop>>),
}

impl From<Option<Prop>> for PropertyFilterValue {
    fn from(prop: Option<Prop>) -> Self {
        prop.map_or(PropertyFilterValue::None, |v| {
            PropertyFilterValue::Single(v)
        })
    }
}

#[derive(Debug, Clone)]
pub struct PropertyFilter {
    pub prop_ref: PropertyRef,
    pub prop_value: PropertyFilterValue,
    pub operator: FilterOperator,
}

impl Display for PropertyFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let prop_ref_str = match &self.prop_ref {
            PropertyRef::Property(name) => format!("{}", name),
            PropertyRef::ConstantProperty(name) => format!("const({})", name),
            PropertyRef::TemporalProperty(name, Temporal::Any) => format!("temporal_any({})", name),
            PropertyRef::TemporalProperty(name, Temporal::Latest) => {
                format!("temporal_latest({})", name)
            }
        };

        match &self.prop_value {
            PropertyFilterValue::None => {
                write!(f, "{} {}", prop_ref_str, self.operator)
            }
            PropertyFilterValue::Single(value) => {
                write!(f, "{} {} {}", prop_ref_str, self.operator, value)
            }
            PropertyFilterValue::Set(values) => {
                let sorted_values = sort_comparable_props(values.iter().collect_vec());
                let values_str = sorted_values
                    .iter()
                    .map(|v| format!("{}", v))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "{} {} [{}]", prop_ref_str, self.operator, values_str)
            }
        }
    }
}

impl PropertyFilter {
    pub fn eq(prop_ref: PropertyRef, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_ref,
            prop_value: PropertyFilterValue::Single(prop_value.into()),
            operator: FilterOperator::Eq,
        }
    }

    pub fn ne(prop_ref: PropertyRef, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_ref,
            prop_value: PropertyFilterValue::Single(prop_value.into()),
            operator: FilterOperator::Ne,
        }
    }

    pub fn le(prop_ref: PropertyRef, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_ref,
            prop_value: PropertyFilterValue::Single(prop_value.into()),
            operator: FilterOperator::Le,
        }
    }

    pub fn ge(prop_ref: PropertyRef, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_ref,
            prop_value: PropertyFilterValue::Single(prop_value.into()),
            operator: FilterOperator::Ge,
        }
    }

    pub fn lt(prop_ref: PropertyRef, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_ref,
            prop_value: PropertyFilterValue::Single(prop_value.into()),
            operator: FilterOperator::Lt,
        }
    }

    pub fn gt(prop_ref: PropertyRef, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_ref,
            prop_value: PropertyFilterValue::Single(prop_value.into()),
            operator: FilterOperator::Gt,
        }
    }

    pub fn includes(prop_ref: PropertyRef, prop_values: impl IntoIterator<Item = Prop>) -> Self {
        Self {
            prop_ref,
            prop_value: PropertyFilterValue::Set(Arc::new(prop_values.into_iter().collect())),
            operator: FilterOperator::In,
        }
    }

    pub fn excludes(prop_ref: PropertyRef, prop_values: impl IntoIterator<Item = Prop>) -> Self {
        Self {
            prop_ref,
            prop_value: PropertyFilterValue::Set(Arc::new(prop_values.into_iter().collect())),
            operator: FilterOperator::NotIn,
        }
    }

    pub fn is_none(prop_ref: PropertyRef) -> Self {
        Self {
            prop_ref,
            prop_value: PropertyFilterValue::None,
            operator: FilterOperator::IsNone,
        }
    }

    pub fn is_some(prop_ref: PropertyRef) -> Self {
        Self {
            prop_ref,
            prop_value: PropertyFilterValue::None,
            operator: FilterOperator::IsSome,
        }
    }

    pub fn fuzzy_search(
        prop_ref: PropertyRef,
        prop_value: impl Into<String>,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> Self {
        Self {
            prop_ref,
            prop_value: PropertyFilterValue::Single(Prop::Str(ArcStr::from(prop_value.into()))),
            operator: FilterOperator::FuzzySearch {
                levenshtein_distance,
                prefix_match,
            },
        }
    }

    pub fn resolve_temporal_prop_ids(&self, meta: &Meta) -> Result<Option<usize>, GraphError> {
        let prop_name = self.prop_ref.name();
        if let PropertyFilterValue::Single(value) = &self.prop_value {
            Ok(meta
                .temporal_prop_meta()
                .get_and_validate(prop_name, value.dtype())?)
        } else {
            Ok(meta.temporal_prop_meta().get_id(prop_name))
        }
    }

    pub fn resolve_constant_prop_ids(&self, meta: &Meta) -> Result<Option<usize>, GraphError> {
        let prop_name = self.prop_ref.name();
        if let PropertyFilterValue::Single(value) = &self.prop_value {
            Ok(meta
                .const_prop_meta()
                .get_and_validate(prop_name, value.dtype())?)
        } else {
            Ok(meta.const_prop_meta().get_id(prop_name))
        }
    }

    pub fn matches(&self, other: Option<&Prop>) -> bool {
        let value = &self.prop_value;
        self.operator.apply_to_property(value, other)
    }

    pub fn matches_node<'graph, G: GraphViewOps<'graph>>(
        &self,
        graph: &G,
        t_prop_id: Option<usize>,
        c_prop_id: Option<usize>,
        node: NodeStorageRef,
    ) -> bool {
        let props = NodeView::new_internal(graph, node.vid()).properties();
        let prop_value = t_prop_id
            .and_then(|prop_id| {
                props
                    .temporal()
                    .get_by_id(prop_id)
                    .and_then(|prop_view| prop_view.latest())
            })
            .or_else(|| c_prop_id.and_then(|prop_id| props.constant().get_by_id(prop_id)));
        self.matches(prop_value.as_ref())
    }
}

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

#[derive(Debug, Clone)]
pub struct NodeFieldFilter(pub Filter);

impl Display for NodeFieldFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone)]
pub struct EdgeFieldFilter(pub Filter);

impl Display for EdgeFieldFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
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

    pub fn includes(
        field_name: impl Into<String>,
        field_values: impl IntoIterator<Item = String>,
    ) -> Self {
        Self {
            field_name: field_name.into(),
            field_value: FilterValue::Set(Arc::new(field_values.into_iter().collect())),
            operator: FilterOperator::In,
        }
    }

    pub fn excludes(
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

    pub fn matches_node<'graph, G: GraphViewOps<'graph>>(
        &self,
        graph: &G,
        node: NodeStorageRef,
    ) -> bool {
        match self.field_name.as_str() {
            "node_name" => self.matches(node.name().as_str()),
            "node_type" => self.matches(graph.node_type(node.vid()).as_deref()),
            _ => false,
        }
    }
}

#[derive(Debug, Clone)]
pub enum CompositeNodeFilter {
    Node(Filter),
    Property(PropertyFilter),
    And(Box<CompositeNodeFilter>, Box<CompositeNodeFilter>),
    Or(Box<CompositeNodeFilter>, Box<CompositeNodeFilter>),
}

impl Display for CompositeNodeFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompositeNodeFilter::Property(filter) => write!(f, "{}", filter),
            CompositeNodeFilter::Node(filter) => write!(f, "{}", filter),
            CompositeNodeFilter::And(left, right) => write!(f, "({} AND {})", left, right),
            CompositeNodeFilter::Or(left, right) => write!(f, "({} OR {})", left, right),
        }
    }
}

#[derive(Debug, Clone)]
pub enum CompositeEdgeFilter {
    Edge(Filter),
    Property(PropertyFilter),
    And(Box<CompositeEdgeFilter>, Box<CompositeEdgeFilter>),
    Or(Box<CompositeEdgeFilter>, Box<CompositeEdgeFilter>),
}

impl Display for CompositeEdgeFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompositeEdgeFilter::Property(filter) => write!(f, "{}", filter),
            CompositeEdgeFilter::Edge(filter) => write!(f, "{}", filter),
            CompositeEdgeFilter::And(left, right) => write!(f, "({} AND {})", left, right),
            CompositeEdgeFilter::Or(left, right) => write!(f, "({} OR {})", left, right),
        }
    }
}

// Fluent Composite Filter Builder APIs
pub trait IntoNodeFilter {
    fn into_node_filter(self) -> CompositeNodeFilter;
}

pub trait IntoEdgeFilter {
    fn into_edge_filter(self) -> CompositeEdgeFilter;
}

impl IntoNodeFilter for CompositeNodeFilter {
    fn into_node_filter(self) -> CompositeNodeFilter {
        self
    }
}

impl IntoEdgeFilter for CompositeEdgeFilter {
    fn into_edge_filter(self) -> CompositeEdgeFilter {
        self
    }
}

impl IntoNodeFilter for PropertyFilter {
    fn into_node_filter(self) -> CompositeNodeFilter {
        CompositeNodeFilter::Property(self)
    }
}

impl IntoEdgeFilter for PropertyFilter {
    fn into_edge_filter(self) -> CompositeEdgeFilter {
        CompositeEdgeFilter::Property(self)
    }
}

impl IntoNodeFilter for NodeFieldFilter {
    fn into_node_filter(self) -> CompositeNodeFilter {
        CompositeNodeFilter::Node(self.0)
    }
}

impl IntoEdgeFilter for EdgeFieldFilter {
    fn into_edge_filter(self) -> CompositeEdgeFilter {
        CompositeEdgeFilter::Edge(self.0)
    }
}

#[derive(Debug, Clone)]
pub struct AndFilter<L, R> {
    left: L,
    right: R,
}

impl<L: Display, R: Display> Display for AndFilter<L, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({} AND {})", self.left, self.right)
    }
}

impl<L: IntoNodeFilter, R: IntoNodeFilter> IntoNodeFilter for AndFilter<L, R> {
    fn into_node_filter(self) -> CompositeNodeFilter {
        CompositeNodeFilter::And(
            Box::new(self.left.into_node_filter()),
            Box::new(self.right.into_node_filter()),
        )
    }
}

impl<L: IntoEdgeFilter, R: IntoEdgeFilter> IntoEdgeFilter for AndFilter<L, R> {
    fn into_edge_filter(self) -> CompositeEdgeFilter {
        CompositeEdgeFilter::And(
            Box::new(self.left.into_edge_filter()),
            Box::new(self.right.into_edge_filter()),
        )
    }
}

#[derive(Debug, Clone)]
pub struct OrFilter<L, R> {
    left: L,
    right: R,
}

impl<L: Display, R: Display> Display for OrFilter<L, R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({} OR {})", self.left, self.right)
    }
}

impl<L: IntoNodeFilter, R: IntoNodeFilter> IntoNodeFilter for OrFilter<L, R> {
    fn into_node_filter(self) -> CompositeNodeFilter {
        CompositeNodeFilter::Or(
            Box::new(self.left.into_node_filter()),
            Box::new(self.right.into_node_filter()),
        )
    }
}

impl<L: IntoEdgeFilter, R: IntoEdgeFilter> IntoEdgeFilter for OrFilter<L, R> {
    fn into_edge_filter(self) -> CompositeEdgeFilter {
        CompositeEdgeFilter::Or(
            Box::new(self.left.into_edge_filter()),
            Box::new(self.right.into_edge_filter()),
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
impl ComposableFilter for NodeFieldFilter {}
impl ComposableFilter for EdgeFieldFilter {}
impl<L, R> ComposableFilter for AndFilter<L, R> {}
impl<L, R> ComposableFilter for OrFilter<L, R> {}

// pub fn resolve_as_property_filter(filter: FilterExpr) -> Result<PropertyFilter, GraphError> {
//     match filter {
//         FilterExpr::Property(prop) => Ok(prop),
//         _ => Err(GraphError::IllegalFilterExpr(
//             filter,
//             "Non-property filter cannot be used in strictly property filtering!".to_string(),
//         )),
//     }
// }

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

    fn includes(&self, values: impl IntoIterator<Item = Prop>) -> PropertyFilter;

    fn excludes(&self, values: impl IntoIterator<Item = Prop>) -> PropertyFilter;

    fn is_none(&self) -> PropertyFilter;

    fn is_some(&self) -> PropertyFilter;

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

    fn includes(&self, values: impl IntoIterator<Item = Prop>) -> PropertyFilter {
        PropertyFilter::includes(self.property_ref(), values.into_iter())
    }

    fn excludes(&self, values: impl IntoIterator<Item = Prop>) -> PropertyFilter {
        PropertyFilter::excludes(self.property_ref(), values.into_iter())
    }

    fn is_none(&self) -> PropertyFilter {
        PropertyFilter::is_none(self.property_ref())
    }

    fn is_some(&self) -> PropertyFilter {
        PropertyFilter::is_some(self.property_ref())
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

pub trait InternalNodeFilterOps: Send + Sync {
    fn field_name(&self) -> &'static str;
}

impl<T: InternalNodeFilterOps> InternalNodeFilterOps for Arc<T> {
    fn field_name(&self) -> &'static str {
        self.deref().field_name()
    }
}

pub trait NodeFilterOps {
    fn eq(&self, value: impl Into<String>) -> NodeFieldFilter;

    fn ne(&self, value: impl Into<String>) -> NodeFieldFilter;

    fn includes(&self, values: impl IntoIterator<Item = String>) -> NodeFieldFilter;

    fn excludes(&self, values: impl IntoIterator<Item = String>) -> NodeFieldFilter;
    fn fuzzy_search(
        &self,
        value: impl Into<String>,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> NodeFieldFilter;
}

impl<T: ?Sized + InternalNodeFilterOps> NodeFilterOps for T {
    fn eq(&self, value: impl Into<String>) -> NodeFieldFilter {
        NodeFieldFilter(Filter::eq(self.field_name(), value))
    }

    fn ne(&self, value: impl Into<String>) -> NodeFieldFilter {
        NodeFieldFilter(Filter::ne(self.field_name(), value))
    }

    fn includes(&self, values: impl IntoIterator<Item = String>) -> NodeFieldFilter {
        NodeFieldFilter(Filter::includes(self.field_name(), values))
    }

    fn excludes(&self, values: impl IntoIterator<Item = String>) -> NodeFieldFilter {
        NodeFieldFilter(Filter::excludes(self.field_name(), values))
    }

    fn fuzzy_search(
        &self,
        value: impl Into<String>,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> NodeFieldFilter {
        NodeFieldFilter(Filter::fuzzy_search(
            self.field_name(),
            value,
            levenshtein_distance,
            prefix_match,
        ))
    }
}

pub struct NodeNameFilterBuilder;

impl InternalNodeFilterOps for NodeNameFilterBuilder {
    fn field_name(&self) -> &'static str {
        "node_name"
    }
}

pub struct NodeTypeFilterBuilder;

impl InternalNodeFilterOps for NodeTypeFilterBuilder {
    fn field_name(&self) -> &'static str {
        "node_type"
    }
}

#[derive(Clone)]
pub struct NodeFilter;

impl NodeFilter {
    pub fn node_name() -> NodeNameFilterBuilder {
        NodeNameFilterBuilder
    }

    pub fn node_type() -> NodeTypeFilterBuilder {
        NodeTypeFilterBuilder
    }
}

pub trait InternalEdgeFilterOps: Send + Sync {
    fn field_name(&self) -> &'static str;
}

impl<T: InternalEdgeFilterOps> InternalEdgeFilterOps for Arc<T> {
    fn field_name(&self) -> &'static str {
        self.deref().field_name()
    }
}

pub trait EdgeFilterOps {
    fn eq(&self, value: impl Into<String>) -> EdgeFieldFilter;

    fn ne(&self, value: impl Into<String>) -> EdgeFieldFilter;

    fn includes(&self, values: impl IntoIterator<Item = String>) -> EdgeFieldFilter;

    fn excludes(&self, values: impl IntoIterator<Item = String>) -> EdgeFieldFilter;

    fn fuzzy_search(
        &self,
        value: impl Into<String>,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> EdgeFieldFilter;
}

impl<T: ?Sized + InternalEdgeFilterOps> EdgeFilterOps for T {
    fn eq(&self, value: impl Into<String>) -> EdgeFieldFilter {
        EdgeFieldFilter(Filter::eq(self.field_name(), value))
    }

    fn ne(&self, value: impl Into<String>) -> EdgeFieldFilter {
        EdgeFieldFilter(Filter::ne(self.field_name(), value))
    }

    fn includes(&self, values: impl IntoIterator<Item = String>) -> EdgeFieldFilter {
        EdgeFieldFilter(Filter::includes(self.field_name(), values))
    }

    fn excludes(&self, values: impl IntoIterator<Item = String>) -> EdgeFieldFilter {
        EdgeFieldFilter(Filter::excludes(self.field_name(), values))
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

impl InternalEdgeFilterOps for EdgeSourceFilterBuilder {
    fn field_name(&self) -> &'static str {
        "src"
    }
}

pub struct EdgeDestinationFilterBuilder;

impl InternalEdgeFilterOps for EdgeDestinationFilterBuilder {
    fn field_name(&self) -> &'static str {
        "dst"
    }
}

#[derive(Clone)]
pub struct EdgeFilter;

impl EdgeFilter {
    pub fn src() -> EdgeSourceFilterBuilder {
        EdgeSourceFilterBuilder
    }

    pub fn dst() -> EdgeDestinationFilterBuilder {
        EdgeDestinationFilterBuilder
    }
}

#[cfg(test)]
mod test_fluent_builder_apis {
    use crate::{
        db::graph::views::filter::{
            ComposableFilter, CompositeEdgeFilter, CompositeNodeFilter, EdgeFilter, EdgeFilterOps,
            Filter, IntoEdgeFilter, IntoNodeFilter, NodeFilter, NodeFilterOps, PropertyFilterOps,
            PropertyRef, Temporal,
        },
        prelude::PropertyFilter,
    };

    #[test]
    fn test_node_property_filter_build() {
        let filter_expr = PropertyFilter::property("p").eq("raphtory");
        let node_property_filter = filter_expr.into_node_filter();
        let node_property_filter2 = CompositeNodeFilter::Property(PropertyFilter::eq(
            PropertyRef::Property("p".to_string()),
            "raphtory",
        ));
        assert_eq!(
            node_property_filter.to_string(),
            node_property_filter2.to_string()
        );
    }

    #[test]
    fn test_node_const_property_filter_build() {
        let filter_expr = PropertyFilter::property("p").constant().eq("raphtory");
        let node_property_filter = filter_expr.into_node_filter();
        let node_property_filter2 = CompositeNodeFilter::Property(PropertyFilter::eq(
            PropertyRef::ConstantProperty("p".to_string()),
            "raphtory",
        ));
        assert_eq!(
            node_property_filter.to_string(),
            node_property_filter2.to_string()
        );
    }

    #[test]
    fn test_node_any_temporal_property_filter_build() {
        let filter_expr = PropertyFilter::property("p")
            .temporal()
            .any()
            .eq("raphtory");
        let node_property_filter = filter_expr.into_node_filter();
        let node_property_filter2 = CompositeNodeFilter::Property(PropertyFilter::eq(
            PropertyRef::TemporalProperty("p".to_string(), Temporal::Any),
            "raphtory",
        ));
        assert_eq!(
            node_property_filter.to_string(),
            node_property_filter2.to_string()
        );
    }

    #[test]
    fn test_node_latest_temporal_property_filter_build() {
        let filter_expr = PropertyFilter::property("p")
            .temporal()
            .latest()
            .eq("raphtory");
        let node_property_filter = filter_expr.into_node_filter();
        let node_property_filter2 = CompositeNodeFilter::Property(PropertyFilter::eq(
            PropertyRef::TemporalProperty("p".to_string(), Temporal::Latest),
            "raphtory",
        ));
        assert_eq!(
            node_property_filter.to_string(),
            node_property_filter2.to_string()
        );
    }

    #[test]
    fn test_node_name_filter_build() {
        let filter_expr = NodeFilter::node_name().eq("raphtory");
        let node_property_filter = filter_expr.into_node_filter();
        let node_property_filter2 = CompositeNodeFilter::Node(Filter::eq("node_name", "raphtory"));
        assert_eq!(
            node_property_filter.to_string(),
            node_property_filter2.to_string()
        );
    }

    #[test]
    fn test_node_type_filter_build() {
        let filter_expr = NodeFilter::node_type().eq("raphtory");
        let node_property_filter = filter_expr.into_node_filter();
        let node_property_filter2 = CompositeNodeFilter::Node(Filter::eq("node_type", "raphtory"));
        assert_eq!(
            node_property_filter.to_string(),
            node_property_filter2.to_string()
        );
    }

    #[test]
    fn test_node_filter_composition() {
        let node_composite_filter = NodeFilter::node_name()
            .eq("fire_nation")
            .and(PropertyFilter::property("p2").constant().eq(2u64))
            .and(PropertyFilter::property("p1").eq(1u64))
            .and(
                PropertyFilter::property("p3")
                    .temporal()
                    .any()
                    .eq(5u64)
                    .or(PropertyFilter::property("p4").temporal().latest().eq(7u64)),
            )
            .or(NodeFilter::node_type().eq("raphtory"))
            .or(PropertyFilter::property("p5").eq(9u64));

        let node_composite_filter2 = CompositeNodeFilter::Or(
            Box::new(CompositeNodeFilter::Or(
                Box::new(CompositeNodeFilter::And(
                    Box::new(CompositeNodeFilter::And(
                        Box::new(CompositeNodeFilter::And(
                            Box::new(CompositeNodeFilter::Node(Filter::eq(
                                "node_name",
                                "fire_nation",
                            ))),
                            Box::new(CompositeNodeFilter::Property(PropertyFilter::eq(
                                PropertyRef::ConstantProperty("p2".to_string()),
                                2u64,
                            ))),
                        )),
                        Box::new(CompositeNodeFilter::Property(PropertyFilter::eq(
                            PropertyRef::Property("p1".to_string()),
                            1u64,
                        ))),
                    )),
                    Box::new(CompositeNodeFilter::Or(
                        Box::new(CompositeNodeFilter::Property(PropertyFilter::eq(
                            PropertyRef::TemporalProperty("p3".to_string(), Temporal::Any),
                            5u64,
                        ))),
                        Box::new(CompositeNodeFilter::Property(PropertyFilter::eq(
                            PropertyRef::TemporalProperty("p4".to_string(), Temporal::Latest),
                            7u64,
                        ))),
                    )),
                )),
                Box::new(CompositeNodeFilter::Node(Filter::eq(
                    "node_type",
                    "raphtory",
                ))),
            )),
            Box::new(CompositeNodeFilter::Property(PropertyFilter::eq(
                PropertyRef::Property("p5".to_string()),
                9u64,
            ))),
        );

        assert_eq!(
            node_composite_filter.to_string(),
            node_composite_filter2.to_string()
        );
    }

    #[test]
    fn test_edge_src_filter_build() {
        let filter_expr = EdgeFilter::src().eq("raphtory");
        let edge_property_filter = filter_expr.into_edge_filter();
        let edge_property_filter2 = CompositeEdgeFilter::Edge(Filter::eq("src", "raphtory"));
        assert_eq!(
            edge_property_filter.to_string(),
            edge_property_filter2.to_string()
        );
    }

    #[test]
    fn test_edge_dst_filter_build() {
        let filter_expr = EdgeFilter::dst().eq("raphtory");
        let edge_property_filter = filter_expr.into_edge_filter();
        let edge_property_filter2 = CompositeEdgeFilter::Edge(Filter::eq("dst", "raphtory"));
        assert_eq!(
            edge_property_filter.to_string(),
            edge_property_filter2.to_string()
        );
    }

    #[test]
    fn test_edge_filter_composition() {
        let edge_composite_filter = EdgeFilter::src()
            .eq("fire_nation")
            .and(PropertyFilter::property("p2").constant().eq(2u64))
            .and(PropertyFilter::property("p1").eq(1u64))
            .and(
                PropertyFilter::property("p3")
                    .temporal()
                    .any()
                    .eq(5u64)
                    .or(PropertyFilter::property("p4").temporal().latest().eq(7u64)),
            )
            .or(EdgeFilter::src().eq("raphtory"))
            .or(PropertyFilter::property("p5").eq(9u64));

        let edge_composite_filter2 = CompositeEdgeFilter::Or(
            Box::new(CompositeEdgeFilter::Or(
                Box::new(CompositeEdgeFilter::And(
                    Box::new(CompositeEdgeFilter::And(
                        Box::new(CompositeEdgeFilter::And(
                            Box::new(CompositeEdgeFilter::Edge(Filter::eq("src", "fire_nation"))),
                            Box::new(CompositeEdgeFilter::Property(PropertyFilter::eq(
                                PropertyRef::ConstantProperty("p2".into()),
                                2u64,
                            ))),
                        )),
                        Box::new(CompositeEdgeFilter::Property(PropertyFilter::eq(
                            PropertyRef::Property("p1".into()),
                            1u64,
                        ))),
                    )),
                    Box::new(CompositeEdgeFilter::Or(
                        Box::new(CompositeEdgeFilter::Property(PropertyFilter::eq(
                            PropertyRef::TemporalProperty("p3".into(), Temporal::Any),
                            5u64,
                        ))),
                        Box::new(CompositeEdgeFilter::Property(PropertyFilter::eq(
                            PropertyRef::TemporalProperty("p4".into(), Temporal::Latest),
                            7u64,
                        ))),
                    )),
                )),
                Box::new(CompositeEdgeFilter::Edge(Filter::eq("src", "raphtory"))),
            )),
            Box::new(CompositeEdgeFilter::Property(PropertyFilter::eq(
                PropertyRef::Property("p5".into()),
                9u64,
            ))),
        );

        assert_eq!(
            edge_composite_filter.to_string(),
            edge_composite_filter2.to_string()
        );
    }
}

// TODO: Add tests for const and temporal properties
#[cfg(test)]
mod test_composite_filters {
    use crate::{
        core::Prop,
        db::graph::views::filter::{
            CompositeEdgeFilter, CompositeNodeFilter, Filter, PropertyFilter, PropertyRef,
        },
    };
    use raphtory_api::core::storage::arc_str::ArcStr;

    #[test]
    fn test_composite_node_filter() {
        assert_eq!(
            "p2 == 2",
            CompositeNodeFilter::Property(PropertyFilter::eq(
                PropertyRef::Property("p2".to_string()),
                2u64
            ))
            .to_string()
        );

        assert_eq!(
            "((((node_type NOT_IN [fire_nation, water_tribe] AND p2 == 2) AND p1 == 1) AND (p3 <= 5 OR p4 IN [2, 10])) OR (node_name == pometry OR p5 == 9))",
            CompositeNodeFilter::Or(Box::new(CompositeNodeFilter::And(
                Box::new(CompositeNodeFilter::And(
                    Box::new(CompositeNodeFilter::And(
                        Box::new(CompositeNodeFilter::Node(Filter::excludes(
                            "node_type",
                            vec!["fire_nation".into(), "water_tribe".into()],
                        ))),
                        Box::new(CompositeNodeFilter::Property(PropertyFilter::eq(
                            PropertyRef::Property("p2".to_string()),
                            2u64,
                        ))),
                    )),
                    Box::new(CompositeNodeFilter::Property(PropertyFilter::eq(
                        PropertyRef::Property("p1".to_string()),
                        1u64,
                    ))),
                )),
                Box::new(CompositeNodeFilter::Or(
                    Box::new(CompositeNodeFilter::Property(PropertyFilter::le(
                        PropertyRef::Property("p3".to_string()),
                        5u64,
                    ))),
                    Box::new(CompositeNodeFilter::Property(PropertyFilter::includes(
                        PropertyRef::Property("p4".to_string()),
                        vec![Prop::U64(10), Prop::U64(2)],
                    ))),
                )),
            )),
            Box::new(CompositeNodeFilter::Or(
                Box::new(CompositeNodeFilter::Node(Filter::eq("node_name", "pometry"))),
                Box::new(CompositeNodeFilter::Property(PropertyFilter::eq(
                    PropertyRef::Property("p5".to_string()),
                    9u64,
                ))),
            )),
        ).to_string()
        );

        assert_eq!(
            "(name FUZZY_SEARCH(1,true) shivam AND nation FUZZY_SEARCH(1,false) air_nomad)",
            CompositeNodeFilter::And(
                Box::from(CompositeNodeFilter::Node(Filter::fuzzy_search(
                    "name", "shivam", 1, true
                ))),
                Box::from(CompositeNodeFilter::Property(PropertyFilter::fuzzy_search(
                    PropertyRef::Property("nation".to_string()),
                    "air_nomad",
                    1,
                    false,
                ))),
            )
            .to_string()
        );
    }

    #[test]
    fn test_composite_edge_filter() {
        assert_eq!(
            "p2 == 2",
            CompositeEdgeFilter::Property(PropertyFilter::eq(
                PropertyRef::Property("p2".to_string()),
                2u64
            ))
            .to_string()
        );

        assert_eq!(
            "((((edge_type NOT_IN [fire_nation, water_tribe] AND p2 == 2) AND p1 == 1) AND (p3 <= 5 OR p4 IN [2, 10])) OR (src == pometry OR p5 == 9))",
            CompositeEdgeFilter::Or(
                Box::new(CompositeEdgeFilter::And(
                    Box::new(CompositeEdgeFilter::And(
                        Box::new(CompositeEdgeFilter::And(
                            Box::new(CompositeEdgeFilter::Edge(Filter::excludes(
                                "edge_type",
                                vec!["fire_nation".into(), "water_tribe".into()],
                            ))),
                            Box::new(CompositeEdgeFilter::Property(PropertyFilter::eq(
                                PropertyRef::Property("p2".to_string()),
                                2u64,
                            ))),
                        )),
                        Box::new(CompositeEdgeFilter::Property(PropertyFilter::eq(
                            PropertyRef::Property("p1".to_string()),
                            1u64,
                        ))),
                    )),
                    Box::new(CompositeEdgeFilter::Or(
                        Box::new(CompositeEdgeFilter::Property(PropertyFilter::le(
                            PropertyRef::Property("p3".to_string()),
                            5u64,
                        ))),
                        Box::new(CompositeEdgeFilter::Property(PropertyFilter::includes(
                            PropertyRef::Property("p4".to_string()),
                            vec![Prop::U64(10), Prop::U64(2)],
                        ))),
                    )),
                )),
                Box::new(CompositeEdgeFilter::Or(
                    Box::new(CompositeEdgeFilter::Edge(Filter::eq("src", "pometry"))),
                    Box::new(CompositeEdgeFilter::Property(PropertyFilter::eq(
                        PropertyRef::Property("p5".to_string()),
                        9u64,
                    ))),
                )),
            )
                .to_string()
        );

        assert_eq!(
            "(name FUZZY_SEARCH(1,true) shivam AND nation FUZZY_SEARCH(1,false) air_nomad)",
            CompositeEdgeFilter::And(
                Box::from(CompositeEdgeFilter::Edge(Filter::fuzzy_search(
                    "name", "shivam", 1, true
                ))),
                Box::from(CompositeEdgeFilter::Property(PropertyFilter::fuzzy_search(
                    PropertyRef::Property("nation".to_string()),
                    "air_nomad",
                    1,
                    false,
                ))),
            )
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
        let filter = PropertyFilter::fuzzy_search(
            PropertyRef::Property("prop".to_string()),
            "pomet",
            2,
            false,
        );
        assert!(filter.matches(Some(&Prop::Str(ArcStr::from("pometry")))));
    }

    #[test]
    fn test_fuzzy_search_property_prefix_match() {
        let filter = PropertyFilter::fuzzy_search(
            PropertyRef::Property("prop".to_string()),
            "pome",
            2,
            false,
        );
        assert!(!filter.matches(Some(&Prop::Str(ArcStr::from("pometry")))));

        let filter = PropertyFilter::fuzzy_search(
            PropertyRef::Property("prop".to_string()),
            "pome",
            2,
            true,
        );
        assert!(filter.matches(Some(&Prop::Str(ArcStr::from("pometry")))));
    }
}
