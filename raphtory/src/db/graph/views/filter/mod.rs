use crate::{
    core::{
        entities::properties::props::Meta, sort_comparable_props, utils::errors::GraphError, Prop,
    },
    db::{
        api::{
            properties::{internal::PropertiesOps, Properties},
            storage::graph::{
                edges::{edge_ref::EdgeStorageRef, edge_storage_ops::EdgeStorageOps},
                nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps},
            },
            view::EdgeViewOps,
        },
        graph::{edge::EdgeView, node::NodeView, views::filter::internal::InternalNodeFilterOps},
    },
    prelude::{GraphViewOps, NodeViewOps},
};
use itertools::Itertools;
use raphtory_api::core::storage::arc_str::{ArcStr, OptionAsStr};
use std::{
    collections::HashSet,
    fmt,
    fmt::{Debug, Display},
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
pub mod node_name_filtered_graph;
pub mod node_or_filtered_graph;
pub mod node_property_filtered_graph;
pub mod node_type_filtered_graph;

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
                FilterOperator::Eq | FilterOperator::Ne => match right {
                    Some(r) => self.operation()(r, l),
                    None => matches!(self, FilterOperator::Ne),
                },
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
                FilterOperator::In | FilterOperator::NotIn => match right {
                    Some(r) => self.collection_operation()(l, &r.to_string()),
                    None => matches!(self, FilterOperator::NotIn),
                },
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

    fn is_property_matched<I: PropertiesOps + Clone>(
        &self,
        t_prop_id: Option<usize>,
        c_prop_id: Option<usize>,
        props: Properties<I>,
    ) -> bool {
        match self.prop_ref {
            PropertyRef::Property(_) => {
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
            PropertyRef::ConstantProperty(_) => {
                let prop_value = c_prop_id.and_then(|prop_id| props.constant().get_by_id(prop_id));
                self.matches(prop_value.as_ref())
            }
            PropertyRef::TemporalProperty(_, Temporal::Any) => t_prop_id.map_or(false, |prop_id| {
                props
                    .temporal()
                    .get_by_id(prop_id)
                    .filter(|prop_view| prop_view.values().any(|v| self.matches(Some(&v))))
                    .is_some()
            }),
            PropertyRef::TemporalProperty(_, Temporal::Latest) => {
                let prop_value = t_prop_id.and_then(|prop_id| {
                    props
                        .temporal()
                        .get_by_id(prop_id)
                        .and_then(|prop_view| prop_view.latest())
                });
                self.matches(prop_value.as_ref())
            }
        }
    }

    pub fn matches_node<'graph, G: GraphViewOps<'graph>>(
        &self,
        graph: &G,
        t_prop_id: Option<usize>,
        c_prop_id: Option<usize>,
        node: NodeStorageRef,
    ) -> bool {
        let props = NodeView::new_internal(graph, node.vid()).properties();
        self.is_property_matched(t_prop_id, c_prop_id, props)
    }

    pub fn matches_edge<'graph, G: GraphViewOps<'graph>>(
        &self,
        graph: &G,
        t_prop_id: Option<usize>,
        c_prop_id: Option<usize>,
        edge: EdgeStorageRef,
    ) -> bool {
        let props = EdgeView::new(graph, edge.out_ref()).properties();
        self.is_property_matched(t_prop_id, c_prop_id, props)
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
            "node_name" => self.matches(Some(&node.id().to_str())),
            // "node_type" => self.matches(graph.node_type(node.vid()).as_deref()),
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

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
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

    fn includes(&self, values: impl IntoIterator<Item = String>) -> Self::NodeFilterType {
        Filter::includes(self.field_name(), values).into()
    }

    fn excludes(&self, values: impl IntoIterator<Item = String>) -> Self::NodeFilterType {
        Filter::excludes(self.field_name(), values).into()
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

    fn includes(&self, values: impl IntoIterator<Item = String>) -> EdgeFieldFilter;

    fn excludes(&self, values: impl IntoIterator<Item = String>) -> EdgeFieldFilter;

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
            AsEdgeFilter, AsNodeFilter, ComposableFilter, CompositeEdgeFilter, CompositeNodeFilter,
            EdgeFilter, EdgeFilterOps, Filter, NodeFilter, NodeFilterBuilderOps, PropertyFilterOps,
            PropertyRef, Temporal,
        },
        prelude::PropertyFilter,
    };

    #[test]
    fn test_node_property_filter_build() {
        let filter_expr = PropertyFilter::property("p").eq("raphtory");
        let node_property_filter = filter_expr.as_node_filter();
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
        let node_property_filter = filter_expr.as_node_filter();
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
        let node_property_filter = filter_expr.as_node_filter();
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
        let node_property_filter = filter_expr.as_node_filter();
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
        let filter_expr = NodeFilter::name().eq("raphtory");
        let node_property_filter = filter_expr.as_node_filter();
        let node_property_filter2 = CompositeNodeFilter::Node(Filter::eq("node_name", "raphtory"));
        assert_eq!(
            node_property_filter.to_string(),
            node_property_filter2.to_string()
        );
    }

    #[test]
    fn test_node_type_filter_build() {
        let filter_expr = NodeFilter::node_type().eq("raphtory");
        let node_property_filter = filter_expr.as_node_filter();
        let node_property_filter2 = CompositeNodeFilter::Node(Filter::eq("node_type", "raphtory"));
        assert_eq!(
            node_property_filter.to_string(),
            node_property_filter2.to_string()
        );
    }

    #[test]
    fn test_node_filter_composition() {
        let node_composite_filter = NodeFilter::name()
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
        let edge_property_filter = filter_expr.as_edge_filter();
        let edge_property_filter2 = CompositeEdgeFilter::Edge(Filter::eq("src", "raphtory"));
        assert_eq!(
            edge_property_filter.to_string(),
            edge_property_filter2.to_string()
        );
    }

    #[test]
    fn test_edge_dst_filter_build() {
        let filter_expr = EdgeFilter::dst().eq("raphtory");
        let edge_property_filter = filter_expr.as_edge_filter();
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

#[cfg(test)]
pub(crate) mod test_filters {
    use super::*;
    use crate::{
        core::IntoProp,
        db::api::{
            mutation::internal::{InternalAdditionOps, InternalPropertyAdditionOps},
            view::StaticGraphViewOps,
        },
        prelude::{
            AdditionOps, EdgePropertyFilterOps, EdgeViewOps, Graph, NodePropertyFilterOps,
            PropertyAdditionOps,
        },
    };

    #[cfg(feature = "search")]
    pub use crate::db::api::view::SearchableGraphOps;

    pub(crate) fn filter_nodes_with<G, F, I: InternalNodeFilterOps>(
        filter: I,
        init_fn: F,
    ) -> Vec<String>
    where
        F: FnOnce() -> G,
        G: StaticGraphViewOps
            + AdditionOps
            + InternalAdditionOps
            + InternalPropertyAdditionOps
            + PropertyAdditionOps,
    {
        let graph = init_fn();

        let fg = graph.filter_nodes(filter).unwrap();
        let mut results = fg.nodes().iter().map(|n| n.name()).collect::<Vec<_>>();
        results.sort();
        results
    }

    pub(crate) fn filter_edges_with<G, F, I: InternalEdgeFilterOps>(
        filter: I,
        init_fn: F,
    ) -> Vec<String>
    where
        F: FnOnce() -> G,
        G: StaticGraphViewOps
            + AdditionOps
            + InternalAdditionOps
            + InternalPropertyAdditionOps
            + PropertyAdditionOps,
    {
        let graph = init_fn();

        let fg = graph.filter_edges(filter).unwrap();
        let mut results = fg
            .edges()
            .iter()
            .map(|e| format!("{}->{}", e.src().name(), e.dst().name()))
            .collect::<Vec<_>>();
        results.sort();
        results
    }

    macro_rules! assert_filter_results {
        ($filter_fn:ident, $filter:expr, $expected_results:expr) => {{
            let filter_results = $filter_fn($filter.clone());
            assert_eq!($expected_results, filter_results);
        }};
    }

    #[cfg(feature = "search")]
    pub(crate) fn search_nodes_with<G, F, I: AsNodeFilter>(filter: I, init_fn: F) -> Vec<String>
    where
        F: FnOnce() -> G,
        G: StaticGraphViewOps
            + AdditionOps
            + InternalAdditionOps
            + InternalPropertyAdditionOps
            + PropertyAdditionOps,
    {
        let graph = init_fn();
        graph.create_index().unwrap();

        let mut results = graph
            .search_nodes(filter, 20, 0)
            .unwrap()
            .into_iter()
            .map(|nv| nv.name())
            .collect::<Vec<_>>();
        results.sort();
        results
    }

    #[cfg(feature = "search")]
    pub(crate) fn search_edges_with<G, F, I: AsEdgeFilter>(filter: I, init_fn: F) -> Vec<String>
    where
        F: FnOnce() -> G,
        G: StaticGraphViewOps
            + AdditionOps
            + InternalAdditionOps
            + InternalPropertyAdditionOps
            + PropertyAdditionOps,
    {
        let graph = init_fn();
        graph.create_index().unwrap();

        let mut results = graph
            .search_edges(filter, 20, 0)
            .unwrap()
            .into_iter()
            .map(|ev| format!("{}->{}", ev.src().name(), ev.dst().name()))
            .collect::<Vec<_>>();
        results.sort();
        results
    }

    #[cfg(feature = "search")]
    macro_rules! assert_search_results {
        ($search_fn:ident, $filter:expr, $expected_results:expr) => {{
            let search_results = $search_fn($filter.clone());
            assert_eq!($expected_results, search_results);
        }};
    }

    #[cfg(not(feature = "search"))]
    macro_rules! assert_search_results {
        ($search_fn:ident, $filter:expr, $expected_results:expr) => {};
    }

    #[cfg(test)]
    mod test_property_semantics {
        use crate::{
            db::api::{
                mutation::internal::{InternalAdditionOps, InternalPropertyAdditionOps},
                view::StaticGraphViewOps,
            },
            prelude::{AdditionOps, GraphViewOps, PropertyAdditionOps},
        };

        #[cfg(test)]
        mod test_node_property_filter_semantics {
            use crate::{
                core::Prop,
                db::{
                    api::{
                        mutation::internal::{InternalAdditionOps, InternalPropertyAdditionOps},
                        view::{node::NodeViewOps, StaticGraphViewOps},
                    },
                    graph::views::filter::PropertyFilterOps,
                },
                prelude::{AdditionOps, Graph, GraphViewOps, PropertyAdditionOps, PropertyFilter},
            };

            #[cfg(feature = "search")]
            pub use crate::db::api::view::SearchableGraphOps;
            #[cfg(feature = "search")]
            use crate::db::graph::views::filter::test_filters::search_nodes_with;

            use crate::db::graph::views::filter::{
                internal::InternalNodeFilterOps, test_filters::filter_nodes_with,
            };

            fn init_graph<
                G: StaticGraphViewOps
                    + AdditionOps
                    + InternalAdditionOps
                    + InternalPropertyAdditionOps
                    + PropertyAdditionOps,
            >(
                graph: G,
            ) -> G {
                let nodes = [
                    (6, "N1", vec![("p1", Prop::U64(2u64))]),
                    (7, "N1", vec![("p1", Prop::U64(1u64))]),
                    (6, "N2", vec![("p1", Prop::U64(1u64))]),
                    (7, "N2", vec![("p1", Prop::U64(2u64))]),
                    (8, "N3", vec![("p1", Prop::U64(1u64))]),
                    (9, "N4", vec![("p1", Prop::U64(1u64))]),
                    (5, "N5", vec![("p1", Prop::U64(1u64))]),
                    (6, "N5", vec![("p1", Prop::U64(2u64))]),
                    (5, "N6", vec![("p1", Prop::U64(1u64))]),
                    (6, "N6", vec![("p1", Prop::U64(1u64))]),
                    (3, "N7", vec![("p1", Prop::U64(1u64))]),
                    (5, "N7", vec![("p1", Prop::U64(1u64))]),
                    (3, "N8", vec![("p1", Prop::U64(1u64))]),
                    (4, "N8", vec![("p1", Prop::U64(2u64))]),
                    (2, "N9", vec![("p1", Prop::U64(2u64))]),
                    (2, "N10", vec![("q1", Prop::U64(0u64))]),
                    (2, "N10", vec![("p1", Prop::U64(3u64))]),
                    (2, "N11", vec![("p1", Prop::U64(3u64))]),
                    (2, "N11", vec![("q1", Prop::U64(0u64))]),
                    (2, "N12", vec![("q1", Prop::U64(0u64))]),
                    (3, "N12", vec![("p1", Prop::U64(3u64))]),
                    (2, "N13", vec![("q1", Prop::U64(0u64))]),
                    (3, "N13", vec![("p1", Prop::U64(3u64))]),
                    (2, "N14", vec![("q1", Prop::U64(0u64))]),
                    (2, "N15", vec![]),
                ];

                for (id, label, props) in nodes.iter() {
                    graph.add_node(*id, label, props.clone(), None).unwrap();
                }

                let constant_properties = [
                    ("N1", [("p1", Prop::U64(1u64))]),
                    ("N4", [("p1", Prop::U64(2u64))]),
                    ("N9", [("p1", Prop::U64(1u64))]),
                    ("N10", [("p1", Prop::U64(1u64))]),
                    ("N11", [("p1", Prop::U64(1u64))]),
                    ("N12", [("p1", Prop::U64(1u64))]),
                    ("N13", [("p1", Prop::U64(1u64))]),
                    ("N14", [("p1", Prop::U64(1u64))]),
                    ("N15", [("p1", Prop::U64(1u64))]),
                ];

                for (node, props) in constant_properties.iter() {
                    graph
                        .node(node)
                        .unwrap()
                        .add_constant_properties(props.clone())
                        .unwrap();
                }

                graph
            }

            fn init_graph_for_secondary_indexes<
                G: StaticGraphViewOps
                    + AdditionOps
                    + InternalAdditionOps
                    + InternalPropertyAdditionOps
                    + PropertyAdditionOps,
            >(
                graph: G,
            ) -> G {
                let graph: G = init_graph(graph);
                let nodes = [
                    (1, "N16", vec![("p1", Prop::U64(2u64))]),
                    (1, "N16", vec![("p1", Prop::U64(1u64))]),
                    (1, "N17", vec![("p1", Prop::U64(1u64))]),
                    (1, "N17", vec![("p1", Prop::U64(2u64))]),
                ];

                for (id, label, props) in nodes.iter() {
                    graph.add_node(*id, label, props.clone(), None).unwrap();
                }

                graph
            }

            fn filter_nodes<I: InternalNodeFilterOps>(filter: I) -> Vec<String> {
                filter_nodes_with(filter, || init_graph(Graph::new()))
            }

            fn filter_nodes_secondary_index<I: InternalNodeFilterOps>(filter: I) -> Vec<String> {
                filter_nodes_with(filter, || init_graph_for_secondary_indexes(Graph::new()))
            }

            #[cfg(feature = "search")]
            fn search_nodes(filter: PropertyFilter) -> Vec<String> {
                search_nodes_with(filter, || init_graph(Graph::new()))
            }

            #[cfg(feature = "search")]
            fn search_nodes_secondary_index(filter: PropertyFilter) -> Vec<String> {
                search_nodes_with(filter, || init_graph_for_secondary_indexes(Graph::new()))
            }

            #[test]
            fn test_constant_semantics() {
                let filter = PropertyFilter::property("p1").constant().eq(1u64);
                let expected_results = vec!["N1", "N10", "N11", "N12", "N13", "N14", "N15", "N9"];
                assert_filter_results!(filter_nodes, filter, expected_results);
                assert_search_results!(search_nodes, filter, expected_results);
            }

            #[test]
            fn test_temporal_any_semantics() {
                let filter = PropertyFilter::property("p1").temporal().any().eq(1u64);
                let expected_results = vec!["N1", "N2", "N3", "N4", "N5", "N6", "N7", "N8"];
                assert_filter_results!(filter_nodes, filter, expected_results);
                assert_search_results!(search_nodes, filter, expected_results);
            }

            #[test]
            fn test_temporal_any_semantics_for_secondary_indexes() {
                let filter = PropertyFilter::property("p1").temporal().any().eq(1u64);
                let expected_results =
                    vec!["N1", "N16", "N17", "N2", "N3", "N4", "N5", "N6", "N7", "N8"];
                assert_filter_results!(filter_nodes_secondary_index, filter, expected_results);
                assert_search_results!(search_nodes_secondary_index, filter, expected_results);
            }

            #[test]
            fn test_temporal_latest_semantics() {
                let filter = PropertyFilter::property("p1").temporal().latest().eq(1u64);
                let expected_results = vec!["N1", "N3", "N4", "N6", "N7"];
                assert_filter_results!(filter_nodes, filter, expected_results);
                assert_search_results!(search_nodes, filter, expected_results);
            }

            #[test]
            fn test_temporal_latest_semantics_for_secondary_indexes() {
                let filter = PropertyFilter::property("p1").temporal().latest().eq(1u64);
                let expected_results = vec!["N1", "N16", "N3", "N4", "N6", "N7"];
                assert_filter_results!(filter_nodes_secondary_index, filter, expected_results);
                assert_search_results!(search_nodes_secondary_index, filter, expected_results);
            }

            #[test]
            fn test_property_semantics() {
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1", "N14", "N15", "N3", "N4", "N6", "N7"];
                assert_filter_results!(filter_nodes, filter, expected_results);
                assert_search_results!(search_nodes, filter, expected_results);
            }

            #[test]
            fn test_property_semantics_for_secondary_indexes() {
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1", "N14", "N15", "N16", "N3", "N4", "N6", "N7"];
                assert_filter_results!(filter_nodes_secondary_index, filter, expected_results);
                assert_search_results!(search_nodes_secondary_index, filter, expected_results);
            }

            #[test]
            fn test_property_semantics_only_constant() {
                // For this graph there won't be any temporal property index for property name "p1".
                fn init_graph<
                    G: StaticGraphViewOps
                        + AdditionOps
                        + InternalAdditionOps
                        + InternalPropertyAdditionOps
                        + PropertyAdditionOps,
                >(
                    graph: G,
                ) -> G {
                    let nodes = [(2, "N1", vec![("q1", Prop::U64(0u64))]), (2, "N2", vec![])];

                    for (id, label, props) in nodes.iter() {
                        graph.add_node(*id, label, props.clone(), None).unwrap();
                    }

                    let constant_properties = [
                        ("N1", [("p1", Prop::U64(1u64))]),
                        ("N2", [("p1", Prop::U64(1u64))]),
                    ];

                    for (node, props) in constant_properties.iter() {
                        graph
                            .node(node)
                            .unwrap()
                            .add_constant_properties(props.clone())
                            .unwrap();
                    }

                    graph
                }

                fn filter_nodes<I: InternalNodeFilterOps>(filter: I) -> Vec<String> {
                    filter_nodes_with(filter, || init_graph(Graph::new()))
                }

                #[cfg(feature = "search")]
                fn search_nodes(filter: PropertyFilter) -> Vec<String> {
                    search_nodes_with(filter, || init_graph(Graph::new()))
                }

                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1", "N2"];
                assert_filter_results!(filter_nodes, filter, expected_results);
                assert_search_results!(search_nodes, filter, expected_results);
            }

            #[test]
            fn test_property_semantics_only_temporal() {
                // For this graph there won't be any constant property index for property name "p1".
                fn init_graph<
                    G: StaticGraphViewOps
                        + AdditionOps
                        + InternalAdditionOps
                        + InternalPropertyAdditionOps
                        + PropertyAdditionOps,
                >(
                    graph: G,
                ) -> G {
                    let nodes = [
                        (1, "N1", vec![("p1", Prop::U64(1u64))]),
                        (2, "N2", vec![("p1", Prop::U64(1u64))]),
                        (3, "N2", vec![("p1", Prop::U64(2u64))]),
                        (2, "N3", vec![("p1", Prop::U64(2u64))]),
                        (3, "N3", vec![("p1", Prop::U64(1u64))]),
                        (3, "N4", vec![]),
                    ];

                    for (id, label, props) in nodes.iter() {
                        graph.add_node(*id, label, props.clone(), None).unwrap();
                    }

                    let constant_properties = [("N1", [("p2", Prop::U64(1u64))])];

                    for (node, props) in constant_properties.iter() {
                        graph
                            .node(node)
                            .unwrap()
                            .add_constant_properties(props.clone())
                            .unwrap();
                    }

                    graph
                }

                fn filter_nodes<I: InternalNodeFilterOps>(filter: I) -> Vec<String> {
                    filter_nodes_with(filter, || init_graph(Graph::new()))
                }

                #[cfg(feature = "search")]
                fn search_nodes(filter: PropertyFilter) -> Vec<String> {
                    search_nodes_with(filter, || init_graph(Graph::new()))
                }

                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1", "N3"];
                assert_filter_results!(filter_nodes, filter, expected_results);
                assert_search_results!(search_nodes, filter, expected_results);
            }
        }

        #[cfg(test)]
        mod test_edge_property_filter_semantics {
            use crate::{
                core::Prop,
                db::{
                    api::{
                        mutation::internal::{InternalAdditionOps, InternalPropertyAdditionOps},
                        view::StaticGraphViewOps,
                    },
                    graph::views::filter::PropertyFilterOps,
                },
                prelude::{
                    AdditionOps, EdgeViewOps, Graph, GraphViewOps, NodeViewOps,
                    PropertyAdditionOps, PropertyFilter,
                },
            };

            #[cfg(feature = "search")]
            pub use crate::db::api::view::SearchableGraphOps;
            #[cfg(feature = "search")]
            use crate::db::graph::views::filter::test_filters::search_edges_with;

            use crate::db::graph::views::filter::{
                internal::InternalEdgeFilterOps, test_filters::filter_edges_with,
            };

            fn init_graph<
                G: StaticGraphViewOps
                    + AdditionOps
                    + InternalAdditionOps
                    + InternalPropertyAdditionOps
                    + PropertyAdditionOps,
            >(
                mut graph: G,
            ) -> G {
                let edges = [
                    (6, "N1", "N2", vec![("p1", Prop::U64(2u64))]),
                    (7, "N1", "N2", vec![("p1", Prop::U64(1u64))]),
                    (6, "N2", "N3", vec![("p1", Prop::U64(1u64))]),
                    (7, "N2", "N3", vec![("p1", Prop::U64(2u64))]),
                    (8, "N3", "N4", vec![("p1", Prop::U64(1u64))]),
                    (9, "N4", "N5", vec![("p1", Prop::U64(1u64))]),
                    (5, "N5", "N6", vec![("p1", Prop::U64(1u64))]),
                    (6, "N5", "N6", vec![("p1", Prop::U64(2u64))]),
                    (5, "N6", "N7", vec![("p1", Prop::U64(1u64))]),
                    (6, "N6", "N7", vec![("p1", Prop::U64(1u64))]),
                    (3, "N7", "N8", vec![("p1", Prop::U64(1u64))]),
                    (5, "N7", "N8", vec![("p1", Prop::U64(1u64))]),
                    (3, "N8", "N9", vec![("p1", Prop::U64(1u64))]),
                    (4, "N8", "N9", vec![("p1", Prop::U64(2u64))]),
                    (2, "N9", "N10", vec![("p1", Prop::U64(2u64))]),
                    (2, "N10", "N11", vec![("q1", Prop::U64(0u64))]),
                    (2, "N10", "N11", vec![("p1", Prop::U64(3u64))]),
                    (2, "N11", "N12", vec![("p1", Prop::U64(3u64))]),
                    (2, "N11", "N12", vec![("q1", Prop::U64(0u64))]),
                    (2, "N12", "N13", vec![("q1", Prop::U64(0u64))]),
                    (3, "N12", "N13", vec![("p1", Prop::U64(3u64))]),
                    (2, "N13", "N14", vec![("q1", Prop::U64(0u64))]),
                    (3, "N13", "N14", vec![("p1", Prop::U64(3u64))]),
                    (2, "N14", "N15", vec![("q1", Prop::U64(0u64))]),
                    (2, "N15", "N1", vec![]),
                ];

                for (time, src, dst, props) in edges {
                    graph.add_edge(time, src, dst, props, None).unwrap();
                }

                let constant_edges = [
                    ("N1", "N2", vec![("p1", Prop::U64(1u64))]),
                    ("N4", "N5", vec![("p1", Prop::U64(2u64))]),
                    ("N9", "N10", vec![("p1", Prop::U64(1u64))]),
                    ("N10", "N11", vec![("p1", Prop::U64(1u64))]),
                    ("N11", "N12", vec![("p1", Prop::U64(1u64))]),
                    ("N12", "N13", vec![("p1", Prop::U64(1u64))]),
                    ("N13", "N14", vec![("p1", Prop::U64(1u64))]),
                    ("N14", "N15", vec![("p1", Prop::U64(1u64))]),
                    ("N15", "N1", vec![("p1", Prop::U64(1u64))]),
                ];

                for (src, dst, props) in constant_edges {
                    graph
                        .edge(src, dst)
                        .unwrap()
                        .add_constant_properties(props.clone(), None)
                        .unwrap();
                }

                graph
            }

            fn init_graph_for_secondary_indexes<
                G: StaticGraphViewOps
                    + AdditionOps
                    + InternalAdditionOps
                    + InternalPropertyAdditionOps
                    + PropertyAdditionOps,
            >(
                graph: G,
            ) -> G {
                let graph: G = init_graph(graph);
                let edge_data = [
                    (1, "N16", "N15", vec![("p1", Prop::U64(2u64))]),
                    (1, "N16", "N15", vec![("p1", Prop::U64(1u64))]),
                    (1, "N17", "N16", vec![("p1", Prop::U64(1u64))]),
                    (1, "N17", "N16", vec![("p1", Prop::U64(2u64))]),
                ];

                for (time, src, dst, props) in edge_data {
                    graph.add_edge(time, src, dst, props, None).unwrap();
                }

                graph
            }

            fn filter_edges<I: InternalEdgeFilterOps>(filter: I) -> Vec<String> {
                filter_edges_with(filter, || init_graph(Graph::new()))
            }

            fn filter_edges_secondary_index<I: InternalEdgeFilterOps>(filter: I) -> Vec<String> {
                filter_edges_with(filter, || init_graph_for_secondary_indexes(Graph::new()))
            }

            #[cfg(feature = "search")]
            fn search_edges(filter: PropertyFilter) -> Vec<String> {
                search_edges_with(filter, || init_graph(Graph::new()))
            }

            #[cfg(feature = "search")]
            fn search_edges_secondary_index(filter: PropertyFilter) -> Vec<String> {
                search_edges_with(filter, || init_graph_for_secondary_indexes(Graph::new()))
            }

            #[test]
            fn test_constant_semantics() {
                let filter = PropertyFilter::property("p1").constant().eq(1u64);
                let expected_results = vec![
                    "N1->N2", "N10->N11", "N11->N12", "N12->N13", "N13->N14", "N14->N15",
                    "N15->N1", "N9->N10",
                ];
                assert_filter_results!(filter_edges, filter, expected_results);
                assert_search_results!(search_edges, filter, expected_results);
            }

            #[test]
            fn test_temporal_any_semantics() {
                let filter = PropertyFilter::property("p1").temporal().any().eq(1u64);
                let expected_results = vec![
                    "N1->N2", "N2->N3", "N3->N4", "N4->N5", "N5->N6", "N6->N7", "N7->N8", "N8->N9",
                ];
                assert_filter_results!(filter_edges, filter, expected_results);
                assert_search_results!(search_edges, filter, expected_results);
            }

            #[test]
            fn test_temporal_any_semantics_for_secondary_indexes() {
                let filter = PropertyFilter::property("p1").temporal().any().eq(1u64);
                let expected_results = vec![
                    "N1->N2", "N16->N15", "N17->N16", "N2->N3", "N3->N4", "N4->N5", "N5->N6",
                    "N6->N7", "N7->N8", "N8->N9",
                ];
                assert_filter_results!(filter_edges_secondary_index, filter, expected_results);
                assert_search_results!(search_edges_secondary_index, filter, expected_results);
            }

            #[test]
            fn test_temporal_latest_semantics() {
                let filter = PropertyFilter::property("p1").temporal().latest().eq(1u64);
                let expected_results = vec!["N1->N2", "N3->N4", "N4->N5", "N6->N7", "N7->N8"];
                assert_filter_results!(filter_edges, filter, expected_results);
                assert_search_results!(search_edges, filter, expected_results);
            }

            #[test]
            fn test_temporal_latest_semantics_for_secondary_indexes() {
                let filter = PropertyFilter::property("p1").temporal().latest().eq(1u64);
                let expected_results =
                    vec!["N1->N2", "N16->N15", "N3->N4", "N4->N5", "N6->N7", "N7->N8"];
                assert_filter_results!(filter_edges_secondary_index, filter, expected_results);
                assert_search_results!(search_edges_secondary_index, filter, expected_results);
            }

            #[test]
            fn test_property_semantics() {
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec![
                    "N1->N2", "N14->N15", "N15->N1", "N3->N4", "N4->N5", "N6->N7", "N7->N8",
                ];
                assert_filter_results!(filter_edges, filter, expected_results);
                assert_search_results!(search_edges, filter, expected_results);
            }

            #[test]
            fn test_property_semantics_for_secondary_indexes() {
                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec![
                    "N1->N2", "N14->N15", "N15->N1", "N16->N15", "N3->N4", "N4->N5", "N6->N7",
                    "N7->N8",
                ];
                assert_filter_results!(filter_edges_secondary_index, filter, expected_results);
                assert_search_results!(search_edges_secondary_index, filter, expected_results);
            }

            #[test]
            fn test_property_semantics_only_constant() {
                // For this graph there won't be any temporal property index for property name "p1".
                fn init_graph<
                    G: StaticGraphViewOps
                        + AdditionOps
                        + InternalAdditionOps
                        + InternalPropertyAdditionOps
                        + PropertyAdditionOps,
                >(
                    graph: G,
                ) -> G {
                    let edges = [
                        (2, "N1", "N2", vec![("q1", Prop::U64(0u64))]),
                        (2, "N2", "N3", vec![]),
                    ];

                    for (time, src, dst, props) in edges {
                        graph.add_edge(time, src, dst, props, None).unwrap();
                    }

                    let constant_edges = [
                        ("N1", "N2", vec![("p1", Prop::U64(1u64))]),
                        ("N2", "N3", vec![("p1", Prop::U64(1u64))]),
                    ];

                    for (src, dst, props) in constant_edges {
                        graph
                            .edge(src, dst)
                            .unwrap()
                            .add_constant_properties(props.clone(), None)
                            .unwrap();
                    }

                    graph
                }

                fn filter_edges<I: InternalEdgeFilterOps>(filter: I) -> Vec<String> {
                    filter_edges_with(filter, || init_graph(Graph::new()))
                }

                #[cfg(feature = "search")]
                fn search_edges(filter: PropertyFilter) -> Vec<String> {
                    search_edges_with(filter, || init_graph(Graph::new()))
                }

                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1->N2", "N2->N3"];
                assert_filter_results!(filter_edges, filter, expected_results);
                assert_search_results!(search_edges, filter, expected_results);
            }

            #[test]
            fn test_property_semantics_only_temporal() {
                // For this graph there won't be any constant property index for property name "p1".
                fn init_graph<
                    G: StaticGraphViewOps
                        + AdditionOps
                        + InternalAdditionOps
                        + InternalPropertyAdditionOps
                        + PropertyAdditionOps,
                >(
                    graph: G,
                ) -> G {
                    let edges = [
                        (1, "N1", "N2", vec![("p1", Prop::U64(1u64))]),
                        (2, "N2", "N3", vec![("p1", Prop::U64(1u64))]),
                        (3, "N2", "N3", vec![("p1", Prop::U64(2u64))]),
                        (2, "N3", "N4", vec![("p1", Prop::U64(2u64))]),
                        (3, "N3", "N4", vec![("p1", Prop::U64(1u64))]),
                        (2, "N4", "N5", vec![]),
                    ];

                    for (time, src, dst, props) in edges {
                        graph.add_edge(time, src, dst, props, None).unwrap();
                    }

                    let constant_edges = [("N1", "N2", vec![("p2", Prop::U64(1u64))])];

                    for (src, dst, props) in constant_edges {
                        graph
                            .edge(src, dst)
                            .unwrap()
                            .add_constant_properties(props.clone(), None)
                            .unwrap();
                    }

                    graph
                }

                fn filter_edges<I: InternalEdgeFilterOps>(filter: I) -> Vec<String> {
                    filter_edges_with(filter, || init_graph(Graph::new()))
                }

                #[cfg(feature = "search")]
                fn search_edges(filter: PropertyFilter) -> Vec<String> {
                    search_edges_with(filter, || init_graph(Graph::new()))
                }

                let filter = PropertyFilter::property("p1").eq(1u64);
                let expected_results = vec!["N1->N2", "N3->N4"];
                assert_filter_results!(filter_edges, filter, expected_results);
                assert_search_results!(search_edges, filter, expected_results);
            }
        }
    }

    use crate::db::graph::views::filter::internal::{InternalEdgeFilterOps, InternalNodeFilterOps};

    fn init_nodes_graph<
        G: StaticGraphViewOps
            + AdditionOps
            + InternalAdditionOps
            + InternalPropertyAdditionOps
            + PropertyAdditionOps,
    >(
        graph: G,
    ) -> G {
        let nodes = [
            (
                1,
                1,
                vec![
                    ("p1", "shivam_kapoor".into_prop()),
                    ("p9", 5u64.into_prop()),
                ],
                Some("fire_nation"),
            ),
            (
                2,
                2,
                vec![("p1", "prop12".into_prop()), ("p2", 2u64.into_prop())],
                Some("air_nomads"),
            ),
            (
                3,
                1,
                vec![
                    ("p1", "shivam_kapoor".into_prop()),
                    ("p9", 5u64.into_prop()),
                ],
                Some("fire_nation"),
            ),
            (
                3,
                3,
                vec![("p2", 6u64.into_prop()), ("p3", 1u64.into_prop())],
                Some("fire_nation"),
            ),
            (
                4,
                1,
                vec![
                    ("p1", "shivam_kapoor".into_prop()),
                    ("p9", 5u64.into_prop()),
                ],
                Some("fire_nation"),
            ),
            (3, 4, vec![("p4", "pometry".into_prop())], None),
            (4, 4, vec![("p5", 12u64.into_prop())], None),
        ];

        for (time, id, props, node_type) in nodes {
            graph.add_node(time, id, props, node_type).unwrap();
        }

        graph
    }

    fn init_edges_graph<
        G: StaticGraphViewOps
            + AdditionOps
            + InternalAdditionOps
            + InternalPropertyAdditionOps
            + PropertyAdditionOps,
    >(
        graph: G,
    ) -> G {
        let edges = [
            (
                1,
                1,
                2,
                vec![("p1", "shivam_kapoor".into_prop())],
                Some("fire_nation"),
            ),
            (
                2,
                1,
                2,
                vec![
                    ("p1", "shivam_kapoor".into_prop()),
                    ("p2", 4u64.into_prop()),
                ],
                Some("fire_nation"),
            ),
            (
                2,
                2,
                3,
                vec![("p1", "prop12".into_prop()), ("p2", 2u64.into_prop())],
                Some("air_nomads"),
            ),
            (
                3,
                3,
                1,
                vec![("p2", 6u64.into_prop()), ("p3", 1u64.into_prop())],
                Some("fire_nation"),
            ),
            (
                3,
                2,
                1,
                vec![("p2", 6u64.into_prop()), ("p3", 1u64.into_prop())],
                None,
            ),
        ];

        for (time, src, dst, props, edge_type) in edges {
            graph.add_edge(time, src, dst, props, edge_type).unwrap();
        }

        graph
    }

    fn filter_nodes<I: InternalNodeFilterOps>(filter: I) -> Vec<String> {
        filter_nodes_with(filter, || init_nodes_graph(Graph::new()))
    }

    fn filter_edges<I: InternalEdgeFilterOps>(filter: I) -> Vec<String> {
        filter_edges_with(filter, || init_edges_graph(Graph::new()))
    }

    #[cfg(test)]
    mod test_node_property_filter {
        use crate::{
            core::Prop,
            db::graph::views::filter::{
                test_filters::{filter_nodes, init_nodes_graph},
                PropertyFilterOps,
            },
            prelude::{Graph, PropertyFilter},
        };

        #[cfg(feature = "search")]
        use crate::db::graph::views::filter::test_filters::search_nodes_with;

        #[cfg(feature = "search")]
        fn search_nodes(filter: PropertyFilter) -> Vec<String> {
            search_nodes_with(filter, || init_nodes_graph(Graph::new()))
        }

        #[test]
        fn test_filter_nodes_for_property_eq() {
            let filter = PropertyFilter::property("p2").eq(2u64);
            let expected_results = vec!["2"];
            assert_filter_results!(filter_nodes, filter, expected_results);
            assert_search_results!(search_nodes, filter, expected_results);
        }

        #[test]
        fn test_filter_nodes_for_property_ne() {
            let filter = PropertyFilter::property("p2").ne(2u64);
            let expected_results = vec!["3"];
            assert_filter_results!(filter_nodes, filter, expected_results);
            assert_search_results!(search_nodes, filter, expected_results);
        }

        #[test]
        fn test_filter_nodes_for_property_lt() {
            let filter = PropertyFilter::property("p2").lt(10u64);
            let expected_results = vec!["2", "3"];
            assert_filter_results!(filter_nodes, filter, expected_results);
            assert_search_results!(search_nodes, filter, expected_results);
        }

        #[test]
        fn test_filter_nodes_for_property_le() {
            let filter = PropertyFilter::property("p2").le(6u64);
            let expected_results = vec!["2", "3"];
            assert_filter_results!(filter_nodes, filter, expected_results);
            assert_search_results!(search_nodes, filter, expected_results);
        }

        #[test]
        fn test_filter_nodes_for_property_gt() {
            let filter = PropertyFilter::property("p2").gt(2u64);
            let expected_results = vec!["3"];
            assert_filter_results!(filter_nodes, filter, expected_results);
            assert_search_results!(search_nodes, filter, expected_results);
        }

        #[test]
        fn test_nodes_for_property_ge() {
            let filter = PropertyFilter::property("p2").ge(2u64);
            let expected_results = vec!["2", "3"];
            assert_filter_results!(filter_nodes, filter, expected_results);
            assert_search_results!(search_nodes, filter, expected_results);
        }

        #[test]
        fn test_filter_nodes_for_property_in() {
            let filter = PropertyFilter::property("p2").includes(vec![Prop::U64(6)]);
            let expected_results = vec!["3"];
            assert_filter_results!(filter_nodes, filter, expected_results);
            assert_search_results!(search_nodes, filter, expected_results);

            let filter = PropertyFilter::property("p2").includes(vec![Prop::U64(2), Prop::U64(6)]);
            let expected_results = vec!["2", "3"];
            assert_filter_results!(filter_nodes, filter, expected_results);
            assert_search_results!(search_nodes, filter, expected_results);
        }

        #[test]
        fn test_filter_nodes_for_property_not_in() {
            let filter = PropertyFilter::property("p2").excludes(vec![Prop::U64(6)]);
            let expected_results = vec!["2"];
            assert_filter_results!(filter_nodes, filter, expected_results);
            assert_search_results!(search_nodes, filter, expected_results);
        }

        #[test]
        fn test_filter_nodes_for_property_is_some() {
            let filter = PropertyFilter::property("p2").is_some();
            let expected_results = vec!["2", "3"];
            assert_filter_results!(filter_nodes, filter, expected_results);
            assert_search_results!(search_nodes, filter, expected_results);
        }

        #[test]
        fn test_filter_nodes_for_property_is_none() {
            let filter = PropertyFilter::property("p2").is_none();
            let expected_results = vec!["1", "4"];
            assert_filter_results!(filter_nodes, filter, expected_results);
            assert_search_results!(search_nodes, filter, expected_results);
        }
    }

    #[cfg(test)]
    mod test_edge_property_filter {
        use crate::{
            core::Prop,
            db::graph::views::filter::{
                test_filters::{filter_edges, init_edges_graph},
                PropertyFilterOps,
            },
            prelude::{Graph, PropertyFilter},
        };

        #[cfg(feature = "search")]
        use crate::db::graph::views::filter::test_filters::search_edges_with;

        #[cfg(feature = "search")]
        fn search_edges(filter: PropertyFilter) -> Vec<String> {
            search_edges_with(filter, || init_edges_graph(Graph::new()))
        }

        #[test]
        fn test_filter_edges_for_property_eq() {
            let filter = PropertyFilter::property("p2").eq(2u64);
            let expected_results = vec!["2->3"];
            assert_filter_results!(filter_edges, filter, expected_results);
            assert_search_results!(search_edges, filter, expected_results);
        }

        #[test]
        fn test_filter_edges_for_property_ne() {
            let filter = PropertyFilter::property("p2").ne(2u64);
            let expected_results = vec!["1->2", "2->1", "3->1"];
            assert_filter_results!(filter_edges, filter, expected_results);
            assert_search_results!(search_edges, filter, expected_results);
        }

        #[test]
        fn test_filter_edges_for_property_lt() {
            let filter = PropertyFilter::property("p2").lt(10u64);
            let expected_results = vec!["1->2", "2->1", "2->3", "3->1"];
            assert_filter_results!(filter_edges, filter, expected_results);
            assert_search_results!(search_edges, filter, expected_results);
        }

        #[test]
        fn test_filter_edges_for_property_le() {
            let filter = PropertyFilter::property("p2").le(6u64);
            let expected_results = vec!["1->2", "2->1", "2->3", "3->1"];
            assert_filter_results!(filter_edges, filter, expected_results);
            assert_search_results!(search_edges, filter, expected_results);
        }

        #[test]
        fn test_filter_edges_for_property_gt() {
            let filter = PropertyFilter::property("p2").gt(2u64);
            let expected_results = vec!["1->2", "2->1", "3->1"];
            assert_filter_results!(filter_edges, filter, expected_results);
            assert_search_results!(search_edges, filter, expected_results);
        }

        #[test]
        fn test_edges_for_property_ge() {
            let filter = PropertyFilter::property("p2").ge(2u64);
            let expected_results = vec!["1->2", "2->1", "2->3", "3->1"];
            assert_filter_results!(filter_edges, filter, expected_results);
            assert_search_results!(search_edges, filter, expected_results);
        }

        #[test]
        fn test_filter_edges_for_property_in() {
            let filter = PropertyFilter::property("p2").includes(vec![Prop::U64(6)]);
            let expected_results = vec!["2->1", "3->1"];
            assert_filter_results!(filter_edges, filter, expected_results);
            assert_search_results!(search_edges, filter, expected_results);

            let filter = PropertyFilter::property("p2").includes(vec![Prop::U64(2), Prop::U64(6)]);
            let expected_results = vec!["2->1", "2->3", "3->1"];
            assert_filter_results!(filter_edges, filter, expected_results);
            assert_search_results!(search_edges, filter, expected_results);
        }

        #[test]
        fn test_filter_edges_for_property_not_in() {
            let filter = PropertyFilter::property("p2").excludes(vec![Prop::U64(6)]);
            let expected_results = vec!["1->2", "2->3"];
            assert_filter_results!(filter_edges, filter, expected_results);
            assert_search_results!(search_edges, filter, expected_results);
        }

        #[test]
        fn test_filter_edges_for_property_is_some() {
            let filter = PropertyFilter::property("p2").is_some();
            let expected_results = vec!["1->2", "2->1", "2->3", "3->1"];
            assert_filter_results!(filter_edges, filter, expected_results);
            assert_search_results!(search_edges, filter, expected_results);
        }

        #[test]
        fn test_filter_edges_for_property_is_none() {
            let filter = PropertyFilter::property("p2").is_none();
            let expected_results = vec!["1->2"];
            // assert_filter_results!(filter_edges, filter, expected_results);
            // assert_search_results!(search_edges, filter, expected_results);
        }
    }

    #[cfg(test)]
    mod test_node_filter {
        #[cfg(feature = "search")]
        use crate::db::graph::views::filter::test_filters::search_nodes_with;
        use crate::{
            db::{
                api::view::node::NodeViewOps,
                graph::views::filter::{
                    test_filters::{filter_nodes, init_nodes_graph},
                    AsNodeFilter, NodeFilter, NodeFilterBuilderOps,
                },
            },
            prelude::{Graph, GraphViewOps, NodePropertyFilterOps},
        };

        #[cfg(feature = "search")]
        fn search_nodes<I: AsNodeFilter>(filter: I) -> Vec<String> {
            search_nodes_with(filter, || init_nodes_graph(Graph::new()))
        }

        #[test]
        fn test_nodes_for_node_name_eq() {
            let filter = NodeFilter::name().eq("3");
            let expected_results = vec!["3"];
            assert_filter_results!(filter_nodes, filter, expected_results);
            assert_search_results!(search_nodes, filter, expected_results);
        }

        #[test]
        fn test_nodes_for_node_name_ne() {
            let filter = NodeFilter::name().ne("2");
            let expected_results = vec!["1", "3", "4"];
            assert_filter_results!(filter_nodes, filter, expected_results);
            assert_search_results!(search_nodes, filter, expected_results);
        }

        #[test]
        fn test_nodes_for_node_name_in() {
            let filter = NodeFilter::name().includes(vec!["1".into()]);
            let expected_results = vec!["1"];
            assert_filter_results!(filter_nodes, filter, expected_results);
            assert_search_results!(search_nodes, filter, expected_results);

            let filter = NodeFilter::name().includes(vec!["2".into(), "3".into()]);
            let expected_results = vec!["2", "3"];
            assert_filter_results!(filter_nodes, filter, expected_results);
            assert_search_results!(search_nodes, filter, expected_results);
        }

        #[test]
        fn test_nodes_for_node_name_not_in() {
            let filter = NodeFilter::name().excludes(vec!["1".into()]);
            let expected_results = vec!["2", "3", "4"];
            assert_filter_results!(filter_nodes, filter, expected_results);
            assert_search_results!(search_nodes, filter, expected_results);
        }

        #[test]
        fn test_nodes_for_node_type_eq() {
            let filter = NodeFilter::node_type().eq("fire_nation");
            let expected_results = vec!["1", "3"];
            assert_filter_results!(filter_nodes, filter, expected_results);
            assert_search_results!(search_nodes, filter, expected_results);
        }

        #[test]
        fn test_nodes_for_node_type_ne() {
            let filter = NodeFilter::node_type().ne("fire_nation");
            let expected_results = vec!["2", "4"];
            assert_filter_results!(filter_nodes, filter, expected_results);
            assert_search_results!(search_nodes, filter, expected_results);
        }

        #[test]
        fn test_nodes_for_node_type_in() {
            let filter = NodeFilter::node_type().includes(vec!["fire_nation".into()]);
            let expected_results = vec!["1", "3"];
            assert_filter_results!(filter_nodes, filter, expected_results);
            assert_search_results!(search_nodes, filter, expected_results);

            let filter =
                NodeFilter::node_type().includes(vec!["fire_nation".into(), "air_nomads".into()]);
            let expected_results = vec!["1", "2", "3"];
            assert_filter_results!(filter_nodes, filter, expected_results);
            assert_search_results!(search_nodes, filter, expected_results);
        }

        #[test]
        fn test_nodes_for_node_type_not_in() {
            let filter = NodeFilter::node_type().excludes(vec!["fire_nation".into()]);
            let expected_results = vec!["2", "4"];
            assert_filter_results!(filter_nodes, filter, expected_results);
            assert_search_results!(search_nodes, filter, expected_results);
        }
    }

    #[cfg(test)]
    mod test_node_composite_filter {
        use crate::{
            db::{
                api::view::node::NodeViewOps,
                graph::views::filter::{
                    internal::InternalNodeFilterOps,
                    test_filters::{filter_nodes_with, init_nodes_graph},
                    AndFilter, AsNodeFilter, ComposableFilter, NodeFilter, NodeFilterBuilderOps,
                    OrFilter, PropertyFilterOps,
                },
            },
            prelude::{Graph, GraphViewOps, PropertyFilter},
        };

        #[cfg(feature = "search")]
        use crate::db::graph::views::filter::test_filters::search_nodes_with;

        fn filter_nodes_and<L, R>(filter: AndFilter<L, R>) -> Vec<String>
        where
            L: InternalNodeFilterOps,
            R: InternalNodeFilterOps,
        {
            filter_nodes_with(filter, || init_nodes_graph(Graph::new()))
        }

        fn filter_nodes_or<L, R>(filter: OrFilter<L, R>) -> Vec<String>
        where
            L: InternalNodeFilterOps,
            R: InternalNodeFilterOps,
        {
            filter_nodes_with(filter, || init_nodes_graph(Graph::new()))
        }

        #[cfg(feature = "search")]
        fn search_nodes_and<L: AsNodeFilter, R: AsNodeFilter>(
            filter: AndFilter<L, R>,
        ) -> Vec<String> {
            search_nodes_with(filter, || init_nodes_graph(Graph::new()))
        }

        #[cfg(feature = "search")]
        fn search_nodes_or<L: AsNodeFilter, R: AsNodeFilter>(
            filter: OrFilter<L, R>,
        ) -> Vec<String> {
            search_nodes_with(filter, || init_nodes_graph(Graph::new()))
        }

        #[test]
        fn test_filter_nodes_by_props_added_at_different_times() {
            let filter = PropertyFilter::property("p4")
                .eq("pometry")
                .and(PropertyFilter::property("p5").eq(12u64));
            let expected_results = vec!["4"];
            assert_filter_results!(filter_nodes_and, filter, expected_results);
            assert_search_results!(search_nodes_and, filter, expected_results);
        }

        #[test]
        fn test_node_composite_filter() {
            let filter = PropertyFilter::property("p2")
                .eq(2u64)
                .and(PropertyFilter::property("p1").eq("kapoor"));
            let expected_results = Vec::<String>::new();
            assert_filter_results!(filter_nodes_and, filter, expected_results);
            assert_search_results!(search_nodes_and, filter, expected_results);

            let filter = PropertyFilter::property("p2")
                .eq(2u64)
                .or(PropertyFilter::property("p1").eq("shivam_kapoor"));
            let expected_results = vec!["1", "2"];
            assert_filter_results!(filter_nodes_or, filter, expected_results);
            assert_search_results!(search_nodes_or, filter, expected_results);

            let filter = PropertyFilter::property("p1")
                .eq("pometry")
                .or(PropertyFilter::property("p2")
                    .eq(6u64)
                    .and(PropertyFilter::property("p3").eq(1u64)));
            let expected_results = vec!["3"];
            assert_filter_results!(filter_nodes_or, filter, expected_results);
            assert_search_results!(search_nodes_or, filter, expected_results);

            let filter = NodeFilter::node_type()
                .eq("fire_nation")
                .and(PropertyFilter::property("p1").eq("prop1"));
            let expected_results = Vec::<String>::new();
            assert_filter_results!(filter_nodes_and, filter, expected_results);
            assert_search_results!(search_nodes_and, filter, expected_results);

            let filter = PropertyFilter::property("p9")
                .eq(5u64)
                .and(PropertyFilter::property("p1").eq("shivam_kapoor"));
            let expected_results = vec!["1"];
            assert_filter_results!(filter_nodes_and, filter, expected_results);
            assert_search_results!(search_nodes_and, filter, expected_results);

            let filter = NodeFilter::node_type()
                .eq("fire_nation")
                .and(PropertyFilter::property("p1").eq("shivam_kapoor"));
            let expected_results = vec!["1"];
            assert_filter_results!(filter_nodes_and, filter, expected_results);
            assert_search_results!(search_nodes_and, filter, expected_results);

            let filter = NodeFilter::name()
                .eq("2")
                .and(PropertyFilter::property("p2").eq(2u64));
            let expected_results = vec!["2"];
            assert_filter_results!(filter_nodes_and, filter, expected_results);
            assert_search_results!(search_nodes_and, filter, expected_results);

            let filter = NodeFilter::name()
                .eq("2")
                .and(PropertyFilter::property("p2").eq(2u64))
                .or(PropertyFilter::property("p9").eq(5u64));
            let expected_results = vec!["1", "2"];
            assert_filter_results!(filter_nodes_or, filter, expected_results);
            assert_search_results!(search_nodes_or, filter, expected_results);
        }
    }

    #[cfg(test)]
    mod test_edge_filter {
        use crate::{
            db::graph::views::filter::{
                test_filters::{filter_edges_with, init_edges_graph},
                EdgeFieldFilter, EdgeFilter, EdgeFilterOps,
            },
            prelude::{EdgeViewOps, Graph, NodeViewOps},
        };

        #[cfg(feature = "search")]
        use crate::db::graph::views::filter::test_filters::search_edges_with;

        fn filter_edges(filter: EdgeFieldFilter) -> Vec<String> {
            filter_edges_with(filter, || init_edges_graph(Graph::new()))
        }

        #[cfg(feature = "search")]
        fn search_edges(filter: EdgeFieldFilter) -> Vec<String> {
            search_edges_with(filter, || init_edges_graph(Graph::new()))
        }

        #[test]
        fn test_filter_edges_for_src_eq() {
            let filter = EdgeFilter::src().eq("3");
            let expected_results = vec!["3->1"];
            assert_filter_results!(filter_edges, filter, expected_results);
            assert_search_results!(search_edges, filter, expected_results);
        }

        #[test]
        fn test_filter_edges_for_src_ne() {
            let filter = EdgeFilter::src().ne("1");
            let expected_results = vec!["2->1", "2->3", "3->1"];
            assert_filter_results!(filter_edges, filter, expected_results);
            assert_search_results!(search_edges, filter, expected_results);
        }

        #[test]
        fn test_filter_edges_for_src_in() {
            let filter = EdgeFilter::src().includes(vec!["1".into()]);
            let expected_results = vec!["1->2"];
            assert_filter_results!(filter_edges, filter, expected_results);
            assert_search_results!(search_edges, filter, expected_results);

            let filter = EdgeFilter::src().includes(vec!["1".into(), "2".into()]);
            let expected_results = vec!["1->2", "2->1", "2->3"];
            assert_filter_results!(filter_edges, filter, expected_results);
            assert_search_results!(search_edges, filter, expected_results);
        }

        #[test]
        fn test_filter_edges_for_src_not_in() {
            let filter = EdgeFilter::src().excludes(vec!["1".into()]);
            let expected_results = vec!["2->1", "2->3", "3->1"];
            assert_filter_results!(filter_edges, filter, expected_results);
            assert_search_results!(search_edges, filter, expected_results);
        }

        #[test]
        fn test_filter_edges_for_dst_eq() {
            let filter = EdgeFilter::dst().eq("2");
            let expected_results = vec!["1->2"];
            assert_filter_results!(filter_edges, filter, expected_results);
            assert_search_results!(search_edges, filter, expected_results);
        }

        #[test]
        fn test_filter_edges_for_dst_ne() {
            let filter = EdgeFilter::dst().ne("2");
            let expected_results = vec!["2->1", "2->3", "3->1"];
            assert_filter_results!(filter_edges, filter, expected_results);
            assert_search_results!(search_edges, filter, expected_results);
        }

        #[test]
        fn test_filter_edges_for_dst_in() {
            let filter = EdgeFilter::dst().includes(vec!["2".into()]);
            let expected_results = vec!["1->2"];
            assert_filter_results!(filter_edges, filter, expected_results);
            assert_search_results!(search_edges, filter, expected_results);

            let filter = EdgeFilter::dst().includes(vec!["2".into(), "3".into()]);
            let expected_results = vec!["1->2", "2->3"];
            assert_filter_results!(filter_edges, filter, expected_results);
            assert_search_results!(search_edges, filter, expected_results);
        }

        #[test]
        fn test_filter_edges_for_dst_not_in() {
            let filter = EdgeFilter::dst().excludes(vec!["1".into()]);
            let expected_results = vec!["1->2", "2->3"];
            assert_filter_results!(filter_edges, filter, expected_results);
            assert_search_results!(search_edges, filter, expected_results);
        }
    }

    #[cfg(test)]
    mod test_edge_composite_filter {
        use crate::{
            db::graph::views::filter::{
                internal::InternalEdgeFilterOps,
                test_filters::{filter_edges_with, init_edges_graph},
                AndFilter, AsEdgeFilter, ComposableFilter, EdgeFieldFilter, EdgeFilter,
                EdgeFilterOps, OrFilter, PropertyFilterOps,
            },
            prelude::{EdgeViewOps, Graph, NodeViewOps, PropertyFilter},
        };

        #[cfg(feature = "search")]
        use crate::db::graph::views::filter::test_filters::search_edges_with;

        fn filter_edges_and<L, R>(filter: AndFilter<L, R>) -> Vec<String>
        where
            L: InternalEdgeFilterOps,
            R: InternalEdgeFilterOps,
        {
            filter_edges_with(filter, || init_edges_graph(Graph::new()))
        }

        fn filter_edges_or<L, R>(filter: OrFilter<L, R>) -> Vec<String>
        where
            L: InternalEdgeFilterOps,
            R: InternalEdgeFilterOps,
        {
            filter_edges_with(filter, || init_edges_graph(Graph::new()))
        }

        #[cfg(feature = "search")]
        fn search_edges_and<L: AsEdgeFilter, R: AsEdgeFilter>(
            filter: AndFilter<L, R>,
        ) -> Vec<String> {
            search_edges_with(filter, || init_edges_graph(Graph::new()))
        }

        #[cfg(feature = "search")]
        fn search_edges_or<L: AsEdgeFilter, R: AsEdgeFilter>(
            filter: OrFilter<L, R>,
        ) -> Vec<String> {
            search_edges_with(filter, || init_edges_graph(Graph::new()))
        }

        #[test]
        fn test_edge_for_src_dst() {
            let filter: AndFilter<EdgeFieldFilter, EdgeFieldFilter> =
                EdgeFilter::src().eq("3").and(EdgeFilter::dst().eq("1"));
            let expected_results = vec!["3->1"];
            assert_filter_results!(filter_edges_and, filter, expected_results);
            assert_search_results!(search_edges_and, filter, expected_results);
        }

        #[test]
        fn test_edge_composite_filter() {
            let filter = PropertyFilter::property("p2")
                .eq(2u64)
                .and(PropertyFilter::property("p1").eq("kapoor"));
            let expected_results = Vec::<String>::new();
            assert_filter_results!(filter_edges_and, filter, expected_results);
            assert_search_results!(search_edges_and, filter, expected_results);

            let filter = PropertyFilter::property("p2")
                .eq(2u64)
                .or(PropertyFilter::property("p1").eq("shivam_kapoor"));
            let expected_results = vec!["1->2", "2->3"];
            assert_filter_results!(filter_edges_or, filter, expected_results);
            assert_search_results!(search_edges_or, filter, expected_results);

            let filter = PropertyFilter::property("p1")
                .eq("pometry")
                .or(PropertyFilter::property("p2")
                    .eq(6u64)
                    .and(PropertyFilter::property("p3").eq(1u64)));
            let expected_results = vec!["2->1", "3->1"];
            assert_filter_results!(filter_edges_or, filter, expected_results);
            assert_search_results!(search_edges_or, filter, expected_results);

            let filter = EdgeFilter::src()
                .eq("13")
                .and(PropertyFilter::property("p1").eq("prop1"));
            let expected_results = Vec::<String>::new();
            assert_filter_results!(filter_edges_and, filter, expected_results);
            assert_search_results!(search_edges_and, filter, expected_results);

            let filter = PropertyFilter::property("p2")
                .eq(4u64)
                .and(PropertyFilter::property("p1").eq("shivam_kapoor"));
            let expected_results = vec!["1->2"];
            assert_filter_results!(filter_edges_and, filter, expected_results);
            assert_search_results!(search_edges_and, filter, expected_results);

            let filter = EdgeFilter::src()
                .eq("1")
                .and(PropertyFilter::property("p1").eq("shivam_kapoor"));
            let expected_results = vec!["1->2"];
            assert_filter_results!(filter_edges_and, filter, expected_results);
            assert_search_results!(search_edges_and, filter, expected_results);

            let filter = EdgeFilter::dst()
                .eq("1")
                .and(PropertyFilter::property("p2").eq(6u64));
            let expected_results = vec!["2->1", "3->1"];
            assert_filter_results!(filter_edges_and, filter, expected_results);
            assert_search_results!(search_edges_and, filter, expected_results);

            let filter = EdgeFilter::src()
                .eq("1")
                .and(PropertyFilter::property("p1").eq("shivam_kapoor"))
                .or(PropertyFilter::property("p3").eq(5u64));
            let expected_results = vec!["1->2"];
            assert_filter_results!(filter_edges_or, filter, expected_results);
            assert_search_results!(search_edges_or, filter, expected_results);
        }
    }
}
