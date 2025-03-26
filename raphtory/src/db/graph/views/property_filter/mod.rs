use crate::core::{
    entities::properties::props::Meta, sort_comparable_props, utils::errors::GraphError, Prop,
};
use itertools::Itertools;
use raphtory_api::core::storage::arc_str::ArcStr;
use std::{collections::HashSet, fmt, fmt::Display, sync::Arc};
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
}

#[derive(Debug, Clone)]
pub enum CompositeNodeFilter {
    Node(Filter),
    Property(PropertyFilter),
    And(Vec<CompositeNodeFilter>),
    Or(Vec<CompositeNodeFilter>),
}

impl Display for CompositeNodeFilter {
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

impl Display for CompositeEdgeFilter {
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

// Fluent Composite Filter Builder APIs
#[derive(Clone, Debug)]
pub enum FilterExpr {
    Node(Filter),
    Edge(Filter),
    Property(PropertyFilter),
    And(Vec<FilterExpr>),
    Or(Vec<FilterExpr>),
}

impl Display for FilterExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl FilterExpr {
    pub fn and(self, other: FilterExpr) -> Self {
        match self {
            FilterExpr::And(mut filters) => {
                filters.push(other);
                FilterExpr::And(filters)
            }
            _ => FilterExpr::And(vec![self, other]),
        }
    }

    pub fn or(self, other: FilterExpr) -> Self {
        match self {
            FilterExpr::Or(mut filters) => {
                filters.push(other);
                FilterExpr::Or(filters)
            }
            _ => FilterExpr::Or(vec![self, other]),
        }
    }
}

pub fn resolve_as_node_filter(filter: FilterExpr) -> Result<CompositeNodeFilter, GraphError> {
    match filter {
        FilterExpr::Property(prop) => Ok(CompositeNodeFilter::Property(prop)),
        FilterExpr::Node(filter) => Ok(CompositeNodeFilter::Node(filter)),
        FilterExpr::And(filters) => Ok(CompositeNodeFilter::And(
            filters
                .into_iter()
                .map(resolve_as_node_filter)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        FilterExpr::Or(filters) => Ok(CompositeNodeFilter::Or(
            filters
                .into_iter()
                .map(resolve_as_node_filter)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        FilterExpr::Edge(_) => Err(GraphError::IllegalFilterExpr(
            filter,
            "Edge filter cannot be used in node filtering!".to_string(),
        )),
    }
}

pub fn resolve_as_edge_filter(filter: FilterExpr) -> Result<CompositeEdgeFilter, GraphError> {
    match filter {
        FilterExpr::Property(prop) => Ok(CompositeEdgeFilter::Property(prop)),
        FilterExpr::Edge(filter) => Ok(CompositeEdgeFilter::Edge(filter)),
        FilterExpr::And(filters) => Ok(CompositeEdgeFilter::And(
            filters
                .into_iter()
                .map(resolve_as_edge_filter)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        FilterExpr::Or(filters) => Ok(CompositeEdgeFilter::Or(
            filters
                .into_iter()
                .map(resolve_as_edge_filter)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        FilterExpr::Node(_) => Err(GraphError::IllegalFilterExpr(
            filter,
            "Node filter cannot be used in edge filtering!".to_string(),
        )),
    }
}

// TODO: This code may go once raphtory APIs start supporting FilterExpr
pub fn resolve_as_property_filter(filter: FilterExpr) -> Result<PropertyFilter, GraphError> {
    match filter {
        FilterExpr::Property(prop) => Ok(prop),
        _ => Err(GraphError::IllegalFilterExpr(
            filter,
            "Non-property filter cannot be used in strictly property filtering!".to_string(),
        )),
    }
}

pub trait PropertyFilterOps {
    fn property_ref(&self) -> PropertyRef;

    fn eq(self, value: impl Into<Prop>) -> FilterExpr
    where
        Self: Sized,
    {
        FilterExpr::Property(PropertyFilter::eq(self.property_ref(), value.into()))
    }

    fn ne(self, value: impl Into<Prop>) -> FilterExpr
    where
        Self: Sized,
    {
        FilterExpr::Property(PropertyFilter::ne(self.property_ref(), value.into()))
    }

    fn le(self, value: impl Into<Prop>) -> FilterExpr
    where
        Self: Sized,
    {
        FilterExpr::Property(PropertyFilter::le(self.property_ref(), value.into()))
    }

    fn ge(self, value: impl Into<Prop>) -> FilterExpr
    where
        Self: Sized,
    {
        FilterExpr::Property(PropertyFilter::ge(self.property_ref(), value.into()))
    }

    fn lt(self, value: impl Into<Prop>) -> FilterExpr
    where
        Self: Sized,
    {
        FilterExpr::Property(PropertyFilter::lt(self.property_ref(), value.into()))
    }

    fn gt(self, value: impl Into<Prop>) -> FilterExpr
    where
        Self: Sized,
    {
        FilterExpr::Property(PropertyFilter::gt(self.property_ref(), value.into()))
    }

    fn includes(self, values: impl IntoIterator<Item = Prop>) -> FilterExpr
    where
        Self: Sized,
    {
        FilterExpr::Property(PropertyFilter::includes(
            self.property_ref(),
            values.into_iter(),
        ))
    }

    fn excludes(self, values: impl IntoIterator<Item = Prop>) -> FilterExpr
    where
        Self: Sized,
    {
        FilterExpr::Property(PropertyFilter::excludes(
            self.property_ref(),
            values.into_iter(),
        ))
    }

    fn is_none(self) -> FilterExpr
    where
        Self: Sized,
    {
        FilterExpr::Property(PropertyFilter::is_none(self.property_ref()))
    }

    fn is_some(self) -> FilterExpr
    where
        Self: Sized,
    {
        FilterExpr::Property(PropertyFilter::is_some(self.property_ref()))
    }

    fn fuzzy_search(
        self,
        prop_value: impl Into<String>,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> FilterExpr
    where
        Self: Sized,
    {
        FilterExpr::Property(PropertyFilter::fuzzy_search(
            self.property_ref(),
            prop_value.into(),
            levenshtein_distance,
            prefix_match,
        ))
    }
}

pub struct PropertyFilterBuilder(String);

impl PropertyFilterBuilder {
    pub fn constant(self) -> ConstPropertyFilterBuilder {
        ConstPropertyFilterBuilder(self.0)
    }

    pub fn temporal(self) -> TemporalPropertyFilterBuilder {
        TemporalPropertyFilterBuilder(self.0)
    }
}

impl PropertyFilterOps for PropertyFilterBuilder {
    fn property_ref(&self) -> PropertyRef {
        PropertyRef::Property(self.0.clone())
    }
}

pub struct ConstPropertyFilterBuilder(String);

impl PropertyFilterOps for ConstPropertyFilterBuilder {
    fn property_ref(&self) -> PropertyRef {
        PropertyRef::ConstantProperty(self.0.clone())
    }
}

pub struct AnyTemporalPropertyFilterBuilder(String);

impl PropertyFilterOps for AnyTemporalPropertyFilterBuilder {
    fn property_ref(&self) -> PropertyRef {
        PropertyRef::TemporalProperty(self.0.clone(), Temporal::Any)
    }
}

pub struct LatestTemporalPropertyFilterBuilder(String);

impl PropertyFilterOps for LatestTemporalPropertyFilterBuilder {
    fn property_ref(&self) -> PropertyRef {
        PropertyRef::TemporalProperty(self.0.clone(), Temporal::Latest)
    }
}

pub struct TemporalPropertyFilterBuilder(String);

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

pub trait NodeFilterOps {
    fn field_name(&self) -> &'static str;

    fn eq(self, value: impl Into<String>) -> FilterExpr
    where
        Self: Sized,
    {
        FilterExpr::Node(Filter::eq(self.field_name(), value))
    }

    fn ne(self, value: impl Into<String>) -> FilterExpr
    where
        Self: Sized,
    {
        FilterExpr::Node(Filter::ne(self.field_name(), value))
    }

    fn includes(self, values: impl IntoIterator<Item = String>) -> FilterExpr
    where
        Self: Sized,
    {
        FilterExpr::Node(Filter::includes(self.field_name(), values))
    }

    fn excludes(self, values: impl IntoIterator<Item = String>) -> FilterExpr
    where
        Self: Sized,
    {
        FilterExpr::Node(Filter::excludes(self.field_name(), values))
    }

    fn fuzzy_search(
        self,
        value: impl Into<String>,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> FilterExpr
    where
        Self: Sized,
    {
        FilterExpr::Node(Filter::fuzzy_search(
            self.field_name(),
            value,
            levenshtein_distance,
            prefix_match,
        ))
    }
}

pub struct NodeNameFilterBuilder;

impl NodeFilterOps for NodeNameFilterBuilder {
    fn field_name(&self) -> &'static str {
        "node_name"
    }
}

pub struct NodeTypeFilterBuilder;

impl NodeFilterOps for NodeTypeFilterBuilder {
    fn field_name(&self) -> &'static str {
        "node_type"
    }
}

pub struct NodeFilter;

impl NodeFilter {
    pub fn node_name() -> NodeNameFilterBuilder {
        NodeNameFilterBuilder
    }

    pub fn node_type() -> NodeTypeFilterBuilder {
        NodeTypeFilterBuilder
    }
}

pub trait EdgeFilterOps {
    fn field_name(&self) -> &'static str;

    fn eq(self, value: impl Into<String>) -> FilterExpr
    where
        Self: Sized,
    {
        FilterExpr::Edge(Filter::eq(self.field_name(), value))
    }

    fn ne(self, value: impl Into<String>) -> FilterExpr
    where
        Self: Sized,
    {
        FilterExpr::Edge(Filter::ne(self.field_name(), value))
    }

    fn includes(self, values: impl IntoIterator<Item = String>) -> FilterExpr
    where
        Self: Sized,
    {
        FilterExpr::Edge(Filter::includes(self.field_name(), values))
    }

    fn excludes(self, values: impl IntoIterator<Item = String>) -> FilterExpr
    where
        Self: Sized,
    {
        FilterExpr::Edge(Filter::excludes(self.field_name(), values))
    }

    fn fuzzy_search(
        self,
        value: impl Into<String>,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> FilterExpr
    where
        Self: Sized,
    {
        FilterExpr::Edge(Filter::fuzzy_search(
            self.field_name(),
            value,
            levenshtein_distance,
            prefix_match,
        ))
    }
}

pub struct EdgeSourceFilterBuilder;

impl EdgeFilterOps for EdgeSourceFilterBuilder {
    fn field_name(&self) -> &'static str {
        "src"
    }
}

pub struct EdgeDestinationFilterBuilder;

impl EdgeFilterOps for EdgeDestinationFilterBuilder {
    fn field_name(&self) -> &'static str {
        "dst"
    }
}

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
    use super::*;
    use PropertyFilter;

    #[test]
    fn test_node_property_filter_build() {
        let filter_expr = PropertyFilter::property("p").eq("raphtory");
        let node_property_filter = resolve_as_node_filter(filter_expr).unwrap();
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
        let node_property_filter = resolve_as_node_filter(filter_expr).unwrap();
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
        let filter_expr = PropertyFilter::property("p").temporal().any().eq("raphtory");
        let node_property_filter = resolve_as_node_filter(filter_expr).unwrap();
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
        let filter_expr = PropertyFilter::property("p").temporal()
            .latest()
            .eq("raphtory");
        let node_property_filter = resolve_as_node_filter(filter_expr).unwrap();
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
        let node_property_filter = resolve_as_node_filter(filter_expr).unwrap();
        let node_property_filter2 = CompositeNodeFilter::Node(Filter::eq("node_name", "raphtory"));
        assert_eq!(
            node_property_filter.to_string(),
            node_property_filter2.to_string()
        );
    }

    #[test]
    fn test_node_type_filter_build() {
        let filter_expr = NodeFilter::node_type().eq("raphtory");
        let node_property_filter = resolve_as_node_filter(filter_expr).unwrap();
        let node_property_filter2 = CompositeNodeFilter::Node(Filter::eq("node_type", "raphtory"));
        assert_eq!(
            node_property_filter.to_string(),
            node_property_filter2.to_string()
        );
    }

    #[test]
    fn test_node_filter_composition() {
        let filter_expr = NodeFilter::node_name()
            .eq("fire_nation")
            .and(PropertyFilter::property("p2").constant().eq(2u64))
            .and(PropertyFilter::property("p1").eq(1u64))
            .and(
                PropertyFilter::property("p3").temporal()
                    .any()
                    .eq(5u64)
                    .or(PropertyFilter::property("p4").temporal().latest().eq(7u64)),
            )
            .or(NodeFilter::node_type().eq("raphtory"))
            .or(PropertyFilter::property("p5").eq(9u64));
        let node_composite_filter = resolve_as_node_filter(filter_expr).unwrap();

        let node_composite_filter2 = CompositeNodeFilter::Or(vec![
            CompositeNodeFilter::And(vec![
                CompositeNodeFilter::Node(Filter::eq("node_name", "fire_nation")),
                CompositeNodeFilter::Property(PropertyFilter::eq(
                    PropertyRef::ConstantProperty("p2".to_string()),
                    2u64,
                )),
                CompositeNodeFilter::Property(PropertyFilter::eq(
                    PropertyRef::Property("p1".to_string()),
                    1u64,
                )),
                CompositeNodeFilter::Or(vec![
                    CompositeNodeFilter::Property(PropertyFilter::eq(
                        PropertyRef::TemporalProperty("p3".to_string(), Temporal::Any),
                        5u64,
                    )),
                    CompositeNodeFilter::Property(PropertyFilter::eq(
                        PropertyRef::TemporalProperty("p4".to_string(), Temporal::Latest),
                        7u64,
                    )),
                ]),
            ]),
            CompositeNodeFilter::Node(Filter::eq("node_type", "raphtory")),
            CompositeNodeFilter::Property(PropertyFilter::eq(
                PropertyRef::Property("p5".to_string()),
                9u64,
            )),
        ]);

        assert_eq!(
            node_composite_filter.to_string(),
            node_composite_filter2.to_string()
        );
    }

    #[test]
    fn test_edge_src_filter_build() {
        let filter_expr = EdgeFilter::src().eq("raphtory");
        let edge_property_filter = resolve_as_edge_filter(filter_expr).unwrap();
        let edge_property_filter2 = CompositeEdgeFilter::Edge(Filter::eq("src", "raphtory"));
        assert_eq!(
            edge_property_filter.to_string(),
            edge_property_filter2.to_string()
        );
    }

    #[test]
    fn test_edge_dst_filter_build() {
        let filter_expr = EdgeFilter::dst().eq("raphtory");
        let edge_property_filter = resolve_as_edge_filter(filter_expr).unwrap();
        let edge_property_filter2 = CompositeEdgeFilter::Edge(Filter::eq("dst", "raphtory"));
        assert_eq!(
            edge_property_filter.to_string(),
            edge_property_filter2.to_string()
        );
    }

    #[test]
    fn test_edge_filter_composition() {
        let filter_expr = EdgeFilter::src()
            .eq("fire_nation")
            .and(PropertyFilter::property("p2").constant().eq(2u64))
            .and(PropertyFilter::property("p1").eq(1u64))
            .and(
                PropertyFilter::property("p3").temporal()
                    .any()
                    .eq(5u64)
                    .or(PropertyFilter::property("p4").temporal().latest().eq(7u64)),
            )
            .or(EdgeFilter::src().eq("raphtory"))
            .or(PropertyFilter::property("p5").eq(9u64));
        let edge_composite_filter = resolve_as_edge_filter(filter_expr).unwrap();

        let edge_composite_filter2 = CompositeEdgeFilter::Or(vec![
            CompositeEdgeFilter::And(vec![
                CompositeEdgeFilter::Edge(Filter::eq("src", "fire_nation")),
                CompositeEdgeFilter::Property(PropertyFilter::eq(
                    PropertyRef::ConstantProperty("p2".to_string()),
                    2u64,
                )),
                CompositeEdgeFilter::Property(PropertyFilter::eq(
                    PropertyRef::Property("p1".to_string()),
                    1u64,
                )),
                CompositeEdgeFilter::Or(vec![
                    CompositeEdgeFilter::Property(PropertyFilter::eq(
                        PropertyRef::TemporalProperty("p3".to_string(), Temporal::Any),
                        5u64,
                    )),
                    CompositeEdgeFilter::Property(PropertyFilter::eq(
                        PropertyRef::TemporalProperty("p4".to_string(), Temporal::Latest),
                        7u64,
                    )),
                ]),
            ]),
            CompositeEdgeFilter::Edge(Filter::eq("src", "raphtory")),
            CompositeEdgeFilter::Property(PropertyFilter::eq(
                PropertyRef::Property("p5".to_string()),
                9u64,
            )),
        ]);

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
        db::graph::views::property_filter::{
            CompositeEdgeFilter, CompositeNodeFilter, Filter, PropertyRef,
        },
        prelude::PropertyFilter,
    };
    use raphtory_api::core::storage::arc_str::ArcStr;

    #[test]
    fn test_composite_node_filter() {
        assert_eq!(
            "NODE_PROPERTY(p2 == 2)",
            CompositeNodeFilter::Property(PropertyFilter::eq(
                PropertyRef::Property("p2".to_string()),
                2u64
            ))
            .to_string()
        );

        assert_eq!(
            "((NODE(node_type NOT_IN [fire_nation, water_tribe])) AND (NODE_PROPERTY(p2 == 2)) AND (NODE_PROPERTY(p1 == 1)) AND ((NODE_PROPERTY(p3 <= 5)) OR (NODE_PROPERTY(p4 IN [2, 10])))) OR (NODE(node_name == pometry)) OR (NODE_PROPERTY(p5 == 9))",
            CompositeNodeFilter::Or(vec![
                CompositeNodeFilter::And(vec![
                    CompositeNodeFilter::Node(Filter::excludes(
                        "node_type",
                        vec!["fire_nation".into(), "water_tribe".into()]
                    )),
                    CompositeNodeFilter::Property(PropertyFilter::eq(PropertyRef::Property("p2".to_string()), 2u64)),
                    CompositeNodeFilter::Property(PropertyFilter::eq(PropertyRef::Property("p1".to_string()), 1u64)),
                    CompositeNodeFilter::Or(vec![
                        CompositeNodeFilter::Property(PropertyFilter::le(PropertyRef::Property("p3".to_string()), 5u64)),
                        CompositeNodeFilter::Property(PropertyFilter::includes(PropertyRef::Property("p4".to_string()), vec![Prop::U64(10), Prop::U64(2)]))
                    ]),
                ]),
                CompositeNodeFilter::Node(Filter::eq("node_name", "pometry")),
                CompositeNodeFilter::Property(PropertyFilter::eq(PropertyRef::Property("p5".to_string()), 9u64)),
            ])
                .to_string()
        );

        assert_eq!(
            "(NODE(name FUZZY_SEARCH(1,true) shivam)) AND (NODE_PROPERTY(nation FUZZY_SEARCH(1,false) air_nomad))",
            CompositeNodeFilter::And(vec![
                CompositeNodeFilter::Node(Filter::fuzzy_search("name", "shivam", 1, true)),
                CompositeNodeFilter::Property(PropertyFilter::fuzzy_search(
                    PropertyRef::Property("nation".to_string()),
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
            CompositeEdgeFilter::Property(PropertyFilter::eq(
                PropertyRef::Property("p2".to_string()),
                2u64
            ))
            .to_string()
        );

        assert_eq!(
            "((EDGE(edge_type NOT_IN [fire_nation, water_tribe])) AND (EDGE_PROPERTY(p2 == 2)) AND (EDGE_PROPERTY(p1 == 1)) AND ((EDGE_PROPERTY(p3 <= 5)) OR (EDGE_PROPERTY(p4 IN [2, 10])))) OR (EDGE(src == pometry)) OR (EDGE_PROPERTY(p5 == 9))",
            CompositeEdgeFilter::Or(vec![
                CompositeEdgeFilter::And(vec![
                    CompositeEdgeFilter::Edge(Filter::excludes(
                        "edge_type",
                        vec!["fire_nation".into(), "water_tribe".into()]
                    )),
                    CompositeEdgeFilter::Property(PropertyFilter::eq(PropertyRef::Property("p2".to_string()), 2u64)),
                    CompositeEdgeFilter::Property(PropertyFilter::eq(PropertyRef::Property("p1".to_string()), 1u64)),
                    CompositeEdgeFilter::Or(vec![
                        CompositeEdgeFilter::Property(PropertyFilter::le(PropertyRef::Property("p3".to_string()), 5u64)),
                        CompositeEdgeFilter::Property(PropertyFilter::includes(PropertyRef::Property("p4".to_string()), vec![Prop::U64(10), Prop::U64(2)]))
                    ]),
                ]),
                CompositeEdgeFilter::Edge(Filter::eq("src", "pometry")),
                CompositeEdgeFilter::Property(PropertyFilter::eq(PropertyRef::Property("p5".to_string()), 9u64)),
            ])
                .to_string()
        );

        assert_eq!(
            "(EDGE(name FUZZY_SEARCH(1,true) shivam)) AND (EDGE_PROPERTY(nation FUZZY_SEARCH(1,false) air_nomad))",
            CompositeEdgeFilter::And(vec![
                CompositeEdgeFilter::Edge(Filter::fuzzy_search("name", "shivam", 1, true)),
                CompositeEdgeFilter::Property(PropertyFilter::fuzzy_search(
                    PropertyRef::Property("nation".to_string()),
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
