use crate::{
    db::{
        api::{
            properties::{internal::InternalPropertiesOps, Metadata, Properties},
            view::{internal::GraphView, node::NodeViewOps, EdgeViewOps},
        },
        graph::{
            edge::EdgeView,
            node::NodeView,
            views::filter::model::{
                edge_filter::{
                    CompositeEdgeFilter, CompositeExplodedEdgeFilter, EdgeFilter,
                    ExplodedEdgeFilter,
                },
                filter_operator::FilterOperator,
                node_filter::{CompositeNodeFilter, NodeFilter},
                TryAsCompositeFilter,
            },
        },
    },
    errors::GraphError,
    prelude::{GraphViewOps, PropertiesOps},
};
use itertools::Itertools;
use raphtory_api::core::{
    entities::{
        properties::{
            meta::Meta,
            prop::{sort_comparable_props, unify_types, Prop, PropType, PropUnwrap},
        },
        EID,
    },
    storage::{arc_str::ArcStr, timeindex::TimeIndexEntry},
};
use raphtory_storage::graph::{
    edges::{edge_ref::EdgeStorageRef, edge_storage_ops::EdgeStorageOps},
    nodes::{node_ref::NodeStorageRef, node_storage_ops::NodeStorageOps},
};
use std::{collections::HashSet, fmt, fmt::Display, marker::PhantomData, ops::Deref, sync::Arc};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListAgg {
    Len,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Temporal {
    Any,
    Latest,
    First,
    All,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PropertyRef {
    Property(String),
    Metadata(String),
    TemporalProperty(String, Temporal),
}

impl Display for PropertyRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PropertyRef::TemporalProperty(name, temporal) => {
                write!(f, "TemporalProperty({}, {:?})", name, temporal)
            }
            PropertyRef::Metadata(name) => write!(f, "Metadata({})", name),
            PropertyRef::Property(name) => write!(f, "Property({})", name),
        }
    }
}

impl PropertyRef {
    pub fn name(&self) -> &str {
        match self {
            PropertyRef::Property(name)
            | PropertyRef::Metadata(name)
            | PropertyRef::TemporalProperty(name, _) => name,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PropertyFilterValue {
    None,
    Single(Prop),
    Set(Arc<HashSet<Prop>>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertyFilter<M> {
    pub prop_ref: PropertyRef,
    pub prop_value: PropertyFilterValue,
    pub operator: FilterOperator,
    pub list_agg: Option<ListAgg>,
    pub _phantom: PhantomData<M>,
}

impl<M> Display for PropertyFilter<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let prop_ref_str = match &self.prop_ref {
            PropertyRef::Property(name) => name.to_string(),
            PropertyRef::Metadata(name) => format!("const({})", name),
            PropertyRef::TemporalProperty(name, Temporal::Any) => format!("temporal_any({})", name),
            PropertyRef::TemporalProperty(name, Temporal::Latest) => {
                format!("temporal_latest({})", name)
            }
            PropertyRef::TemporalProperty(name, Temporal::First) => {
                format!("temporal_first({})", name)
            }
            PropertyRef::TemporalProperty(name, Temporal::All) => {
                format!("temporal_all({})", name)
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

impl<M> PropertyFilter<M> {
    pub fn with_list_agg(mut self, agg: Option<ListAgg>) -> Self {
        self.list_agg = agg;
        self
    }

    pub fn eq(prop_ref: PropertyRef, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_ref,
            prop_value: PropertyFilterValue::Single(prop_value.into()),
            operator: FilterOperator::Eq,
            list_agg: None,
            _phantom: PhantomData,
        }
    }

    pub fn ne(prop_ref: PropertyRef, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_ref,
            prop_value: PropertyFilterValue::Single(prop_value.into()),
            operator: FilterOperator::Ne,
            list_agg: None,
            _phantom: PhantomData,
        }
    }

    pub fn le(prop_ref: PropertyRef, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_ref,
            prop_value: PropertyFilterValue::Single(prop_value.into()),
            operator: FilterOperator::Le,
            list_agg: None,
            _phantom: PhantomData,
        }
    }

    pub fn ge(prop_ref: PropertyRef, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_ref,
            prop_value: PropertyFilterValue::Single(prop_value.into()),
            operator: FilterOperator::Ge,
            list_agg: None,
            _phantom: PhantomData,
        }
    }

    pub fn lt(prop_ref: PropertyRef, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_ref,
            prop_value: PropertyFilterValue::Single(prop_value.into()),
            operator: FilterOperator::Lt,
            list_agg: None,
            _phantom: PhantomData,
        }
    }

    pub fn gt(prop_ref: PropertyRef, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_ref,
            prop_value: PropertyFilterValue::Single(prop_value.into()),
            operator: FilterOperator::Gt,
            list_agg: None,
            _phantom: PhantomData,
        }
    }

    pub fn is_in(prop_ref: PropertyRef, prop_values: impl IntoIterator<Item = Prop>) -> Self {
        Self {
            prop_ref,
            prop_value: PropertyFilterValue::Set(Arc::new(prop_values.into_iter().collect())),
            operator: FilterOperator::In,
            list_agg: None,
            _phantom: PhantomData,
        }
    }

    pub fn is_not_in(prop_ref: PropertyRef, prop_values: impl IntoIterator<Item = Prop>) -> Self {
        Self {
            prop_ref,
            prop_value: PropertyFilterValue::Set(Arc::new(prop_values.into_iter().collect())),
            operator: FilterOperator::NotIn,
            list_agg: None,
            _phantom: PhantomData,
        }
    }

    pub fn is_none(prop_ref: PropertyRef) -> Self {
        Self {
            prop_ref,
            prop_value: PropertyFilterValue::None,
            operator: FilterOperator::IsNone,
            list_agg: None,
            _phantom: PhantomData,
        }
    }

    pub fn is_some(prop_ref: PropertyRef) -> Self {
        Self {
            prop_ref,
            prop_value: PropertyFilterValue::None,
            operator: FilterOperator::IsSome,
            list_agg: None,
            _phantom: PhantomData,
        }
    }

    pub fn starts_with(prop_ref: PropertyRef, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_ref,
            prop_value: PropertyFilterValue::Single(prop_value.into()),
            operator: FilterOperator::StartsWith,
            list_agg: None,
            _phantom: PhantomData,
        }
    }

    pub fn ends_with(prop_ref: PropertyRef, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_ref,
            prop_value: PropertyFilterValue::Single(prop_value.into()),
            operator: FilterOperator::EndsWith,
            list_agg: None,
            _phantom: PhantomData,
        }
    }

    pub fn contains(prop_ref: PropertyRef, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_ref,
            prop_value: PropertyFilterValue::Single(prop_value.into()),
            operator: FilterOperator::Contains,
            list_agg: None,
            _phantom: PhantomData,
        }
    }

    pub fn not_contains(prop_ref: PropertyRef, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_ref,
            prop_value: PropertyFilterValue::Single(prop_value.into()),
            operator: FilterOperator::NotContains,
            list_agg: None,
            _phantom: PhantomData,
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
            list_agg: None,
            _phantom: PhantomData,
        }
    }

    fn validate_single_dtype(
        &self,
        expected: &PropType,
        expect_map: bool,
    ) -> Result<PropType, GraphError> {
        let filter_dtype = match &self.prop_value {
            PropertyFilterValue::None => {
                return Err(GraphError::InvalidFilterExpectSingleGotNone(self.operator))
            }
            PropertyFilterValue::Single(value) => {
                if expect_map {
                    value.dtype().homogeneous_map_value_type().ok_or_else(|| {
                        GraphError::InvalidHomogeneousMap(expected.clone(), value.dtype())
                    })?
                } else {
                    value.dtype()
                }
            }
            PropertyFilterValue::Set(_) => {
                return Err(GraphError::InvalidFilterExpectSingleGotSet(self.operator))
            }
        };
        unify_types(expected, &filter_dtype, &mut false)
            .map_err(|e| e.with_name(self.prop_ref.name().to_owned()))?;
        Ok(filter_dtype)
    }

    fn validate(&self, dtype: &PropType, expect_map: bool) -> Result<(), GraphError> {
        match self.operator {
            FilterOperator::Eq | FilterOperator::Ne => {
                self.validate_single_dtype(dtype, expect_map)?;
            }
            FilterOperator::Lt | FilterOperator::Le | FilterOperator::Gt | FilterOperator::Ge => {
                let filter_dtype = self.validate_single_dtype(dtype, expect_map)?;
                if !filter_dtype.has_cmp() {
                    return Err(GraphError::InvalidFilterCmp(filter_dtype));
                }
            }
            FilterOperator::In | FilterOperator::NotIn => match &self.prop_value {
                PropertyFilterValue::None => {
                    return Err(GraphError::InvalidFilterExpectSetGotNone(self.operator))
                }
                PropertyFilterValue::Single(_) => {
                    return Err(GraphError::InvalidFilterExpectSetGotSingle(self.operator))
                }
                PropertyFilterValue::Set(_) => {}
            },
            FilterOperator::IsSome | FilterOperator::IsNone => {}
            FilterOperator::StartsWith
            | FilterOperator::EndsWith
            | FilterOperator::Contains
            | FilterOperator::NotContains
            | FilterOperator::FuzzySearch { .. } => {
                match &self.prop_value {
                    PropertyFilterValue::None => {
                        return Err(GraphError::InvalidFilterExpectSingleGotNone(self.operator))
                    }
                    PropertyFilterValue::Single(v) => {
                        if !matches!(dtype, PropType::Str) || !matches!(v.dtype(), PropType::Str) {
                            return Err(GraphError::InvalidContains(self.operator));
                        }
                    }
                    PropertyFilterValue::Set(_) => {
                        return Err(GraphError::InvalidFilterExpectSingleGotSet(self.operator))
                    }
                };
            }
        }
        Ok(())
    }

    fn validate_list_agg_operator(&self) -> Result<(), GraphError> {
        use FilterOperator::*;
        if self.list_agg.is_none() {
            return Ok(());
        }
        match self.operator {
            Eq | Ne | Lt | Le | Gt | Ge | In | NotIn => Ok(()),
            IsSome
            | IsNone
            | StartsWith
            | EndsWith
            | Contains
            | NotContains
            | FuzzySearch { .. } => Err(GraphError::InvalidFilter(format!(
                "operator {} is not supported with list aggregation {:?}; allowed: EQ, NE, LT, LE, GT, GE, IN, NOT_IN",
                self.operator, self.list_agg
            ))),
        }
    }

    // Validates if all the elements in the list are of same numeric type
    fn validate_prop_dtype_is_valid_for_list_agg(
        &self,
        dtype: &PropType,
    ) -> Result<(), GraphError> {
        use ListAgg::*;
        match dtype {
            PropType::List(inner) => {
                match self.list_agg.expect("Called only if list_agg is Some") {
                    Len => { // Len works for any list element type
                        Ok(())
                    }
                    Sum | Avg | Min | Max => {  // These require numeric element types
                        if inner.has_cmp() && inner.is_numeric() {
                            Ok(())
                        } else {
                            Err(GraphError::InvalidFilter(format!(
                                "List aggregation {:?} requires numeric list elements, but property type is {:?}",
                                self.list_agg, dtype
                            )))
                        }
                    }
                }
            }
            _ => Err(GraphError::InvalidFilter(format!(
                "List aggregation {:?} is only supported on list properties, but property type is {:?}",
                self.list_agg, dtype
            ))),
        }
    }

    // For EQ/NE/LT/LE/GT/GE on an aggregated value (Single prop filter clause)
    fn validate_agg_single_filter_clause_prop(&self, eff: &PropType) -> Result<(), GraphError> {
        let _ = self.validate_single_dtype(eff, false)?;
        // If strictly numeric op, ensure the eff type can compare numerically.
        if self.operator.is_strictly_numeric_operation() && !eff.has_cmp() {
            return Err(GraphError::InvalidFilterCmp(eff.clone()));
        }
        Ok(())
    }

    // For IN/NOT_IN on an aggregated value (Set prop filter clause)
    fn validate_agg_set_filter_clause_prop(&self, eff: &PropType) -> Result<(), GraphError> {
        match &self.prop_value {
            PropertyFilterValue::Set(set) => {
                // Every element in the set must unify with `eff`
                for v in set.iter() {
                    let vty = v.dtype();
                    let _ = unify_types(eff, &vty, &mut false)
                        .map_err(|e| e.with_name(format!("aggregate({:?})", self.list_agg)))?;
                }
                Ok(())
            }
            PropertyFilterValue::Single(_) | PropertyFilterValue::None => {
                Err(GraphError::InvalidFilterExpectSetGotSingle(self.operator))
            }
        }
    }

    fn validate_agg(&self, dtype: &PropType, agg: ListAgg) -> Result<(), GraphError> {
        fn effective_dtype_for_list_agg(agg: ListAgg) -> PropType {
            match agg {
                ListAgg::Len => PropType::I64,
                ListAgg::Sum | ListAgg::Avg | ListAgg::Min | ListAgg::Max => PropType::F64,
            }
        }

        self.validate_list_agg_operator()?;
        self.validate_prop_dtype_is_valid_for_list_agg(&dtype)?;
        let eff = effective_dtype_for_list_agg(agg);
        match self.operator {
            FilterOperator::Eq
            | FilterOperator::Ne
            | FilterOperator::Lt
            | FilterOperator::Le
            | FilterOperator::Gt
            | FilterOperator::Ge => self.validate_agg_single_filter_clause_prop(&eff),
            FilterOperator::In | FilterOperator::NotIn => {
                self.validate_agg_set_filter_clause_prop(&eff)
            }
            _ => {
                unreachable!("Other filter operators are invalidated by validate_list_agg_operator")
            }
        }
    }

    pub fn resolve_prop_id(
        &self,
        meta: &Meta,
        expect_map: bool,
    ) -> Result<Option<usize>, GraphError> {
        let (name, is_static) = match &self.prop_ref {
            PropertyRef::Metadata(n) => (n.as_str(), true),
            PropertyRef::Property(n) | PropertyRef::TemporalProperty(n, _) => (n.as_str(), false),
        };

        match meta.get_prop_id_and_type(name, is_static) {
            None => Ok(None),
            Some((id, dtype)) => {
                if let Some(agg) = self.list_agg {
                    self.validate_agg(&dtype, agg)?;
                } else {
                    self.validate(&dtype, is_static && expect_map)?;
                }
                Ok(Some(id))
            }
        }
    }

    fn aggregate_list_value(list_prop: &Prop, agg: ListAgg) -> Option<Prop> {
        use Prop::*;
        let vals = match list_prop {
            List(v) => v,
            _ => return None,
        };

        match agg {
            ListAgg::Len => Some(I64(vals.len() as i64)),
            ListAgg::Sum | ListAgg::Avg | ListAgg::Min | ListAgg::Max => {
                let mut count = 0usize;
                let mut sum = 0.0f64;
                let mut minv: Option<f64> = None;
                let mut maxv: Option<f64> = None;

                for p in vals.iter() {
                    let v = p.as_f64()?;
                    sum += v;
                    count += 1;
                    minv = Some(if let Some(m) = minv { m.min(v) } else { v });
                    maxv = Some(if let Some(m) = maxv { m.max(v) } else { v });
                }
                println!("count: {}", count);
                println!("sum: {}", sum);
                println!("minv: {:?}", minv);
                println!("maxv: {:?}", maxv);
                if count == 0 {
                    return None;
                }

                let out = match agg {
                    ListAgg::Sum => sum,
                    ListAgg::Avg => sum / (count as f64),
                    ListAgg::Min => minv.unwrap(),
                    ListAgg::Max => maxv.unwrap(),
                    _ => unreachable!(),
                };
                Some(F64(out))
            }
        }
    }

    pub fn matches(&self, other: Option<&Prop>) -> bool {
        if let Some(agg) = self.list_agg {
            let agg_other = other.and_then(|x| Self::aggregate_list_value(x, agg));
            println!("agg_other: {:?}", agg_other);
            self.operator
                .apply_to_property(&self.prop_value, agg_other.as_ref())
        } else {
            self.operator.apply_to_property(&self.prop_value, other)
        }
    }

    fn is_property_matched<I: InternalPropertiesOps + Clone>(
        &self,
        t_prop_id: Option<usize>,
        props: Properties<I>,
    ) -> bool {
        match self.prop_ref {
            PropertyRef::Property(_) => {
                let prop_value = t_prop_id.and_then(|prop_id| props.get_by_id(prop_id));
                self.matches(prop_value.as_ref())
            }
            PropertyRef::Metadata(_) => false,
            PropertyRef::TemporalProperty(_, Temporal::Any) => t_prop_id.is_some_and(|prop_id| {
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
            PropertyRef::TemporalProperty(_, Temporal::First) => {
                let prop_value = t_prop_id.and_then(|prop_id| {
                    props
                        .temporal()
                        .get_by_id(prop_id)
                        .and_then(|prop_view| prop_view.first())
                });
                self.matches(prop_value.as_ref())
            }
            PropertyRef::TemporalProperty(_, Temporal::All) => t_prop_id.is_some_and(|prop_id| {
                props
                    .temporal()
                    .get_by_id(prop_id)
                    .filter(|prop_view| {
                        let has_any = prop_view.values().next().is_some();
                        let all_ok = prop_view.values().all(|v| self.matches(Some(&v)));
                        has_any && all_ok
                    })
                    .is_some()
            }),
        }
    }

    fn is_metadata_matched<I: InternalPropertiesOps + Clone>(
        &self,
        c_prop_id: Option<usize>,
        props: Metadata<I>,
    ) -> bool {
        match self.prop_ref {
            PropertyRef::Metadata(_) => {
                let prop_value = c_prop_id.and_then(|id| props.get_by_id(id));
                self.matches(prop_value.as_ref())
            }
            _ => false,
        }
    }

    pub fn matches_node<'graph, G: GraphViewOps<'graph>>(
        &self,
        graph: &G,
        prop_id: Option<usize>,
        node: NodeStorageRef,
    ) -> bool {
        let node = NodeView::new_internal(graph, node.vid());
        match self.prop_ref {
            PropertyRef::Metadata(_) => {
                let props = node.metadata();
                self.is_metadata_matched(prop_id, props)
            }
            PropertyRef::TemporalProperty(_, _) | PropertyRef::Property(_) => {
                let props = node.properties();
                self.is_property_matched(prop_id, props)
            }
        }
    }

    pub fn matches_edge<'graph, G: GraphViewOps<'graph>>(
        &self,
        graph: &G,
        prop_id: Option<usize>,
        edge: EdgeStorageRef,
    ) -> bool {
        let edge = EdgeView::new(graph, edge.out_ref());
        match self.prop_ref {
            PropertyRef::Metadata(_) => {
                let props = edge.metadata();
                self.is_metadata_matched(prop_id, props)
            }
            PropertyRef::TemporalProperty(_, _) | PropertyRef::Property(_) => {
                let props = edge.properties();
                self.is_property_matched(prop_id, props)
            }
        }
    }

    pub fn matches_exploded_edge<G: GraphView>(
        &self,
        graph: &G,
        prop_id: Option<usize>,
        e: EID,
        t: TimeIndexEntry,
        layer: usize,
    ) -> bool {
        let edge = EdgeView::new(graph, graph.core_edge(e).out_ref().at(t).at_layer(layer));
        match self.prop_ref {
            PropertyRef::Metadata(_) => {
                let props = edge.metadata();
                self.is_metadata_matched(prop_id, props)
            }
            PropertyRef::TemporalProperty(_, _) | PropertyRef::Property(_) => {
                let props = edge.properties();
                self.is_property_matched(prop_id, props)
            }
        }
    }
}

impl TryAsCompositeFilter for PropertyFilter<NodeFilter> {
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Ok(CompositeNodeFilter::Property(self.clone()))
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

impl TryAsCompositeFilter for PropertyFilter<EdgeFilter> {
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Ok(CompositeEdgeFilter::Property(self.clone()))
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }
}

impl TryAsCompositeFilter for PropertyFilter<ExplodedEdgeFilter> {
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        Ok(CompositeExplodedEdgeFilter::Property(self.clone()))
    }
}

pub trait InternalPropertyFilterOps: Send + Sync {
    type Marker: Clone + Send + Sync + 'static;

    fn property_ref(&self) -> PropertyRef;

    fn list_agg(&self) -> Option<ListAgg> {
        None
    }
}

impl<T: InternalPropertyFilterOps> InternalPropertyFilterOps for Arc<T> {
    type Marker = T::Marker;

    fn property_ref(&self) -> PropertyRef {
        self.deref().property_ref()
    }

    fn list_agg(&self) -> Option<ListAgg> {
        self.deref().list_agg()
    }
}

pub trait PropertyFilterOps: InternalPropertyFilterOps {
    fn eq(&self, value: impl Into<Prop>) -> PropertyFilter<Self::Marker>;

    fn ne(&self, value: impl Into<Prop>) -> PropertyFilter<Self::Marker>;

    fn le(&self, value: impl Into<Prop>) -> PropertyFilter<Self::Marker>;

    fn ge(&self, value: impl Into<Prop>) -> PropertyFilter<Self::Marker>;

    fn lt(&self, value: impl Into<Prop>) -> PropertyFilter<Self::Marker>;

    fn gt(&self, value: impl Into<Prop>) -> PropertyFilter<Self::Marker>;

    fn is_in(&self, values: impl IntoIterator<Item = Prop>) -> PropertyFilter<Self::Marker>;

    fn is_not_in(&self, values: impl IntoIterator<Item = Prop>) -> PropertyFilter<Self::Marker>;

    fn is_none(&self) -> PropertyFilter<Self::Marker>;

    fn is_some(&self) -> PropertyFilter<Self::Marker>;

    fn starts_with(&self, value: impl Into<Prop>) -> PropertyFilter<Self::Marker>;

    fn ends_with(&self, value: impl Into<Prop>) -> PropertyFilter<Self::Marker>;

    fn contains(&self, value: impl Into<Prop>) -> PropertyFilter<Self::Marker>;

    fn not_contains(&self, value: impl Into<Prop>) -> PropertyFilter<Self::Marker>;

    fn fuzzy_search(
        &self,
        prop_value: impl Into<String>,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> PropertyFilter<Self::Marker>;
}

impl<T: ?Sized + InternalPropertyFilterOps> PropertyFilterOps for T {
    fn eq(&self, value: impl Into<Prop>) -> PropertyFilter<Self::Marker> {
        PropertyFilter::eq(self.property_ref(), value.into()).with_list_agg(self.list_agg())
    }

    fn ne(&self, value: impl Into<Prop>) -> PropertyFilter<Self::Marker> {
        PropertyFilter::ne(self.property_ref(), value.into()).with_list_agg(self.list_agg())
    }

    fn le(&self, value: impl Into<Prop>) -> PropertyFilter<Self::Marker> {
        PropertyFilter::le(self.property_ref(), value.into()).with_list_agg(self.list_agg())
    }

    fn ge(&self, value: impl Into<Prop>) -> PropertyFilter<Self::Marker> {
        PropertyFilter::ge(self.property_ref(), value.into()).with_list_agg(self.list_agg())
    }

    fn lt(&self, value: impl Into<Prop>) -> PropertyFilter<Self::Marker> {
        PropertyFilter::lt(self.property_ref(), value.into()).with_list_agg(self.list_agg())
    }

    fn gt(&self, value: impl Into<Prop>) -> PropertyFilter<Self::Marker> {
        PropertyFilter::gt(self.property_ref(), value.into()).with_list_agg(self.list_agg())
    }

    fn is_in(&self, values: impl IntoIterator<Item = Prop>) -> PropertyFilter<Self::Marker> {
        PropertyFilter::is_in(self.property_ref(), values).with_list_agg(self.list_agg())
    }

    fn is_not_in(&self, values: impl IntoIterator<Item = Prop>) -> PropertyFilter<Self::Marker> {
        PropertyFilter::is_not_in(self.property_ref(), values).with_list_agg(self.list_agg())
    }

    fn is_none(&self) -> PropertyFilter<Self::Marker> {
        PropertyFilter::is_none(self.property_ref()).with_list_agg(self.list_agg())
    }

    fn is_some(&self) -> PropertyFilter<Self::Marker> {
        PropertyFilter::is_some(self.property_ref()).with_list_agg(self.list_agg())
    }

    fn starts_with(&self, value: impl Into<Prop>) -> PropertyFilter<Self::Marker> {
        PropertyFilter::starts_with(self.property_ref(), value.into())
            .with_list_agg(self.list_agg())
    }

    fn ends_with(&self, value: impl Into<Prop>) -> PropertyFilter<Self::Marker> {
        PropertyFilter::ends_with(self.property_ref(), value.into()).with_list_agg(self.list_agg())
    }

    fn contains(&self, value: impl Into<Prop>) -> PropertyFilter<Self::Marker> {
        PropertyFilter::contains(self.property_ref(), value.into()).with_list_agg(self.list_agg())
    }

    fn not_contains(&self, value: impl Into<Prop>) -> PropertyFilter<Self::Marker> {
        PropertyFilter::not_contains(self.property_ref(), value.into())
            .with_list_agg(self.list_agg())
    }

    fn fuzzy_search(
        &self,
        prop_value: impl Into<String>,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> PropertyFilter<Self::Marker> {
        PropertyFilter::fuzzy_search(
            self.property_ref(),
            prop_value.into(),
            levenshtein_distance,
            prefix_match,
        )
        .with_list_agg(self.list_agg())
    }
}

#[derive(Clone)]
pub struct PropertyFilterBuilder<M>(pub String, PhantomData<M>);

impl<M> PropertyFilterBuilder<M> {
    pub fn new(prop: impl Into<String>) -> Self {
        Self(prop.into(), PhantomData)
    }
}

impl<M: Send + Sync + Clone + 'static> InternalPropertyFilterOps for PropertyFilterBuilder<M> {
    type Marker = M;
    fn property_ref(&self) -> PropertyRef {
        PropertyRef::Property(self.0.clone())
    }
}

impl<M> PropertyFilterBuilder<M> {
    pub fn temporal(self) -> TemporalPropertyFilterBuilder<M> {
        TemporalPropertyFilterBuilder(self.0, PhantomData)
    }
}

#[derive(Clone)]
pub struct MetadataFilterBuilder<M>(pub String, PhantomData<M>);

impl<M> MetadataFilterBuilder<M> {
    pub fn new(prop: impl Into<String>) -> Self {
        Self(prop.into(), PhantomData)
    }
}

impl<M: Send + Sync + Clone + 'static> InternalPropertyFilterOps for MetadataFilterBuilder<M> {
    type Marker = M;
    fn property_ref(&self) -> PropertyRef {
        PropertyRef::Metadata(self.0.clone())
    }
}

#[derive(Clone)]
pub struct TemporalPropertyFilterBuilder<M>(pub String, PhantomData<M>);

impl<M> TemporalPropertyFilterBuilder<M> {
    pub fn any(self) -> AnyTemporalPropertyFilterBuilder<M> {
        AnyTemporalPropertyFilterBuilder(self.0, PhantomData)
    }

    pub fn latest(self) -> LatestTemporalPropertyFilterBuilder<M> {
        LatestTemporalPropertyFilterBuilder(self.0, PhantomData)
    }

    pub fn first(self) -> FirstTemporalPropertyFilterBuilder<M> {
        FirstTemporalPropertyFilterBuilder(self.0, PhantomData)
    }

    pub fn all(self) -> AllTemporalPropertyFilterBuilder<M> {
        AllTemporalPropertyFilterBuilder(self.0, PhantomData)
    }
}

#[derive(Clone)]
pub struct AnyTemporalPropertyFilterBuilder<M>(pub String, PhantomData<M>);

impl<M: Send + Sync + Clone + 'static> InternalPropertyFilterOps
    for AnyTemporalPropertyFilterBuilder<M>
{
    type Marker = M;
    fn property_ref(&self) -> PropertyRef {
        PropertyRef::TemporalProperty(self.0.clone(), Temporal::Any)
    }
}

#[derive(Clone)]
pub struct LatestTemporalPropertyFilterBuilder<M>(pub String, PhantomData<M>);

impl<M: Send + Sync + Clone + 'static> InternalPropertyFilterOps
    for LatestTemporalPropertyFilterBuilder<M>
{
    type Marker = M;
    fn property_ref(&self) -> PropertyRef {
        PropertyRef::TemporalProperty(self.0.clone(), Temporal::Latest)
    }
}

#[derive(Clone)]
pub struct FirstTemporalPropertyFilterBuilder<M>(pub String, PhantomData<M>);

impl<M: Send + Sync + Clone + 'static> InternalPropertyFilterOps
    for FirstTemporalPropertyFilterBuilder<M>
{
    type Marker = M;
    fn property_ref(&self) -> PropertyRef {
        PropertyRef::TemporalProperty(self.0.clone(), Temporal::First)
    }
}

#[derive(Clone)]
pub struct AllTemporalPropertyFilterBuilder<M>(pub String, PhantomData<M>);

impl<M: Send + Sync + Clone + 'static> InternalPropertyFilterOps
    for AllTemporalPropertyFilterBuilder<M>
{
    type Marker = M;
    fn property_ref(&self) -> PropertyRef {
        PropertyRef::TemporalProperty(self.0.clone(), Temporal::All)
    }
}

#[derive(Clone)]
pub struct LenFilterBuilder<M>(pub String, PhantomData<M>);

#[derive(Clone)]
pub struct SumFilterBuilder<M>(pub String, PhantomData<M>);

#[derive(Clone)]
pub struct AvgFilterBuilder<M>(pub String, PhantomData<M>);

#[derive(Clone)]
pub struct MinFilterBuilder<M>(pub String, PhantomData<M>);

#[derive(Clone)]
pub struct MaxFilterBuilder<M>(pub String, PhantomData<M>);

pub trait ListAggOps<M>: Sized {
    fn name(&self) -> &String;

    fn len(self) -> LenFilterBuilder<M> {
        LenFilterBuilder(self.name().clone(), PhantomData)
    }

    fn sum(self) -> SumFilterBuilder<M> {
        SumFilterBuilder(self.name().clone(), PhantomData)
    }

    fn avg(self) -> AvgFilterBuilder<M> {
        AvgFilterBuilder(self.name().clone(), PhantomData)
    }

    fn min(self) -> MinFilterBuilder<M> {
        MinFilterBuilder(self.name().clone(), PhantomData)
    }

    fn max(self) -> MaxFilterBuilder<M> {
        MaxFilterBuilder(self.name().clone(), PhantomData)
    }
}

impl<M> ListAggOps<M> for PropertyFilterBuilder<M> {
    fn name(&self) -> &String {
        &self.0
    }
}
impl<M> ListAggOps<M> for MetadataFilterBuilder<M> {
    fn name(&self) -> &String {
        &self.0
    }
}
impl<M> ListAggOps<M> for AnyTemporalPropertyFilterBuilder<M> {
    fn name(&self) -> &String {
        &self.0
    }
}
impl<M> ListAggOps<M> for LatestTemporalPropertyFilterBuilder<M> {
    fn name(&self) -> &String {
        &self.0
    }
}
impl<M> ListAggOps<M> for FirstTemporalPropertyFilterBuilder<M> {
    fn name(&self) -> &String {
        &self.0
    }
}
impl<M> ListAggOps<M> for AllTemporalPropertyFilterBuilder<M> {
    fn name(&self) -> &String {
        &self.0
    }
}

impl<M: Send + Sync + Clone + 'static> InternalPropertyFilterOps for LenFilterBuilder<M> {
    type Marker = M;
    fn property_ref(&self) -> PropertyRef {
        PropertyRef::Property(self.0.clone())
    }
    fn list_agg(&self) -> Option<ListAgg> {
        Some(ListAgg::Len)
    }
}

impl<M: Send + Sync + Clone + 'static> InternalPropertyFilterOps for SumFilterBuilder<M> {
    type Marker = M;
    fn property_ref(&self) -> PropertyRef {
        PropertyRef::Property(self.0.clone())
    }
    fn list_agg(&self) -> Option<ListAgg> {
        Some(ListAgg::Sum)
    }
}

impl<M: Send + Sync + Clone + 'static> InternalPropertyFilterOps for AvgFilterBuilder<M> {
    type Marker = M;
    fn property_ref(&self) -> PropertyRef {
        PropertyRef::Property(self.0.clone())
    }
    fn list_agg(&self) -> Option<ListAgg> {
        Some(ListAgg::Avg)
    }
}

impl<M: Send + Sync + Clone + 'static> InternalPropertyFilterOps for MinFilterBuilder<M> {
    type Marker = M;
    fn property_ref(&self) -> PropertyRef {
        PropertyRef::Property(self.0.clone())
    }
    fn list_agg(&self) -> Option<ListAgg> {
        Some(ListAgg::Min)
    }
}

impl<M: Send + Sync + Clone + 'static> InternalPropertyFilterOps for MaxFilterBuilder<M> {
    type Marker = M;
    fn property_ref(&self) -> PropertyRef {
        PropertyRef::Property(self.0.clone())
    }
    fn list_agg(&self) -> Option<ListAgg> {
        Some(ListAgg::Max)
    }
}
