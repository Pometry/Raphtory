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
        if self.list_agg.is_none() {
            return Ok(());
        }
        match self.operator {
            FilterOperator::Eq
            | FilterOperator::Ne
            | FilterOperator::Lt
            | FilterOperator::Le
            | FilterOperator::Gt
            | FilterOperator::Ge
            | FilterOperator::In
            | FilterOperator::NotIn => Ok(()),
            FilterOperator::IsSome
            | FilterOperator::IsNone
            | FilterOperator::StartsWith
            | FilterOperator::EndsWith
            | FilterOperator::Contains
            | FilterOperator::NotContains
            | FilterOperator::FuzzySearch { .. } => Err(GraphError::InvalidFilter(format!(
                "Operator {} is not supported with list aggregation {:?}; allowed: EQ, NE, LT, LE, GT, GE, IN, NOT_IN",
                self.operator, self.list_agg
            ))),
        }
    }

    /// Effective result dtype for (agg, element PropType) as per semantic:
    /// Len -> U64
    /// Avg -> F64
    /// Sum -> U64 (unsigned), I64 (signed), F64 (float)
    /// Min/Max -> same as element type (numeric only)
    fn effective_dtype_for_list_agg_with_inner(
        agg: ListAgg,
        inner: &PropType,
    ) -> Result<PropType, GraphError> {
        Ok(match agg {
            ListAgg::Len => PropType::U64,
            ListAgg::Avg => PropType::F64,
            ListAgg::Sum => match inner {
                PropType::U8 | PropType::U16 | PropType::U32 | PropType::U64 => PropType::U64,
                PropType::I32 | PropType::I64 => PropType::I64,
                PropType::F32 | PropType::F64 => PropType::F64,
                _ => {
                    return Err(GraphError::InvalidFilter(format!(
                        "SUM requires numeric list; got element type {:?}",
                        inner
                    )))
                }
            },
            ListAgg::Min | ListAgg::Max => match inner {
                PropType::U8
                | PropType::U16
                | PropType::U32
                | PropType::U64
                | PropType::I32
                | PropType::I64
                | PropType::F32
                | PropType::F64 => inner.clone(), // same as element type
                _ => {
                    return Err(GraphError::InvalidFilter(format!(
                        "{:?} requires numeric list; got element type {:?}",
                        agg, inner
                    )))
                }
            },
        })
    }

    fn validate_list_agg_and_effective_dtype(
        &self,
        prop_dtype: &PropType,
    ) -> Result<PropType, GraphError> {
        if let PropType::List(inner) = prop_dtype {
            Self::effective_dtype_for_list_agg_with_inner(self.list_agg.unwrap(), inner)
        } else {
            Err(GraphError::InvalidFilter(format!(
                "List aggregation {:?} is only supported on list properties; got {:?}",
                self.list_agg, prop_dtype
            )))
        }
    }

    fn validate_agg_single_filter_clause_prop(&self, eff: &PropType) -> Result<(), GraphError> {
        let _ = self.validate_single_dtype(eff, false)?;
        // If strictly numeric op, ensure the eff type can compare numerically.
        if self.operator.is_strictly_numeric_operation() && !eff.has_cmp() {
            return Err(GraphError::InvalidFilterCmp(eff.clone()));
        }
        Ok(())
    }

    fn validate_agg_set_filter_clause_prop(&self) -> Result<(), GraphError> {
        match &self.prop_value {
            PropertyFilterValue::Set(_) => Ok(()), // filter clause prop list doesn't need to be homogenous
            PropertyFilterValue::Single(_) | PropertyFilterValue::None => {
                Err(GraphError::InvalidFilterExpectSetGotSingle(self.operator))
            }
        }
    }

    fn validate_agg(&self, dtype: &PropType) -> Result<(), GraphError> {
        self.validate_list_agg_operator()?;
        let eff = self.validate_list_agg_and_effective_dtype(&dtype)?;
        match self.operator {
            FilterOperator::Eq
            | FilterOperator::Ne
            | FilterOperator::Lt
            | FilterOperator::Le
            | FilterOperator::Gt
            | FilterOperator::Ge => self.validate_agg_single_filter_clause_prop(&eff),
            FilterOperator::In | FilterOperator::NotIn => {
                self.validate_agg_set_filter_clause_prop()
            }
            _ => unreachable!("blocked by validate_list_agg_operator"),
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
                if let Some(_) = self.list_agg {
                    self.validate_agg(&dtype)?;
                } else {
                    self.validate(&dtype, is_static && expect_map)?;
                }
                Ok(Some(id))
            }
        }
    }

    fn scan_u64_sum(vals: &[Prop]) -> Option<(bool, u64, u128)> {
        let mut sum64: u64 = 0;
        let mut sum128: u128 = 0;
        let mut promoted = false;

        for p in vals {
            let x = p.as_u64_lossless()?;
            if !promoted {
                if let Some(s) = sum64.checked_add(x) {
                    sum64 = s;
                } else {
                    promoted = true;
                    sum128 = (sum64 as u128) + (x as u128);
                }
            } else {
                sum128 += x as u128;
            }
        }
        Some((promoted, sum64, sum128))
    }

    fn scan_i64_sum(vals: &[Prop]) -> Option<(bool, i64, i128)> {
        let mut sum64: i64 = 0;
        let mut sum128: i128 = 0;
        let mut promoted = false;

        for p in vals {
            let x = p.as_i64_lossless()?;
            if !promoted {
                if let Some(s) = sum64.checked_add(x) {
                    sum64 = s;
                } else {
                    promoted = true;
                    sum128 = (sum64 as i128) + (x as i128);
                }
            } else {
                sum128 += x as i128;
            }
        }
        Some((promoted, sum64, sum128))
    }

    fn scan_u64_min_max(vals: &[Prop]) -> Option<(u64, u64)> {
        let mut it = vals.iter();
        let first = it.next()?.as_u64_lossless()?;
        let mut min_v = first;
        let mut max_v = first;
        for p in it {
            let x = p.as_u64_lossless()?;
            if x < min_v {
                min_v = x;
            }
            if x > max_v {
                max_v = x;
            }
        }
        Some((min_v, max_v))
    }

    fn scan_i64_min_max(vals: &[Prop]) -> Option<(i64, i64)> {
        let mut it = vals.iter();
        let first = it.next()?.as_i64_lossless()?;
        let mut min_v = first;
        let mut max_v = first;
        for p in it {
            let x = p.as_i64_lossless()?;
            if x < min_v {
                min_v = x;
            }
            if x > max_v {
                max_v = x;
            }
        }
        Some((min_v, max_v))
    }

    fn scan_f64_sum_count(vals: &[Prop]) -> Option<(f64, u64)> {
        let mut sum = 0.0f64;
        let mut count = 0u64;
        for p in vals {
            let x = p.as_f64_lossless()?;
            if !x.is_finite() {
                return None;
            }
            sum += x;
            count += 1;
        }
        Some((sum, count))
    }

    fn scan_f64_min_max(vals: &[Prop]) -> Option<(f64, f64)> {
        let mut it = vals.iter();
        let first = it.next()?.as_f64_lossless()?;
        if !first.is_finite() {
            return None;
        }
        let mut min_v = first;
        let mut max_v = first;
        for p in it {
            let x = p.as_f64_lossless()?;
            if !x.is_finite() {
                return None;
            }
            if x < min_v {
                min_v = x;
            }
            if x > max_v {
                max_v = x;
            }
        }
        Some((min_v, max_v))
    }

    fn reduce_unsigned(vals: &[Prop], ret_minmax: fn(u64) -> Prop, agg: ListAgg) -> Option<Prop> {
        match agg {
            ListAgg::Sum => {
                let (promoted, s64, s128) = Self::scan_u64_sum(vals)?;
                Some(if promoted {
                    Prop::U64(u64::try_from(s128).ok()?)
                } else {
                    Prop::U64(s64)
                })
            }
            ListAgg::Avg => {
                let (promoted, s64, s128) = Self::scan_u64_sum(vals)?;
                let count = vals.len() as u64;
                let s = if promoted { s128 as f64 } else { s64 as f64 };
                Some(Prop::F64(s / (count as f64)))
            }
            ListAgg::Min => Self::scan_u64_min_max(vals).map(|(mn, _)| ret_minmax(mn)),
            ListAgg::Max => Self::scan_u64_min_max(vals).map(|(_, mx)| ret_minmax(mx)),
            ListAgg::Len => unreachable!(),
        }
    }

    fn reduce_signed(vals: &[Prop], ret_minmax: fn(i64) -> Prop, agg: ListAgg) -> Option<Prop> {
        match agg {
            ListAgg::Sum => {
                let (promoted, s64, s128) = Self::scan_i64_sum(vals)?;
                Some(if promoted {
                    Prop::I64(i64::try_from(s128).ok()?)
                } else {
                    Prop::I64(s64)
                })
            }
            ListAgg::Avg => {
                let (promoted, s64, s128) = Self::scan_i64_sum(vals)?;
                let count = vals.len() as u64;
                let s = if promoted { s128 as f64 } else { s64 as f64 };
                Some(Prop::F64(s / (count as f64)))
            }
            ListAgg::Min => Self::scan_i64_min_max(vals).map(|(mn, _)| ret_minmax(mn)),
            ListAgg::Max => Self::scan_i64_min_max(vals).map(|(_, mx)| ret_minmax(mx)),
            ListAgg::Len => unreachable!(),
        }
    }

    fn reduce_float(vals: &[Prop], ret_minmax: fn(f64) -> Prop, agg: ListAgg) -> Option<Prop> {
        match agg {
            ListAgg::Sum => Self::scan_f64_sum_count(vals).map(|(sum, _)| Prop::F64(sum)),
            ListAgg::Avg => {
                let (sum, count) = Self::scan_f64_sum_count(vals)?;
                Some(Prop::F64(sum / (count as f64)))
            }
            ListAgg::Min => Self::scan_f64_min_max(vals).map(|(mn, _)| ret_minmax(mn)),
            ListAgg::Max => Self::scan_f64_min_max(vals).map(|(_, mx)| ret_minmax(mx)),
            ListAgg::Len => unreachable!(),
        }
    }

    fn aggregate_list_value(&self, list_prop: &Prop, agg: ListAgg) -> Option<Prop> {
        let vals = match list_prop {
            Prop::List(v) => v.as_slice(),
            _ => return None,
        };
        if vals.is_empty() {
            return match agg {
                ListAgg::Len => Some(Prop::U64(0)),
                _ => None,
            };
        }

        if let ListAgg::Len = agg {
            return Some(Prop::U64(vals.len() as u64));
        }

        let inner_ty = match list_prop.dtype() {
            PropType::List(inner) => *inner,
            _ => unreachable!(),
        };
        match inner_ty {
            PropType::U8 => Self::reduce_unsigned(vals, |x| Prop::U8(x as u8), agg),
            PropType::U16 => Self::reduce_unsigned(vals, |x| Prop::U16(x as u16), agg),
            PropType::U32 => Self::reduce_unsigned(vals, |x| Prop::U32(x as u32), agg),
            PropType::U64 => Self::reduce_unsigned(vals, |x| Prop::U64(x), agg),

            PropType::I32 => Self::reduce_signed(vals, |x| Prop::I32(x as i32), agg),
            PropType::I64 => Self::reduce_signed(vals, |x| Prop::I64(x), agg),

            PropType::F32 => Self::reduce_float(vals, |x| Prop::F32(x as f32), agg),
            PropType::F64 => Self::reduce_float(vals, |x| Prop::F64(x), agg),

            _ => None,
        }
    }

    pub fn matches(&self, other: Option<&Prop>) -> bool {
        if let Some(agg) = self.list_agg {
            let agg_other = other.and_then(|x| self.aggregate_list_value(x, agg));
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
pub struct LenFilterBuilder<M>(pub PropertyRef, PhantomData<M>);

#[derive(Clone)]
pub struct SumFilterBuilder<M>(pub PropertyRef, PhantomData<M>);

#[derive(Clone)]
pub struct AvgFilterBuilder<M>(pub PropertyRef, PhantomData<M>);

#[derive(Clone)]
pub struct MinFilterBuilder<M>(pub PropertyRef, PhantomData<M>);

#[derive(Clone)]
pub struct MaxFilterBuilder<M>(pub PropertyRef, PhantomData<M>);

pub trait ListAggOps<M>: Sized {
    fn property_ref_for_self(&self) -> PropertyRef;

    fn len(self) -> LenFilterBuilder<M> {
        LenFilterBuilder(self.property_ref_for_self(), PhantomData)
    }

    fn sum(self) -> SumFilterBuilder<M> {
        SumFilterBuilder(self.property_ref_for_self(), PhantomData)
    }

    fn avg(self) -> AvgFilterBuilder<M> {
        AvgFilterBuilder(self.property_ref_for_self(), PhantomData)
    }

    fn min(self) -> MinFilterBuilder<M> {
        MinFilterBuilder(self.property_ref_for_self(), PhantomData)
    }

    fn max(self) -> MaxFilterBuilder<M> {
        MaxFilterBuilder(self.property_ref_for_self(), PhantomData)
    }
}

impl<M> ListAggOps<M> for PropertyFilterBuilder<M> {
    fn property_ref_for_self(&self) -> PropertyRef {
        PropertyRef::Property(self.0.clone())
    }
}

impl<M> ListAggOps<M> for MetadataFilterBuilder<M> {
    fn property_ref_for_self(&self) -> PropertyRef {
        PropertyRef::Metadata(self.0.clone())
    }
}

impl<M> ListAggOps<M> for AnyTemporalPropertyFilterBuilder<M> {
    fn property_ref_for_self(&self) -> PropertyRef {
        PropertyRef::TemporalProperty(self.0.clone(), Temporal::Any)
    }
}

impl<M> ListAggOps<M> for LatestTemporalPropertyFilterBuilder<M> {
    fn property_ref_for_self(&self) -> PropertyRef {
        PropertyRef::TemporalProperty(self.0.clone(), Temporal::Latest)
    }
}

impl<M> ListAggOps<M> for FirstTemporalPropertyFilterBuilder<M> {
    fn property_ref_for_self(&self) -> PropertyRef {
        PropertyRef::TemporalProperty(self.0.clone(), Temporal::First)
    }
}

impl<M> ListAggOps<M> for AllTemporalPropertyFilterBuilder<M> {
    fn property_ref_for_self(&self) -> PropertyRef {
        PropertyRef::TemporalProperty(self.0.clone(), Temporal::All)
    }
}

impl<M: Send + Sync + Clone + 'static> InternalPropertyFilterOps for LenFilterBuilder<M> {
    type Marker = M;

    fn property_ref(&self) -> PropertyRef {
        self.0.clone()
    }

    fn list_agg(&self) -> Option<ListAgg> {
        Some(ListAgg::Len)
    }
}

impl<M: Send + Sync + Clone + 'static> InternalPropertyFilterOps for SumFilterBuilder<M> {
    type Marker = M;

    fn property_ref(&self) -> PropertyRef {
        self.0.clone()
    }

    fn list_agg(&self) -> Option<ListAgg> {
        Some(ListAgg::Sum)
    }
}

impl<M: Send + Sync + Clone + 'static> InternalPropertyFilterOps for AvgFilterBuilder<M> {
    type Marker = M;

    fn property_ref(&self) -> PropertyRef {
        self.0.clone()
    }

    fn list_agg(&self) -> Option<ListAgg> {
        Some(ListAgg::Avg)
    }
}

impl<M: Send + Sync + Clone + 'static> InternalPropertyFilterOps for MinFilterBuilder<M> {
    type Marker = M;

    fn property_ref(&self) -> PropertyRef {
        self.0.clone()
    }

    fn list_agg(&self) -> Option<ListAgg> {
        Some(ListAgg::Min)
    }
}

impl<M: Send + Sync + Clone + 'static> InternalPropertyFilterOps for MaxFilterBuilder<M> {
    type Marker = M;

    fn property_ref(&self) -> PropertyRef {
        self.0.clone()
    }

    fn list_agg(&self) -> Option<ListAgg> {
        Some(ListAgg::Max)
    }
}
