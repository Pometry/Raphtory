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
            prop::{sort_comparable_props, unify_types, Prop, PropType},
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
pub enum Op {
    // Selectors
    First,
    Last,
    // Aggregators
    Len,
    Sum,
    Avg,
    Min,
    Max,
    // Qualifiers
    Any,
    All,
}

impl Op {
    #[inline]
    pub fn is_selector(self) -> bool {
        matches!(self, Op::First | Op::Last)
    }

    #[inline]
    pub fn is_aggregator(self) -> bool {
        matches!(self, Op::Len | Op::Sum | Op::Avg | Op::Min | Op::Max)
    }

    #[inline]
    pub fn is_qualifier(self) -> bool {
        matches!(self, Op::Any | Op::All)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PropertyRef {
    Property(String),
    Metadata(String),
    TemporalProperty(String),
}

impl Display for PropertyRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PropertyRef::TemporalProperty(name) => write!(f, "TemporalProperty({})", name),
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
            | PropertyRef::TemporalProperty(name) => name,
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
    pub ops: Vec<Op>, // at most 2 (validated)
    pub _phantom: PhantomData<M>,
}

impl<M> Display for PropertyFilter<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut expr = match &self.prop_ref {
            PropertyRef::Property(name) => name.to_string(),
            PropertyRef::Metadata(name) => format!("const({})", name),
            PropertyRef::TemporalProperty(name) => format!("temporal({})", name),
        };

        for op in &self.ops {
            expr = match op {
                Op::First => format!("first({})", expr),
                Op::Last => format!("last({})", expr),
                Op::Len => format!("len({})", expr),
                Op::Sum => format!("sum({})", expr),
                Op::Avg => format!("avg({})", expr),
                Op::Min => format!("min({})", expr),
                Op::Max => format!("max({})", expr),
                Op::Any => format!("any({})", expr),
                Op::All => format!("all({})", expr),
            };
        }

        match &self.prop_value {
            PropertyFilterValue::None => write!(f, "{} {}", expr, self.operator),
            PropertyFilterValue::Single(value) => write!(f, "{} {} {}", expr, self.operator, value),
            PropertyFilterValue::Set(values) => {
                let sorted = sort_comparable_props(values.iter().collect_vec());
                let values_str = sorted.iter().map(|v| format!("{}", v)).join(", ");
                write!(f, "{} {} [{}]", expr, self.operator, values_str)
            }
        }
    }
}

enum ValueType {
    Seq(Vec<Prop>),
    Scalar(Option<Prop>),
}

impl<M> PropertyFilter<M> {
    #[inline]
    pub fn with_op(mut self, op: Op) -> Self {
        self.ops.push(op);
        self
    }

    #[inline]
    pub fn with_ops(mut self, ops: impl IntoIterator<Item = Op>) -> Self {
        self.ops.extend(ops);
        self
    }

    pub fn eq(prop_ref: PropertyRef, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_ref,
            prop_value: PropertyFilterValue::Single(prop_value.into()),
            operator: FilterOperator::Eq,
            ops: vec![],
            _phantom: PhantomData,
        }
    }

    pub fn ne(prop_ref: PropertyRef, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_ref,
            prop_value: PropertyFilterValue::Single(prop_value.into()),
            operator: FilterOperator::Ne,
            ops: vec![],
            _phantom: PhantomData,
        }
    }

    pub fn le(prop_ref: PropertyRef, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_ref,
            prop_value: PropertyFilterValue::Single(prop_value.into()),
            operator: FilterOperator::Le,
            ops: vec![],
            _phantom: PhantomData,
        }
    }

    pub fn ge(prop_ref: PropertyRef, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_ref,
            prop_value: PropertyFilterValue::Single(prop_value.into()),
            operator: FilterOperator::Ge,
            ops: vec![],
            _phantom: PhantomData,
        }
    }

    pub fn lt(prop_ref: PropertyRef, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_ref,
            prop_value: PropertyFilterValue::Single(prop_value.into()),
            operator: FilterOperator::Lt,
            ops: vec![],
            _phantom: PhantomData,
        }
    }

    pub fn gt(prop_ref: PropertyRef, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_ref,
            prop_value: PropertyFilterValue::Single(prop_value.into()),
            operator: FilterOperator::Gt,
            ops: vec![],
            _phantom: PhantomData,
        }
    }

    pub fn is_in(prop_ref: PropertyRef, prop_values: impl IntoIterator<Item = Prop>) -> Self {
        Self {
            prop_ref,
            prop_value: PropertyFilterValue::Set(Arc::new(prop_values.into_iter().collect())),
            operator: FilterOperator::In,
            ops: vec![],
            _phantom: PhantomData,
        }
    }

    pub fn is_not_in(prop_ref: PropertyRef, prop_values: impl IntoIterator<Item = Prop>) -> Self {
        Self {
            prop_ref,
            prop_value: PropertyFilterValue::Set(Arc::new(prop_values.into_iter().collect())),
            operator: FilterOperator::NotIn,
            ops: vec![],
            _phantom: PhantomData,
        }
    }

    pub fn is_none(prop_ref: PropertyRef) -> Self {
        Self {
            prop_ref,
            prop_value: PropertyFilterValue::None,
            operator: FilterOperator::IsNone,
            ops: vec![],
            _phantom: PhantomData,
        }
    }

    pub fn is_some(prop_ref: PropertyRef) -> Self {
        Self {
            prop_ref,
            prop_value: PropertyFilterValue::None,
            operator: FilterOperator::IsSome,
            ops: vec![],
            _phantom: PhantomData,
        }
    }

    pub fn starts_with(prop_ref: PropertyRef, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_ref,
            prop_value: PropertyFilterValue::Single(prop_value.into()),
            operator: FilterOperator::StartsWith,
            ops: vec![],
            _phantom: PhantomData,
        }
    }

    pub fn ends_with(prop_ref: PropertyRef, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_ref,
            prop_value: PropertyFilterValue::Single(prop_value.into()),
            operator: FilterOperator::EndsWith,
            ops: vec![],
            _phantom: PhantomData,
        }
    }

    pub fn contains(prop_ref: PropertyRef, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_ref,
            prop_value: PropertyFilterValue::Single(prop_value.into()),
            operator: FilterOperator::Contains,
            ops: vec![],
            _phantom: PhantomData,
        }
    }

    pub fn not_contains(prop_ref: PropertyRef, prop_value: impl Into<Prop>) -> Self {
        Self {
            prop_ref,
            prop_value: PropertyFilterValue::Single(prop_value.into()),
            operator: FilterOperator::NotContains,
            ops: vec![],
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
            ops: vec![],
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

    fn validate_operator_against_dtype(
        &self,
        dtype: &PropType,
        expect_map: bool,
    ) -> Result<(), GraphError> {
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
            | FilterOperator::FuzzySearch { .. } => match &self.prop_value {
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
            },
        }
        Ok(())
    }

    /// Enforce the OK/NOK matrix and compute the **effective dtype**
    /// after applying the op chain (used to validate the final operator).
    ///
    /// Op Pair Validity Matrix (first op × second op)
    ///                 Selector     Aggregator     Qualifier
    /// Selector           NOK          OK             OK
    /// Aggregator         NOK          NOK            NOK
    /// Qualifier          NOK          OK             OK
    ///
    /// Single-op cases (standalone):
    ///   Selector   → OK
    ///   Aggregator → OK
    ///   Qualifier  → OK
    fn validate_chain_and_infer_effective_dtype(
        &self,
        src_dtype: &PropType,
        is_temporal: bool,
    ) -> Result<PropType, GraphError> {
        fn agg_result_dtype(
            inner: &PropType,
            op: Op,
            ctx: &'static str,
        ) -> Result<PropType, GraphError> {
            use PropType::*;
            Ok(match op {
                Op::Len => U64,

                Op::Sum => match inner {
                    U8 | U16 | U32 | U64 => U64,
                    I32 | I64 => I64,
                    F32 | F64 => F64,
                    _ => {
                        return Err(GraphError::InvalidFilter(format!(
                            "sum() {} requires numeric",
                            ctx
                        )))
                    }
                },

                Op::Avg => match inner {
                    U8 | U16 | U32 | U64 | I32 | I64 | F32 | F64 => F64,
                    _ => {
                        return Err(GraphError::InvalidFilter(format!(
                            "avg() {} requires numeric",
                            ctx
                        )))
                    }
                },

                Op::Min | Op::Max => match inner {
                    U8 | U16 | U32 | U64 | I32 | I64 | F32 | F64 => inner.clone(),
                    _ => {
                        return Err(GraphError::InvalidFilter(format!(
                            "{:?} {} requires numeric",
                            op, ctx
                        )))
                    }
                },

                _ => unreachable!(),
            })
        }

        if self.ops.len() > 2 {
            return Err(GraphError::InvalidFilter(
                "At most two list/temporal operations are allowed.".into(),
            ));
        }

        let used_qualifier = self.ops.iter().any(|o| o.is_qualifier());
        if used_qualifier && !is_temporal {
            if matches!(
                self.operator,
                FilterOperator::IsSome | FilterOperator::IsNone
            ) {
                return Err(GraphError::InvalidFilter(
                    "Operator IS_SOME/IS_NONE is not supported with element qualifiers; apply it to the list itself (without elem qualifiers).".into()
                ));
            }
        }

        let used_agg = self.ops.iter().any(|o| o.is_aggregator());
        let disallowed_with_agg = matches!(
            self.operator,
            FilterOperator::StartsWith
                | FilterOperator::EndsWith
                | FilterOperator::Contains
                | FilterOperator::NotContains
                | FilterOperator::FuzzySearch { .. }
                | FilterOperator::IsNone
                | FilterOperator::IsSome
        );
        if used_agg && disallowed_with_agg {
            return Err(GraphError::InvalidFilter(format!(
                "Operator {} is not supported with list aggregation",
                self.operator
            )));
        }

        let require_iterable =
            |op: Op, shape_is_seq: bool, shape_is_list: bool| -> Result<(), GraphError> {
                if op.is_selector() {
                    if !shape_is_seq {
                        return Err(GraphError::InvalidFilter(format!(
                            "{:?} requires list or temporal source",
                            op
                        )));
                    }
                } else if op.is_aggregator() {
                    if !(shape_is_seq || shape_is_list) {
                        return Err(GraphError::InvalidFilter(format!(
                            "{:?} requires list or temporal source",
                            op
                        )));
                    }
                } else if op.is_qualifier() {
                    if !(shape_is_seq || shape_is_list) {
                        return Err(GraphError::InvalidFilter(format!(
                            "{:?} requires list or temporal source",
                            op
                        )));
                    }
                }
                Ok(())
            };

        let (shape_is_list, elem_ty) = (matches!(src_dtype, PropType::List(_)), src_dtype.clone());

        // Pair rules (OK/NOK)
        let pair_ok = |a: Op, b: Op| -> bool {
            // NOK
            if a.is_selector() && b.is_selector() {
                return false;
            } // [Sel, Sel]
            if a.is_aggregator() && (b.is_selector() || b.is_qualifier() || b.is_aggregator()) {
                return false; // [Agg, Sel] / [Agg, Qual] / [Agg, Agg]
            }
            if a.is_qualifier() && b.is_selector() {
                return false;
            } // [Qual, Sel]
              // OK
            if a.is_selector() && (b.is_aggregator() || b.is_qualifier()) {
                return true;
            } // [Sel, Agg] / [Sel, Qual]
            if a.is_qualifier() && (b.is_aggregator() || b.is_qualifier()) {
                return true;
            } // [Qual, Agg] / [Qual, Qual]
            true
        };

        match (is_temporal, self.ops.as_slice()) {
            // Catch-all: if 3 or more ops are present, reject
            (false, &[_, _, _, ..]) | (true, &[_, _, _, ..]) => Err(GraphError::InvalidFilter(
                "At most two list/temporal operations are allowed.".into(),
            )),

            // No ops
            (true, []) => {
                // effective dtype: List<elem_ty> (comparing to the full time-series)
                Ok(PropType::List(Box::new(elem_ty.clone())))
            }
            (false, []) => Ok(elem_ty),

            // Non-temporal: exactly one op (must be Agg or Qual) on List<T>
            (false, [op]) => {
                if !op.is_aggregator() && !op.is_qualifier() {
                    return Err(GraphError::InvalidFilter(format!(
                        "Non-temporal properties support only aggregators/qualifiers; got {:?}",
                        op
                    )));
                }
                if !shape_is_list {
                    return Err(GraphError::InvalidFilter(format!(
                        "{:?} requires list; property is {:?}",
                        op, elem_ty
                    )));
                }
                let inner = elem_ty.inner().ok_or_else(|| {
                    GraphError::InvalidFilter(format!("Expected list type, got {:?}", elem_ty))
                })?;
                if op.is_aggregator() {
                    agg_result_dtype(inner, *op, "requires numeric list")
                } else {
                    Ok(inner.clone())
                }
            }

            // Non-temporal: two ops -> not allowed
            (false, [_a, _b]) => Err(GraphError::InvalidFilter(
                "Non-temporal properties support at most one op.".into(),
            )),

            // Temporal: one op
            (true, [op]) => {
                require_iterable(*op, true, shape_is_list)?;
                if op.is_selector() {
                    // Selecting a single instant from the temporal sequence.
                    // If the temporal element type is T, `first/last` yields T.
                    // If the temporal element type is List<U>, it yields List<U>.
                    let eff = elem_ty.clone();
                    return Ok(eff);
                }

                let eff = if op.is_aggregator() {
                    if shape_is_list {
                        return Err(GraphError::InvalidFilter(format!(
                            "{:?} over temporal requires scalar elements; got List",
                            op
                        )));
                    }
                    return agg_result_dtype(&elem_ty, *op, "over time");
                } else {
                    if let PropType::List(inner) = &elem_ty {
                        inner.as_ref().clone()
                    } else {
                        elem_ty.clone()
                    }
                };
                Ok(eff)
            }

            // Temporal: two ops
            (true, [a, b]) => {
                if !pair_ok(*a, *b) {
                    return Err(GraphError::InvalidFilter(format!(
                        "Invalid op pair: {:?} then {:?}",
                        a, b
                    )));
                }
                match (*a, *b) {
                    (sa, sb) if sa.is_selector() && sb.is_aggregator() => {
                        if !shape_is_list {
                            return Err(GraphError::InvalidFilter(
                                "Selector then aggregator requires Seq[List[T]] (temporal of lists)".into(),
                            ));
                        }
                        let inner = elem_ty.inner().ok_or_else(|| {
                            GraphError::InvalidFilter(format!(
                                "Expected list type, got {:?}",
                                elem_ty
                            ))
                        })?;
                        agg_result_dtype(inner, sb, "requires numeric")
                    }
                    (sa, sb) if sa.is_selector() && sb.is_qualifier() => {
                        if !shape_is_list {
                            return Err(GraphError::InvalidFilter(
                                "Selector then qualifier requires Seq[List[T]]".into(),
                            ));
                        }
                        let inner = elem_ty.inner().ok_or_else(|| {
                            GraphError::InvalidFilter(format!(
                                "Expected list type, got {:?}",
                                elem_ty
                            ))
                        })?;
                        Ok(inner.clone())
                    }
                    (qa, qb) if qa.is_qualifier() && qb.is_aggregator() => {
                        if !shape_is_list {
                            return Err(GraphError::InvalidFilter(
                                "Qualifier then aggregator requires Seq[List[T]]".into(),
                            ));
                        }
                        let inner = elem_ty.inner().ok_or_else(|| {
                            GraphError::InvalidFilter(format!(
                                "Expected list type, got {:?}",
                                elem_ty
                            ))
                        })?;
                        agg_result_dtype(inner, qb, "requires numeric")
                    }
                    (qa, qb) if qa.is_qualifier() && qb.is_qualifier() => {
                        // Two qualifiers on a temporal property: operator applies to INNER ELEMENTS.
                        // We must validate the operator against the *inner element type*, not Bool.
                        if !shape_is_list {
                            return Err(GraphError::InvalidFilter(
                                "Two qualifiers on a temporal property require Seq[List[T]]".into(),
                            ));
                        }
                        let inner = elem_ty.inner().ok_or_else(|| {
                            GraphError::InvalidFilter(format!(
                                "Expected list type, got {:?}",
                                elem_ty
                            ))
                        })?;
                        Ok(inner.clone())
                    }
                    _ => unreachable!(),
                }
            }
        }
    }

    pub fn resolve_prop_id(&self, meta: &Meta, expect_map: bool) -> Result<usize, GraphError> {
        let (name, is_static, is_temporal) = match &self.prop_ref {
            PropertyRef::Metadata(n) => (n.as_str(), true, false),
            PropertyRef::Property(n) => (n.as_str(), false, false),
            PropertyRef::TemporalProperty(n) => (n.as_str(), false, true),
        };

        let (id, original_dtype) = match meta.get_prop_id_and_type(name, is_static) {
            None => {
                return if is_static {
                    Err(GraphError::MetadataMissingError(name.to_string()))
                } else {
                    Err(GraphError::PropertyMissingError(name.to_string()))
                }
            }
            Some((id, dtype)) => (id, dtype),
        };

        // Decide map-semantics for metadata
        let is_original_map = matches!(original_dtype, PropType::Map(..));
        let rhs_is_map = matches!(self.prop_value, PropertyFilterValue::Single(Prop::Map(_)));
        let expect_map_now = if is_static && rhs_is_map {
            true
        } else {
            is_static && expect_map && is_original_map
        };

        // Validate chain and final operator with correct semantics
        let eff = self.validate_chain_and_infer_effective_dtype(&original_dtype, is_temporal)?;

        // (Redundant safety) final check
        self.validate_operator_against_dtype(&eff, expect_map_now)?;
        Ok(id)
    }

    fn aggregate_values(vals: &[Prop], op: Op) -> Option<Prop> {
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

        fn reduce_unsigned(vals: &[Prop], ret_minmax: fn(u64) -> Prop, op: Op) -> Option<Prop> {
            match op {
                Op::Sum => {
                    let (promoted, s64, s128) = scan_u64_sum(vals)?;
                    Some(if promoted {
                        Prop::U64(u64::try_from(s128).ok()?)
                    } else {
                        Prop::U64(s64)
                    })
                }
                Op::Avg => {
                    let (promoted, s64, s128) = scan_u64_sum(vals)?;
                    let count = vals.len() as u64;
                    let s = if promoted { s128 as f64 } else { s64 as f64 };
                    Some(Prop::F64(s / (count as f64)))
                }
                Op::Min => scan_u64_min_max(vals).map(|(mn, _)| ret_minmax(mn)),
                Op::Max => scan_u64_min_max(vals).map(|(_, mx)| ret_minmax(mx)),
                Op::Len | Op::First | Op::Last | Op::Any | Op::All => unreachable!(),
            }
        }

        fn reduce_signed(vals: &[Prop], ret_minmax: fn(i64) -> Prop, op: Op) -> Option<Prop> {
            match op {
                Op::Sum => {
                    let (promoted, s64, s128) = scan_i64_sum(vals)?;
                    Some(if promoted {
                        Prop::I64(i64::try_from(s128).ok()?)
                    } else {
                        Prop::I64(s64)
                    })
                }
                Op::Avg => {
                    let (promoted, s64, s128) = scan_i64_sum(vals)?;
                    let count = vals.len() as u64;
                    let s = if promoted { s128 as f64 } else { s64 as f64 };
                    Some(Prop::F64(s / (count as f64)))
                }
                Op::Min => scan_i64_min_max(vals).map(|(mn, _)| ret_minmax(mn)),
                Op::Max => scan_i64_min_max(vals).map(|(_, mx)| ret_minmax(mx)),
                Op::Len | Op::First | Op::Last | Op::Any | Op::All => unreachable!(),
            }
        }

        fn reduce_float(vals: &[Prop], ret_minmax: fn(f64) -> Prop, op: Op) -> Option<Prop> {
            match op {
                Op::Sum => scan_f64_sum_count(vals).map(|(sum, _)| Prop::F64(sum)),
                Op::Avg => {
                    let (sum, count) = scan_f64_sum_count(vals)?;
                    Some(Prop::F64(sum / (count as f64)))
                }
                Op::Min => scan_f64_min_max(vals).map(|(mn, _)| ret_minmax(mn)),
                Op::Max => scan_f64_min_max(vals).map(|(_, mx)| ret_minmax(mx)),
                Op::Len | Op::First | Op::Last | Op::Any | Op::All => unreachable!(),
            }
        }

        match op {
            Op::Len => Some(Prop::U64(vals.len() as u64)),
            Op::Sum | Op::Avg | Op::Min | Op::Max => {
                if vals.is_empty() {
                    return None;
                }
                let inner = vals[0].dtype();
                match inner {
                    PropType::U8 => reduce_unsigned(vals, |x| Prop::U8(x as u8), op),
                    PropType::U16 => reduce_unsigned(vals, |x| Prop::U16(x as u16), op),
                    PropType::U32 => reduce_unsigned(vals, |x| Prop::U32(x as u32), op),
                    PropType::U64 => reduce_unsigned(vals, |x| Prop::U64(x), op),

                    PropType::I32 => reduce_signed(vals, |x| Prop::I32(x as i32), op),
                    PropType::I64 => reduce_signed(vals, |x| Prop::I64(x), op),

                    PropType::F32 => reduce_float(vals, |x| Prop::F32(x as f32), op),
                    PropType::F64 => reduce_float(vals, |x| Prop::F64(x), op),
                    _ => None,
                }
            }
            Op::First | Op::Last | Op::Any | Op::All => unreachable!(),
        }
    }

    fn apply_two_qualifiers_temporal(&self, prop: &[Prop], outer: Op, inner: Op) -> bool {
        debug_assert!(outer.is_qualifier() && inner.is_qualifier());

        let mut per_time: Vec<bool> = Vec::with_capacity(prop.len());
        for v in prop {
            // Only lists participate. Non-lists => "no elements" at that time.
            let elems: &[Prop] = match v {
                Prop::List(inner_vals) => inner_vals.as_slice(),
                _ => &[], // <-- do NOT coerce a scalar into a 1-element list
            };

            let inner_ok = match inner {
                Op::Any => elems
                    .iter()
                    .any(|e| self.operator.apply_to_property(&self.prop_value, Some(e))),
                Op::All => {
                    // All requires at least one element.
                    !elems.is_empty()
                        && elems
                            .iter()
                            .all(|e| self.operator.apply_to_property(&self.prop_value, Some(e)))
                }
                _ => unreachable!(),
            };

            per_time.push(inner_ok);
        }

        match outer {
            Op::Any => per_time.into_iter().any(|b| b),
            Op::All => !per_time.is_empty() && per_time.into_iter().all(|b| b),
            _ => unreachable!(),
        }
    }

    fn eval_ops(&self, mut state: ValueType) -> (Option<Prop>, Option<Vec<Prop>>, Option<Op>) {
        let mut qualifier: Option<Op> = None;

        let has_later_reduce = |ops: &[Op], i: usize| -> bool {
            ops.iter()
                .enumerate()
                .skip(i + 1)
                .any(|(_, op)| op.is_selector() || op.is_aggregator() || op.is_qualifier())
        };

        let flatten_one = |vals: Vec<Prop>| -> Vec<Prop> {
            let mut out = Vec::new();
            for p in vals {
                if let Prop::List(inner) = p {
                    out.extend(inner.as_slice().iter().cloned());
                } else {
                    out.push(p);
                }
            }
            out
        };

        let per_elem_map = |vals: Vec<Prop>, op: Op| -> Vec<Prop> {
            vals.into_iter()
                .filter_map(|p| match (op, p) {
                    (Op::Len, Prop::List(inner)) => Some(Prop::U64(inner.len() as u64)),
                    (Op::Sum, Prop::List(inner)) => {
                        Self::aggregate_values(inner.as_slice(), Op::Sum)
                    }
                    (Op::Avg, Prop::List(inner)) => {
                        Self::aggregate_values(inner.as_slice(), Op::Avg)
                    }
                    (Op::Min, Prop::List(inner)) => {
                        Self::aggregate_values(inner.as_slice(), Op::Min)
                    }
                    (Op::Max, Prop::List(inner)) => {
                        Self::aggregate_values(inner.as_slice(), Op::Max)
                    }
                    _ => None,
                })
                .collect()
        };

        for (i, op) in self.ops.iter().enumerate() {
            match *op {
                Op::First => {
                    state = match state {
                        ValueType::Seq(vs) => ValueType::Scalar(vs.first().cloned()),
                        ValueType::Scalar(s) => ValueType::Scalar(s),
                    };
                }
                Op::Last => {
                    state = match state {
                        ValueType::Seq(vs) => ValueType::Scalar(vs.last().cloned()),
                        ValueType::Scalar(s) => ValueType::Scalar(s),
                    };
                }
                Op::Len | Op::Sum | Op::Avg | Op::Min | Op::Max => {
                    state = match state {
                        ValueType::Seq(vs) => {
                            if matches!(vs.first(), Some(Prop::List(_))) {
                                ValueType::Seq(per_elem_map(vs, *op))
                            } else {
                                ValueType::Scalar(Self::aggregate_values(&vs, *op))
                            }
                        }
                        ValueType::Scalar(Some(Prop::List(inner))) => {
                            ValueType::Scalar(Self::aggregate_values(inner.as_slice(), *op))
                        }
                        ValueType::Scalar(Some(_)) => ValueType::Scalar(None),
                        ValueType::Scalar(None) => ValueType::Scalar(None),
                    };
                }
                Op::Any | Op::All => {
                    qualifier = Some(*op);
                    let later = has_later_reduce(&self.ops, i);
                    state = match state {
                        ValueType::Seq(vs) => {
                            if later {
                                ValueType::Seq(vs)
                            } else {
                                ValueType::Seq(flatten_one(vs))
                            }
                        }
                        ValueType::Scalar(Some(Prop::List(inner))) => {
                            if later {
                                ValueType::Seq(vec![Prop::List(inner)])
                            } else {
                                ValueType::Seq(inner.as_slice().to_vec())
                            }
                        }
                        ValueType::Scalar(Some(p)) => ValueType::Seq(vec![p]),
                        ValueType::Scalar(None) => ValueType::Seq(vec![]),
                    };
                }
            }
        }

        if let Some(q) = qualifier {
            let elems = match state {
                ValueType::Seq(vs) => vs,
                ValueType::Scalar(Some(Prop::List(inner))) => inner.as_slice().to_vec(),
                ValueType::Scalar(Some(p)) => vec![p],
                ValueType::Scalar(None) => vec![],
            };
            return (None, Some(elems), Some(q));
        }

        match state {
            ValueType::Scalar(v) => (v, None, None),
            ValueType::Seq(vs) => (None, Some(vs), None),
        }
    }

    fn apply_eval(
        &self,
        reduced: Option<Prop>,
        maybe_seq: Option<Vec<Prop>>,
        qualifier: Option<Op>,
    ) -> bool {
        // 1) Reduced scalar -> compare directly
        // For example:
        //   1. NodeFilter::property("temp").temporal().avg().ge(Prop::F64(10.0))
        //   2. NodeFilter::property("readings").temporal().first().len().eq(Prop::U64(3))
        //   3. NodeFilter::property("scores").avg().gt(Prop::F64(0.5))
        if let Some(value) = reduced {
            return self
                .operator
                .apply_to_property(&self.prop_value, Some(&value));
        }

        // 2) Qualifier over a sequence (ANY/ALL)
        // For example:
        //   1. NodeFilter::property("tags").any().eq(Prop::Str("gold".into()))
        //   2. NodeFilter::property("price").temporal().any().gt(Prop::F64(100.0))
        if let Some(q) = qualifier {
            let vals = maybe_seq.unwrap_or_default();
            if vals.is_empty() {
                return false;
            }
            let chk = |v: &Prop| self.operator.apply_to_property(&self.prop_value, Some(v));
            return match q {
                Op::Any => vals.iter().any(chk),
                Op::All => vals.iter().all(chk),
                _ => unreachable!(),
            };
        }

        // 3) Compare whole sequence as a List, or missing value
        // For example:
        //   1. NodeFilter::property("temperature").temporal().eq(Prop::List(vec![...]))
        //   2. NodeFilter::property("tags").eq(Prop::List(vec!["gold", "silver"]))
        if let Some(seq) = maybe_seq {
            let full = Prop::List(Arc::new(seq));
            self.operator
                .apply_to_property(&self.prop_value, Some(&full))
        } else {
            self.operator.apply_to_property(&self.prop_value, None)
        }
    }

    fn eval_scalar_and_apply(&self, prop: Option<Prop>) -> bool {
        let (reduced, maybe_seq, qualifier) = self.eval_ops(ValueType::Scalar(prop));
        self.apply_eval(reduced, maybe_seq, qualifier)
    }

    fn eval_temporal_and_apply(&self, props: Vec<Prop>) -> bool {
        // Special-case: two qualifiers on temporal -> directly compute final bool.
        // For example:
        //   1. NodeFilter::property("p_flags").temporal().all().all().eq(Prop::u64(1))
        //   2. NodeFilter::property("p_flags").temporal().any().all().eq(Prop::bool(true))
        if self.ops.len() == 2 && self.ops[0].is_qualifier() && self.ops[1].is_qualifier() {
            return self.apply_two_qualifiers_temporal(&props, self.ops[0], self.ops[1]);
        }
        let (reduced, maybe_seq, qualifier) = self.eval_ops(ValueType::Seq(props));
        self.apply_eval(reduced, maybe_seq, qualifier)
    }

    pub fn matches(&self, other: Option<&Prop>) -> bool {
        if self.ops.is_empty() {
            return self.operator.apply_to_property(&self.prop_value, other);
        }
        self.eval_scalar_and_apply(other.cloned())
    }

    fn is_property_matched<I: InternalPropertiesOps + Clone>(
        &self,
        t_prop_id: usize,
        props: Properties<I>,
    ) -> bool {
        match self.prop_ref {
            PropertyRef::Property(_) => {
                let prop = props.get_by_id(t_prop_id);
                self.matches(prop.as_ref())
            }
            PropertyRef::Metadata(_) => false,
            PropertyRef::TemporalProperty(_) => {
                let Some(tview) = props.temporal().get_by_id(t_prop_id) else {
                    return false;
                };
                let props: Vec<Prop> = tview.values().collect();
                self.eval_temporal_and_apply(props)
            }
        }
    }

    fn is_metadata_matched<I: InternalPropertiesOps + Clone>(
        &self,
        c_prop_id: usize,
        props: Metadata<I>,
    ) -> bool {
        match self.prop_ref {
            PropertyRef::Metadata(_) => {
                let prop_value = props.get_by_id(c_prop_id);
                self.matches(prop_value.as_ref())
            }
            _ => false,
        }
    }

    pub fn matches_node<'graph, G: GraphViewOps<'graph>>(
        &self,
        graph: &G,
        prop_id: usize,
        node: NodeStorageRef,
    ) -> bool {
        let node = NodeView::new_internal(graph, node.vid());
        match self.prop_ref {
            PropertyRef::Metadata(_) => {
                let props = node.metadata();
                self.is_metadata_matched(prop_id, props)
            }
            PropertyRef::TemporalProperty(_) | PropertyRef::Property(_) => {
                let props = node.properties();
                self.is_property_matched(prop_id, props)
            }
        }
    }

    pub fn matches_edge<'graph, G: GraphViewOps<'graph>>(
        &self,
        graph: &G,
        prop_id: usize,
        edge: EdgeStorageRef,
    ) -> bool {
        let edge = EdgeView::new(graph, edge.out_ref());
        match self.prop_ref {
            PropertyRef::Metadata(_) => {
                let props = edge.metadata();
                self.is_metadata_matched(prop_id, props)
            }
            PropertyRef::TemporalProperty(_) | PropertyRef::Property(_) => {
                let props = edge.properties();
                self.is_property_matched(prop_id, props)
            }
        }
    }

    pub fn matches_exploded_edge<G: GraphView>(
        &self,
        graph: &G,
        prop_id: usize,
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
            PropertyRef::TemporalProperty(_) | PropertyRef::Property(_) => {
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

    fn ops(&self) -> &[Op] {
        &[]
    }
}

impl<T: InternalPropertyFilterOps> InternalPropertyFilterOps for Arc<T> {
    type Marker = T::Marker;
    fn property_ref(&self) -> PropertyRef {
        self.deref().property_ref()
    }
    fn ops(&self) -> &[Op] {
        self.deref().ops()
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
        PropertyFilter::eq(self.property_ref(), value.into()).with_ops(self.ops().iter().copied())
    }

    fn ne(&self, value: impl Into<Prop>) -> PropertyFilter<Self::Marker> {
        PropertyFilter::ne(self.property_ref(), value.into()).with_ops(self.ops().iter().copied())
    }

    fn le(&self, value: impl Into<Prop>) -> PropertyFilter<Self::Marker> {
        PropertyFilter::le(self.property_ref(), value.into()).with_ops(self.ops().iter().copied())
    }

    fn ge(&self, value: impl Into<Prop>) -> PropertyFilter<Self::Marker> {
        PropertyFilter::ge(self.property_ref(), value.into()).with_ops(self.ops().iter().copied())
    }

    fn lt(&self, value: impl Into<Prop>) -> PropertyFilter<Self::Marker> {
        PropertyFilter::lt(self.property_ref(), value.into()).with_ops(self.ops().iter().copied())
    }

    fn gt(&self, value: impl Into<Prop>) -> PropertyFilter<Self::Marker> {
        PropertyFilter::gt(self.property_ref(), value.into()).with_ops(self.ops().iter().copied())
    }

    fn is_in(&self, values: impl IntoIterator<Item = Prop>) -> PropertyFilter<Self::Marker> {
        PropertyFilter::is_in(self.property_ref(), values).with_ops(self.ops().iter().copied())
    }

    fn is_not_in(&self, values: impl IntoIterator<Item = Prop>) -> PropertyFilter<Self::Marker> {
        PropertyFilter::is_not_in(self.property_ref(), values).with_ops(self.ops().iter().copied())
    }

    fn is_none(&self) -> PropertyFilter<Self::Marker> {
        PropertyFilter::is_none(self.property_ref()).with_ops(self.ops().iter().copied())
    }

    fn is_some(&self) -> PropertyFilter<Self::Marker> {
        PropertyFilter::is_some(self.property_ref()).with_ops(self.ops().iter().copied())
    }

    fn starts_with(&self, value: impl Into<Prop>) -> PropertyFilter<Self::Marker> {
        PropertyFilter::starts_with(self.property_ref(), value.into())
            .with_ops(self.ops().iter().copied())
    }

    fn ends_with(&self, value: impl Into<Prop>) -> PropertyFilter<Self::Marker> {
        PropertyFilter::ends_with(self.property_ref(), value.into())
            .with_ops(self.ops().iter().copied())
    }

    fn contains(&self, value: impl Into<Prop>) -> PropertyFilter<Self::Marker> {
        PropertyFilter::contains(self.property_ref(), value.into())
            .with_ops(self.ops().iter().copied())
    }

    fn not_contains(&self, value: impl Into<Prop>) -> PropertyFilter<Self::Marker> {
        PropertyFilter::not_contains(self.property_ref(), value.into())
            .with_ops(self.ops().iter().copied())
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
        .with_ops(self.ops().iter().copied())
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
pub struct OpChainBuilder<M> {
    pub prop_ref: PropertyRef,
    pub ops: Vec<Op>,
    pub _phantom: PhantomData<M>,
}

impl<M> OpChainBuilder<M> {
    pub fn with_op(mut self, op: Op) -> Self {
        self.ops.push(op);
        self
    }

    pub fn with_ops(mut self, ops: impl IntoIterator<Item = Op>) -> Self {
        self.ops.extend(ops);
        self
    }

    pub fn first(self) -> Self {
        self.with_op(Op::First)
    }

    pub fn last(self) -> Self {
        self.with_op(Op::Last)
    }

    pub fn any(self) -> Self {
        self.with_op(Op::Any)
    }

    pub fn all(self) -> Self {
        self.with_op(Op::All)
    }

    pub fn len(self) -> Self {
        self.with_op(Op::Len)
    }

    pub fn sum(self) -> Self {
        self.with_op(Op::Sum)
    }

    pub fn avg(self) -> Self {
        self.with_op(Op::Avg)
    }

    pub fn min(self) -> Self {
        self.with_op(Op::Min)
    }

    pub fn max(self) -> Self {
        self.with_op(Op::Max)
    }
}

impl<M: Send + Sync + Clone + 'static> InternalPropertyFilterOps for OpChainBuilder<M> {
    type Marker = M;

    fn property_ref(&self) -> PropertyRef {
        self.prop_ref.clone()
    }

    fn ops(&self) -> &[Op] {
        &self.ops
    }
}

pub trait ElemQualifierOps: InternalPropertyFilterOps {
    fn any(&self) -> OpChainBuilder<Self::Marker>
    where
        Self: Sized,
    {
        OpChainBuilder {
            prop_ref: self.property_ref(),
            ops: self.ops().iter().copied().chain([Op::Any]).collect(),
            _phantom: PhantomData,
        }
    }

    fn all(&self) -> OpChainBuilder<Self::Marker>
    where
        Self: Sized,
    {
        OpChainBuilder {
            prop_ref: self.property_ref(),
            ops: self.ops().iter().copied().chain([Op::All]).collect(),
            _phantom: PhantomData,
        }
    }
}
impl<T: InternalPropertyFilterOps> ElemQualifierOps for T {}

impl<M> PropertyFilterBuilder<M> {
    pub fn temporal(self) -> OpChainBuilder<M> {
        OpChainBuilder {
            prop_ref: PropertyRef::TemporalProperty(self.0),
            ops: vec![],
            _phantom: PhantomData,
        }
    }
}

pub trait ListAggOps: InternalPropertyFilterOps + Sized {
    fn len(&self) -> OpChainBuilder<Self::Marker> {
        OpChainBuilder {
            prop_ref: self.property_ref(),
            ops: self.ops().iter().copied().chain([Op::Len]).collect(),
            _phantom: PhantomData,
        }
    }

    fn sum(&self) -> OpChainBuilder<Self::Marker> {
        OpChainBuilder {
            prop_ref: self.property_ref(),
            ops: self.ops().iter().copied().chain([Op::Sum]).collect(),
            _phantom: PhantomData,
        }
    }

    fn avg(&self) -> OpChainBuilder<Self::Marker> {
        OpChainBuilder {
            prop_ref: self.property_ref(),
            ops: self.ops().iter().copied().chain([Op::Avg]).collect(),
            _phantom: PhantomData,
        }
    }

    fn min(&self) -> OpChainBuilder<Self::Marker> {
        OpChainBuilder {
            prop_ref: self.property_ref(),
            ops: self.ops().iter().copied().chain([Op::Min]).collect(),
            _phantom: PhantomData,
        }
    }

    fn max(&self) -> OpChainBuilder<Self::Marker> {
        OpChainBuilder {
            prop_ref: self.property_ref(),
            ops: self.ops().iter().copied().chain([Op::Max]).collect(),
            _phantom: PhantomData,
        }
    }
}
impl<T: InternalPropertyFilterOps + Sized> ListAggOps for T {}
