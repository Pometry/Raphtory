use crate::{
    db::{
        api::{
            properties::{internal::InternalPropertiesOps, Metadata, Properties},
            view::{
                internal::{GraphView, NodeTimeSemanticsOps},
                node::NodeViewOps,
                EdgeViewOps,
            },
        },
        graph::{
            edge::EdgeView,
            node::NodeView,
            views::filter::{
                internal::CreateFilter,
                model::{
                    edge_filter::{CompositeEdgeFilter, EdgeFilter},
                    exploded_edge_filter::{CompositeExplodedEdgeFilter, ExplodedEdgeFilter},
                    filter_operator::FilterOperator,
                    node_filter::{CompositeNodeFilter, NodeFilter},
                    TryAsCompositeFilter, Wrap,
                },
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
use std::{collections::HashSet, fmt, fmt::Display, ops::Deref, sync::Arc};

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
    // Qualifiers (quantifiers)
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

#[derive(Clone, Debug, PartialEq, Eq)]
enum Shape {
    Scalar(PropType),
    List(Box<PropType>),
    Seq(Box<Shape>),
    Quantified(Box<Shape>, Op),
}

fn flatten_quantified_depth(mut s: &Shape) -> (&Shape, usize) {
    let mut depth = 0;
    while let Shape::Quantified(inner, _) = s {
        depth += 1;
        s = inner;
    }
    (s, depth)
}

#[inline]
fn agg_result_dtype(inner: &PropType, op: Op, ctx: &str) -> Result<PropType, GraphError> {
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
    pub ops: Vec<Op>, // validated by validate_chain_and_infer_effective_dtype
    pub entity: M,
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

    #[inline]
    fn has_aggregator(&self) -> bool {
        self.ops.iter().copied().any(Op::is_aggregator)
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

    #[inline]
    fn has_elem_qualifier(&self) -> bool {
        self.ops.iter().any(|op| matches!(op, Op::Any | Op::All))
    }

    #[inline]
    fn is_temporal_ref(&self) -> bool {
        matches!(self.prop_ref, PropertyRef::TemporalProperty(_))
    }

    #[inline]
    fn has_temporal_first_qualifier(&self) -> bool {
        self.is_temporal_ref() && matches!(self.ops.first(), Some(Op::Any | Op::All))
    }

    fn validate_operator_against_dtype(
        &self,
        dtype: &PropType,
        expect_map: bool,
    ) -> Result<(), GraphError> {
        if matches!(
            self.operator,
            FilterOperator::IsSome | FilterOperator::IsNone
        ) {
            if self.has_elem_qualifier() && !self.has_temporal_first_qualifier() {
                return Err(GraphError::InvalidFilter(
                    "Invalid filter: Operator IS_SOME/IS_NONE is not supported with element qualifiers; apply it to the list itself (without elem qualifiers).".into()
                ));
            }
        }

        if self.has_aggregator() {
            match self.operator {
                FilterOperator::StartsWith
                | FilterOperator::EndsWith
                | FilterOperator::Contains
                | FilterOperator::NotContains
                | FilterOperator::IsNone
                | FilterOperator::IsSome => {
                    return Err(GraphError::InvalidFilter(format!(
                        "Operator {} is not supported with list aggregation",
                        self.operator
                    )));
                }
                _ => {}
            }
        }

        match self.operator {
            FilterOperator::Eq | FilterOperator::Ne => {
                let _ = self.validate_single_dtype(dtype, expect_map)?;
            }
            FilterOperator::Lt | FilterOperator::Le | FilterOperator::Gt | FilterOperator::Ge => {
                let fd = self.validate_single_dtype(dtype, expect_map)?;
                if !fd.has_cmp() {
                    return Err(GraphError::InvalidFilterCmp(fd));
                }
            }
            FilterOperator::IsIn | FilterOperator::IsNotIn => match &self.prop_value {
                PropertyFilterValue::Set(_) => {}
                PropertyFilterValue::None => {
                    return Err(GraphError::InvalidFilterExpectSetGotNone(self.operator))
                }
                PropertyFilterValue::Single(_) => {
                    return Err(GraphError::InvalidFilterExpectSetGotSingle(self.operator))
                }
            },
            FilterOperator::IsSome | FilterOperator::IsNone => {}
            FilterOperator::StartsWith
            | FilterOperator::EndsWith
            | FilterOperator::Contains
            | FilterOperator::NotContains
            | FilterOperator::FuzzySearch { .. } => match &self.prop_value {
                PropertyFilterValue::Single(v)
                    if matches!(dtype, PropType::Str) && matches!(v.dtype(), PropType::Str) => {}
                PropertyFilterValue::None => {
                    return Err(GraphError::InvalidFilterExpectSingleGotNone(self.operator))
                }
                PropertyFilterValue::Set(_) => {
                    return Err(GraphError::InvalidFilterExpectSingleGotSet(self.operator))
                }
                _ => return Err(GraphError::InvalidContains(self.operator)),
            },
        }

        Ok(())
    }

    // helper used at the bottom; include if you don't already have it
    #[inline]
    fn peel_list_n(mut t: PropType, mut n: usize) -> Result<PropType, GraphError> {
        while n > 0 {
            match t {
                PropType::List(inner) => {
                    t = *inner;
                    n -= 1;
                }
                other => {
                    return Err(GraphError::InvalidFilter(format!(
                        "Too many any/all quantifiers for list depth (stopped at {:?})",
                        other
                    )))
                }
            }
        }
        Ok(t)
    }

    fn validate_chain_and_infer_effective_dtype(
        &self,
        src_dtype: &PropType,
        is_temporal: bool,
    ) -> Result<PropType, GraphError> {
        use Shape::*;

        // Ordering guard for qualifiers/aggregators/selectors ===
        // Disallow:
        //  1) aggregator after qualifier, unless the *first* qualifier is temporal
        //  2) qualifier after aggregator (always illegal)
        //
        // Ensures:
        //   - property(...).all().len() -> error
        //   - property(...).sum().any() -> error
        //   - property(...).temporal().all().len() -> OK
        //   - property(...).temporal().first().all().len() -> error
        let mut saw_agg = false;
        let mut saw_qual = false;
        let mut saw_selector_before_first_qual = false;
        let mut first_qual_is_temporal = false;

        #[inline]
        fn agg_name(op: Op) -> &'static str {
            match op {
                Op::Len => "len",
                Op::Sum => "sum",
                Op::Avg => "avg",
                Op::Min => "min",
                Op::Max => "max",
                _ => unreachable!(),
            }
        }

        for &op in &self.ops {
            match op {
                Op::First | Op::Last => {
                    if !saw_qual {
                        // A selector before the first qualifier collapses the temporal sequence,
                        // so the upcoming first qualifier cannot be considered temporal.
                        saw_selector_before_first_qual = true;
                    }
                }
                Op::Any | Op::All => {
                    // Qualifier after any aggregation is always illegal.
                    if saw_agg {
                        return Err(GraphError::InvalidFilter(
                            "Element qualifiers (any/all) cannot be used after a list aggregation (len/sum/avg/min/max).".into()
                        ));
                    }
                    // Record once whether the very first qualifier is "temporal-first"
                    // (chain is temporal and no selector has run yet).
                    if !saw_qual && is_temporal && !saw_selector_before_first_qual {
                        first_qual_is_temporal = true;
                    }
                    saw_qual = true;
                }
                Op::Len | Op::Sum | Op::Avg | Op::Min | Op::Max => {
                    // Aggregator after a qualifier is illegal unless that first qualifier is temporal.
                    if saw_qual && !first_qual_is_temporal {
                        return Err(GraphError::InvalidFilter(format!(
                            "List aggregation {} cannot be used after an element qualifier (any/all)",
                            agg_name(op)
                        )));
                    }
                    saw_agg = true;
                }
            }
        }

        // Build base shape
        let base_shape: Shape = if is_temporal {
            let inner: Shape = match src_dtype {
                PropType::List(inner) => List(inner.clone()),
                t => Scalar(t.clone()),
            };
            Seq(Box::new(inner))
        } else {
            match src_dtype {
                PropType::List(inner) => List(inner.clone()),
                t => Scalar(t.clone()),
            }
        };

        // Defer selectors to the end of validation
        let mut selectors: Vec<Op> = Vec::new();
        let mut others: Vec<Op> = Vec::new();
        for &op in &self.ops {
            if op.is_selector() {
                selectors.push(op);
            } else {
                others.push(op);
            }
        }
        let mut ops_for_validation: Vec<Op> = Vec::with_capacity(self.ops.len());
        ops_for_validation.extend(others);
        ops_for_validation.extend(selectors);

        // Walk the shape backwards through the ops
        let mut shape = base_shape;
        for &op in ops_for_validation.iter().rev() {
            shape = match op {
                Op::First | Op::Last => match shape {
                    Seq(inner) => *inner,
                    other => {
                        return Err(GraphError::InvalidFilter(format!(
                            "{:?} requires temporal sequence (Seq), got {:?}",
                            op, other
                        )))
                    }
                },

                Op::Any | Op::All => match shape {
                    // nesting quantifiers is allowed
                    Quantified(_, _) => Quantified(Box::new(shape), op),

                    // element-level quantifier over list
                    List(_) => Quantified(Box::new(shape), op),

                    // **temporal** quantifier: do NOT consume a list level here
                    Seq(inner) => match *inner {
                        List(_) | Quantified(_, _) => {
                            Seq(Box::new(Quantified(Box::new(*inner), op)))
                        }
                        Scalar(t) => Seq(Box::new(Quantified(Box::new(Scalar(t)), op))),
                        other => {
                            return Err(GraphError::InvalidFilter(format!(
                                "{:?} requires list elements over time; got Seq({:?})",
                                op, other
                            )))
                        }
                    },

                    Scalar(t) => Quantified(Box::new(Scalar(t)), op),
                },

                Op::Len | Op::Sum | Op::Avg | Op::Min | Op::Max => match shape {
                    List(inner) => {
                        let out = agg_result_dtype(&*inner, op, "over list")?;
                        Scalar(out)
                    }
                    Seq(inner) => match *inner {
                        Scalar(t) => {
                            let out = agg_result_dtype(&t, op, "over time")?;
                            Scalar(out)
                        }
                        List(t) => {
                            let out = agg_result_dtype(&*t, op, "over time of lists")?;
                            Scalar(out)
                        }
                        Quantified(q_inner, qop) => {
                            let mapped_inner = match *q_inner {
                                List(t) => {
                                    let out = agg_result_dtype(
                                        &*t,
                                        op,
                                        "under temporal quantifier over list",
                                    )?;
                                    Scalar(out)
                                }
                                Scalar(t) => {
                                    return Err(GraphError::InvalidFilter(format!(
                                        "{:?} under temporal quantifier requires list elements; got Scalar({:?})",
                                        op, t
                                    )));
                                }
                                Seq(_) | Quantified(_, _) => unreachable!(),
                            };
                            Seq(Box::new(Quantified(Box::new(mapped_inner), qop)))
                        }
                        Seq(_) => unreachable!(),
                    },
                    Quantified(q_inner, qop) => {
                        let mapped_inner = match *q_inner {
                            List(t) => {
                                let out = agg_result_dtype(&*t, op, "under quantifier over list")?;
                                Scalar(out)
                            }
                            Scalar(t) => {
                                return Err(GraphError::InvalidFilter(format!(
                                    "{:?} under quantifier requires list elements; got Scalar({:?})",
                                    op, t
                                )));
                            }
                            Seq(_) | Quantified(_, _) => unreachable!(),
                        };
                        Quantified(Box::new(mapped_inner), qop)
                    }
                    Scalar(t) => {
                        return Err(GraphError::InvalidFilter(format!(
                            "{:?} requires list or temporal sequence, got Scalar({:?})",
                            op, t
                        )));
                    }
                },
            };
        }

        // Compute effective dtype
        let eff = match shape {
            Scalar(t) => t,
            List(t) => PropType::List(t),

            // pure (non-temporal) quantifier(s): consume list depth
            Quantified(_, _) => {
                let (base, qdepth) = flatten_quantified_depth(&shape);
                match base {
                    List(t) => Self::peel_list_n(PropType::List(t.clone()), qdepth)?,
                    Scalar(t) => t.clone(),
                    _ => {
                        return Err(GraphError::InvalidFilter(
                            "Quantifier requires list or scalar input".into(),
                        ))
                    }
                }
            }

            // temporal case: the first quantifier is temporal → ignore one quantifier for depth
            Seq(inner) => match *inner {
                Scalar(t) => PropType::List(Box::new(t)),
                List(t) => PropType::List(Box::new(PropType::List(t))),
                Quantified(_, _) => {
                    let (base, q_depth_total) = flatten_quantified_depth(&*inner);
                    let q_depth_lists = q_depth_total.saturating_sub(1); // skip the temporal one
                    match base {
                        List(t) => Self::peel_list_n(PropType::List(t.clone()), q_depth_lists)?,
                        Scalar(t) => t.clone(),
                        _ => {
                            return Err(GraphError::InvalidFilter(
                                "Temporal quantifier requires list or scalar elements per time"
                                    .into(),
                            ))
                        }
                    }
                }
                Seq(_) => unreachable!(),
            },
        };

        Ok(eff)
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

        let is_original_map = matches!(original_dtype, PropType::Map(..));
        let rhs_is_map = matches!(self.prop_value, PropertyFilterValue::Single(Prop::Map(_)));
        let expect_map_now = if is_static && rhs_is_map {
            true
        } else {
            is_static && expect_map && is_original_map
        };

        let eff = self.validate_chain_and_infer_effective_dtype(&original_dtype, is_temporal)?;

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

    fn reduce_qualifiers_rec(
        &self,
        quals: &[Op],
        v: &Prop,
        predicate: &dyn Fn(&Prop) -> bool,
    ) -> bool {
        if quals.is_empty() {
            return predicate(v);
        }
        let (q, rest) = (quals[0], &quals[1..]);

        if let Prop::List(inner) = v {
            let elems = inner.as_slice();
            let check = |e: &Prop| {
                if rest.is_empty() {
                    predicate(e)
                } else {
                    self.reduce_qualifiers_rec(rest, e, predicate)
                }
            };
            return match q {
                Op::Any => elems.iter().any(check),
                Op::All => !elems.is_empty() && elems.iter().all(check),
                _ => unreachable!(),
            };
        }

        if rest.is_empty() {
            return match q {
                Op::Any | Op::All => predicate(v),
                _ => unreachable!(),
            };
        }
        self.reduce_qualifiers_rec(rest, v, predicate)
    }

    fn apply_agg_to_prop(p: &Prop, op: Op) -> Option<Prop> {
        match (op, p) {
            (Op::Len, Prop::List(inner)) => Some(Prop::U64(inner.len() as u64)),
            (Op::Sum, Prop::List(inner))
            | (Op::Avg, Prop::List(inner))
            | (Op::Min, Prop::List(inner))
            | (Op::Max, Prop::List(inner)) => Self::aggregate_values(inner.as_slice(), op),

            (Op::Len, _) => Some(Prop::U64(1)),

            (Op::Sum, Prop::U8(x)) => Some(Prop::U8(*x)),
            (Op::Sum, Prop::U16(x)) => Some(Prop::U16(*x)),
            (Op::Sum, Prop::U32(x)) => Some(Prop::U32(*x)),
            (Op::Sum, Prop::U64(x)) => Some(Prop::U64(*x)),
            (Op::Sum, Prop::I32(x)) => Some(Prop::I32(*x)),
            (Op::Sum, Prop::I64(x)) => Some(Prop::I64(*x)),
            (Op::Sum, Prop::F32(x)) => {
                if x.is_finite() {
                    Some(Prop::F32(*x))
                } else {
                    None
                }
            }
            (Op::Sum, Prop::F64(x)) => {
                if x.is_finite() {
                    Some(Prop::F64(*x))
                } else {
                    None
                }
            }

            (Op::Avg, Prop::U8(x)) => Some(Prop::F64(*x as f64)),
            (Op::Avg, Prop::U16(x)) => Some(Prop::F64(*x as f64)),
            (Op::Avg, Prop::U32(x)) => Some(Prop::F64(*x as f64)),
            (Op::Avg, Prop::U64(x)) => Some(Prop::F64(*x as f64)),
            (Op::Avg, Prop::I32(x)) => Some(Prop::F64(*x as f64)),
            (Op::Avg, Prop::I64(x)) => Some(Prop::F64(*x as f64)),
            (Op::Avg, Prop::F32(x)) => {
                if x.is_finite() {
                    Some(Prop::F32(*x))
                } else {
                    None
                }
            }
            (Op::Avg, Prop::F64(x)) => {
                if x.is_finite() {
                    Some(Prop::F64(*x))
                } else {
                    None
                }
            }

            (Op::Min, Prop::U8(x)) => Some(Prop::U8(*x)),
            (Op::Min, Prop::U16(x)) => Some(Prop::U16(*x)),
            (Op::Min, Prop::U32(x)) => Some(Prop::U32(*x)),
            (Op::Min, Prop::U64(x)) => Some(Prop::U64(*x)),
            (Op::Min, Prop::I32(x)) => Some(Prop::I32(*x)),
            (Op::Min, Prop::I64(x)) => Some(Prop::I64(*x)),
            (Op::Min, Prop::F32(x)) => {
                if x.is_finite() {
                    Some(Prop::F32(*x))
                } else {
                    None
                }
            }
            (Op::Min, Prop::F64(x)) => {
                if x.is_finite() {
                    Some(Prop::F64(*x))
                } else {
                    None
                }
            }

            (Op::Max, Prop::U8(x)) => Some(Prop::U8(*x)),
            (Op::Max, Prop::U16(x)) => Some(Prop::U16(*x)),
            (Op::Max, Prop::U32(x)) => Some(Prop::U32(*x)),
            (Op::Max, Prop::U64(x)) => Some(Prop::U64(*x)),
            (Op::Max, Prop::I32(x)) => Some(Prop::I32(*x)),
            (Op::Max, Prop::I64(x)) => Some(Prop::I64(*x)),
            (Op::Max, Prop::F32(x)) => {
                if x.is_finite() {
                    Some(Prop::F32(*x))
                } else {
                    None
                }
            }
            (Op::Max, Prop::F64(x)) => {
                if x.is_finite() {
                    Some(Prop::F64(*x))
                } else {
                    None
                }
            }

            (Op::Sum, _) | (Op::Avg, _) | (Op::Min, _) | (Op::Max, _) => None,

            _ => None,
        }
    }

    fn eval_ops(&self, mut state: ValueType) -> (Option<Prop>, Option<Vec<Prop>>, Vec<Op>, bool) {
        let mut qualifiers: Vec<Op> = Vec::new();
        let mut seq_is_temporal = matches!(state, ValueType::Seq(..));
        let mut seen_qual_before_agg = false;

        let per_step_map = |vals: Vec<Prop>, op: Op| -> Vec<Prop> {
            vals.into_iter()
                .filter_map(|p| Self::apply_agg_to_prop(&p, op))
                .collect()
        };

        // Aggregate OVER TIME (when no prior qualifier).
        let reduce_over_seq = |vs: Vec<Prop>, op: Op| -> Option<Prop> {
            match op {
                Op::Len => Some(Prop::U64(vs.len() as u64)),
                Op::Sum | Op::Avg | Op::Min | Op::Max => {
                    if vs.is_empty() || matches!(vs.first(), Some(Prop::List(_))) {
                        return None;
                    }
                    Self::aggregate_values(&vs, op)
                }
                _ => None,
            }
        };

        for op in &self.ops {
            match *op {
                Op::First | Op::Last => {
                    state = match state {
                        ValueType::Seq(vs) => {
                            seq_is_temporal = false;
                            let v = if matches!(*op, Op::First) {
                                vs.first()
                            } else {
                                vs.last()
                            };
                            ValueType::Scalar(v.cloned())
                        }
                        s @ ValueType::Scalar(_) => s,
                    };
                }

                Op::Len | Op::Sum | Op::Avg | Op::Min | Op::Max => {
                    state = match state {
                        ValueType::Seq(vs) if seen_qual_before_agg => {
                            ValueType::Seq(per_step_map(vs, *op))
                        }
                        ValueType::Seq(vs) => {
                            seq_is_temporal = false;
                            ValueType::Scalar(reduce_over_seq(vs, *op))
                        }
                        ValueType::Scalar(Some(Prop::List(inner))) => {
                            ValueType::Scalar(Self::aggregate_values(inner.as_slice(), *op))
                        }
                        ValueType::Scalar(Some(p)) => {
                            ValueType::Scalar(Self::apply_agg_to_prop(&p, *op))
                        }
                        ValueType::Scalar(None) => ValueType::Scalar(None),
                    };
                }

                Op::Any | Op::All => {
                    qualifiers.push(*op);
                    seen_qual_before_agg = true;
                    state = match state {
                        ValueType::Seq(vs) => ValueType::Seq(vs), // still temporal
                        ValueType::Scalar(Some(Prop::List(inner))) => {
                            seq_is_temporal = false;
                            ValueType::Seq(vec![Prop::List(inner)])
                        }
                        ValueType::Scalar(Some(p)) => {
                            seq_is_temporal = false;
                            ValueType::Seq(vec![p])
                        }
                        ValueType::Scalar(None) => {
                            seq_is_temporal = false;
                            ValueType::Seq(vec![])
                        }
                    };
                }
            }
        }

        match state {
            ValueType::Scalar(v) => (v, None, qualifiers, seq_is_temporal),
            ValueType::Seq(vs) => (None, Some(vs), qualifiers, seq_is_temporal),
        }
    }

    fn apply_eval(
        &self,
        reduced: Option<Prop>,
        maybe_seq: Option<Vec<Prop>>,
        qualifiers: Vec<Op>,
        seq_is_temporal: bool,
    ) -> bool {
        if let Some(value) = reduced {
            return self
                .operator
                .apply_to_property(&self.prop_value, Some(&value));
        }

        if !qualifiers.is_empty() {
            let (temporal_q_opt, elem_quals) = if seq_is_temporal {
                (Some(qualifiers[0]), &qualifiers[1..])
            } else {
                (None, &qualifiers[..])
            };
            let pred = |p: &Prop| self.operator.apply_to_property(&self.prop_value, Some(p));
            let Some(seq) = maybe_seq else { return false };

            if let Some(tq) = temporal_q_opt {
                let mut saw = false;
                match tq {
                    Op::All => {
                        for p in &seq {
                            saw = true;
                            let ok = if elem_quals.is_empty() {
                                pred(p)
                            } else {
                                self.reduce_qualifiers_rec(elem_quals, p, &pred)
                            };
                            if !ok {
                                return false;
                            }
                        }
                        return saw;
                    }
                    Op::Any => {
                        for p in &seq {
                            let ok = if elem_quals.is_empty() {
                                pred(p)
                            } else {
                                self.reduce_qualifiers_rec(elem_quals, p, &pred)
                            };
                            if ok {
                                return true;
                            }
                        }
                        return false;
                    }
                    _ => unreachable!(),
                }
            } else {
                for p in &seq {
                    let ok = if elem_quals.is_empty() {
                        pred(p)
                    } else {
                        self.reduce_qualifiers_rec(elem_quals, p, &pred)
                    };
                    if ok {
                        return true;
                    }
                }
                return false;
            }
        }

        if let Some(seq) = maybe_seq {
            let full = Prop::List(Arc::new(seq));
            self.operator
                .apply_to_property(&self.prop_value, Some(&full))
        } else {
            self.operator.apply_to_property(&self.prop_value, None)
        }
    }

    fn eval_scalar_and_apply(&self, prop: Option<Prop>) -> bool {
        let (r, s, q, is_t) = self.eval_ops(ValueType::Scalar(prop));
        self.apply_eval(r, s, q, is_t)
    }

    fn eval_temporal_and_apply(&self, props: Vec<Prop>) -> bool {
        let (r, s, q, is_t) = self.eval_ops(ValueType::Seq(props));
        self.apply_eval(r, s, q, is_t)
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
                let seq = tview.values().collect();
                self.eval_temporal_and_apply(seq)
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
        let node_view = NodeView::new_internal(graph, node.vid());

        match self.prop_ref {
            PropertyRef::Metadata(_) => {
                let props = node_view.metadata();
                self.is_metadata_matched(prop_id, props)
            }

            PropertyRef::Property(_) => {
                let props = node_view.properties();
                self.is_property_matched(prop_id, props)
            }

            PropertyRef::TemporalProperty(_) => {
                let props = node_view.properties();
                let Some(tview) = props.temporal().get_by_id(prop_id) else {
                    return false;
                };

                let seq = tview.values().collect();

                if self.ops.is_empty() {
                    return self.eval_temporal_and_apply(seq);
                }

                // If the FIRST quantifier is a temporal quantifier and it's ALL,
                // require the temporal property to be present on EVERY node update time.
                if self
                    .ops
                    .iter()
                    .copied()
                    .find(|op| matches!(op, Op::Any | Op::All))
                    == Some(Op::All)
                {
                    let semantics = graph.node_time_semantics();
                    let core_node = graph.core_node(node_view.node);

                    let node_update_count =
                        semantics.node_updates(core_node.as_ref(), graph).count();
                    let prop_time_count = seq.len();

                    // Missing at any timepoint? Leading temporal ALL must fail.
                    if prop_time_count < node_update_count {
                        return false;
                    }
                }

                self.eval_temporal_and_apply(seq)
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
            PropertyRef::TemporalProperty(_) => {
                let seq: Vec<Prop> = edge
                    .properties()
                    .temporal()
                    .get_by_id(prop_id)
                    .map(|tv| tv.values().collect())
                    .unwrap_or_default();
                self.eval_temporal_and_apply(seq)
            }
            PropertyRef::Property(_) => {
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

pub trait CombinedFilter: CreateFilter + TryAsCompositeFilter + Clone + 'static {}

impl<T: CreateFilter + TryAsCompositeFilter + Clone + 'static> CombinedFilter for T {}

pub trait InternalPropertyFilterBuilderOps: Send + Sync + Wrap {
    type Marker: Clone + Send + Sync + 'static;

    fn property_ref(&self) -> PropertyRef;

    fn ops(&self) -> &[Op];

    fn entity(&self) -> Self::Marker;
}

impl<T: InternalPropertyFilterBuilderOps> InternalPropertyFilterBuilderOps for Arc<T> {
    type Marker = T::Marker;

    fn property_ref(&self) -> PropertyRef {
        self.deref().property_ref()
    }

    fn ops(&self) -> &[Op] {
        self.deref().ops()
    }

    fn entity(&self) -> Self::Marker {
        self.deref().entity()
    }
}

pub trait PropertyFilterOps: InternalPropertyFilterBuilderOps {
    fn eq(&self, value: impl Into<Prop>) -> Self::Wrapped<PropertyFilter<Self::Marker>>;
    fn ne(&self, value: impl Into<Prop>) -> Self::Wrapped<PropertyFilter<Self::Marker>>;
    fn le(&self, value: impl Into<Prop>) -> Self::Wrapped<PropertyFilter<Self::Marker>>;
    fn ge(&self, value: impl Into<Prop>) -> Self::Wrapped<PropertyFilter<Self::Marker>>;
    fn lt(&self, value: impl Into<Prop>) -> Self::Wrapped<PropertyFilter<Self::Marker>>;
    fn gt(&self, value: impl Into<Prop>) -> Self::Wrapped<PropertyFilter<Self::Marker>>;
    fn is_in(
        &self,
        values: impl IntoIterator<Item = Prop>,
    ) -> Self::Wrapped<PropertyFilter<Self::Marker>>;
    fn is_not_in(
        &self,
        values: impl IntoIterator<Item = Prop>,
    ) -> Self::Wrapped<PropertyFilter<Self::Marker>>;
    fn is_none(&self) -> Self::Wrapped<PropertyFilter<Self::Marker>>;
    fn is_some(&self) -> Self::Wrapped<PropertyFilter<Self::Marker>>;
    fn starts_with(&self, value: impl Into<Prop>) -> Self::Wrapped<PropertyFilter<Self::Marker>>;
    fn ends_with(&self, value: impl Into<Prop>) -> Self::Wrapped<PropertyFilter<Self::Marker>>;
    fn contains(&self, value: impl Into<Prop>) -> Self::Wrapped<PropertyFilter<Self::Marker>>;
    fn not_contains(&self, value: impl Into<Prop>) -> Self::Wrapped<PropertyFilter<Self::Marker>>;
    fn fuzzy_search(
        &self,
        prop_value: impl Into<String>,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> Self::Wrapped<PropertyFilter<Self::Marker>>;
}

impl<T: ?Sized + InternalPropertyFilterBuilderOps> PropertyFilterOps for T {
    fn eq(&self, value: impl Into<Prop>) -> Self::Wrapped<PropertyFilter<Self::Marker>> {
        let filter = PropertyFilter {
            prop_ref: self.property_ref(),
            prop_value: PropertyFilterValue::Single(value.into()),
            operator: FilterOperator::Eq,
            ops: self.ops().to_vec(),
            entity: self.entity(),
        };
        self.wrap(filter)
    }

    fn ne(&self, value: impl Into<Prop>) -> Self::Wrapped<PropertyFilter<Self::Marker>> {
        let filter = PropertyFilter {
            prop_ref: self.property_ref(),
            prop_value: PropertyFilterValue::Single(value.into()),
            operator: FilterOperator::Ne,
            ops: self.ops().to_vec(),
            entity: self.entity(),
        };
        self.wrap(filter)
    }

    fn le(&self, value: impl Into<Prop>) -> Self::Wrapped<PropertyFilter<Self::Marker>> {
        let filter = PropertyFilter {
            prop_ref: self.property_ref(),
            prop_value: PropertyFilterValue::Single(value.into()),
            operator: FilterOperator::Le,
            ops: self.ops().to_vec(),
            entity: self.entity(),
        };
        self.wrap(filter)
    }

    fn ge(&self, value: impl Into<Prop>) -> Self::Wrapped<PropertyFilter<Self::Marker>> {
        let filter = PropertyFilter {
            prop_ref: self.property_ref(),
            prop_value: PropertyFilterValue::Single(value.into()),
            operator: FilterOperator::Ge,
            ops: self.ops().to_vec(),
            entity: self.entity(),
        };
        self.wrap(filter)
    }

    fn lt(&self, value: impl Into<Prop>) -> Self::Wrapped<PropertyFilter<Self::Marker>> {
        let filter = PropertyFilter {
            prop_ref: self.property_ref(),
            prop_value: PropertyFilterValue::Single(value.into()),
            operator: FilterOperator::Lt,
            ops: self.ops().to_vec(),
            entity: self.entity(),
        };
        self.wrap(filter)
    }

    fn gt(&self, value: impl Into<Prop>) -> Self::Wrapped<PropertyFilter<Self::Marker>> {
        let filter = PropertyFilter {
            prop_ref: self.property_ref(),
            prop_value: PropertyFilterValue::Single(value.into()),
            operator: FilterOperator::Gt,
            ops: self.ops().to_vec(),
            entity: self.entity(),
        };
        self.wrap(filter)
    }

    fn is_in(
        &self,
        values: impl IntoIterator<Item = Prop>,
    ) -> Self::Wrapped<PropertyFilter<Self::Marker>> {
        let filter = PropertyFilter {
            prop_ref: self.property_ref(),
            prop_value: PropertyFilterValue::Set(Arc::new(values.into_iter().collect())),
            operator: FilterOperator::IsIn,
            ops: self.ops().to_vec(),
            entity: self.entity(),
        };
        self.wrap(filter)
    }

    fn is_not_in(
        &self,
        values: impl IntoIterator<Item = Prop>,
    ) -> Self::Wrapped<PropertyFilter<Self::Marker>> {
        let filter = PropertyFilter {
            prop_ref: self.property_ref(),
            prop_value: PropertyFilterValue::Set(Arc::new(values.into_iter().collect())),
            operator: FilterOperator::IsNotIn,
            ops: self.ops().to_vec(),
            entity: self.entity(),
        };
        self.wrap(filter)
    }

    fn is_none(&self) -> Self::Wrapped<PropertyFilter<Self::Marker>> {
        let filter = PropertyFilter {
            prop_ref: self.property_ref(),
            prop_value: PropertyFilterValue::None,
            operator: FilterOperator::IsNone,
            ops: self.ops().to_vec(),
            entity: self.entity(),
        };
        self.wrap(filter)
    }

    fn is_some(&self) -> Self::Wrapped<PropertyFilter<Self::Marker>> {
        let filter = PropertyFilter {
            prop_ref: self.property_ref(),
            prop_value: PropertyFilterValue::None,
            operator: FilterOperator::IsSome,
            ops: self.ops().to_vec(),
            entity: self.entity(),
        };
        self.wrap(filter)
    }

    fn starts_with(&self, value: impl Into<Prop>) -> Self::Wrapped<PropertyFilter<Self::Marker>> {
        let filter = PropertyFilter {
            prop_ref: self.property_ref(),
            prop_value: PropertyFilterValue::Single(value.into()),
            operator: FilterOperator::StartsWith,
            ops: self.ops().to_vec(),
            entity: self.entity(),
        };
        self.wrap(filter)
    }

    fn ends_with(&self, value: impl Into<Prop>) -> Self::Wrapped<PropertyFilter<Self::Marker>> {
        let filter = PropertyFilter {
            prop_ref: self.property_ref(),
            prop_value: PropertyFilterValue::Single(value.into()),
            operator: FilterOperator::EndsWith,
            ops: self.ops().to_vec(),
            entity: self.entity(),
        };
        self.wrap(filter)
    }

    fn contains(&self, value: impl Into<Prop>) -> Self::Wrapped<PropertyFilter<Self::Marker>> {
        let filter = PropertyFilter {
            prop_ref: self.property_ref(),
            prop_value: PropertyFilterValue::Single(value.into()),
            operator: FilterOperator::Contains,
            ops: self.ops().to_vec(),
            entity: self.entity(),
        };
        self.wrap(filter)
    }

    fn not_contains(&self, value: impl Into<Prop>) -> Self::Wrapped<PropertyFilter<Self::Marker>> {
        let filter = PropertyFilter {
            prop_ref: self.property_ref(),
            prop_value: PropertyFilterValue::Single(value.into()),
            operator: FilterOperator::NotContains,
            ops: self.ops().to_vec(),
            entity: self.entity(),
        };
        self.wrap(filter)
    }

    fn fuzzy_search(
        &self,
        prop_value: impl Into<String>,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> Self::Wrapped<PropertyFilter<Self::Marker>> {
        let filter = PropertyFilter {
            prop_ref: self.property_ref(),
            prop_value: PropertyFilterValue::Single(Prop::Str(ArcStr::from(prop_value.into()))),
            operator: FilterOperator::FuzzySearch {
                levenshtein_distance,
                prefix_match,
            },
            ops: self.ops().to_vec(),
            entity: self.entity(),
        };
        self.wrap(filter)
    }
}

#[derive(Clone)]
pub struct PropertyFilterBuilder<M>(String, pub M);

impl<M> Wrap for PropertyFilterBuilder<M> {
    type Wrapped<T> = T;

    fn wrap<T>(&self, value: T) -> Self::Wrapped<T> {
        value
    }
}

impl<M> PropertyFilterBuilder<M> {
    pub fn new(prop: impl Into<String>, entity: M) -> Self {
        Self(prop.into(), entity)
    }
}

impl<M> InternalPropertyFilterBuilderOps for PropertyFilterBuilder<M>
where
    M: Send + Sync + Clone + 'static,
    PropertyFilter<M>: CombinedFilter,
{
    type Marker = M;

    fn property_ref(&self) -> PropertyRef {
        PropertyRef::Property(self.0.clone())
    }

    fn ops(&self) -> &[Op] {
        &[]
    }

    fn entity(&self) -> Self::Marker {
        self.1.clone()
    }
}

#[derive(Clone)]
pub struct MetadataFilterBuilder<M>(pub String, pub M);

impl<M> Wrap for MetadataFilterBuilder<M> {
    type Wrapped<T> = T;

    fn wrap<T>(&self, value: T) -> Self::Wrapped<T> {
        value
    }
}

impl<M> MetadataFilterBuilder<M> {
    pub fn new(prop: impl Into<String>, entity: M) -> Self {
        Self(prop.into(), entity)
    }
}

impl<M> InternalPropertyFilterBuilderOps for MetadataFilterBuilder<M>
where
    M: Send + Sync + Clone + 'static,
    PropertyFilter<M>: CombinedFilter,
{
    type Marker = M;

    fn property_ref(&self) -> PropertyRef {
        PropertyRef::Metadata(self.0.clone())
    }

    fn ops(&self) -> &[Op] {
        &[]
    }

    fn entity(&self) -> Self::Marker {
        self.1.clone()
    }
}

#[derive(Clone)]
pub struct OpChainBuilder<M> {
    pub prop_ref: PropertyRef,
    pub ops: Vec<Op>,
    pub entity: M,
}

impl<M> Wrap for OpChainBuilder<M> {
    type Wrapped<T> = T;

    fn wrap<T>(&self, value: T) -> Self::Wrapped<T> {
        value
    }
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

impl<M> InternalPropertyFilterBuilderOps for OpChainBuilder<M>
where
    M: Send + Sync + Clone + 'static,
    PropertyFilter<M>: CombinedFilter,
{
    type Marker = M;

    fn property_ref(&self) -> PropertyRef {
        self.prop_ref.clone()
    }

    fn ops(&self) -> &[Op] {
        &self.ops
    }

    fn entity(&self) -> Self::Marker {
        self.entity.clone()
    }
}

pub trait ElemQualifierOps: InternalPropertyFilterBuilderOps {
    fn any(&self) -> Self::Wrapped<OpChainBuilder<Self::Marker>>
    where
        Self: Sized,
    {
        let builder = OpChainBuilder {
            prop_ref: self.property_ref(),
            ops: self.ops().iter().copied().chain([Op::Any]).collect(),
            entity: self.entity(),
        };
        self.wrap(builder)
    }

    fn all(&self) -> Self::Wrapped<OpChainBuilder<Self::Marker>>
    where
        Self: Sized,
    {
        let builder = OpChainBuilder {
            prop_ref: self.property_ref(),
            ops: self.ops().iter().copied().chain([Op::All]).collect(),
            entity: self.entity(),
        };
        self.wrap(builder)
    }
}

impl<T: InternalPropertyFilterBuilderOps> ElemQualifierOps for T {}

impl<M> PropertyFilterBuilder<M> {
    pub fn temporal(self) -> OpChainBuilder<M> {
        OpChainBuilder {
            prop_ref: PropertyRef::TemporalProperty(self.0),
            ops: vec![],
            entity: self.1,
        }
    }
}

pub trait ListAggOps: InternalPropertyFilterBuilderOps + Sized {
    fn len(&self) -> Self::Wrapped<OpChainBuilder<Self::Marker>> {
        let builder = OpChainBuilder {
            prop_ref: self.property_ref(),
            ops: self.ops().iter().copied().chain([Op::Len]).collect(),
            entity: self.entity(),
        };
        self.wrap(builder)
    }

    fn sum(&self) -> Self::Wrapped<OpChainBuilder<Self::Marker>> {
        let builder = OpChainBuilder {
            prop_ref: self.property_ref(),
            ops: self.ops().iter().copied().chain([Op::Sum]).collect(),
            entity: self.entity(),
        };
        self.wrap(builder)
    }

    fn avg(&self) -> Self::Wrapped<OpChainBuilder<Self::Marker>> {
        let builder = OpChainBuilder {
            prop_ref: self.property_ref(),
            ops: self.ops().iter().copied().chain([Op::Avg]).collect(),
            entity: self.entity(),
        };
        self.wrap(builder)
    }

    fn min(&self) -> Self::Wrapped<OpChainBuilder<Self::Marker>> {
        let builder = OpChainBuilder {
            prop_ref: self.property_ref(),
            ops: self.ops().iter().copied().chain([Op::Min]).collect(),
            entity: self.entity(),
        };
        self.wrap(builder)
    }

    fn max(&self) -> Self::Wrapped<OpChainBuilder<Self::Marker>> {
        let builder = OpChainBuilder {
            prop_ref: self.property_ref(),
            ops: self.ops().iter().copied().chain([Op::Max]).collect(),
            entity: self.entity(),
        };
        self.wrap(builder)
    }

    fn first(&self) -> Self::Wrapped<OpChainBuilder<Self::Marker>> {
        let builder = OpChainBuilder {
            prop_ref: self.property_ref(),
            ops: self.ops().iter().copied().chain([Op::First]).collect(),
            entity: self.entity(),
        };
        self.wrap(builder)
    }

    fn last(&self) -> Self::Wrapped<OpChainBuilder<Self::Marker>> {
        let builder = OpChainBuilder {
            prop_ref: self.property_ref(),
            ops: self.ops().iter().copied().chain([Op::Last]).collect(),
            entity: self.entity(),
        };
        self.wrap(builder)
    }
}

impl<T: InternalPropertyFilterBuilderOps + Sized> ListAggOps for T {}
