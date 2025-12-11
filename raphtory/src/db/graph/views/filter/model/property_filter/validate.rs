use crate::{
    db::graph::views::filter::model::{
        property_filter::PropertyFilterValue, FilterOperator, Op, PropertyRef,
    },
    errors::GraphError,
    prelude::{EdgeViewOps, GraphViewOps, NodeViewOps, PropertiesOps, PropertyFilter},
};
use raphtory_api::core::entities::properties::prop::{unify_types, PropType};
use raphtory_storage::graph::nodes::node_storage_ops::NodeStorageOps;
use std::fmt::Display;
use Shape::{List, Quantified, Scalar};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Shape {
    Scalar(PropType),
    List(Box<PropType>),
    Seq(Box<Shape>),
    Quantified(Box<Shape>, Op),
}

pub fn flatten_quantified_depth(mut s: &Shape) -> (&Shape, usize) {
    let mut depth = 0;
    while let Shape::Quantified(inner, _) = s {
        depth += 1;
        s = inner;
    }
    (s, depth)
}

#[inline]
pub fn agg_result_dtype(inner: &PropType, op: Op, ctx: &str) -> Result<PropType, GraphError> {
    use raphtory_api::core::entities::properties::prop::PropType::*;
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

#[inline]
pub fn peel_list_n(mut t: PropType, mut n: usize) -> Result<PropType, GraphError> {
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

impl<M> PropertyFilter<M> {
    pub fn validate_single_dtype(
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
    pub fn has_aggregator(&self) -> bool {
        self.ops.iter().copied().any(Op::is_aggregator)
    }

    #[inline]
    pub fn has_elem_qualifier(&self) -> bool {
        self.ops.iter().any(|op| matches!(op, Op::Any | Op::All))
    }

    #[inline]
    pub fn is_temporal_ref(&self) -> bool {
        matches!(self.prop_ref, PropertyRef::TemporalProperty(_))
    }

    #[inline]
    pub fn has_temporal_first_qualifier(&self) -> bool {
        self.is_temporal_ref() && matches!(self.ops.first(), Some(Op::Any | Op::All))
    }

    pub fn validate_operator_against_dtype(
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

    pub fn validate_chain_and_infer_effective_dtype(
        &self,
        src_dtype: &PropType,
        is_temporal: bool,
    ) -> Result<PropType, GraphError> {
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
            Shape::Seq(Box::new(inner))
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
                    Shape::Seq(inner) => *inner,
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
                    Shape::Seq(inner) => match *inner {
                        List(_) | Quantified(_, _) => {
                            Shape::Seq(Box::new(Quantified(Box::new(*inner), op)))
                        }
                        Scalar(t) => Shape::Seq(Box::new(Quantified(Box::new(Scalar(t)), op))),
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
                    Shape::Seq(inner) => match *inner {
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
                                Shape::Seq(_) | Quantified(_, _) => unreachable!(),
                            };
                            Shape::Seq(Box::new(Quantified(Box::new(mapped_inner), qop)))
                        }
                        Shape::Seq(_) => unreachable!(),
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
                            Shape::Seq(_) | Quantified(_, _) => unreachable!(),
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
                    List(t) => peel_list_n(PropType::List(t.clone()), qdepth)?,
                    Scalar(t) => t.clone(),
                    _ => {
                        return Err(GraphError::InvalidFilter(
                            "Quantifier requires list or scalar input".into(),
                        ))
                    }
                }
            }

            // temporal case: the first quantifier is temporal → ignore one quantifier for depth
            Shape::Seq(inner) => match *inner {
                Scalar(t) => PropType::List(Box::new(t)),
                List(t) => PropType::List(Box::new(PropType::List(t))),
                Quantified(_, _) => {
                    let (base, q_depth_total) = flatten_quantified_depth(&*inner);
                    let q_depth_lists = q_depth_total.saturating_sub(1); // skip the temporal one
                    match base {
                        List(t) => peel_list_n(PropType::List(t.clone()), q_depth_lists)?,
                        Scalar(t) => t.clone(),
                        _ => {
                            return Err(GraphError::InvalidFilter(
                                "Temporal quantifier requires list or scalar elements per time"
                                    .into(),
                            ))
                        }
                    }
                }
                Shape::Seq(_) => unreachable!(),
            },
        };

        Ok(eff)
    }
}
