use crate::{db::graph::views::filter::model::Op, prelude::PropertyFilter};
use raphtory_api::core::{entities::properties::prop::Prop, utils::generalised_reduce};

enum ValueType {
    Seq(Vec<Prop>),
    Scalar(Option<Prop>),
}

pub fn aggregate_values<I: IntoIterator<Item = Prop>>(vals: I, op: Op) -> Option<Prop> {
    match op {
        Op::Len => Some(Prop::U64(vals.into_iter().count() as u64)),
        Op::Sum => generalised_reduce(vals, |a, b| a.add(b), |first| first.is_numeric()),
        Op::Avg => Prop::mean(vals),
        Op::Min => generalised_reduce(vals, |a, b| a.min(b), |_| true),
        Op::Max => generalised_reduce(vals, |a, b| a.max(b), |_| true),
        Op::First | Op::Last | Op::Any | Op::All => unreachable!(),
    }
}

pub fn apply_agg_to_prop(p: &Prop, op: Op) -> Option<Prop> {
    match (op, p) {
        (Op::Len, Prop::List(inner)) => Some(Prop::U64(inner.len() as u64)),
        (Op::Sum, Prop::List(inner))
        | (Op::Avg, Prop::List(inner))
        | (Op::Min, Prop::List(inner))
        | (Op::Max, Prop::List(inner)) => aggregate_values(inner.iter(), op),

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

impl<M> PropertyFilter<M> {
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
            let mut elems = inner.iter().peekable();
            let check = |e: &Prop| {
                if rest.is_empty() {
                    predicate(e)
                } else {
                    self.reduce_qualifiers_rec(rest, e, predicate)
                }
            };
            return match q {
                Op::Any => elems.any(|p| check(&p)),
                Op::All => elems.peek().is_some() && elems.all(|p| check(&p)),
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

    fn eval_ops(&self, mut state: ValueType) -> (Option<Prop>, Option<Vec<Prop>>, Vec<Op>, bool) {
        let mut qualifiers: Vec<Op> = Vec::new();
        let mut seq_is_temporal = matches!(state, ValueType::Seq(..));
        let mut seen_qual_before_agg = false;

        let per_step_map = |vals: Vec<Prop>, op: Op| -> Vec<Prop> {
            vals.into_iter()
                .filter_map(|p| apply_agg_to_prop(&p, op))
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
                    aggregate_values(vs, op)
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
                            ValueType::Scalar(aggregate_values(inner.iter(), *op))
                        }
                        ValueType::Scalar(Some(p)) => ValueType::Scalar(apply_agg_to_prop(&p, *op)),
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
            let full = Prop::List(seq.into());
            self.operator
                .apply_to_property(&self.prop_value, Some(&full))
        } else {
            self.operator.apply_to_property(&self.prop_value, None)
        }
    }

    pub fn eval_scalar_and_apply(&self, prop: Option<Prop>) -> bool {
        let (r, s, q, is_t) = self.eval_ops(ValueType::Scalar(prop));
        self.apply_eval(r, s, q, is_t)
    }

    pub fn eval_temporal_and_apply(&self, props: Vec<Prop>) -> bool {
        let (r, s, q, is_t) = self.eval_ops(ValueType::Seq(props));
        self.apply_eval(r, s, q, is_t)
    }
}
