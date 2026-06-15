//! Runtime edge evaluators — given an EdgeRef, return a typed value.
//!
//! Parallel to `node_expr/ops.rs` — same design, different subject.

use crate::db::{
    api::{
        properties::internal::{InternalMetadataOps, InternalTemporalPropertyViewOps},
        state::ops::Const,
        view::internal::GraphView,
    },
    graph::{
        edge::EdgeView,
        views::filter::model::property_filter::evaluate::aggregate_values,
    },
};
use raphtory_api::core::entities::{
    edges::edge_ref::EdgeRef,
    properties::prop::{Prop, PropType},
};
use raphtory_storage::graph::graph::GraphStorage;

use super::EdgeOp;
use std::sync::Arc;

// ─────────────────────────────────────────────────────────────────────────────
// Arc<dyn EdgeOp> — blanket impl so Arc-boxed ops satisfy EdgeOp
// ─────────────────────────────────────────────────────────────────────────────

impl<'a, V: Clone + Send + Sync> EdgeOp for Arc<dyn EdgeOp<Output = V> + 'a> {
    type Output = V;

    fn apply(&self, storage: &GraphStorage, edge: EdgeRef) -> V {
        self.as_ref().apply(storage, edge)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Const<V> — constant literal op (RHS in comparisons)
// ─────────────────────────────────────────────────────────────────────────────

impl<V: Clone + Send + Sync + 'static> EdgeOp for Const<V> {
    type Output = V;

    fn apply(&self, _storage: &GraphStorage, _edge: EdgeRef) -> V {
        self.0.clone()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// EdgePropOp<G> — latest temporal property value by pre-resolved column ID
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub(crate) struct EdgePropOp<G> {
    pub(crate) graph: G,
    pub(crate) prop_id: usize,
}

impl<G: GraphView> EdgeOp for EdgePropOp<G> {
    type Output = Option<Prop>;

    fn apply(&self, _storage: &GraphStorage, edge: EdgeRef) -> Option<Prop> {
        EdgeView::new(&self.graph, edge).temporal_value(self.prop_id)
    }

    fn prop_type(&self) -> PropType {
        self.graph
            .edge_meta()
            .temporal_prop_mapper()
            .get_dtype(self.prop_id)
            .unwrap_or_default()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// EdgeMetaOp<G> — static metadata field by pre-resolved column ID
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub(crate) struct EdgeMetaOp<G> {
    pub(crate) graph: G,
    pub(crate) prop_id: usize,
}

impl<G: GraphView> EdgeOp for EdgeMetaOp<G> {
    type Output = Option<Prop>;

    fn apply(&self, _storage: &GraphStorage, edge: EdgeRef) -> Option<Prop> {
        EdgeView::new(&self.graph, edge).get_metadata(self.prop_id)
    }

    fn prop_type(&self) -> PropType {
        self.graph
            .edge_meta()
            .metadata_mapper()
            .get_dtype(self.prop_id)
            .unwrap_or_default()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// TemporalEdgePropOp<G> — all temporal values for a property in the view window
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub(crate) struct TemporalEdgePropOp<G> {
    pub(crate) graph: G,
    pub(crate) prop_id: usize,
}

impl<G: GraphView> EdgeOp for TemporalEdgePropOp<G> {
    type Output = Prop;

    fn apply(&self, _storage: &GraphStorage, edge: EdgeRef) -> Prop {
        let vals: Vec<Prop> = EdgeView::new(&self.graph, edge)
            .temporal_iter(self.prop_id)
            .map(|(_, v)| v)
            .collect();
        Prop::List(raphtory_api::core::entities::properties::prop::PropArray::from(vals))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// BinaryCmpEdgeOp<'g> — compares two EdgeOp outputs, returns bool
// ─────────────────────────────────────────────────────────────────────────────

use crate::db::graph::views::filter::model::filter_operator::{BinaryOp, Comparable};

#[derive(Clone)]
pub(crate) struct BinaryCmpEdgeOp<'g, L> {
    pub(crate) left: Arc<dyn EdgeOp<Output = L> + 'g>,
    pub(crate) right: Arc<dyn EdgeOp<Output = L> + 'g>,
    pub(crate) op: BinaryOp,
}

impl<'g, L: Comparable + Clone + Send + Sync + 'static> EdgeOp for BinaryCmpEdgeOp<'g, L> {
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, edge: EdgeRef) -> bool {
        let l = self.left.apply(storage, edge);
        let r = self.right.apply(storage, edge);
        L::binary_cmp(&self.op, &l, &r)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// UnaryEdgeOp<'g, I> — is_some / is_none on Option<I>-valued expressions
// ─────────────────────────────────────────────────────────────────────────────

use crate::db::graph::views::filter::model::filter_operator::UnaryOp;

#[derive(Clone)]
pub(crate) struct UnaryEdgeOp<'g, I: Clone + Send + Sync + 'static> {
    pub(crate) inner: Arc<dyn EdgeOp<Output = Option<I>> + 'g>,
    pub(crate) op: UnaryOp,
}

impl<'g, I: Clone + Send + Sync + 'static> EdgeOp for UnaryEdgeOp<'g, I> {
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, edge: EdgeRef) -> bool {
        let v = self.inner.apply(storage, edge);
        match self.op {
            UnaryOp::IsSome => v.is_some(),
            UnaryOp::IsNone => v.is_none(),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// AnyEdgeOp<'g> / AllEdgeOp<'g> — quantifier ops over Prop::List
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub(crate) struct AnyEdgeOp<'g> {
    pub(crate) inner: Arc<dyn EdgeOp<Output = bool> + 'g>,
}

impl<'g> EdgeOp for AnyEdgeOp<'g> {
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, edge: EdgeRef) -> bool {
        self.inner.apply(storage, edge)
    }
}

#[derive(Clone)]
pub(crate) struct AllEdgeOp<'g> {
    pub(crate) inner: Arc<dyn EdgeOp<Output = bool> + 'g>,
}

impl<'g> EdgeOp for AllEdgeOp<'g> {
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, edge: EdgeRef) -> bool {
        self.inner.apply(storage, edge)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// PropListEdgeCmpOp<'g> — compares each element of a Prop::List against a RHS
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub(crate) struct PropListEdgeCmpOp<'g> {
    pub(crate) temporal_op: Arc<dyn EdgeOp<Output = Prop> + 'g>,
    pub(crate) rhs: Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>,
    pub(crate) cmp_op: BinaryOp,
    pub(crate) any: bool,
}

impl<'g> EdgeOp for PropListEdgeCmpOp<'g> {
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, edge: EdgeRef) -> bool {
        let list_prop = self.temporal_op.apply(storage, edge);
        let rhs_val = self.rhs.apply(storage, edge);
        let vals = match list_prop {
            Prop::List(v) => v,
            _ => return false,
        };
        let mut results = vals.iter().map(|v| {
            rhs_val
                .as_ref()
                .map(|r| Prop::binary_cmp(&self.cmp_op, &v, r))
                .unwrap_or(false)
        });
        if self.any {
            results.any(|b| b)
        } else {
            results.all(|b| b)
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Aggregator EdgeOps — reduce a Prop::List to a scalar Option<Prop>
// ─────────────────────────────────────────────────────────────────────────────

macro_rules! impl_agg_edge_op {
    ($name:ident, |$vals:ident: Vec<Prop>| $body:expr) => {
        #[derive(Clone)]
        pub(crate) struct $name<'g> {
            pub(crate) inner: Arc<dyn EdgeOp<Output = Prop> + 'g>,
        }

        impl<'g> EdgeOp for $name<'g> {
            type Output = Option<Prop>;

            fn apply(&self, storage: &GraphStorage, edge: EdgeRef) -> Option<Prop> {
                let list_prop = self.inner.apply(storage, edge);
                let $vals: Vec<Prop> = match list_prop {
                    Prop::List(v) => v.iter().collect(),
                    _ => return None,
                };
                if $vals.is_empty() {
                    return None;
                }
                $body
            }
        }
    };
}

use crate::db::graph::views::filter::model::{
    filter_operator::{SetOp, StringComparable, StringOp},
    property_filter::Op,
};
use raphtory_api::core::{
    entities::properties::prop::PropArray,
    storage::arc_str::ArcStr,
};
use std::collections::HashSet;
use std::hash::Hash;

impl_agg_edge_op!(SumEdgeOp, |vals: Vec<Prop>| aggregate_values(&vals, Op::Sum));
impl_agg_edge_op!(AvgEdgeOp, |vals: Vec<Prop>| aggregate_values(&vals, Op::Avg));
impl_agg_edge_op!(MinEdgeOp, |vals: Vec<Prop>| aggregate_values(&vals, Op::Min));
impl_agg_edge_op!(MaxEdgeOp, |vals: Vec<Prop>| aggregate_values(&vals, Op::Max));
impl_agg_edge_op!(FirstEdgeOp, |vals: Vec<Prop>| vals.into_iter().next());
impl_agg_edge_op!(LastEdgeOp, |vals: Vec<Prop>| vals.into_iter().last());
// LenEdgeOp written explicitly: Output = usize, not Option<Prop>
#[derive(Clone)]
pub(crate) struct LenEdgeOp<'g> {
    pub(crate) inner: Arc<dyn EdgeOp<Output = Prop> + 'g>,
}

impl<'g> EdgeOp for LenEdgeOp<'g> {
    type Output = usize;

    fn apply(&self, storage: &GraphStorage, edge: EdgeRef) -> usize {
        match self.inner.apply(storage, edge) {
            Prop::List(v) => v.iter().count(),
            _ => 0,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// StringEdgeOp<'g, T> — applies a StringOp to two EdgeOp<Output = T> values
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub(crate) struct StringEdgeOp<'g, T: StringComparable> {
    pub(crate) left: Arc<dyn EdgeOp<Output = T> + 'g>,
    pub(crate) right: Arc<dyn EdgeOp<Output = T> + 'g>,
    pub(crate) op: StringOp,
}

impl<'g, T: StringComparable> EdgeOp for StringEdgeOp<'g, T> {
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, edge: EdgeRef) -> bool {
        T::string_cmp(
            &self.op,
            &self.left.apply(storage, edge),
            &self.right.apply(storage, edge),
        )
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// PropValueSetEdgeOp<'g> — is_in / is_not_in for Option<Prop> (linear scan)
// ─────────────────────────────────────────────────────────────────────────────

/// Checks whether an `Option<Prop>` value is in (or not in) a fixed `Vec<Prop>`.
/// Uses linear scan because `Prop` may contain floats that don't implement `Hash`.
#[derive(Clone)]
pub(crate) struct PropValueSetEdgeOp<'g> {
    pub(crate) inner: Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>,
    pub(crate) values: Vec<Prop>,
    pub(crate) op: SetOp,
}

impl<'g> EdgeOp for PropValueSetEdgeOp<'g> {
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, edge: EdgeRef) -> bool {
        match self.inner.apply(storage, edge) {
            None => false,
            Some(v) => match self.op {
                SetOp::IsIn => self.values.iter().any(|x| x == &v),
                SetOp::IsNotIn => self.values.iter().all(|x| x != &v),
            },
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SetEdgeOp<'g, I> — is_in / is_not_in for Option<I> (HashSet, O(1))
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub(crate) struct SetEdgeOp<'g, I: Eq + Hash + Clone + Send + Sync + 'static> {
    pub(crate) inner: Arc<dyn EdgeOp<Output = Option<I>> + 'g>,
    pub(crate) values: Arc<HashSet<I>>,
    pub(crate) op: SetOp,
}

impl<'g, I: Eq + Hash + Clone + Send + Sync + 'static> EdgeOp for SetEdgeOp<'g, I> {
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, edge: EdgeRef) -> bool {
        match self.inner.apply(storage, edge) {
            None => false,
            Some(v) => match self.op {
                SetOp::IsIn => self.values.contains(&v),
                SetOp::IsNotIn => !self.values.contains(&v),
            },
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// PropListInSetEdgeOp<'g> — element-wise set-membership test on a Prop::List
// ─────────────────────────────────────────────────────────────────────────────

/// Checks each element of a `Prop::List` against a fixed `Vec<Prop>`, producing
/// `Prop::List([Bool, …])`.  The result is then reduced by `AnyEdgeOp` or `AllEdgeOp`.
#[derive(Clone)]
pub(crate) struct PropListInSetEdgeOp<'g> {
    pub(crate) inner: Arc<dyn EdgeOp<Output = Prop> + 'g>,
    pub(crate) values: Vec<Prop>,
    pub(crate) op: SetOp,
}

impl<'g> EdgeOp for PropListInSetEdgeOp<'g> {
    type Output = Prop;

    fn apply(&self, storage: &GraphStorage, edge: EdgeRef) -> Prop {
        let Prop::List(arr) = self.inner.apply(storage, edge) else {
            return Prop::List(PropArray::from(vec![]));
        };
        let bools: Vec<Prop> = arr
            .iter()
            .map(|v| {
                Prop::Bool(match self.op {
                    SetOp::IsIn => self.values.iter().any(|x| x == &v),
                    SetOp::IsNotIn => self.values.iter().all(|x| x != &v),
                })
            })
            .collect();
        Prop::List(PropArray::from(bools))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// PropListStringEdgeOp<'g> — element-wise string comparison on a Prop::List
// ─────────────────────────────────────────────────────────────────────────────

/// Applies a `StringOp` to each element of a `Prop::List` against a scalar RHS,
/// producing `Prop::List([Bool, …])`.  Reduced by `AnyEdgeOp` or `AllEdgeOp`.
#[derive(Clone)]
pub(crate) struct PropListStringEdgeOp<'g> {
    pub(crate) inner: Arc<dyn EdgeOp<Output = Prop> + 'g>,
    pub(crate) rhs: ArcStr,
    pub(crate) op: StringOp,
}

impl<'g> EdgeOp for PropListStringEdgeOp<'g> {
    type Output = Prop;

    fn apply(&self, storage: &GraphStorage, edge: EdgeRef) -> Prop {
        let Prop::List(arr) = self.inner.apply(storage, edge) else {
            return Prop::List(PropArray::from(vec![]));
        };
        let rhs = Some(Prop::Str(self.rhs.clone()));
        let bools: Vec<Prop> = arr
            .iter()
            .map(|v| Prop::Bool(Option::<Prop>::string_cmp(&self.op, &Some(v), &rhs)))
            .collect();
        Prop::List(PropArray::from(bools))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// AnyPropEdgeOp / AllPropEdgeOp — reduce a Prop::List([Bool, …]) to bool
// ─────────────────────────────────────────────────────────────────────────────

fn prop_any_edge(prop: &Prop) -> bool {
    match prop {
        Prop::Bool(b) => *b,
        Prop::List(arr) => arr.iter().any(|p| prop_any_edge(&p)),
        _ => false,
    }
}

fn prop_all_edge(prop: &Prop) -> bool {
    match prop {
        Prop::Bool(b) => *b,
        Prop::List(arr) => !arr.is_empty() && arr.iter().all(|p| prop_all_edge(&p)),
        _ => false,
    }
}

/// Wraps a `PropListInSetEdgeOp` or `PropListStringEdgeOp` and returns `true` if
/// at least one element of the resulting `Prop::List([Bool, …])` is `true`.
#[derive(Clone)]
pub(crate) struct AnyPropEdgeOp<'g> {
    pub(crate) inner: Arc<dyn EdgeOp<Output = Prop> + 'g>,
}

impl<'g> EdgeOp for AnyPropEdgeOp<'g> {
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, edge: EdgeRef) -> bool {
        prop_any_edge(&self.inner.apply(storage, edge))
    }
}

/// Wraps a `PropListInSetEdgeOp` or `PropListStringEdgeOp` and returns `true` only
/// if every element of the resulting `Prop::List([Bool, …])` is `true` (and the
/// list is non-empty).
#[derive(Clone)]
pub(crate) struct AllPropEdgeOp<'g> {
    pub(crate) inner: Arc<dyn EdgeOp<Output = Prop> + 'g>,
}

impl<'g> EdgeOp for AllPropEdgeOp<'g> {
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, edge: EdgeRef) -> bool {
        prop_all_edge(&self.inner.apply(storage, edge))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// UnwrapOptPropEdgeOp<'g> — converts Option<Prop> → Prop for nested aggregation
// ─────────────────────────────────────────────────────────────────────────────

/// Converts `Option<Prop>` → `Prop` so that aggregator ops can operate on a value
/// produced by a prior aggregation step.
///
/// - `Some(Prop::List(arr))` → `Prop::List(arr)` (pass through)
/// - `Some(v)`               → `Prop::List([v])` (single-element list)
/// - `None`                  → `Prop::List([])` (empty — yields None from next aggregator)
#[derive(Clone)]
pub(crate) struct UnwrapOptPropEdgeOp<'g> {
    pub(crate) inner: Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>,
}

impl<'g> EdgeOp for UnwrapOptPropEdgeOp<'g> {
    type Output = Prop;

    fn apply(&self, storage: &GraphStorage, edge: EdgeRef) -> Prop {
        match self.inner.apply(storage, edge) {
            Some(Prop::List(arr)) => Prop::List(arr),
            Some(v) => Prop::List(PropArray::from(vec![v])),
            None => Prop::List(PropArray::from(vec![])),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// NestedMapEdgeOp<'g> — element-wise aggregation / quantification on a Prop::List
// ─────────────────────────────────────────────────────────────────────────────

/// Applies a per-element operation to each element of an outer `Prop::List`.
///
/// Used for chained expressions like `.temporal().any().sum()`:
/// the outer list is `Prop::List([list_t1, list_t2, …])` and for each inner
/// `list_ti` the op is applied, producing `Prop::List([result_t1, result_t2, …])`.
/// The outer `AnyPropEdgeOp` / `AllPropEdgeOp` then reduces the result list.
#[derive(Clone)]
pub(crate) struct NestedMapEdgeOp<'g> {
    pub(crate) inner: Arc<dyn EdgeOp<Output = Prop> + 'g>,
    pub(crate) op: Op,
}

impl<'g> EdgeOp for NestedMapEdgeOp<'g> {
    type Output = Prop;

    fn apply(&self, storage: &GraphStorage, edge: EdgeRef) -> Prop {
        let outer = self.inner.apply(storage, edge);
        let Prop::List(arr) = outer else {
            return Prop::List(PropArray::from(vec![]));
        };
        let mapped: Vec<Prop> = arr
            .iter()
            .map(|elem| match elem {
                Prop::List(inner_arr) => {
                    let vals: Vec<Prop> = inner_arr.iter().collect();
                    match self.op {
                        Op::Sum => aggregate_values(&vals, Op::Sum)
                            .unwrap_or(Prop::List(PropArray::from(vec![]))),
                        Op::Avg => aggregate_values(&vals, Op::Avg)
                            .unwrap_or(Prop::List(PropArray::from(vec![]))),
                        Op::Min => aggregate_values(&vals, Op::Min)
                            .unwrap_or(Prop::List(PropArray::from(vec![]))),
                        Op::Max => aggregate_values(&vals, Op::Max)
                            .unwrap_or(Prop::List(PropArray::from(vec![]))),
                        Op::First => vals.into_iter().next()
                            .unwrap_or(Prop::List(PropArray::from(vec![]))),
                        Op::Last => vals.into_iter().last()
                            .unwrap_or(Prop::List(PropArray::from(vec![]))),
                        Op::Len => Prop::U64(inner_arr.len() as u64),
                        Op::Any => Prop::Bool(prop_any_edge(&Prop::List(inner_arr))),
                        Op::All => Prop::Bool(prop_all_edge(&Prop::List(inner_arr))),
                    }
                }
                other => other,
            })
            .collect();
        Prop::List(PropArray::from(mapped))
    }
}
