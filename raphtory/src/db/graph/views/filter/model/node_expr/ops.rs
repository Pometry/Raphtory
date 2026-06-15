//! Runtime evaluators — given a node ID, return a typed value.
//!
//! A [`NodeOp`] is a *compiled* expression: name→ID lookups are resolved and the
//! op holds a reference to the graph view it was compiled against.
//! `apply(storage, vid)` returns the value in O(1).
//!
//! Ops are produced by [`NodeExpr::create_node_op`] — never constructed directly.
//!
//! # Evaluation pipeline
//!
//! ```text
//! NodeFilter::property("age")          ← NodeExpr (pure data)
//!   .create_node_op(graph)?            ← resolve "age" → prop_id = 3
//!  ──► NodePropOp { graph, prop_id: 3 }  ← NodeOp: apply() reads column 3 in O(1)
//!
//! NodeFilter::property("age").gt(30i64)   ← BinaryCmpNodeFilter (pure data)
//!   .create_node_filter(graph)?
//!  ──► BinaryCmpNodeOp { left: NodePropOp, right: ConstNodeOp(30), op: Gt }
//!        apply: Prop::binary_cmp(Gt, age_value, 30)
//!
//! NodeFilter::temporal_property("score").sum()  ← SumExpr (pure data)
//!   .create_node_op(graph)?
//!  ──► SumNodeOp { inner: TemporalNodePropOp { graph, prop_id: 7 } }
//!        apply: collect Prop::List temporal values, then aggregate_values(Sum)
//! ```
//!
//! # Quantified evaluation
//!
//! [`PropListCompareOp`] applies a [`BinaryOp`] element-wise to a `Prop::List`,
//! then [`AnyNodeOp`] / [`AllNodeOp`] reduce the boolean list:
//!
//! ```text
//! temporal values = [8, 12, 5],  rhs = 10
//! PropListCompareOp(Gt) → Prop::List([false, true, false])
//! AnyNodeOp             → true    (at least one matched)
//! AllNodeOp             → false   (not all matched)
//! ```

use crate::{
    db::{
        api::{
            properties::PropertiesOps,
            state::ops::NodeOp,
            view::{internal::GraphView, NodeViewOps},
        },
        graph::views::filter::model::{
            filter_operator::{BinaryOp, Comparable, SetOp, StringComparable, StringOp, UnaryOp},
            property_filter::{evaluate::aggregate_values, Op},
        },
    },
    prelude::GraphViewOps,
};
use raphtory_api::core::{
    entities::{
        properties::prop::{Prop, PropArray, PropType},
        VID,
    },
    storage::arc_str::ArcStr,
};
use raphtory_storage::graph::graph::GraphStorage;
use std::{collections::HashSet, hash::Hash, sync::Arc};

// ─────────────────────────────────────────────────────────────────────────────
// NodePropOp<G> — latest property value by pre-resolved column ID
// ─────────────────────────────────────────────────────────────────────────────

/// Internal op produced by [`Property::create_node_op`] — not constructed directly.
///
/// `Property("age")` resolves `"age"` → `prop_id` once at compile time;
/// every `apply` call then reads column `prop_id` in O(1).
#[derive(Clone)]
pub(crate) struct NodePropOp<G> {
    pub(crate) graph: G,
    pub(crate) prop_id: usize,
}

impl<G: GraphView> NodeOp for NodePropOp<G> {
    type Output = Option<Prop>;

    fn apply(&self, _storage: &GraphStorage, node: VID) -> Option<Prop> {
        self.graph.node(node)?.properties().get_by_id(self.prop_id)
    }

    fn prop_type(&self) -> PropType {
        self.graph
            .node_meta()
            .temporal_prop_mapper()
            .get_dtype(self.prop_id)
            .unwrap_or_default()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// NodeMetaOp<G> — static metadata field by pre-resolved column ID
// ─────────────────────────────────────────────────────────────────────────────

/// Internal op produced by [`Metadata::create_node_op`] — not constructed directly.
///
/// Same as [`NodePropOp`] but reads from the static metadata column instead of
/// temporal properties.
#[derive(Clone)]
pub(crate) struct NodeMetaOp<G> {
    pub(crate) graph: G,
    pub(crate) prop_id: usize,
}

impl<G: GraphView> NodeOp for NodeMetaOp<G> {
    type Output = Option<Prop>;

    fn apply(&self, _storage: &GraphStorage, node: VID) -> Option<Prop> {
        self.graph.node(node)?.metadata().get_by_id(self.prop_id)
    }

    fn prop_type(&self) -> PropType {
        self.graph
            .node_meta()
            .metadata_mapper()
            .get_dtype(self.prop_id)
            .unwrap_or_default()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// TemporalNodePropOp<G> — all temporal values for a property within the window
// ─────────────────────────────────────────────────────────────────────────────

/// Internal op produced by [`TemporalPropertyExpr::create_node_op`] — not constructed directly.
///
/// Collects all recorded values within the current view window into a `Prop::List`.
/// That list is then consumed by aggregator ops (`SumNodeOp`, `LenNodeOp`, …) or
/// by `PropListCompareOp` for quantified comparisons.
#[derive(Clone)]
pub(crate) struct TemporalNodePropOp<G> {
    pub(crate) graph: G,
    pub(crate) prop_id: usize,
}

impl<G: GraphView> NodeOp for TemporalNodePropOp<G> {
    type Output = Prop;

    fn apply(&self, _storage: &GraphStorage, node: VID) -> Prop {
        let vals: Vec<Prop> = (&&self.graph)
            .node(node)
            .and_then(|n| {
                n.properties()
                    .temporal()
                    .get_by_id(self.prop_id)
                    .map(|tpv| tpv.values().collect())
            })
            .unwrap_or_default();
        Prop::List(PropArray::from(vals))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Aggregator NodeOps — compile-time resolved against a concrete graph view
//
// Each is an internal op produced by its corresponding expr's create_node_op:
//   SumExpr::create_node_op   → SumNodeOp    (Output = Option<Prop>)
//   AvgExpr::create_node_op   → AvgNodeOp    (Output = Option<Prop>)
//   MinExpr::create_node_op   → MinNodeOp    (Output = Option<Prop>)
//   MaxExpr::create_node_op   → MaxNodeOp    (Output = Option<Prop>)
//   FirstExpr::create_node_op → FirstNodeOp  (Output = Option<Prop>)
//   LastExpr::create_node_op  → LastNodeOp   (Output = Option<Prop>)
//   LenExpr::create_node_op   → LenNodeOp    (Output = usize)
// ─────────────────────────────────────────────────────────────────────────────

macro_rules! impl_agg_node_op {
    ($name:ident, $output:ty, $body:expr) => {
        pub struct $name<'g> {
            pub(crate) inner: Arc<dyn NodeOp<Output = Prop> + 'g>,
        }

        impl<'g> Clone for $name<'g> {
            fn clone(&self) -> Self {
                Self {
                    inner: self.inner.clone(),
                }
            }
        }

        impl<'g> NodeOp for $name<'g> {
            type Output = $output;

            fn apply(&self, storage: &GraphStorage, node: VID) -> $output {
                let vals: Vec<Prop> = match self.inner.apply(storage, node) {
                    Prop::List(arr) => arr.iter().collect(),
                    _ => vec![],
                };
                ($body)(vals)
            }
        }
    };
}

impl_agg_node_op!(SumNodeOp, Option<Prop>, |vals: Vec<Prop>| {
    aggregate_values(&vals, Op::Sum)
});
impl_agg_node_op!(AvgNodeOp, Option<Prop>, |vals: Vec<Prop>| {
    aggregate_values(&vals, Op::Avg)
});
impl_agg_node_op!(MinNodeOp, Option<Prop>, |vals: Vec<Prop>| {
    aggregate_values(&vals, Op::Min)
});
impl_agg_node_op!(MaxNodeOp, Option<Prop>, |vals: Vec<Prop>| {
    aggregate_values(&vals, Op::Max)
});
impl_agg_node_op!(FirstNodeOp, Option<Prop>, |vals: Vec<Prop>| {
    vals.into_iter().next()
});
impl_agg_node_op!(LastNodeOp, Option<Prop>, |vals: Vec<Prop>| {
    vals.into_iter().last()
});
impl_agg_node_op!(LenNodeOp, usize, |vals: Vec<Prop>| vals.len());

// ─────────────────────────────────────────────────────────────────────────────
// AnyNodeOp / AllNodeOp — unary reducers over a Prop::List of booleans
// ─────────────────────────────────────────────────────────────────────────────

fn prop_any(prop: &Prop) -> bool {
    match prop {
        Prop::Bool(b) => *b,
        Prop::List(arr) => arr.iter().any(|p| prop_any(&p)),
        _ => false,
    }
}

fn prop_all(prop: &Prop) -> bool {
    match prop {
        Prop::Bool(b) => *b,
        Prop::List(arr) => !arr.is_empty() && arr.iter().all(|p| prop_all(&p)),
        _ => false,
    }
}

/// Internal op produced by `QuantifiedNodeFilter<_, AnyMode, _>::create_node_filter`.
///
/// Wraps a `PropListCompareOp` and returns `true` if at least one element of the
/// resulting `Prop::List([Bool, …])` is `true`.
///
/// e.g. `NodeFilter::temporal_property("score").any().gt(10i64)` ultimately compiles
/// to `AnyNodeOp { inner: PropListCompareOp { …, op: Gt } }`.
pub struct AnyNodeOp<'g> {
    pub(crate) inner: Arc<dyn NodeOp<Output = Prop> + 'g>,
}

impl<'g> Clone for AnyNodeOp<'g> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<'g> NodeOp for AnyNodeOp<'g> {
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, node: VID) -> bool {
        prop_any(&self.inner.apply(storage, node))
    }
}

/// Internal op produced by `QuantifiedNodeFilter<_, AllMode, _>::create_node_filter`.
///
/// Like [`AnyNodeOp`] but returns `true` only if every element is `true`
/// (and the list is non-empty).
pub struct AllNodeOp<'g> {
    pub(crate) inner: Arc<dyn NodeOp<Output = Prop> + 'g>,
}

impl<'g> Clone for AllNodeOp<'g> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<'g> NodeOp for AllNodeOp<'g> {
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, node: VID) -> bool {
        prop_all(&self.inner.apply(storage, node))
    }
}

/// Internal op produced inside `QuantifiedNodeFilter::create_node_filter`.
///
/// Applies `BinaryOp` element-wise to a `Prop::List` (from `TemporalNodePropOp`)
/// against a scalar RHS, producing `Prop::List([Bool, Bool, …])`.
/// That boolean list is then reduced by [`AnyNodeOp`] or [`AllNodeOp`].
pub(crate) struct PropListCompareOp<'g> {
    pub(crate) inner: Arc<dyn NodeOp<Output = Prop> + 'g>,
    pub(crate) rhs: Arc<dyn NodeOp<Output = Option<Prop>> + 'g>,
    pub(crate) op: BinaryOp,
}

impl<'g> Clone for PropListCompareOp<'g> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            rhs: self.rhs.clone(),
            op: self.op,
        }
    }
}

impl<'g> NodeOp for PropListCompareOp<'g> {
    type Output = Prop;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Prop {
        let Some(rhs) = self.rhs.apply(storage, node) else {
            return Prop::List(PropArray::from(vec![]));
        };
        let prop = self.inner.apply(storage, node);
        match prop {
            Prop::List(arr) => {
                let bools: Vec<Prop> = arr
                    .iter()
                    .map(|v| Prop::Bool(Prop::binary_cmp(&self.op, &v, &rhs)))
                    .collect();
                Prop::List(PropArray::from(bools))
            }
            other => Prop::Bool(Prop::binary_cmp(&self.op, &other, &rhs)),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// PropListInSetOp<'g> — element-wise set-membership test on a Prop::List
// ─────────────────────────────────────────────────────────────────────────────

/// Checks each element of a `Prop::List` against a fixed `Vec<Prop>`, producing
/// `Prop::List([Bool, …])`.  The result is then reduced by [`AnyNodeOp`] or [`AllNodeOp`].
pub(crate) struct PropListInSetOp<'g> {
    pub(crate) inner: Arc<dyn NodeOp<Output = Prop> + 'g>,
    pub(crate) values: Vec<Prop>,
    pub(crate) op: SetOp,
}

impl<'g> Clone for PropListInSetOp<'g> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            values: self.values.clone(),
            op: self.op,
        }
    }
}

impl<'g> NodeOp for PropListInSetOp<'g> {
    type Output = Prop;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Prop {
        let Prop::List(arr) = self.inner.apply(storage, node) else {
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
// PropListStringOp<'g> — element-wise string comparison on a Prop::List
// ─────────────────────────────────────────────────────────────────────────────

/// Applies a [`StringOp`] to each element of a `Prop::List` against a scalar RHS,
/// producing `Prop::List([Bool, …])`.  Reduced by [`AnyNodeOp`] or [`AllNodeOp`].
pub(crate) struct PropListStringOp<'g> {
    pub(crate) inner: Arc<dyn NodeOp<Output = Prop> + 'g>,
    pub(crate) rhs: ArcStr,
    pub(crate) op: StringOp,
}

impl<'g> Clone for PropListStringOp<'g> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            rhs: self.rhs.clone(),
            op: self.op,
        }
    }
}

impl<'g> NodeOp for PropListStringOp<'g> {
    type Output = Prop;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Prop {
        let Prop::List(arr) = self.inner.apply(storage, node) else {
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
// NestedMapNodeOp<'g> — element-wise aggregation / quantification on a Prop::List
// ─────────────────────────────────────────────────────────────────────────────

/// Applies a per-element operation to each element of an outer `Prop::List`.
///
/// Used for chained expressions like `.temporal().any().sum()`:
/// the outer list is `Prop::List([list_t1, list_t2, …])` and for each inner
/// `list_ti` the op is applied, producing `Prop::List([result_t1, result_t2, …])`.
/// The outer `AnyNodeOp` / `AllNodeOp` then reduces the result list.
///
/// Scalar elements are passed through unchanged.
pub(crate) struct NestedMapNodeOp<'g> {
    pub(crate) inner: Arc<dyn NodeOp<Output = Prop> + 'g>,
    pub(crate) op: Op,
}

impl<'g> Clone for NestedMapNodeOp<'g> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            op: self.op,
        }
    }
}

impl<'g> NodeOp for NestedMapNodeOp<'g> {
    type Output = Prop;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Prop {
        let outer = self.inner.apply(storage, node);
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
                        Op::Any => Prop::Bool(prop_any(&Prop::List(inner_arr))),
                        Op::All => Prop::Bool(prop_all(&Prop::List(inner_arr))),
                    }
                }
                other => other,
            })
            .collect();
        Prop::List(PropArray::from(mapped))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// UnwrapOptPropOp<'g> — converts Option<Prop> → Prop for nested aggregation
// ─────────────────────────────────────────────────────────────────────────────

/// Converts `Option<Prop>` → `Prop` so that aggregator ops (`SumNodeOp`, etc.)
/// can operate on a value produced by a prior aggregation step.
///
/// Used internally when chaining e.g. `.temporal().last().sum()`:
/// `LastExpr` outputs `Option<Prop::List([...]))`, `UnwrapOptPropOp` makes that
/// available as `Prop` for the next-level `SumNodeOp`.
///
/// - `Some(Prop::List(arr))` → `Prop::List(arr)` (pass through as-is)
/// - `Some(v)`               → `Prop::List([v])` (single-element list)
/// - `None`                  → `Prop::List([])` (empty — yields None from aggregators)
pub(crate) struct UnwrapOptPropOp<'g> {
    pub(crate) inner: Arc<dyn NodeOp<Output = Option<Prop>> + 'g>,
}

impl<'g> Clone for UnwrapOptPropOp<'g> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<'g> NodeOp for UnwrapOptPropOp<'g> {
    type Output = Prop;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Prop {
        match self.inner.apply(storage, node) {
            Some(Prop::List(arr)) => Prop::List(arr),
            Some(v) => Prop::List(PropArray::from(vec![v])),
            None => Prop::List(PropArray::from(vec![])),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// PropValueSetNodeOp<'g> — is_in / is_not_in for Option<Prop> (linear scan)
// ─────────────────────────────────────────────────────────────────────────────

/// Checks whether an `Option<Prop>` value is in (or not in) a fixed `Vec<Prop>`.
/// Uses linear scan because `Prop` may contain floats (`F32`, `F64`) which don't
/// implement `Hash`.
pub struct PropValueSetNodeOp<'g> {
    pub(crate) inner: Arc<dyn NodeOp<Output = Option<Prop>> + 'g>,
    pub(crate) values: Vec<Prop>,
    pub(crate) op: SetOp,
}

impl<'g> Clone for PropValueSetNodeOp<'g> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            values: self.values.clone(),
            op: self.op,
        }
    }
}

impl<'g> NodeOp for PropValueSetNodeOp<'g> {
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, node: VID) -> bool {
        match self.inner.apply(storage, node) {
            None => false,
            Some(v) => match self.op {
                SetOp::IsIn => self.values.iter().any(|x| x == &v),
                SetOp::IsNotIn => self.values.iter().all(|x| x != &v),
            },
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// BinaryCmpNodeOp<'g, T> — compares two NodeOp<Output = T> using BinaryOp
// ─────────────────────────────────────────────────────────────────────────────

/// Internal op produced by [`BinaryCmpNodeFilter::create_node_filter`].
///
/// Holds two compiled `NodeOp<Output = T>` and applies `T::binary_cmp` per node.
/// The `'g` lifetime bounds both ops to the graph view they were compiled against.
///
/// e.g. `NodeFilter::property("age").gt(30i64)` compiles to:
/// `BinaryCmpNodeOp { left: NodePropOp(prop_id=3), right: ConstNodeOp(30), op: Gt }`
#[derive(Clone)]
pub struct BinaryCmpNodeOp<'g, T: Comparable> {
    pub(crate) left: Arc<dyn NodeOp<Output = T> + 'g>,
    pub(crate) right: Arc<dyn NodeOp<Output = T> + 'g>,
    pub(crate) op: BinaryOp,
}

impl<'g, T: Comparable + Clone + Send + Sync + 'static> NodeOp for BinaryCmpNodeOp<'g, T> {
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, node: VID) -> bool {
        let lv = self.left.apply(storage, node);
        let rv = self.right.apply(storage, node);
        T::binary_cmp(&self.op, &lv, &rv)
    }

    fn prop_type(&self) -> PropType {
        PropType::Bool
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// StringNodeOp<'g, T> — applies a StringOp to two NodeOp<Output = T>
// ─────────────────────────────────────────────────────────────────────────────

/// Internal op produced by [`StringNodeFilter::create_node_filter`].
///
/// e.g. `NodeFilter::name().starts_with("Al")` compiles to:
/// `StringNodeOp { left: NameOp, right: ConstNodeOp("Al"), op: StartsWith }`
#[derive(Clone)]
pub struct StringNodeOp<'g, T: StringComparable> {
    pub(crate) left: Arc<dyn NodeOp<Output = T> + 'g>,
    pub(crate) right: Arc<dyn NodeOp<Output = T> + 'g>,
    pub(crate) op: StringOp,
}

impl<'g, T: StringComparable> NodeOp for StringNodeOp<'g, T> {
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, node: VID) -> bool {
        T::string_cmp(
            &self.op,
            &self.left.apply(storage, node),
            &self.right.apply(storage, node),
        )
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// UnaryNodeOp<'g, T> — evaluates is_some / is_none
// ─────────────────────────────────────────────────────────────────────────────

/// Internal op produced by [`UnaryNodeFilter::create_node_filter`].
///
/// e.g. `NodeFilter::property("age").is_some()` compiles to:
/// `UnaryNodeOp { inner: NodePropOp(prop_id=3), op: IsSome }`
#[derive(Clone)]
pub struct UnaryNodeOp<'g, I: Clone + Send + Sync + 'static> {
    pub(crate) inner: Arc<dyn NodeOp<Output = Option<I>> + 'g>,
    pub(crate) op: UnaryOp,
}

impl<'g, I: Clone + Send + Sync + 'static> NodeOp for UnaryNodeOp<'g, I> {
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, node: VID) -> bool {
        let v = self.inner.apply(storage, node);
        match self.op {
            UnaryOp::IsSome => v.is_some(),
            UnaryOp::IsNone => v.is_none(),
        }
    }

    fn prop_type(&self) -> PropType {
        PropType::Bool
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SetNodeOp<'g, T> — evaluates is_in / is_not_in
// ─────────────────────────────────────────────────────────────────────────────

/// Internal op produced by [`SetNodeFilter::create_node_filter`].
///
/// e.g. `NodeFilter::node_type().is_in(["Person", "Account"])` compiles to:
/// `SetNodeOp { inner: TypeOp, op: IsIn, values: {"Person", "Account"} }`
#[derive(Clone)]
pub struct SetNodeOp<'g, I: Eq + Hash + Clone + Send + Sync + 'static> {
    pub(crate) inner: Arc<dyn NodeOp<Output = Option<I>> + 'g>,
    pub(crate) op: SetOp,
    pub(crate) values: Arc<HashSet<I>>,
}

impl<'g, I: Eq + Hash + Clone + Send + Sync + 'static> NodeOp for SetNodeOp<'g, I> {
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, node: VID) -> bool {
        let v = self.inner.apply(storage, node);
        match self.op {
            SetOp::IsIn => v.as_ref().map(|x| self.values.contains(x)).unwrap_or(false),
            SetOp::IsNotIn => v
                .as_ref()
                .map(|x| !self.values.contains(x))
                .unwrap_or(false),
        }
    }
}
