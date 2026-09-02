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
//! NodeFilter.property("age")          ← NodeExpr (pure data)
//!   .create_node_op(graph)?           ← resolve "age" → prop_id = 3
//!  ──► NodePropOp { graph, prop_id: 3 }  ← NodeOp: apply() reads column 3 in O(1)
//!
//! NodeFilter.property("age").gt(30i64)   ← BinaryCmpExpr (pure data)
//!   .create_node_filter(graph)?
//!  ──► BinaryCmpNodeOp { left: NodePropOp, right: Const(Some(I64(30))), op: Gt }
//!        apply: Prop::binary_cmp(Gt, age_value, Some(I64(30)))
//!
//! NodeFilter.property("score").temporal().sum()  ← SumExpr (pure data)
//!   .create_node_op(graph)?
//!  ──► SumNodeOp { inner: TemporalNodePropOp { graph, prop_id: 7 } }
//!        apply: collect Prop::List temporal values, then aggregate_list_values(Sum)
//! ```
//!
//! # Quantified evaluation
//!
//! Filter types (`BinaryCmpExpr`, `StringExpr`, `PropValueSetExpr`) also
//! implement `NodeExpr`, producing list-aware ops for mid-chain use before `.any()`/`.all()`:
//!
//! ```text
//! temporal values = [8, 12, 5],  rhs = 10
//! .gt(10i64) as NodeExpr  →  ListAwareCmpNodeOp → Prop::List([false, true, false])
//! .any()                  →  AnyNodeOp reduces boolean list → Prop::Bool(true)
//! Eq Bool(true)           →  true    (at least one matched)
//! ```

use super::EdgeOp;
use crate::{
    db::{
        api::{
            properties::PropertiesOps,
            state::ops::NodeOp,
            view::{
                internal::{GraphView, NodeList},
                NodeViewOps,
            },
        },
        graph::views::filter::model::{
            filter_operator::{BinaryOp, Comparable, SetOp, StringComparable, StringOp, UnaryOp},
            property_filter::evaluate::{
                aggregate_list_values, scan_f64_sum_count, scan_i64_sum, scan_u64_sum,
            },
        },
    },
    prelude::GraphViewOps,
};
use raphtory_api::core::entities::{
    edges::edge_ref::EdgeRef,
    properties::prop::{IntoProp, Prop, PropArray, PropType},
    VID,
};
use raphtory_storage::graph::graph::GraphStorage;
use std::sync::Arc;
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
    fn domain(&self, _storage: &GraphStorage) -> NodeList {
        self.graph.node_list()
    }

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
    fn domain(&self, _storage: &GraphStorage) -> NodeList {
        self.graph.node_list()
    }

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
/// Collects all recorded values within the current view window into a `Some(Prop::List([...]))`.
/// That list is then consumed by aggregator ops (`SumNodeOp`, `LenNodeOp`, …) or
/// by `ListAwareCmpNodeOp` for element-wise comparisons before `.any()`/`.all()` reduction.
#[derive(Clone)]
pub(crate) struct TemporalNodePropOp<G> {
    pub(crate) graph: G,
    pub(crate) prop_id: usize,
}

impl<G: GraphView> NodeOp for TemporalNodePropOp<G> {
    fn domain(&self, _storage: &GraphStorage) -> NodeList {
        self.graph.node_list()
    }

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

macro_rules! impl_agg_entity_op {
    ($node_name:ident, $edge_name:ident, $body:expr) => {
        #[derive(Clone)]
        pub struct $node_name<'g> {
            pub inner: Arc<dyn NodeOp<Output = Option<Prop>> + 'g>,
        }

        impl<'g> NodeOp for $node_name<'g> {
            fn domain(&self, _storage: &GraphStorage) -> NodeList {
                self.inner.domain(_storage)
            }

            type Output = Option<Prop>;

            fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
                ($body)(self.inner.apply(storage, node))
            }
        }

        #[derive(Clone)]
        pub struct $edge_name<'g> {
            pub inner: Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>,
        }

        impl<'g> EdgeOp for $edge_name<'g> {
            type Output = Option<Prop>;

            fn apply(&self, storage: &GraphStorage, edge: EdgeRef) -> Option<Prop> {
                ($body)(self.inner.apply(storage, edge))
            }
        }
    };
}

impl_agg_entity_op!(SumNodeOp, SumEdgeOp, |vals| {
    aggregate_list_values(vals, &|pi| {
        let mut vals = pi.peekable();
        if vals.peek().is_none() {
            return None;
        }
        let inner = vals.peek().unwrap().dtype();
        match inner {
            PropType::U8 | PropType::U16 | PropType::U32 | PropType::U64 => {
                let (promoted, s64, s128, _) = scan_u64_sum(vals)?;
                Some(if promoted {
                    Prop::U64(u64::try_from(s128).ok()?)
                } else {
                    Prop::U64(s64)
                })
            }
            PropType::I32 | PropType::I64 => {
                let (promoted, s64, s128, _) = scan_i64_sum(vals)?;
                Some(if promoted {
                    Prop::I64(i64::try_from(s128).ok()?)
                } else {
                    Prop::I64(s64)
                })
            }
            PropType::F32 | PropType::F64 => {
                scan_f64_sum_count(vals).map(|(sum, _)| Prop::F64(sum))
            }
            _ => None,
        }
    })
});

impl_agg_entity_op!(AvgNodeOp, AvgEdgeOp, |vals| {
    aggregate_list_values(vals, &|pi| {
        let mut vals = pi.peekable();
        if vals.peek().is_none() {
            return None;
        }
        let inner = vals.peek().unwrap().dtype();
        match inner {
            PropType::U8 | PropType::U16 | PropType::U32 | PropType::U64 => {
                let (promoted, s64, s128, count) = scan_u64_sum(vals)?;
                let s = if promoted { s128 as f64 } else { s64 as f64 };
                Some(Prop::F64(s / (count as f64)))
            }

            PropType::I32 | PropType::I64 => {
                let (promoted, s64, s128, count) = scan_i64_sum(vals)?;
                let s = if promoted { s128 as f64 } else { s64 as f64 };
                Some(Prop::F64(s / (count as f64)))
            }

            PropType::F32 | PropType::F64 => {
                let (sum, count) = scan_f64_sum_count(vals)?;
                Some(Prop::F64(sum / (count as f64)))
            }

            _ => None,
        }
    })
});
impl_agg_entity_op!(MinNodeOp, MinEdgeOp, |vals| {
    aggregate_list_values(vals, &|pi| {
        let mut it = pi;
        let first = it.next()?;
        it.fold(Some(first), |acc, v| acc.and_then(|a| a.min(v)))
    })
});
impl_agg_entity_op!(MaxNodeOp, MaxEdgeOp, |vals| {
    aggregate_list_values(vals, &|pi| {
        let mut it = pi;
        let first = it.next()?;
        it.fold(Some(first), |acc, v| acc.and_then(|a| a.max(v)))
    })
});
impl_agg_entity_op!(FirstNodeOp, FirstEdgeOp, |vals| {
    // Pick the first temporal entry as-is (whether scalar or list).
    // aggregate_values would recurse into list entries and pick the first
    // *element* within each entry, which is wrong for list-typed properties.
    match vals? {
        Prop::List(x) => x.iter_all().find_map(|v| v),
        _ => None,
    }
});
impl_agg_entity_op!(LastNodeOp, LastEdgeOp, |vals| {
    // Pick the last temporal entry as-is (whether scalar or list).
    match vals? {
        Prop::List(x) => x.iter_all().filter_map(|v| v).last(),
        _ => None,
    }
});
impl_agg_entity_op!(LenNodeOp, LenEdgeOp, |vals| {
    aggregate_list_values(vals, &|pi| Some(pi.count().into_prop()))
});
impl_agg_entity_op!(AnyNodeOp, AnyEdgeOp, |vals| {
    aggregate_list_values(vals, &|mut pi| {
        Some(Prop::Bool(pi.any(|r| r == Prop::Bool(true))))
    })
});
impl_agg_entity_op!(AllNodeOp, AllEdgeOp, |vals| {
    aggregate_list_values(vals, &|mut pi| {
        let mut saw_any = false;
        let all_true = pi.all(|r| {
            saw_any = true;
            r == Prop::Bool(true)
        });
        Some(Prop::Bool(saw_any && all_true))
    })
});

// ─────────────────────────────────────────────────────────────────────────────
// ListAwareCmpNodeOp / ListAwareStringNodeOp / ListAwareSetNodeOp
//
// These ops implement NodeExpr for BinaryCmpExpr, StringExpr, and
// PropValueSetExpr respectively, enabling mid-chain use before .any()/.all().
//
// Each uses broadcasting so that comparisons applied to a `Prop::List(...)`
// fan out element-wise; scalar inputs are passed through to the op directly.
//   temporal().gt(5).any()
//   temporal().contains("rock").all()
//   temporal().is_in([...]).any()
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub(crate) struct ListAwareCmpNodeOp<'g> {
    pub(crate) left: Arc<dyn NodeOp<Output = Option<Prop>> + 'g>,
    pub(crate) right: Arc<dyn NodeOp<Output = Option<Prop>> + 'g>,
    pub(crate) op: BinaryOp,
}

impl<'g> NodeOp for ListAwareCmpNodeOp<'g> {
    fn domain(&self, _storage: &GraphStorage) -> NodeList {
        NodeList::All
    }

    type Output = Option<Prop>;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Option<Prop> {
        let lv = self.left.apply(storage, node);
        let rhs = self.right.apply(storage, node);
        let op = &self.op;
        broadcast_binary(lv, rhs, &|lv, rhs| {
            Some(Prop::Bool(Prop::binary_cmp(op, &lv?, &rhs?)))
        })
    }
}

#[derive(Clone)]
pub(crate) struct ListAwareStringNodeOp<'g> {
    pub(crate) left: Arc<dyn NodeOp<Output = Option<Prop>> + 'g>,
    pub(crate) right: Arc<dyn NodeOp<Output = Option<Prop>> + 'g>,
    pub(crate) op: StringOp,
}

impl<'g> NodeOp for ListAwareStringNodeOp<'g> {
    fn domain(&self, _storage: &GraphStorage) -> NodeList {
        NodeList::All
    }

    type Output = Option<Prop>;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Option<Prop> {
        let lv = self.left.apply(storage, node);
        let rhs = self.right.apply(storage, node);
        let op = &self.op;
        broadcast_binary(lv, rhs, &|lv, rhs| {
            Some(Prop::Bool(Option::<Prop>::string_cmp(op, &lv, &rhs)))
        })
    }
}

// [1,2,3] == [1,2,3]
// [4,5,6] > [1,2,3]

pub fn broadcast_unary(v: Option<Prop>, op: impl Fn(Option<Prop>) -> Option<Prop>) -> Option<Prop> {
    match v {
        Some(Prop::List(v)) => Some(Prop::List(v.iter_all().map(|l| op(l)).flatten().collect())),
        _ => op(v),
    }
}

pub fn broadcast_binary(
    l: Option<Prop>,
    r: Option<Prop>,
    op: &impl Fn(Option<Prop>, Option<Prop>) -> Option<Prop>,
) -> Option<Prop> {
    let l = l?;
    let r = r?;

    match (l, r) {
        (Prop::List(l), Prop::List(r)) => {
            if l.len() == r.len() {
                Some(Prop::List(
                    l.iter_all()
                        .zip(r.iter_all())
                        .map(|(l, r)| op(l, r))
                        .flatten()
                        .collect(),
                ))
            } else {
                None
            }
        }
        (Prop::List(l), r) => Some(Prop::List(
            l.iter_all()
                .map(|l| broadcast_binary(l, Some(r.clone()), op))
                .flatten()
                .collect(),
        )),
        (l, Prop::List(r)) => Some(Prop::List(
            r.iter_all()
                .map(|r| broadcast_binary(Some(l.clone()), r, op))
                .flatten()
                .collect(),
        )),
        (l, r) => op(Some(l), Some(r)),
    }
}

#[derive(Clone)]
pub(crate) struct ListAwareSetNodeOp<'g> {
    pub(crate) inner: Arc<dyn NodeOp<Output = Option<Prop>> + 'g>,
    pub(crate) values: Vec<Prop>,
    pub(crate) op: SetOp,
}

impl<'g> NodeOp for ListAwareSetNodeOp<'g> {
    fn domain(&self, _storage: &GraphStorage) -> NodeList {
        NodeList::All
    }

    type Output = Option<Prop>;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Option<Prop> {
        let vals = self.inner.apply(storage, node);
        let values = &self.values;
        let op = &self.op;
        broadcast_unary(vals, |v| {
            let v = v?;
            Some(Prop::Bool(match op {
                SetOp::IsIn => values
                    .iter()
                    .any(|x| Prop::binary_cmp(&BinaryOp::Eq, x, &v)),
                SetOp::IsNotIn => values
                    .iter()
                    .all(|x| Prop::binary_cmp(&BinaryOp::Ne, x, &v)),
            }))
        })
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ListAwareUnaryNodeOp — element-wise is_some / is_none via broadcast_unary
//
// Unlike `UnaryNodeOp` (which returns `bool` for use in `CreateFilter`), this
// op returns `Option<Prop::Bool>` so it can plug into the expression chain via
// `CreateOp`. The closure intentionally does NOT `?`-propagate the inner
// `None` — the whole purpose of `is_some`/`is_none` is to test that case.
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub(crate) struct ListAwareUnaryNodeOp<'g> {
    pub(crate) inner: Arc<dyn NodeOp<Output = Option<Prop>> + 'g>,
    pub(crate) op: UnaryOp,
}

impl<'g> NodeOp for ListAwareUnaryNodeOp<'g> {
    fn domain(&self, _storage: &GraphStorage) -> NodeList {
        NodeList::All
    }

    type Output = Option<Prop>;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Option<Prop> {
        let vals = self.inner.apply(storage, node);
        let op = &self.op;
        broadcast_unary(vals, |v| {
            Some(Prop::Bool(match op {
                UnaryOp::IsSome => v.is_some(),
                UnaryOp::IsNone => v.is_none(),
            }))
        })
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
    fn domain(&self, _storage: &GraphStorage) -> NodeList {
        NodeList::All
    }

    type Output = bool;

    fn apply(&self, storage: &GraphStorage, node: VID) -> bool {
        match self.inner.apply(storage, node) {
            None => false,
            Some(v) => match self.op {
                SetOp::IsIn => self
                    .values
                    .iter()
                    .any(|x| Prop::binary_cmp(&BinaryOp::Eq, x, &v)),
                SetOp::IsNotIn => self
                    .values
                    .iter()
                    .all(|x| Prop::binary_cmp(&BinaryOp::Ne, x, &v)),
            },
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// BinaryCmpNodeOp<'g, T> — compares two NodeOp<Output = T> using BinaryOp
// ─────────────────────────────────────────────────────────────────────────────

/// Internal op produced by [`BinaryCmpExpr::create_node_filter`].
///
/// Holds two compiled `NodeOp<Output = T>` and applies `T::binary_cmp` per node.
/// The `'g` lifetime bounds both ops to the graph view they were compiled against.
///
/// e.g. `NodeFilter.property("age").gt(30i64)` compiles to:
/// `BinaryCmpNodeOp { left: NodePropOp(prop_id=3), right: Const(Some(I64(30))), op: Gt }`
#[derive(Clone)]
pub struct BinaryCmpNodeOp<'g, T: Comparable> {
    pub(crate) left: Arc<dyn NodeOp<Output = T> + 'g>,
    pub(crate) right: Arc<dyn NodeOp<Output = T> + 'g>,
    pub(crate) op: BinaryOp,
}

impl<'g, T: Comparable + Clone + Send + Sync + 'static> NodeOp for BinaryCmpNodeOp<'g, T> {
    fn domain(&self, _storage: &GraphStorage) -> NodeList {
        NodeList::All
    }

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

/// Internal op produced by [`StringExpr::create_node_filter`].
///
/// e.g. `NodeFilter.name().starts_with("Al")` compiles to:
/// `StringNodeOp { left: Name.map(...), right: Const(Some(Str("Al"))), op: StartsWith }`
#[derive(Clone)]
pub struct StringNodeOp<'g, T: StringComparable> {
    pub(crate) left: Arc<dyn NodeOp<Output = T> + 'g>,
    pub(crate) right: Arc<dyn NodeOp<Output = T> + 'g>,
    pub(crate) op: StringOp,
}

impl<'g, T: StringComparable> NodeOp for StringNodeOp<'g, T> {
    fn domain(&self, _storage: &GraphStorage) -> NodeList {
        NodeList::All
    }

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

/// Internal op produced by [`UnaryExpr::create_node_filter`].
///
/// e.g. `NodeFilter.property("age").is_some::<Prop>()` compiles to:
/// `UnaryNodeOp { inner: NodePropOp(prop_id=3), op: IsSome }`
#[derive(Clone)]
pub struct UnaryNodeOp<'g, I: Clone + Send + Sync + 'static> {
    pub(crate) inner: Arc<dyn NodeOp<Output = Option<I>> + 'g>,
    pub(crate) op: UnaryOp,
}

impl<'g, I: Clone + Send + Sync + 'static> NodeOp for UnaryNodeOp<'g, I> {
    fn domain(&self, _storage: &GraphStorage) -> NodeList {
        NodeList::All
    }

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
