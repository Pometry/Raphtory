//! Runtime evaluators — given a node ID, return a typed value.
//!
//! A [`NodeOp`] is a *compiled* expression: name→ID lookups are resolved and the
//! op holds a reference to the graph view it was compiled against.
//! `apply(storage, vid)` returns the value in O(1).
//!
//! Ops are produced by compiling a filter tree (`model::expr`) against a
//! graph — never constructed directly.
//!
//! # Evaluation pipeline
//!
//! ```text
//! NodeFilter.property("age")          ← Expr<NodeLeaf> (pure data)
//!   compiled against graph            ← resolve "age" → prop_id = 3
//!  ──► NodePropOp { graph, prop_id: 3 }  ← NodeOp: apply() reads column 3 in O(1)
//!
//! NodeFilter.property("age").gt(30i64)   ← Expr::Cmp (pure data)
//!  ──► a comparison op over NodePropOp and Const(Some(I64(30)))
//!        apply: Prop::binary_cmp(Gt, age_value, Some(I64(30)))
//!
//! NodeFilter.property("tags").sum()  ← Expr::Agg (pure data)
//!  ──► SumNodeOp { inner: NodePropOp { graph, prop_id: 7 } }
//!        apply: reduce the list value with aggregate_list_values(Sum)
//! ```
//!
//! # Qualified evaluation
//!
//! A comparison against a list-valued side gives one answer per element;
//! `.any()` / `.all()` written after it collapse that list:
//!
//! ```text
//! temporal values = [8, 12, 5],  rhs = 10
//! .gt(10i64)   →  Prop::List([false, true, false])   (element-wise, in `model::expr`)
//! .any()       →  AnyNodeOp reduces the list → Prop::Bool(true)
//! ```

use crate::{
    db::{
        api::{
            properties::PropertiesOps,
            state::ops::{Id, NodeOp},
            view::{
                internal::{GraphView, InnerFilterOps, NodeList},
                NodeViewOps,
            },
        },
        graph::{
            node::NodeView,
            views::filter::model::{
                edge_expr::EdgeOp,
                expr::{
                    typing::{truthy, BinaryKernel, UnaryKernel},
                    Agg,
                },
                property_filter::evaluate::{
                    aggregate_list_values, scan_f64_sum_count, scan_i64_sum, scan_u64_sum,
                },
            },
        },
    },
    prelude::GraphViewOps,
};
use bigdecimal::BigDecimal;
use raphtory_api::core::{
    entities::{
        properties::prop::{IntoProp, Prop, PropType},
        GidType, LayerId, VID,
    },
    storage::timeindex::EventTime,
};
use raphtory_storage::graph::graph::GraphStorage;
use std::sync::Arc;
use storage::EdgeEntryRef;
// ─────────────────────────────────────────────────────────────────────────────
// NodePropOp<G> — latest property value by pre-resolved column ID
// ─────────────────────────────────────────────────────────────────────────────

/// The op a node property term compiles to — not constructed directly.
///
/// `Property("age")` resolves `"age"` → `prop_id` once at compile time;
/// every `apply` call then reads column `prop_id` in O(1).
#[derive(Clone)]
pub(crate) struct NodePropOp<G> {
    pub(crate) graph: G,
    pub(crate) prop_id: usize,
    /// Whether the term's own view (e.g. `NodeFilter.window(..)`) can hide
    /// nodes the enclosing filter keeps; see [`view_node`].
    pub(crate) narrows: bool,
}

/// The node as `graph` sees it. When `graph` is the graph the enclosing filter
/// runs on, that filter has already decided the node belongs to it, so the node
/// is read as it is. When the term carries its own view that can hide nodes
/// (`narrows`, e.g. `NodeFilter.window(..)`), the node may be missing from it,
/// and a term on a missing node is `None`.
#[inline]
pub(crate) fn view_node<G: GraphView>(
    graph: &G,
    narrows: bool,
    node: VID,
) -> Option<NodeView<'_, &G>> {
    if narrows {
        (&graph).node(node)
    } else {
        Some(NodeView::new_internal(graph, node))
    }
}

impl<G: GraphView> NodeOp for NodePropOp<G> {
    fn domain(&self, _storage: &GraphStorage) -> NodeList {
        self.graph.node_list()
    }

    type Output = Option<Prop>;

    fn apply(&self, _storage: &GraphStorage, node: VID) -> Option<Prop> {
        view_node(&self.graph, self.narrows, node)?
            .properties()
            .get_by_id(self.prop_id)
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

/// The op a node metadata term compiles to — not constructed directly.
///
/// Same as [`NodePropOp`] but reads from the static metadata column instead of
/// temporal properties.
#[derive(Clone)]
pub(crate) struct NodeMetaOp<G> {
    pub(crate) graph: G,
    pub(crate) prop_id: usize,
    /// Whether the term's own view (e.g. `NodeFilter.window(..)`) can hide
    /// nodes the enclosing filter keeps; see [`view_node`].
    pub(crate) narrows: bool,
}

impl<G: GraphView> NodeOp for NodeMetaOp<G> {
    fn domain(&self, _storage: &GraphStorage) -> NodeList {
        self.graph.node_list()
    }

    type Output = Option<Prop>;

    fn apply(&self, _storage: &GraphStorage, node: VID) -> Option<Prop> {
        view_node(&self.graph, self.narrows, node)?
            .metadata()
            .get_by_id(self.prop_id)
    }

    fn prop_type(&self) -> PropType {
        self.graph
            .node_meta()
            .metadata_mapper()
            .get_dtype(self.prop_id)
            .unwrap_or_default()
    }
}

/// Whether `graph` holds the node apart from time: `false` for a node it
/// leaves out with `exclude_nodes`, `subgraph`, `subgraph_node_types` or a
/// node filter.
#[inline]
pub(crate) fn node_shown<G: GraphView>(graph: &G, node: VID) -> bool {
    !graph.filtered_inner() || graph.filter_node_inner(graph.core_node(node).as_ref())
}

// ─────────────────────────────────────────────────────────────────────────────
// ShownNodeOp<G, F> — a term that answers `hidden` for a node the view hides
// ─────────────────────────────────────────────────────────────────────────────

/// A term read through `graph` that answers `hidden` for a node `graph` hides
/// apart from time, as it answers for a node a window leaves out (`0` for a
/// degree, `false` for `is_active`).
#[derive(Clone)]
pub(crate) struct ShownNodeOp<G, F: NodeOp> {
    pub(crate) graph: G,
    pub(crate) term: F,
    pub(crate) hidden: F::Output,
}

impl<G: GraphView, F: NodeOp> NodeOp for ShownNodeOp<G, F> {
    type Output = F::Output;

    fn domain(&self, storage: &GraphStorage) -> NodeList {
        self.term.domain(storage)
    }

    fn prop_type(&self) -> PropType {
        self.term.prop_type()
    }

    fn apply(&self, storage: &GraphStorage, node: VID) -> F::Output {
        if node_shown(&self.graph, node) {
            self.term.apply(storage, node)
        } else {
            self.hidden.clone()
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// NodeIdOp — the node id as a value, typed by the graph's id type
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub(crate) struct NodeIdOp {
    pub(crate) id_type: Option<GidType>,
}

impl NodeOp for NodeIdOp {
    type Output = Option<Prop>;

    fn domain(&self, _storage: &GraphStorage) -> NodeList {
        NodeList::All
    }

    fn prop_type(&self) -> PropType {
        match self.id_type {
            Some(GidType::Str) => PropType::Str,
            Some(GidType::U64) => PropType::U64,
            None => PropType::Empty,
        }
    }

    fn apply(&self, storage: &GraphStorage, node: VID) -> Option<Prop> {
        Some(Id.apply(storage, node).into_prop())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// TemporalNodePropOp<G> — all temporal values for a property within the window
// ─────────────────────────────────────────────────────────────────────────────

/// The op a temporal node property term compiles to — not constructed directly.
///
/// Collects all recorded values within the current view window into a `Some(Prop::List([...]))`
/// that aggregations and `any()`/`all()` tests read.
#[derive(Clone)]
pub(crate) struct TemporalNodePropOp<G> {
    pub(crate) graph: G,
    pub(crate) prop_id: usize,
    /// Whether the term's own view (e.g. `NodeFilter.window(..)`) can hide
    /// nodes the enclosing filter keeps; see [`view_node`].
    pub(crate) narrows: bool,
}

impl<G: GraphView> NodeOp for TemporalNodePropOp<G> {
    fn domain(&self, _storage: &GraphStorage) -> NodeList {
        self.graph.node_list()
    }

    fn prop_type(&self) -> PropType {
        self.graph
            .node_meta()
            .temporal_prop_mapper()
            .get_dtype(self.prop_id)
            .map_or(PropType::Empty, |dt| PropType::List(Box::new(dt)))
    }

    type Output = Option<Prop>;

    fn apply(&self, _storage: &GraphStorage, node: VID) -> Option<Prop> {
        let vals: Vec<Prop> = view_node(&self.graph, self.narrows, node)
            .and_then(|n| {
                n.properties()
                    .temporal()
                    .get_by_id(self.prop_id)
                    .map(|tpv| tpv.values().collect())
            })
            .unwrap_or_default();
        // Every value in one property's history has the property's type, so
        // the list always unifies.
        Prop::list(vals).ok()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Aggregator NodeOps — compile-time resolved against a concrete graph view
//
// Each is the op an aggregate of a node tree compiles to:
//   sum()   → SumNodeOp      avg()  → AvgNodeOp      min() → MinNodeOp
//   max()   → MaxNodeOp      first() → FirstNodeOp   last() → LastNodeOp
//   len()   → LenNodeOp      earliest() → EarliestNodeOp
//   latest() → LatestNodeOp
// ─────────────────────────────────────────────────────────────────────────────

/// Aggregations collapse the innermost list level; outer levels survive so a
/// pending qualifier still sees per-element results. `elem_out` names the type a
/// single aggregation step produces from the element type it collapses.
fn agg_out_type_with(pt: PropType, elem_out: &dyn Fn(PropType) -> PropType) -> PropType {
    match pt {
        PropType::List(inner) => match *inner {
            nested @ PropType::List(_) => {
                PropType::List(Box::new(agg_out_type_with(nested, elem_out)))
            }
            elem => elem_out(elem),
        },
        other => other,
    }
}

/// `scalar` names a fixed output type (`None` keeps the element type, as for
/// min/max/first/last).
fn agg_out_type(pt: PropType, scalar: Option<PropType>) -> PropType {
    agg_out_type_with(pt, &|elem| scalar.clone().unwrap_or(elem))
}

/// The type a sum produces from its element type. Integer sums widen: the
/// evaluator below accumulates every unsigned width into a `U64`, every signed
/// width into an `I64`, and either into a `Decimal` when that overflows.
/// Floats keep their width, as `Prop::add` does. Keep the arms in step with
/// the evaluator's.
fn sum_out_type(pt: PropType) -> PropType {
    agg_out_type_with(pt, &|elem| match elem {
        PropType::U8 | PropType::U16 | PropType::U32 | PropType::U64 => PropType::U64,
        PropType::I32 | PropType::I64 => PropType::I64,
        other => other,
    })
}

macro_rules! impl_agg_entity_op {
    ($node_name:ident, $edge_name:ident, $out_pt:expr, $body:expr) => {
        #[derive(Clone)]
        pub struct $node_name<'g> {
            pub inner: Arc<dyn NodeOp<Output = Option<Prop>> + 'g>,
        }

        impl<'g> NodeOp for $node_name<'g> {
            fn domain(&self, _storage: &GraphStorage) -> NodeList {
                self.inner.domain(_storage)
            }

            type Output = Option<Prop>;

            fn prop_type(&self) -> PropType {
                ($out_pt)(self.inner.prop_type())
            }

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

            fn prop_type(&self) -> PropType {
                ($out_pt)(self.inner.prop_type())
            }

            fn apply(&self, storage: &GraphStorage, edge: EdgeEntryRef) -> Option<Prop> {
                ($body)(self.inner.apply(storage, edge))
            }

            fn apply_layer(
                &self,
                storage: &GraphStorage,
                edge: EdgeEntryRef,
                layer: LayerId,
            ) -> Option<Prop> {
                ($body)(self.inner.apply_layer(storage, edge, layer))
            }

            fn apply_exploded(
                &self,
                storage: &GraphStorage,
                edge: EdgeEntryRef,
                layer: LayerId,
                t: EventTime,
            ) -> Option<Prop> {
                ($body)(self.inner.apply_exploded(storage, edge, layer, t))
            }
        }
    };
}

/// One reduction over the elements of one list, `Last` from the back.
pub(crate) fn reduce_list(
    agg: Agg,
    mut items: Box<dyn DoubleEndedIterator<Item = Prop> + '_>,
) -> Option<Prop> {
    match agg {
        Agg::Last => items.next_back(),
        _ => fold_values(agg, items),
    }
}

/// One reduction over a stream of values, front to back. A caller that can
/// read the values from the back answers `Last` and `Latest` itself instead
/// of walking to the end.
pub(crate) fn fold_values(agg: Agg, vals: impl Iterator<Item = Prop>) -> Option<Prop> {
    let mut vals = vals.peekable();
    match agg {
        Agg::Sum => match vals.peek()?.dtype() {
            PropType::U8 | PropType::U16 | PropType::U32 | PropType::U64 => {
                let (promoted, s64, s128, _) = scan_u64_sum(vals)?;
                Some(if promoted {
                    // A sum past u64 promotes to Decimal and still compares.
                    match u64::try_from(s128) {
                        Ok(v) => Prop::U64(v),
                        Err(_) => Prop::Decimal(BigDecimal::from(s128)),
                    }
                } else {
                    Prop::U64(s64)
                })
            }
            PropType::I32 | PropType::I64 => {
                let (promoted, s64, s128, _) = scan_i64_sum(vals)?;
                Some(if promoted {
                    match i64::try_from(s128) {
                        Ok(v) => Prop::I64(v),
                        Err(_) => Prop::Decimal(BigDecimal::from(s128)),
                    }
                } else {
                    Prop::I64(s64)
                })
            }
            PropType::F32 => scan_f64_sum_count(vals).map(|(sum, _)| Prop::F32(sum as f32)),
            PropType::F64 => scan_f64_sum_count(vals).map(|(sum, _)| Prop::F64(sum)),
            _ => None,
        },
        Agg::Avg => match vals.peek()?.dtype() {
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
        },
        Agg::Min => {
            let first = vals.next()?;
            vals.fold(Some(first), |acc, v| acc.and_then(|a| a.min(v)))
        }
        Agg::Max => {
            let first = vals.next()?;
            vals.fold(Some(first), |acc, v| acc.and_then(|a| a.max(v)))
        }
        Agg::First | Agg::Earliest => vals.next(),
        Agg::Last | Agg::Latest => vals.last(),
        Agg::Len => Some(vals.count().into_prop()),
    }
}

/// The type `agg` produces from a value of type `pt`.
pub(crate) fn agg_out_pt(agg: Agg, pt: PropType) -> PropType {
    match agg {
        Agg::Sum => sum_out_type(pt),
        Agg::Avg => agg_out_type(pt, Some(PropType::F64)),
        Agg::Min | Agg::Max | Agg::First | Agg::Last => agg_out_type(pt, None),
        Agg::Len => agg_out_type(pt, Some(PropType::U64)),
        Agg::Earliest | Agg::Latest => update_type(pt),
    }
}

impl_agg_entity_op!(
    SumNodeOp,
    SumEdgeOp,
    |pt| agg_out_pt(Agg::Sum, pt),
    |vals| { aggregate_list_values(vals, &|pi| reduce_list(Agg::Sum, pi)) }
);
impl_agg_entity_op!(
    AvgNodeOp,
    AvgEdgeOp,
    |pt| agg_out_pt(Agg::Avg, pt),
    |vals| { aggregate_list_values(vals, &|pi| reduce_list(Agg::Avg, pi)) }
);
impl_agg_entity_op!(
    MinNodeOp,
    MinEdgeOp,
    |pt| agg_out_pt(Agg::Min, pt),
    |vals| { aggregate_list_values(vals, &|pi| reduce_list(Agg::Min, pi)) }
);
impl_agg_entity_op!(
    MaxNodeOp,
    MaxEdgeOp,
    |pt| agg_out_pt(Agg::Max, pt),
    |vals| { aggregate_list_values(vals, &|pi| reduce_list(Agg::Max, pi)) }
);
impl_agg_entity_op!(
    FirstNodeOp,
    FirstEdgeOp,
    |pt| agg_out_pt(Agg::First, pt),
    |vals| { aggregate_list_values(vals, &|pi| reduce_list(Agg::First, pi)) }
);
impl_agg_entity_op!(
    LastNodeOp,
    LastEdgeOp,
    |pt| agg_out_pt(Agg::Last, pt),
    |vals| { aggregate_list_values(vals, &|pi| reduce_list(Agg::Last, pi)) }
);
/// The type one update of a history has: the history's element type.
fn update_type(pt: PropType) -> PropType {
    match pt {
        PropType::List(inner) => *inner,
        other => other,
    }
}
impl_agg_entity_op!(
    EarliestNodeOp,
    EarliestEdgeOp,
    |pt| agg_out_pt(Agg::Earliest, pt),
    |vals| {
        // The earliest update as it is, scalar or list.
        match vals? {
            Prop::List(x) => x.iter_all().find_map(|v| v),
            _ => None,
        }
    }
);
impl_agg_entity_op!(
    LatestNodeOp,
    LatestEdgeOp,
    |pt| agg_out_pt(Agg::Latest, pt),
    |vals| {
        // The latest update as it is, scalar or list.
        match vals? {
            Prop::List(x) => x.iter_all().rev().find_map(|v| v),
            _ => None,
        }
    }
);
impl_agg_entity_op!(
    LenNodeOp,
    LenEdgeOp,
    |pt| agg_out_pt(Agg::Len, pt),
    |vals| { aggregate_list_values(vals, &|pi| reduce_list(Agg::Len, pi)) }
);
impl_agg_entity_op!(
    AnyNodeOp,
    AnyEdgeOp,
    |pt| agg_out_type(pt, Some(PropType::Bool)),
    |vals| {
        aggregate_list_values(vals, &|mut pi| {
            Some(Prop::Bool(pi.any(|r| r == Prop::Bool(true))))
        })
    }
);
impl_agg_entity_op!(
    AllNodeOp,
    AllEdgeOp,
    |pt| agg_out_type(pt, Some(PropType::Bool)),
    |vals| {
        aggregate_list_values(vals, &|mut pi| {
            let mut saw_any = false;
            let all_true = pi.all(|r| {
                saw_any = true;
                r == Prop::Bool(true)
            });
            Some(Prop::Bool(saw_any && all_true))
        })
    }
);

pub fn broadcast_unary(v: Option<Prop>, op: impl Fn(Option<Prop>) -> Option<Prop>) -> Option<Prop> {
    match v {
        // The kernels passed here answer every element with a `Bool`, so the
        // list always unifies.
        Some(Prop::List(v)) => Prop::list(v.iter_all().filter_map(op)).ok(),
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

    // The kernels passed here answer every element with a `Bool` (or, one
    // level down, a list of them), so each list built below always unifies.
    match (l, r) {
        (Prop::List(l), Prop::List(r)) => {
            if l.len() == r.len() {
                Prop::list(l.iter_all().zip(r.iter_all()).filter_map(|(l, r)| op(l, r))).ok()
            } else {
                None
            }
        }
        (Prop::List(l), r) => Prop::list(
            l.iter_all()
                .filter_map(|l| broadcast_binary(l, Some(r.clone()), op)),
        )
        .ok(),
        (l, Prop::List(r)) => Prop::list(
            r.iter_all()
                .filter_map(|r| broadcast_binary(Some(l.clone()), r, op)),
        )
        .ok(),
        (l, r) => op(Some(l), Some(r)),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Value ops — a boolean (or element-wise boolean) built from other values
// ─────────────────────────────────────────────────────────────────────────────

/// Two values combined by `kernel` with the test's `param`, producing a value
/// of type `out`. The kernel is chosen for the test's shape when the op is built.
pub struct BinaryValueNodeOp<'g, P> {
    pub(crate) left: Arc<dyn NodeOp<Output = Option<Prop>> + 'g>,
    pub(crate) right: Arc<dyn NodeOp<Output = Option<Prop>> + 'g>,
    pub(crate) param: P,
    pub(crate) kernel: BinaryKernel<P>,
    pub(crate) out: PropType,
}

impl<'g, P: Send + Sync> NodeOp for BinaryValueNodeOp<'g, P> {
    type Output = Option<Prop>;

    fn domain(&self, _storage: &GraphStorage) -> NodeList {
        NodeList::All
    }

    fn prop_type(&self) -> PropType {
        self.out.clone()
    }

    fn apply(&self, storage: &GraphStorage, node: VID) -> Option<Prop> {
        (self.kernel)(
            &self.param,
            self.left.apply(storage, node),
            self.right.apply(storage, node),
        )
    }
}

/// One value mapped by `kernel` with the test's `param`, producing a value of
/// type `out`.
pub struct UnaryValueNodeOp<'g, P> {
    pub(crate) inner: Arc<dyn NodeOp<Output = Option<Prop>> + 'g>,
    pub(crate) param: P,
    pub(crate) kernel: UnaryKernel<P>,
    pub(crate) out: PropType,
}

impl<'g, P: Send + Sync> NodeOp for UnaryValueNodeOp<'g, P> {
    type Output = Option<Prop>;

    fn domain(&self, _storage: &GraphStorage) -> NodeList {
        NodeList::All
    }

    fn prop_type(&self) -> PropType {
        self.out.clone()
    }

    fn apply(&self, storage: &GraphStorage, node: VID) -> Option<Prop> {
        (self.kernel)(&self.param, self.inner.apply(storage, node))
    }
}

/// `and` over boolean values, stopping at the first that does not hold.
pub struct AllOfNodeOp<'g> {
    pub(crate) items: Vec<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>>,
}

impl<'g> NodeOp for AllOfNodeOp<'g> {
    type Output = Option<Prop>;

    fn domain(&self, _storage: &GraphStorage) -> NodeList {
        NodeList::All
    }

    fn prop_type(&self) -> PropType {
        PropType::Bool
    }

    fn apply(&self, storage: &GraphStorage, node: VID) -> Option<Prop> {
        let hit = self
            .items
            .iter()
            .all(|item| truthy(&item.apply(storage, node)));
        Some(Prop::Bool(hit))
    }
}

/// `or` over boolean values, stopping at the first that holds.
pub struct AnyOfNodeOp<'g> {
    pub(crate) items: Vec<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>>,
}

impl<'g> NodeOp for AnyOfNodeOp<'g> {
    type Output = Option<Prop>;

    fn domain(&self, _storage: &GraphStorage) -> NodeList {
        NodeList::All
    }

    fn prop_type(&self) -> PropType {
        PropType::Bool
    }

    fn apply(&self, storage: &GraphStorage, node: VID) -> Option<Prop> {
        let hit = self
            .items
            .iter()
            .any(|item| truthy(&item.apply(storage, node)));
        Some(Prop::Bool(hit))
    }
}
