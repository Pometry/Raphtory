//! Edge expressions — what value an edge can produce.
//!
//! Mirrors [`node_expr`] exactly, but the subject is an edge rather than a node.
//! All expressions produce `Option<Prop>` — no associated output type.
//!
//! # Two-phase pipeline (same as node_expr)
//!
//! ```text
//! ┌─ Build phase (pure data, no graph) ──────────────────────┐
//! │  EdgeFilter.property("weight")    ← EdgeExpr              │
//! │  .eq(5.0f64)                      ← BinaryCmpEdgeFilter   │
//! └──────────────────────────────────────────────────────────┘
//!          │  create_edge_op(graph)?   ← resolve name → prop_id
//!          ▼
//! ┌─ Compile phase (graph-bound op) ─────────────────────────┐
//! │  EdgePropOp { graph, prop_id }   ← EdgeOp                │
//! │  apply(storage, edge_ref)                                 │
//! │    → edge_ref reads column prop_id in O(1)               │
//! └──────────────────────────────────────────────────────────┘
//! ```

use crate::{
    db::{
        api::view::internal::GraphView,
        graph::views::filter::model::{
            filter_operator::{BinaryOp, SetOp, StringOp, UnaryOp},
            node_expr::EntityExpr,
            node_expr::filters::TemporalProp,
            CreateView, EdgeFilterFactory, MetadataExpr, PropertyExpr,
        },
    },
    errors::GraphError,
};
use raphtory_api::core::entities::{
    edges::edge_ref::EdgeRef,
    properties::prop::{Prop, PropType},
};
use raphtory_storage::graph::graph::GraphStorage;
use std::{marker::PhantomData, sync::Arc};

pub mod exprs;
pub mod filters;
pub mod ops;

pub use exprs::*;
pub use filters::*;
use filters::BinaryCmpEdgeFilter;
use crate::db::graph::views::filter::model::{ FirstExpr, LastExpr, LenExpr, MaxExpr, MinExpr, SumExpr};
use crate::db::graph::views::filter::model::node_expr::{AllExpr, AnyExpr, AvgExpr};
pub use super::{Metadata, Property};

// ─────────────────────────────────────────────────────────────────────────────
// EdgeOp — compiled evaluator: EdgeRef → typed value
// ─────────────────────────────────────────────────────────────────────────────

/// A compiled edge evaluator: given an [`EdgeRef`], returns a typed value.
///
/// Parallel to [`NodeOp`] — same contract but the subject is an edge.
pub trait EdgeOp: Send + Sync {
    type Output: Clone + Send + Sync;

    fn apply(&self, _storage: &GraphStorage, edge: EdgeRef) -> Self::Output;

    fn prop_type(&self) -> PropType {
        PropType::Empty
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// EdgeExpr — typed expression describing what to compute per edge
// ─────────────────────────────────────────────────────────────────────────────

/// A typed expression that produces a value per edge.
///
/// Parallel to [`NodeExpr`] — all expressions produce `Option<Prop>`; no associated output type.
///
/// Usage:
/// ```rust,ignore
/// EdgeFilter.property("weight").gt(5.0f64)
/// EdgeFilter.property("tag").temporal().sum().gt(100i64)
/// EdgeFilter.property("label").temporal().into_expr().contains("foo").any()
/// ```
pub trait EdgeExpr: EntityExpr + Clone + Send + Sync + 'static {
    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError>;
}


// ─────────────────────────────────────────────────────────────────────────────
// EdgePropertyExprOps — fluent comparison API for edge-side property expressions
// ─────────────────────────────────────────────────────────────────────────────

/// Presence and set-membership operators for `PropertyExpr<E>` and `MetadataExpr<E>`
/// on the edge side.
pub trait EdgePropertyExprOps: EdgeExpr + Sized {
    fn is_some(self) -> UnaryEdgeFilter<Self, Prop> {
        UnaryEdgeFilter { expr: self, op: UnaryOp::IsSome, _phantom: PhantomData }
    }
    fn is_none(self) -> UnaryEdgeFilter<Self, Prop> {
        UnaryEdgeFilter { expr: self, op: UnaryOp::IsNone, _phantom: PhantomData }
    }
    fn is_in(
        self,
        values: impl IntoIterator<Item = Prop>,
    ) -> PropValueSetEdgeFilter<Self> {
        PropValueSetEdgeFilter {
            expr: self,
            values: values.into_iter().collect(),
            op: SetOp::IsIn,
        }
    }
    fn is_not_in(
        self,
        values: impl IntoIterator<Item = Prop>,
    ) -> PropValueSetEdgeFilter<Self> {
        PropValueSetEdgeFilter {
            expr: self,
            values: values.into_iter().collect(),
            op: SetOp::IsNotIn,
        }
    }
    fn is_true(self) -> BinaryCmpEdgeFilter<Self, Prop> {
        BinaryCmpEdgeFilter::new(self, BinaryOp::Eq, Prop::Bool(true))
    }
    fn is_false(self) -> BinaryCmpEdgeFilter<Self, Prop> {
        BinaryCmpEdgeFilter::new(self, BinaryOp::Eq, Prop::Bool(false))
    }
}

impl<E: CreateView + EdgeFilterFactory + Clone + Send + Sync + 'static> EdgePropertyExprOps
    for PropertyExpr<E>
{
}

impl<E: CreateView + EdgeFilterFactory + Clone + Send + Sync + 'static> EdgePropertyExprOps
    for MetadataExpr<E>
{
}

// ─────────────────────────────────────────────────────────────────────────────
// EdgeTemporalPropOps — fluent temporal API for edge-side TemporalProp
// ─────────────────────────────────────────────────────────────────────────────

/// Temporal aggregation/quantification on `TemporalProp<E>` when `E: EdgeFilterFactory`.
///
/// Provides both aggregation (`.sum()`, `.avg()`, `.len()`, …) and direct
/// element-wise comparison (`.gt()`, `.eq()`, `.contains()`, …) so users
/// never have to call `.into_expr()` explicitly:
///
/// ```rust,ignore
/// EdgeFilter.property("score").temporal().sum().gt(100i64)
/// EdgeFilter.property("score").temporal().gt(10i64).any()
/// EdgeFilter.property("score").temporal().len().gt(3usize)
/// EdgeFilter.property("label").temporal().contains("rock").any()
/// ```
pub trait EdgeTemporalPropOps: Sized {
    type ViewExpr: CreateView + EdgeFilterFactory + Clone + Send + Sync + 'static;
    fn into_temporal_parts(self) -> (Self::ViewExpr, String);

    fn into_expr(self) -> TemporalEdgePropExpr<Self::ViewExpr> {
        let (view_expr, name) = self.into_temporal_parts();
        TemporalEdgePropExpr::new(view_expr, name)
    }
    fn sum(self) -> SumExpr<TemporalEdgePropExpr<Self::ViewExpr>> {
        SumExpr(self.into_expr())
    }
    fn avg(self) -> AvgExpr<TemporalEdgePropExpr<Self::ViewExpr>> {
        AvgExpr(self.into_expr())
    }
    fn min(self) -> MinExpr<TemporalEdgePropExpr<Self::ViewExpr>> {
        MinExpr(self.into_expr())
    }
    fn max(self) -> MaxExpr<TemporalEdgePropExpr<Self::ViewExpr>> {
        MaxExpr(self.into_expr())
    }
    fn first(self) -> FirstExpr<TemporalEdgePropExpr<Self::ViewExpr>> {
        FirstExpr(self.into_expr())
    }
    fn last(self) -> LastExpr<TemporalEdgePropExpr<Self::ViewExpr>> {
        LastExpr(self.into_expr())
    }
    fn len(self) -> LenExpr<TemporalEdgePropExpr<Self::ViewExpr>> {
        LenExpr(self.into_expr())
    }
    fn any(self) -> AnyExpr<TemporalEdgePropExpr<Self::ViewExpr>> {
        AnyExpr(self.into_expr())
    }
    fn all(self) -> AllExpr<TemporalEdgePropExpr<Self::ViewExpr>> {
        AllExpr(self.into_expr())
    }

    // Direct comparison — no .into_expr() needed
    fn gt<R: EdgeExpr>(self, rhs: R) -> BinaryCmpEdgeFilter<TemporalEdgePropExpr<Self::ViewExpr>, R> {
        BinaryCmpEdgeFilter::new(self.into_expr(), BinaryOp::Gt, rhs)
    }
    fn ge<R: EdgeExpr>(self, rhs: R) -> BinaryCmpEdgeFilter<TemporalEdgePropExpr<Self::ViewExpr>, R> {
        BinaryCmpEdgeFilter::new(self.into_expr(), BinaryOp::Ge, rhs)
    }
    fn lt<R: EdgeExpr>(self, rhs: R) -> BinaryCmpEdgeFilter<TemporalEdgePropExpr<Self::ViewExpr>, R> {
        BinaryCmpEdgeFilter::new(self.into_expr(), BinaryOp::Lt, rhs)
    }
    fn le<R: EdgeExpr>(self, rhs: R) -> BinaryCmpEdgeFilter<TemporalEdgePropExpr<Self::ViewExpr>, R> {
        BinaryCmpEdgeFilter::new(self.into_expr(), BinaryOp::Le, rhs)
    }
    fn eq<R: EdgeExpr>(self, rhs: R) -> BinaryCmpEdgeFilter<TemporalEdgePropExpr<Self::ViewExpr>, R> {
        BinaryCmpEdgeFilter::new(self.into_expr(), BinaryOp::Eq, rhs)
    }
    fn ne<R: EdgeExpr>(self, rhs: R) -> BinaryCmpEdgeFilter<TemporalEdgePropExpr<Self::ViewExpr>, R> {
        BinaryCmpEdgeFilter::new(self.into_expr(), BinaryOp::Ne, rhs)
    }
    fn contains<R: EdgeExpr>(self, rhs: R) -> StringEdgeFilter<TemporalEdgePropExpr<Self::ViewExpr>, R> {
        StringEdgeFilter::new(self.into_expr(), StringOp::Contains, rhs)
    }
    fn starts_with<R: EdgeExpr>(self, rhs: R) -> StringEdgeFilter<TemporalEdgePropExpr<Self::ViewExpr>, R> {
        StringEdgeFilter::new(self.into_expr(), StringOp::StartsWith, rhs)
    }
    fn ends_with<R: EdgeExpr>(self, rhs: R) -> StringEdgeFilter<TemporalEdgePropExpr<Self::ViewExpr>, R> {
        StringEdgeFilter::new(self.into_expr(), StringOp::EndsWith, rhs)
    }
    fn not_contains<R: EdgeExpr>(self, rhs: R) -> StringEdgeFilter<TemporalEdgePropExpr<Self::ViewExpr>, R> {
        StringEdgeFilter::new(self.into_expr(), StringOp::NotContains, rhs)
    }
    fn fuzzy_search<R: EdgeExpr>(
        self,
        rhs: R,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> StringEdgeFilter<TemporalEdgePropExpr<Self::ViewExpr>, R> {
        StringEdgeFilter::new(
            self.into_expr(),
            StringOp::FuzzySearch { levenshtein_distance, prefix_match },
            rhs,
        )
    }
    fn is_true(self) -> BinaryCmpEdgeFilter<TemporalEdgePropExpr<Self::ViewExpr>, Prop> {
        BinaryCmpEdgeFilter::new(self.into_expr(), BinaryOp::Eq, Prop::Bool(true))
    }
    fn is_false(self) -> BinaryCmpEdgeFilter<TemporalEdgePropExpr<Self::ViewExpr>, Prop> {
        BinaryCmpEdgeFilter::new(self.into_expr(), BinaryOp::Eq, Prop::Bool(false))
    }
}

impl<E: CreateView + EdgeFilterFactory + Clone + Send + Sync + 'static> EdgeTemporalPropOps
    for TemporalProp<E>
{
    type ViewExpr = E;
    fn into_temporal_parts(self) -> (E, String) {
        (self.view_expr, self.name)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// EdgeExprFilterOps — comparison operators on any EdgeExpr
// ─────────────────────────────────────────────────────────────────────────────

/// Comparison, string, set, and presence operators on any [`EdgeExpr`].
///
/// `.any()` / `.all()` are terminal: they wrap `self` in `AnyExpr`/`AllExpr` and compare
/// to `Bool(true)`. For element-wise comparison before reduction, chain in order:
/// `.gt(10i64).any()` not `.any().gt(10i64)`.
///
/// ```rust,ignore
/// EdgeFilter.property("weight").gt(5.0f64)
/// EdgeFilter.property("tag").temporal().into_expr().contains("foo").any()
/// EdgeFilter.property("count").temporal().sum().gt(100i64)
/// ```
pub trait EdgeExprFilterOps: EdgeExpr + Sized {
    fn gt<R: EdgeExpr>(self, rhs: R) -> BinaryCmpEdgeFilter<Self, R> {
        BinaryCmpEdgeFilter::new(self, BinaryOp::Gt, rhs)
    }
    fn ge<R: EdgeExpr>(self, rhs: R) -> BinaryCmpEdgeFilter<Self, R> {
        BinaryCmpEdgeFilter::new(self, BinaryOp::Ge, rhs)
    }
    fn lt<R: EdgeExpr>(self, rhs: R) -> BinaryCmpEdgeFilter<Self, R> {
        BinaryCmpEdgeFilter::new(self, BinaryOp::Lt, rhs)
    }
    fn le<R: EdgeExpr>(self, rhs: R) -> BinaryCmpEdgeFilter<Self, R> {
        BinaryCmpEdgeFilter::new(self, BinaryOp::Le, rhs)
    }
    fn eq<R: EdgeExpr>(self, rhs: R) -> BinaryCmpEdgeFilter<Self, R> {
        BinaryCmpEdgeFilter::new(self, BinaryOp::Eq, rhs)
    }
    fn ne<R: EdgeExpr>(self, rhs: R) -> BinaryCmpEdgeFilter<Self, R> {
        BinaryCmpEdgeFilter::new(self, BinaryOp::Ne, rhs)
    }
    fn starts_with<R: EdgeExpr>(self, rhs: R) -> StringEdgeFilter<Self, R> {
        StringEdgeFilter::new(self, StringOp::StartsWith, rhs)
    }
    fn ends_with<R: EdgeExpr>(self, rhs: R) -> StringEdgeFilter<Self, R> {
        StringEdgeFilter::new(self, StringOp::EndsWith, rhs)
    }
    fn contains<R: EdgeExpr>(self, rhs: R) -> StringEdgeFilter<Self, R> {
        StringEdgeFilter::new(self, StringOp::Contains, rhs)
    }
    fn not_contains<R: EdgeExpr>(self, rhs: R) -> StringEdgeFilter<Self, R> {
        StringEdgeFilter::new(self, StringOp::NotContains, rhs)
    }
    fn fuzzy_search<R: EdgeExpr>(
        self,
        rhs: R,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> StringEdgeFilter<Self, R> {
        StringEdgeFilter::new(
            self,
            StringOp::FuzzySearch { levenshtein_distance, prefix_match },
            rhs,
        )
    }
    fn is_in(self, values: impl IntoIterator<Item = Prop>) -> PropValueSetEdgeFilter<Self> {
        PropValueSetEdgeFilter { expr: self, values: values.into_iter().collect(), op: SetOp::IsIn }
    }
    fn is_not_in(self, values: impl IntoIterator<Item = Prop>) -> PropValueSetEdgeFilter<Self> {
        PropValueSetEdgeFilter { expr: self, values: values.into_iter().collect(), op: SetOp::IsNotIn }
    }
    fn any(self) -> BinaryCmpEdgeFilter<AnyExpr<Self>, Prop> {
        BinaryCmpEdgeFilter::new(AnyExpr(self), BinaryOp::Eq, Prop::Bool(true))
    }
    fn all(self) -> BinaryCmpEdgeFilter<AllExpr<Self>, Prop> {
        BinaryCmpEdgeFilter::new(AllExpr(self), BinaryOp::Eq, Prop::Bool(true))
    }
}

impl<E: EdgeExpr> EdgeExprFilterOps for E {}

