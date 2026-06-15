//! Edge expressions — what value an edge can produce.
//!
//! Mirrors [`node_expr`] exactly, but the subject is an edge rather than a node.
//!
//! # Two-phase pipeline (same as node_expr)
//!
//! ```text
//! ┌─ Build phase (pure data, no graph) ──────────────────────┐
//! │  EdgeFilter::property("weight")   ← EdgePropertyExpr      │
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
            filter_operator::{BinaryOp, SetOp, StringComparable, StringOp, UnaryOp},
            node_expr::{AllMode, AnyMode, QuantifierMode},
            node_expr::filters::TemporalProp,
            property_filter::Op,
            CreateView, EdgeFilterFactory, MetadataExpr, PropertyExpr,
        },
    },
    errors::GraphError,
};
use raphtory_api::core::{
    entities::{
        edges::edge_ref::EdgeRef,
        properties::prop::{Prop, PropType},
    },
    storage::arc_str::ArcStr,
};
use raphtory_storage::graph::graph::GraphStorage;
use std::{collections::HashSet, hash::Hash, marker::PhantomData, sync::Arc};

pub mod exprs;
pub mod filters;
pub mod ops;

pub use exprs::*;
pub use filters::*;
pub use ops::*;
use crate::db::graph::views::filter::model::{AvgExpr, FirstExpr, LastExpr, MaxExpr, MinExpr, SumExpr};
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
/// Parallel to [`NodeExpr`] — same two-phase design.
pub trait EdgeExpr: Clone + Send + Sync + 'static {
    type Output: Clone + Send + Sync + 'static;

    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Self::Output> + 'g>, GraphError>;

    fn prop_type(&self) -> PropType {
        PropType::Empty
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// EdgeAggregated<E> — builder that produces BinaryCmpEdgeFilter
// ─────────────────────────────────────────────────────────────────────────────

/// Returned by `EdgeTemporalProp::sum()`, `.first()`, etc.
/// Calling `.eq()` etc. produces a `BinaryCmpEdgeFilter`.
pub struct EdgeAggregated<E: EdgeExpr> {
    pub(crate) expr: E,
}

impl<E: EdgeExpr<Output = Option<Prop>>> EdgeAggregated<E> {
    fn finish<R: IntoPropEdgeExpr>(
        self,
        op: crate::db::graph::views::filter::model::filter_operator::BinaryOp,
        rhs: R,
    ) -> BinaryCmpEdgeFilter<E, R::Expr> {
        BinaryCmpEdgeFilter::new(self.expr, op, rhs.into_prop_edge_expr())
    }

    pub fn eq<R: IntoPropEdgeExpr>(self, rhs: R) -> BinaryCmpEdgeFilter<E, R::Expr> {
        self.finish(crate::db::graph::views::filter::model::filter_operator::BinaryOp::Eq, rhs)
    }
    pub fn ne<R: IntoPropEdgeExpr>(self, rhs: R) -> BinaryCmpEdgeFilter<E, R::Expr> {
        self.finish(crate::db::graph::views::filter::model::filter_operator::BinaryOp::Ne, rhs)
    }
    pub fn gt<R: IntoPropEdgeExpr>(self, rhs: R) -> BinaryCmpEdgeFilter<E, R::Expr> {
        self.finish(crate::db::graph::views::filter::model::filter_operator::BinaryOp::Gt, rhs)
    }
    pub fn ge<R: IntoPropEdgeExpr>(self, rhs: R) -> BinaryCmpEdgeFilter<E, R::Expr> {
        self.finish(crate::db::graph::views::filter::model::filter_operator::BinaryOp::Ge, rhs)
    }
    pub fn lt<R: IntoPropEdgeExpr>(self, rhs: R) -> BinaryCmpEdgeFilter<E, R::Expr> {
        self.finish(crate::db::graph::views::filter::model::filter_operator::BinaryOp::Lt, rhs)
    }
    pub fn le<R: IntoPropEdgeExpr>(self, rhs: R) -> BinaryCmpEdgeFilter<E, R::Expr> {
        self.finish(crate::db::graph::views::filter::model::filter_operator::BinaryOp::Le, rhs)
    }
    pub fn is_some(self) -> UnaryEdgeFilter<E, Prop> {
        UnaryEdgeFilter {
            expr: self.expr,
            op: crate::db::graph::views::filter::model::filter_operator::UnaryOp::IsSome,
            _phantom: std::marker::PhantomData,
        }
    }
    pub fn is_none(self) -> UnaryEdgeFilter<E, Prop> {
        UnaryEdgeFilter {
            expr: self.expr,
            op: crate::db::graph::views::filter::model::filter_operator::UnaryOp::IsNone,
            _phantom: std::marker::PhantomData,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// EdgeQuantified<E, Q> — builder that produces QuantifiedEdgeFilter
// ─────────────────────────────────────────────────────────────────────────────

/// Returned by `EdgeTemporalProp::any()` / `all()`.
/// Calling `.eq()` etc. produces a `QuantifiedEdgeFilter`.
pub struct EdgeQuantified<E, Q>
where
    E: EdgeExpr<Output = Prop>,
    Q: QuantifierMode,
{
    pub(crate) expr: E,
    pub(crate) _q: std::marker::PhantomData<Q>,
}

impl<E, Q> EdgeQuantified<E, Q>
where
    E: EdgeExpr<Output = Prop>,
    Q: QuantifierMode,
{
    fn finish<R: IntoPropEdgeExpr>(
        self,
        op: crate::db::graph::views::filter::model::filter_operator::BinaryOp,
        rhs: R,
    ) -> QuantifiedEdgeFilter<E, Q, R::Expr> {
        QuantifiedEdgeFilter::new(self.expr, op, rhs.into_prop_edge_expr())
    }

    pub fn eq<R: IntoPropEdgeExpr>(self, rhs: R) -> QuantifiedEdgeFilter<E, Q, R::Expr> {
        self.finish(crate::db::graph::views::filter::model::filter_operator::BinaryOp::Eq, rhs)
    }
    pub fn ne<R: IntoPropEdgeExpr>(self, rhs: R) -> QuantifiedEdgeFilter<E, Q, R::Expr> {
        self.finish(crate::db::graph::views::filter::model::filter_operator::BinaryOp::Ne, rhs)
    }
    pub fn gt<R: IntoPropEdgeExpr>(self, rhs: R) -> QuantifiedEdgeFilter<E, Q, R::Expr> {
        self.finish(crate::db::graph::views::filter::model::filter_operator::BinaryOp::Gt, rhs)
    }
    pub fn ge<R: IntoPropEdgeExpr>(self, rhs: R) -> QuantifiedEdgeFilter<E, Q, R::Expr> {
        self.finish(crate::db::graph::views::filter::model::filter_operator::BinaryOp::Ge, rhs)
    }
    pub fn lt<R: IntoPropEdgeExpr>(self, rhs: R) -> QuantifiedEdgeFilter<E, Q, R::Expr> {
        self.finish(crate::db::graph::views::filter::model::filter_operator::BinaryOp::Lt, rhs)
    }
    pub fn le<R: IntoPropEdgeExpr>(self, rhs: R) -> QuantifiedEdgeFilter<E, Q, R::Expr> {
        self.finish(crate::db::graph::views::filter::model::filter_operator::BinaryOp::Le, rhs)
    }

    pub fn is_in(self, values: impl IntoIterator<Item = Prop>) -> QuantifiedIsInEdgeFilter<E, Q> {
        QuantifiedIsInEdgeFilter {
            expr: self.expr,
            values: values.into_iter().collect(),
            op: SetOp::IsIn,
            _q: PhantomData,
        }
    }

    pub fn is_not_in(self, values: impl IntoIterator<Item = Prop>) -> QuantifiedIsInEdgeFilter<E, Q> {
        QuantifiedIsInEdgeFilter {
            expr: self.expr,
            values: values.into_iter().collect(),
            op: SetOp::IsNotIn,
            _q: PhantomData,
        }
    }

    fn string_finish(self, op: StringOp, rhs: &str) -> QuantifiedStringEdgeFilter<E, Q> {
        QuantifiedStringEdgeFilter {
            expr: self.expr,
            rhs: ArcStr::from(rhs),
            op,
            _q: PhantomData,
        }
    }

    pub fn starts_with(self, rhs: &str) -> QuantifiedStringEdgeFilter<E, Q> {
        self.string_finish(StringOp::StartsWith, rhs)
    }

    pub fn ends_with(self, rhs: &str) -> QuantifiedStringEdgeFilter<E, Q> {
        self.string_finish(StringOp::EndsWith, rhs)
    }

    pub fn contains(self, rhs: &str) -> QuantifiedStringEdgeFilter<E, Q> {
        self.string_finish(StringOp::Contains, rhs)
    }

    pub fn not_contains(self, rhs: &str) -> QuantifiedStringEdgeFilter<E, Q> {
        self.string_finish(StringOp::NotContains, rhs)
    }

    pub fn sum(self) -> EdgeQuantified<NestedMapEdgeExpr<E>, Q> {
        EdgeQuantified { expr: NestedMapEdgeExpr { inner: self.expr, op: Op::Sum }, _q: PhantomData }
    }
    pub fn avg(self) -> EdgeQuantified<NestedMapEdgeExpr<E>, Q> {
        EdgeQuantified { expr: NestedMapEdgeExpr { inner: self.expr, op: Op::Avg }, _q: PhantomData }
    }
    pub fn min(self) -> EdgeQuantified<NestedMapEdgeExpr<E>, Q> {
        EdgeQuantified { expr: NestedMapEdgeExpr { inner: self.expr, op: Op::Min }, _q: PhantomData }
    }
    pub fn max(self) -> EdgeQuantified<NestedMapEdgeExpr<E>, Q> {
        EdgeQuantified { expr: NestedMapEdgeExpr { inner: self.expr, op: Op::Max }, _q: PhantomData }
    }
    pub fn first(self) -> EdgeQuantified<NestedMapEdgeExpr<E>, Q> {
        EdgeQuantified { expr: NestedMapEdgeExpr { inner: self.expr, op: Op::First }, _q: PhantomData }
    }
    pub fn last(self) -> EdgeQuantified<NestedMapEdgeExpr<E>, Q> {
        EdgeQuantified { expr: NestedMapEdgeExpr { inner: self.expr, op: Op::Last }, _q: PhantomData }
    }
    pub fn len(self) -> EdgeQuantified<NestedMapEdgeExpr<E>, Q> {
        EdgeQuantified { expr: NestedMapEdgeExpr { inner: self.expr, op: Op::Len }, _q: PhantomData }
    }
    pub fn any(self) -> EdgeQuantified<NestedMapEdgeExpr<E>, AnyMode> {
        EdgeQuantified { expr: NestedMapEdgeExpr { inner: self.expr, op: Op::Any }, _q: PhantomData }
    }
    pub fn all(self) -> EdgeQuantified<NestedMapEdgeExpr<E>, AllMode> {
        EdgeQuantified { expr: NestedMapEdgeExpr { inner: self.expr, op: Op::All }, _q: PhantomData }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// EdgePropertyExprOps — fluent comparison API for edge-side property expressions
// ─────────────────────────────────────────────────────────────────────────────

pub trait EdgePropertyExprOps: EdgeExpr<Output = Option<Prop>> + Sized {
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

pub trait EdgeTemporalPropOps: Sized {
    type ViewExpr: CreateView + EdgeFilterFactory + Clone + Send + Sync + 'static;
    fn into_temporal_parts(self) -> (Self::ViewExpr, String);

    fn any(self) -> EdgeQuantified<TemporalEdgePropExpr<Self::ViewExpr>, AnyMode> {
        let (view_expr, name) = self.into_temporal_parts();
        EdgeQuantified { expr: TemporalEdgePropExpr::new(view_expr, name), _q: PhantomData }
    }
    fn all(self) -> EdgeQuantified<TemporalEdgePropExpr<Self::ViewExpr>, AllMode> {
        let (view_expr, name) = self.into_temporal_parts();
        EdgeQuantified { expr: TemporalEdgePropExpr::new(view_expr, name), _q: PhantomData }
    }
    fn sum(self) -> EdgeAggregated<SumExpr<TemporalEdgePropExpr<Self::ViewExpr>>> {
        let (view_expr, name) = self.into_temporal_parts();
        EdgeAggregated { expr: SumExpr(TemporalEdgePropExpr::new(view_expr, name)) }
    }
    fn avg(self) -> EdgeAggregated<AvgExpr<TemporalEdgePropExpr<Self::ViewExpr>>> {
        let (view_expr, name) = self.into_temporal_parts();
        EdgeAggregated { expr: AvgExpr(TemporalEdgePropExpr::new(view_expr, name)) }
    }
    fn min(self) -> EdgeAggregated<MinExpr<TemporalEdgePropExpr<Self::ViewExpr>>> {
        let (view_expr, name) = self.into_temporal_parts();
        EdgeAggregated { expr: MinExpr(TemporalEdgePropExpr::new(view_expr, name)) }
    }
    fn max(self) -> EdgeAggregated<MaxExpr<TemporalEdgePropExpr<Self::ViewExpr>>> {
        let (view_expr, name) = self.into_temporal_parts();
        EdgeAggregated { expr: MaxExpr(TemporalEdgePropExpr::new(view_expr, name)) }
    }
    fn first(self) -> EdgeAggregated<FirstExpr<TemporalEdgePropExpr<Self::ViewExpr>>> {
        let (view_expr, name) = self.into_temporal_parts();
        EdgeAggregated { expr: FirstExpr(TemporalEdgePropExpr::new(view_expr, name)) }
    }
    fn last(self) -> EdgeAggregated<LastExpr<TemporalEdgePropExpr<Self::ViewExpr>>> {
        let (view_expr, name) = self.into_temporal_parts();
        EdgeAggregated { expr: LastExpr(TemporalEdgePropExpr::new(view_expr, name)) }
    }
    fn len(self) -> LenEdgeExpr<TemporalEdgePropExpr<Self::ViewExpr>> {
        let (view_expr, name) = self.into_temporal_parts();
        LenEdgeExpr(TemporalEdgePropExpr::new(view_expr, name))
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

/// Comparison operators on any [`EdgeExpr`], regardless of output type.
///
/// Unlike [`EdgePropertyExprOps`] (which is limited to `Output = Option<Prop>`),
/// this trait works for any output type — in particular `usize` for `.len()`:
///
/// ```rust,ignore
/// EdgeFilter.temporal_property("count").len().gt(3usize)
/// EdgeFilter.temporal_property("count").len().eq(0usize)
/// ```
pub trait EdgeExprFilterOps: EdgeExpr + Sized {
    fn gt<R: EdgeExpr<Output = Self::Output>>(self, rhs: R) -> BinaryCmpEdgeFilter<Self, R> {
        BinaryCmpEdgeFilter::new(self, BinaryOp::Gt, rhs)
    }
    fn ge<R: EdgeExpr<Output = Self::Output>>(self, rhs: R) -> BinaryCmpEdgeFilter<Self, R> {
        BinaryCmpEdgeFilter::new(self, BinaryOp::Ge, rhs)
    }
    fn lt<R: EdgeExpr<Output = Self::Output>>(self, rhs: R) -> BinaryCmpEdgeFilter<Self, R> {
        BinaryCmpEdgeFilter::new(self, BinaryOp::Lt, rhs)
    }
    fn le<R: EdgeExpr<Output = Self::Output>>(self, rhs: R) -> BinaryCmpEdgeFilter<Self, R> {
        BinaryCmpEdgeFilter::new(self, BinaryOp::Le, rhs)
    }
    fn eq<R: EdgeExpr<Output = Self::Output>>(self, rhs: R) -> BinaryCmpEdgeFilter<Self, R> {
        BinaryCmpEdgeFilter::new(self, BinaryOp::Eq, rhs)
    }
    fn ne<R: EdgeExpr<Output = Self::Output>>(self, rhs: R) -> BinaryCmpEdgeFilter<Self, R> {
        BinaryCmpEdgeFilter::new(self, BinaryOp::Ne, rhs)
    }
    fn starts_with<R: EdgeExpr<Output = Self::Output>>(
        self,
        rhs: R,
    ) -> StringEdgeFilter<Self, R>
    where
        Self::Output: StringComparable,
    {
        StringEdgeFilter::new(self, StringOp::StartsWith, rhs)
    }
    fn ends_with<R: EdgeExpr<Output = Self::Output>>(
        self,
        rhs: R,
    ) -> StringEdgeFilter<Self, R>
    where
        Self::Output: StringComparable,
    {
        StringEdgeFilter::new(self, StringOp::EndsWith, rhs)
    }
    fn contains<R: EdgeExpr<Output = Self::Output>>(
        self,
        rhs: R,
    ) -> StringEdgeFilter<Self, R>
    where
        Self::Output: StringComparable,
    {
        StringEdgeFilter::new(self, StringOp::Contains, rhs)
    }
    fn not_contains<R: EdgeExpr<Output = Self::Output>>(
        self,
        rhs: R,
    ) -> StringEdgeFilter<Self, R>
    where
        Self::Output: StringComparable,
    {
        StringEdgeFilter::new(self, StringOp::NotContains, rhs)
    }
    fn fuzzy_search<R: EdgeExpr<Output = Self::Output>>(
        self,
        rhs: R,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> StringEdgeFilter<Self, R>
    where
        Self::Output: StringComparable,
    {
        StringEdgeFilter::new(
            self,
            StringOp::FuzzySearch { levenshtein_distance, prefix_match },
            rhs,
        )
    }
}

impl<E: EdgeExpr> EdgeExprFilterOps for E {}

// ─────────────────────────────────────────────────────────────────────────────
// EdgeAggregated string convenience — mirrors NodeAggregated::contains etc.
// ─────────────────────────────────────────────────────────────────────────────

impl<E: EdgeExpr<Output = Option<Prop>>> EdgeAggregated<E> {
    fn str_finish(self, op: StringOp, rhs: &str) -> StringEdgeFilter<E, Prop> {
        StringEdgeFilter::new(self.expr, op, Prop::Str(ArcStr::from(rhs)))
    }

    pub fn starts_with(self, rhs: &str) -> StringEdgeFilter<E, Prop> {
        self.str_finish(StringOp::StartsWith, rhs)
    }
    pub fn ends_with(self, rhs: &str) -> StringEdgeFilter<E, Prop> {
        self.str_finish(StringOp::EndsWith, rhs)
    }
    pub fn contains(self, rhs: &str) -> StringEdgeFilter<E, Prop> {
        self.str_finish(StringOp::Contains, rhs)
    }
    pub fn not_contains(self, rhs: &str) -> StringEdgeFilter<E, Prop> {
        self.str_finish(StringOp::NotContains, rhs)
    }
    pub fn fuzzy_search(
        self,
        rhs: &str,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> StringEdgeFilter<E, Prop> {
        self.str_finish(
            StringOp::FuzzySearch { levenshtein_distance, prefix_match },
            rhs,
        )
    }
    pub fn is_in(
        self,
        values: impl IntoIterator<Item = Prop>,
    ) -> PropValueSetEdgeFilter<E> {
        PropValueSetEdgeFilter {
            expr: self.expr,
            values: values.into_iter().collect(),
            op: SetOp::IsIn,
        }
    }
    pub fn is_not_in(
        self,
        values: impl IntoIterator<Item = Prop>,
    ) -> PropValueSetEdgeFilter<E> {
        PropValueSetEdgeFilter {
            expr: self.expr,
            values: values.into_iter().collect(),
            op: SetOp::IsNotIn,
        }
    }

    pub fn sum(self) -> EdgeAggregated<SumExpr<UnwrapOptPropEdgeExpr<E>>> {
        EdgeAggregated { expr: SumExpr(UnwrapOptPropEdgeExpr(self.expr)) }
    }
    pub fn avg(self) -> EdgeAggregated<AvgExpr<UnwrapOptPropEdgeExpr<E>>> {
        EdgeAggregated { expr: AvgExpr(UnwrapOptPropEdgeExpr(self.expr)) }
    }
    pub fn min(self) -> EdgeAggregated<MinExpr<UnwrapOptPropEdgeExpr<E>>> {
        EdgeAggregated { expr: MinExpr(UnwrapOptPropEdgeExpr(self.expr)) }
    }
    pub fn max(self) -> EdgeAggregated<MaxExpr<UnwrapOptPropEdgeExpr<E>>> {
        EdgeAggregated { expr: MaxExpr(UnwrapOptPropEdgeExpr(self.expr)) }
    }
    pub fn first(self) -> EdgeAggregated<FirstExpr<UnwrapOptPropEdgeExpr<E>>> {
        EdgeAggregated { expr: FirstExpr(UnwrapOptPropEdgeExpr(self.expr)) }
    }
    pub fn last(self) -> EdgeAggregated<LastExpr<UnwrapOptPropEdgeExpr<E>>> {
        EdgeAggregated { expr: LastEdgeExpr(UnwrapOptPropEdgeExpr(self.expr)) }
    }
    pub fn len(self) -> LenEdgeExpr<UnwrapOptPropEdgeExpr<E>> {
        LenEdgeExpr(UnwrapOptPropEdgeExpr(self.expr))
    }
    pub fn any(self) -> EdgeQuantified<UnwrapOptPropEdgeExpr<E>, AnyMode> {
        EdgeQuantified { expr: UnwrapOptPropEdgeExpr(self.expr), _q: PhantomData }
    }
    pub fn all(self) -> EdgeQuantified<UnwrapOptPropEdgeExpr<E>, AllMode> {
        EdgeQuantified { expr: UnwrapOptPropEdgeExpr(self.expr), _q: PhantomData }
    }
}
