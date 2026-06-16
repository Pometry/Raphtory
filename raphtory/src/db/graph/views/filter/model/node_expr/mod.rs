use crate::{
    db::api::{state::ops::NodeOp, view::internal::GraphView},
    db::graph::views::filter::model::{
        CreateView,
        filter_operator::{BinaryOp, StringOp},
        node_filter::NodeFilterFactory,
    },
    errors::GraphError,
};
use raphtory_api::core::entities::properties::prop::{Prop, PropType};
use std::sync::Arc;

pub mod exprs;
pub mod filters;
pub mod ops;

#[cfg(test)]
mod tests;

pub use exprs::*;
pub use filters::*;
pub use ops::*;
use crate::db::graph::views::filter::model::edge_expr::EdgeOp;
pub use super::{Metadata, Property};

// ─────────────────────────────────────────────────────────────────────────────
// NodeExpr — typed node expression with associated Output type
// ─────────────────────────────────────────────────────────────────────────────

/// A typed expression that produces a value per node.
///
/// All expressions produce `Option<Prop>` — field values (`id`, `name`, `degree`) are
/// mapped into `Prop` variants; absent values (missing property, unset node type) map to `None`.
///
/// Calling `create_node_op` resolves name→ID lookups once against the graph,
/// returning a `NodeOp` that evaluates in O(1) per node.
///
/// Usage:
/// ```rust,ignore
/// NodeFilter.degree().gt(2usize)
/// NodeFilter.out_degree().gt(NodeFilter.in_degree())
/// NodeFilter.property("age").gt(30i64)
/// NodeFilter.name().eq("Alice")
/// NodeFilter.property("score").temporal().sum().gt(100i64)
/// NodeFilter.property("score").temporal().gt(10i64).any()
/// ```
///
pub trait NodeExpr: EntityExpr + Clone + Send + Sync + 'static {
    /// Compile the expression against a specific graph view.
    ///
    /// Any name→ID resolution (property, metadata) happens here, once.
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError>;
}

pub trait EntityExpr: Clone + Send + Sync + 'static {
    /// A priory known type (for early validation where possible)
    fn prop_type(&self) -> PropType {
        PropType::Empty
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// NodeTemporalPropOps — aggregation and direct comparison on TemporalProp<E>
// ─────────────────────────────────────────────────────────────────────────────

/// Aggregation and comparison operators on `TemporalProp<E>` when `E: NodeFilterFactory`.
///
/// Provides both aggregation (`.sum()`, `.avg()`, `.len()`, …) and direct
/// element-wise comparison (`.gt()`, `.eq()`, `.contains()`, …) so users
/// never have to call `.into_expr()` explicitly:
///
/// ```rust,ignore
/// NodeFilter.property("score").temporal().sum().gt(100i64)
/// NodeFilter.property("score").temporal().gt(10i64).any()
/// NodeFilter.property("score").temporal().len().gt(3usize)
/// NodeFilter.property("label").temporal().contains("rock").any()
/// ```
pub trait NodeTemporalPropOps: Sized {
    type ViewExpr: CreateView + NodeFilterFactory + Clone + Send + Sync + 'static;
    fn into_temporal_parts(self) -> (Self::ViewExpr, String);

    fn into_expr(self) -> TemporalPropertyExpr<Self::ViewExpr> {
        let (view_expr, name) = self.into_temporal_parts();
        TemporalPropertyExpr { view_expr, name }
    }
    fn sum(self) -> SumExpr<TemporalPropertyExpr<Self::ViewExpr>> {
        SumExpr(self.into_expr())
    }
    fn avg(self) -> AvgExpr<TemporalPropertyExpr<Self::ViewExpr>> {
        AvgExpr(self.into_expr())
    }
    fn min(self) -> MinExpr<TemporalPropertyExpr<Self::ViewExpr>> {
        MinExpr(self.into_expr())
    }
    fn max(self) -> MaxExpr<TemporalPropertyExpr<Self::ViewExpr>> {
        MaxExpr(self.into_expr())
    }
    fn first(self) -> FirstExpr<TemporalPropertyExpr<Self::ViewExpr>> {
        FirstExpr(self.into_expr())
    }
    fn last(self) -> LastExpr<TemporalPropertyExpr<Self::ViewExpr>> {
        LastExpr(self.into_expr())
    }
    fn len(self) -> LenExpr<TemporalPropertyExpr<Self::ViewExpr>> {
        LenExpr(self.into_expr())
    }
    fn any(self) -> AnyExpr<TemporalPropertyExpr<Self::ViewExpr>> {
        AnyExpr(self.into_expr())
    }
    fn all(self) -> AllExpr<TemporalPropertyExpr<Self::ViewExpr>> {
        AllExpr(self.into_expr())
    }

    fn gt<R: NodeExpr>(self, rhs: R) -> BinaryCmpNodeFilter<TemporalPropertyExpr<Self::ViewExpr>, R> {
        BinaryCmpNodeFilter::new(self.into_expr(), BinaryOp::Gt, rhs)
    }
    fn ge<R: NodeExpr>(self, rhs: R) -> BinaryCmpNodeFilter<TemporalPropertyExpr<Self::ViewExpr>, R> {
        BinaryCmpNodeFilter::new(self.into_expr(), BinaryOp::Ge, rhs)
    }
    fn lt<R: NodeExpr>(self, rhs: R) -> BinaryCmpNodeFilter<TemporalPropertyExpr<Self::ViewExpr>, R> {
        BinaryCmpNodeFilter::new(self.into_expr(), BinaryOp::Lt, rhs)
    }
    fn le<R: NodeExpr>(self, rhs: R) -> BinaryCmpNodeFilter<TemporalPropertyExpr<Self::ViewExpr>, R> {
        BinaryCmpNodeFilter::new(self.into_expr(), BinaryOp::Le, rhs)
    }
    fn eq<R: NodeExpr>(self, rhs: R) -> BinaryCmpNodeFilter<TemporalPropertyExpr<Self::ViewExpr>, R> {
        BinaryCmpNodeFilter::new(self.into_expr(), BinaryOp::Eq, rhs)
    }
    fn ne<R: NodeExpr>(self, rhs: R) -> BinaryCmpNodeFilter<TemporalPropertyExpr<Self::ViewExpr>, R> {
        BinaryCmpNodeFilter::new(self.into_expr(), BinaryOp::Ne, rhs)
    }
    fn contains<R: NodeExpr>(self, rhs: R) -> StringNodeFilter<TemporalPropertyExpr<Self::ViewExpr>, R> {
        StringNodeFilter::new(self.into_expr(), StringOp::Contains, rhs)
    }
    fn starts_with<R: NodeExpr>(self, rhs: R) -> StringNodeFilter<TemporalPropertyExpr<Self::ViewExpr>, R> {
        StringNodeFilter::new(self.into_expr(), StringOp::StartsWith, rhs)
    }
    fn ends_with<R: NodeExpr>(self, rhs: R) -> StringNodeFilter<TemporalPropertyExpr<Self::ViewExpr>, R> {
        StringNodeFilter::new(self.into_expr(), StringOp::EndsWith, rhs)
    }
    fn not_contains<R: NodeExpr>(self, rhs: R) -> StringNodeFilter<TemporalPropertyExpr<Self::ViewExpr>, R> {
        StringNodeFilter::new(self.into_expr(), StringOp::NotContains, rhs)
    }
    fn fuzzy_search<R: NodeExpr>(
        self,
        rhs: R,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> StringNodeFilter<TemporalPropertyExpr<Self::ViewExpr>, R> {
        StringNodeFilter::new(
            self.into_expr(),
            StringOp::FuzzySearch { levenshtein_distance, prefix_match },
            rhs,
        )
    }
    fn is_true(self) -> BinaryCmpNodeFilter<TemporalPropertyExpr<Self::ViewExpr>, Prop> {
        BinaryCmpNodeFilter::new(self.into_expr(), BinaryOp::Eq, Prop::Bool(true))
    }
    fn is_false(self) -> BinaryCmpNodeFilter<TemporalPropertyExpr<Self::ViewExpr>, Prop> {
        BinaryCmpNodeFilter::new(self.into_expr(), BinaryOp::Eq, Prop::Bool(false))
    }
}

impl<E: CreateView + NodeFilterFactory + Clone + Send + Sync + 'static> NodeTemporalPropOps
    for TemporalProp<E>
{
    type ViewExpr = E;
    fn into_temporal_parts(self) -> (E, String) {
        (self.view_expr, self.name)
    }
}
