use crate::{
    db::{
        api::{state::ops::NodeOp, view::internal::GraphView},
        graph::views::filter::model::CreateView,
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

pub use super::{Metadata, Property};
use crate::db::graph::views::filter::model::{edge_expr::EdgeOp, node_filter::NodeFilter};
pub use exprs::*;
pub use filters::*;
pub use ops::*;

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
    type Marker: Copy + Default + 'static;

    fn entity() -> Self::Marker {
        Self::Marker::default()
    }

    /// A priory known type (for early validation where possible)
    fn prop_type(&self) -> PropType {
        PropType::Empty
    }

    /// Whether this expression can produce `None` at runtime.
    ///
    /// Defaults to `true` (most expressions read optional properties).
    /// Override to `false` for expressions that always produce `Some(_)`
    /// (e.g. degree). Filters like `is_some`/`is_none` are meaningless on
    /// non-nullable expressions and should be rejected at compile time.
    fn nullable(&self) -> bool {
        true
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// TemporalPropOps — unified aggregation and comparison on TemporalProp<E>
// ─────────────────────────────────────────────────────────────────────────────

/// Aggregation and comparison operators on `TemporalProp<E>`, unified for both
/// node-side (`E: NodeFilterFactory`) and edge-side (`E: EdgeFilterFactory`).
///
/// ```rust,ignore
/// NodeFilter.property("score").temporal().sum().gt(100i64)
/// NodeFilter.property("score").temporal().gt(10i64).any()
/// EdgeFilter.property("score").temporal().sum().gt(100i64)
/// EdgeFilter.property("score").temporal().gt(10i64).any()
/// ```
pub trait TemporalPropOps: Sized {
    type ViewExpr: CreateView + EntityExpr + Clone + Send + Sync + 'static;
    fn into_temporal_parts(self) -> (Self::ViewExpr, String);

    fn into_expr(self) -> TemporalExpr<Self::ViewExpr> {
        let (view_expr, name) = self.into_temporal_parts();
        TemporalExpr { view_expr, name }
    }
    fn sum(self) -> SumExpr<TemporalExpr<Self::ViewExpr>> { SumExpr(self.into_expr()) }
    fn avg(self) -> AvgExpr<TemporalExpr<Self::ViewExpr>> { AvgExpr(self.into_expr()) }
    fn min(self) -> MinExpr<TemporalExpr<Self::ViewExpr>> { MinExpr(self.into_expr()) }
    fn max(self) -> MaxExpr<TemporalExpr<Self::ViewExpr>> { MaxExpr(self.into_expr()) }
    fn first(self) -> FirstExpr<TemporalExpr<Self::ViewExpr>> { FirstExpr(self.into_expr()) }
    fn last(self) -> LastExpr<TemporalExpr<Self::ViewExpr>> { LastExpr(self.into_expr()) }
    fn len(self) -> LenExpr<TemporalExpr<Self::ViewExpr>> { LenExpr(self.into_expr()) }
    fn any(self) -> AnyExpr<TemporalExpr<Self::ViewExpr>> { AnyExpr(self.into_expr()) }
    fn all(self) -> AllExpr<TemporalExpr<Self::ViewExpr>> { AllExpr(self.into_expr()) }

    fn gt<R: EntityExpr>(self, rhs: R) -> BinaryCmpExpr<TemporalExpr<Self::ViewExpr>, R, <Self::ViewExpr as EntityExpr>::Marker> {
        self.into_expr().gt(rhs)
    }
    fn ge<R: EntityExpr>(self, rhs: R) -> BinaryCmpExpr<TemporalExpr<Self::ViewExpr>, R, <Self::ViewExpr as EntityExpr>::Marker> {
        self.into_expr().ge(rhs)
    }
    fn lt<R: EntityExpr>(self, rhs: R) -> BinaryCmpExpr<TemporalExpr<Self::ViewExpr>, R, <Self::ViewExpr as EntityExpr>::Marker> {
        self.into_expr().lt(rhs)
    }
    fn le<R: EntityExpr>(self, rhs: R) -> BinaryCmpExpr<TemporalExpr<Self::ViewExpr>, R, <Self::ViewExpr as EntityExpr>::Marker> {
        self.into_expr().le(rhs)
    }
    fn eq<R: EntityExpr>(self, rhs: R) -> BinaryCmpExpr<TemporalExpr<Self::ViewExpr>, R, <Self::ViewExpr as EntityExpr>::Marker> {
        self.into_expr().eq(rhs)
    }
    fn ne<R: EntityExpr>(self, rhs: R) -> BinaryCmpExpr<TemporalExpr<Self::ViewExpr>, R, <Self::ViewExpr as EntityExpr>::Marker> {
        self.into_expr().ne(rhs)
    }
    fn contains<R: EntityExpr>(self, rhs: R) -> StringExpr<TemporalExpr<Self::ViewExpr>, R, <Self::ViewExpr as EntityExpr>::Marker> {
        self.into_expr().contains(rhs)
    }
    fn starts_with<R: EntityExpr>(self, rhs: R) -> StringExpr<TemporalExpr<Self::ViewExpr>, R, <Self::ViewExpr as EntityExpr>::Marker> {
        self.into_expr().starts_with(rhs)
    }
    fn ends_with<R: EntityExpr>(self, rhs: R) -> StringExpr<TemporalExpr<Self::ViewExpr>, R, <Self::ViewExpr as EntityExpr>::Marker> {
        self.into_expr().ends_with(rhs)
    }
    fn not_contains<R: EntityExpr>(self, rhs: R) -> StringExpr<TemporalExpr<Self::ViewExpr>, R, <Self::ViewExpr as EntityExpr>::Marker> {
        self.into_expr().not_contains(rhs)
    }
    fn fuzzy_search<R: EntityExpr>(
        self,
        rhs: R,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> StringExpr<TemporalExpr<Self::ViewExpr>, R, <Self::ViewExpr as EntityExpr>::Marker> {
        self.into_expr().fuzzy_search(rhs, levenshtein_distance, prefix_match)
    }
    fn is_in<V: Into<Prop>>(self, values: impl IntoIterator<Item = V>) -> PropValueSetExpr<TemporalExpr<Self::ViewExpr>, <Self::ViewExpr as EntityExpr>::Marker> {
        self.into_expr().is_in(values)
    }
    fn is_not_in<V: Into<Prop>>(self, values: impl IntoIterator<Item = V>) -> PropValueSetExpr<TemporalExpr<Self::ViewExpr>, <Self::ViewExpr as EntityExpr>::Marker> {
        self.into_expr().is_not_in(values)
    }
    fn is_true(self) -> BinaryCmpExpr<TemporalExpr<Self::ViewExpr>, Prop, <Self::ViewExpr as EntityExpr>::Marker> {
        self.into_expr().is_true()
    }
    fn is_false(self) -> BinaryCmpExpr<TemporalExpr<Self::ViewExpr>, Prop, <Self::ViewExpr as EntityExpr>::Marker> {
        self.into_expr().is_false()
    }
}

impl<E: CreateView + EntityExpr + Clone + Send + Sync + 'static> TemporalPropOps for TemporalProp<E> {
    type ViewExpr = E;
    fn into_temporal_parts(self) -> (E, String) {
        (self.view_expr, self.name)
    }
}
