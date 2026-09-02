use crate::{
    db::{
        api::{state::ops::NodeOp, view::internal::GraphView},
        graph::views::filter::model::CreateView,
    },
    errors::GraphError,
};
use raphtory_api::core::entities::properties::prop::{Prop, PropType};
use std::sync::Arc;

pub mod dyn_expr;
pub mod exprs;
pub mod filters;
pub mod ops;

#[cfg(test)]
mod tests;

pub use super::{Metadata, Property};
use crate::db::graph::views::filter::model::{
    edge_expr::EdgeOp, filter_operator::ElemQual, EntityMarker,
};
pub use dyn_expr::*;
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
pub trait CreateOp: EntityExpr + Clone + Send + Sync + 'static {
    /// Compile the expression against a specific graph view.
    ///
    /// Any name→ID resolution (property, metadata) happens here, once.
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        Err(GraphError::NotNodeFilter)
    }

    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        Err(GraphError::NotEdgeFilter)
    }

    /// Compile the expression for use as the lhs of a comparison, separating
    /// any leading `any()`/`all()` qualifiers from the value expression they
    /// qualify. The default has no qualifiers; `AnyExpr`/`AllExpr` strip
    /// themselves and record their collapse mode instead of aggregating.
    fn create_qualified_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<(Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, Vec<ElemQual>), GraphError> {
        Ok((self.create_node_op(graph)?, Vec::new()))
    }

    fn create_qualified_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<(Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, Vec<ElemQual>), GraphError> {
        Ok((self.create_edge_op(graph)?, Vec::new()))
    }
}

pub trait Marker: Into<EntityMarker> + Copy + Send + Sync + 'static {}

impl<M: Into<EntityMarker> + Copy + Send + Sync + 'static> Marker for M {}

pub trait EntityExpr: Clone + Send + Sync + 'static {
    type Marker: Marker;

    fn entity(&self) -> Self::Marker;

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

/// Marker for types that initiate a filter expression chain (LHS receiver for
/// `.eq` / `.gt` / `.contains` / ...).
///
/// Scoped narrowly (not blanket-impl'd for every `EntityExpr`) to avoid name
/// collisions with stdlib methods like `str::contains` / `PartialOrd::gt` on
/// primitive `EntityExpr` types (`String`, `&str`, `usize`, numerics, `Prop`).
///
/// Mirrors the same trick used by `EntityAggOps` for `min`/`max`/`sum`.
pub trait EntityExprBuilder: EntityExpr {}

/// Scopes an expression to a view chain: the inner expression is compiled against the view the
/// chain constructs over the incoming graph. This is how a factory chain (window, latest, layers)
/// carries its view into a unit expression such as a validity predicate.
#[derive(Clone)]
pub struct Scoped<V, T> {
    pub view: V,
    pub inner: T,
}

impl<V: CreateView, T: EntityExpr> EntityExpr for Scoped<V, T> {
    type Marker = T::Marker;

    fn entity(&self) -> Self::Marker {
        self.inner.entity()
    }

    fn prop_type(&self) -> PropType {
        self.inner.prop_type()
    }

    fn nullable(&self) -> bool {
        self.inner.nullable()
    }
}

impl<V: CreateView, T: CreateOp> CreateOp for Scoped<V, T> {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        self.inner.create_node_op(self.view.create_view(graph)?)
    }

    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        self.inner.create_edge_op(self.view.create_view(graph)?)
    }
}
