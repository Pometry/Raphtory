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

