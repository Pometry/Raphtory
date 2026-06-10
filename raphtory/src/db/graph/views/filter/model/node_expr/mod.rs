use crate::{
    db::api::{state::ops::NodeOp, view::internal::GraphView},
    errors::GraphError,
};
use raphtory_api::core::entities::properties::prop::PropType;
use std::sync::Arc;

pub mod exprs;
pub mod filters;
pub mod ops;

#[cfg(test)]
mod tests;

pub use exprs::*;
pub use filters::*;
pub use ops::*;

// ─────────────────────────────────────────────────────────────────────────────
// NodeExpr — typed node expression with associated Output type
// ─────────────────────────────────────────────────────────────────────────────

/// A typed expression that produces a value per node.
///
/// `Output` carries nullability only where the value can genuinely be absent:
/// `Option<Prop>` for properties/metadata, `Option<ArcStr>` for node type.
/// Always-present values use non-optional types: `usize` for degree, `String` for name.
///
/// Calling `create_node_op` resolves name→ID lookups once against the graph,
/// returning a `NodeOp` that evaluates in O(1) per node.
///
/// Usage:
/// ```rust,ignore
/// NodeFilter::degree().gt(2usize)
/// NodeFilter::out_degree().gt(NodeFilter::in_degree())
/// NodeFilter::property("age").gt(30i64)
/// NodeFilter::name().eq("Alice")
/// ```
///
pub trait NodeExpr: Clone + Send + Sync + 'static {
    type Output: Clone + Send + Sync + 'static;

    /// Compile the expression against a specific graph view.
    ///
    /// Any name→ID resolution (property, metadata) happens here, once.
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Self::Output> + 'g>, GraphError>;

    /// A priory known type (for early validation where possible)
    fn prop_type(&self) -> PropType {
        PropType::Empty
    }
}
