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
//! │  .eq(5.0f64)                      ← BinaryCmpExpr   │
//! └──────────────────────────────────────────────────────────┘
//!          │  create_edge_op(graph)?   ← resolve name → prop_id
//!          ▼
//! ┌─ Compile phase (graph-bound op) ─────────────────────────┐
//! │  EdgePropOp { graph, prop_id }   ← EdgeOp                │
//! │  apply(storage, edge_ref)                                 │
//! │    → edge_ref reads column prop_id in O(1)               │
//! └──────────────────────────────────────────────────────────┘
//! ```

use raphtory_api::core::entities::{edges::edge_ref::EdgeRef, properties::prop::PropType};
use raphtory_storage::graph::graph::GraphStorage;

pub mod filters;
pub mod ops;

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

    /// Returns `Some(value)` if the edge op has a constant global value
    fn const_value(&self) -> Option<Self::Output> {
        None
    }
}
