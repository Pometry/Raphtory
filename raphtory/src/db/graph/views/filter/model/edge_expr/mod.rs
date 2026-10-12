//! The per-edge evaluator an edge tree compiles to: [`EdgeOp`], the edge
//! counterpart of [`NodeOp`], with the ops that implement it in [`ops`].
//!
//! # Two-phase pipeline (same as for a node tree)
//!
//! ```text
//! ┌─ Build phase (pure data, no graph) ──────────────────────┐
//! │  EdgeFilter.property("weight")    ← PropertyTerm<EdgeLeaf>│
//! │  .eq(5.0f64)                      ← Expr::Cmp             │
//! └──────────────────────────────────────────────────────────┘
//!          │  compiled against graph   ← resolve name → prop_id
//!          ▼
//! ┌─ Compile phase (graph-bound op) ─────────────────────────┐
//! │  EdgePropOp { graph, prop_id }   ← EdgeOp                │
//! │  apply(storage, edge_ref)                                 │
//! │    → edge_ref reads column prop_id in O(1)               │
//! └──────────────────────────────────────────────────────────┘
//! ```

use raphtory_api::core::{
    entities::{properties::prop::PropType, LayerId},
    storage::timeindex::EventTime,
};
use raphtory_storage::graph::graph::GraphStorage;
use storage::EdgeEntryRef;

pub mod ops;

// ─────────────────────────────────────────────────────────────────────────────
// EdgeOp — compiled evaluator: storage entry → typed value
// ─────────────────────────────────────────────────────────────────────────────

/// A compiled edge evaluator: given an edge's storage entry, returns a typed value.
///
/// Parallel to [`NodeOp`] — same contract but the subject is an edge.
pub trait EdgeOp: Send + Sync {
    type Output: Clone + Send + Sync;

    /// The value for the edge as a whole.
    fn apply(&self, storage: &GraphStorage, edge: EdgeEntryRef) -> Self::Output;

    /// The value for the edge seen in one layer.
    fn apply_layer(
        &self,
        storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
    ) -> Self::Output;

    /// The value for one exploded instance of the edge.
    fn apply_exploded(
        &self,
        storage: &GraphStorage,
        edge: EdgeEntryRef,
        layer: LayerId,
        t: EventTime,
    ) -> Self::Output;

    fn prop_type(&self) -> PropType {
        PropType::Empty
    }

    /// Returns `Some(value)` if the edge op has a constant global value
    fn const_value(&self) -> Option<Self::Output> {
        None
    }
}
