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
//! │  .eq(5.0f64)                      ← BinaryCmpFilter   │
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
        graph::views::filter::model::node_expr::EntityExpr,
    },
    errors::GraphError,
};
use raphtory_api::core::entities::{
    edges::edge_ref::EdgeRef,
    properties::prop::{Prop, PropType},
};
use raphtory_storage::graph::graph::GraphStorage;
use std::sync::Arc;

pub mod exprs;
pub mod filters;
pub mod ops;

pub use exprs::*;
pub use filters::*;
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
pub(crate) trait EdgeExpr: EntityExpr + Clone + Send + Sync + 'static {
    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError>;
}



