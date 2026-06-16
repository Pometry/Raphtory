//! EdgeExpr impls for the shared Property/Metadata structs and scalar types.

use super::{ops::{EdgeMetaOp, EdgePropOp, TemporalEdgePropOp}, EdgeExpr, EdgeOp};
use crate::{
    db::api::{state::ops::Const, view::internal::GraphView},
    db::graph::views::filter::model::{node_expr::EntityExpr, Metadata, Property},
    errors::GraphError,
};
use raphtory_api::core::entities::properties::prop::Prop;
use std::sync::Arc;
// ─────────────────────────────────────────────────────────────────────────────
// Property / Metadata — EdgeExpr impls
// ─────────────────────────────────────────────────────────────────────────────

impl EdgeExpr for Property {
    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let (prop_id, _) = graph
            .edge_meta()
            .get_prop_id_and_type(&self.name, false)
            .ok_or_else(|| GraphError::PropertyMissingError(self.name.clone()))?;
        Ok(Arc::new(EdgePropOp { graph, prop_id }))
    }
}

impl EdgeExpr for Metadata {
    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let (prop_id, _) = graph
            .edge_meta()
            .get_prop_id_and_type(&self.name, true)
            .ok_or_else(|| GraphError::MetadataMissingError(self.name.clone()))?;
        Ok(Arc::new(EdgeMetaOp { graph, prop_id }))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Scalar EdgeExpr impls — literal RHS values
// ─────────────────────────────────────────────────────────────────────────────

impl EdgeExpr for Prop {
    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        Ok(Arc::new(Const(Some(self.clone()))))
    }
}

macro_rules! impl_edge_expr_for_numeric {
    ($prim:ty, $variant:ident) => {
        impl EdgeExpr for $prim {
            fn create_edge_op<'g, G: GraphView + 'g>(
                &self,
                _graph: G,
            ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
                Ok(Arc::new(Const(Some(Prop::$variant(*self)))))
            }
        }
    };
}

impl_edge_expr_for_numeric!(i32, I32);
impl_edge_expr_for_numeric!(i64, I64);
impl_edge_expr_for_numeric!(u32, U32);
impl_edge_expr_for_numeric!(u64, U64);
impl_edge_expr_for_numeric!(f32, F32);
impl_edge_expr_for_numeric!(f64, F64);
impl_edge_expr_for_numeric!(bool, Bool);
impl_edge_expr_for_numeric!(u8, U8);
impl_edge_expr_for_numeric!(u16, U16);

impl EdgeExpr for &'static str {
    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        Ok(Arc::new(Const(Some(Prop::Str(
            raphtory_api::core::storage::arc_str::ArcStr::from(*self),
        )))))
    }
}

impl EdgeExpr for usize {
    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        Ok(Arc::new(Const(Some(Prop::U64(*self as u64)))))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// TemporalEdgePropExpr<E> — all temporal values of a property in the view window
// ─────────────────────────────────────────────────────────────────────────────

/// Parallel to `TemporalPropertyExpr` but for edges: reads from `edge_meta()`.
///
/// Produced by `EdgeTemporalPropOps::into_expr()` on `TemporalProp<E>` when `E: EdgeFilterFactory`,
/// or implicitly inside `.sum()`, `.any()`, etc.
/// Returns `Some(Prop::List([...]))` of all temporal values within the view window.
#[derive(Clone)]
pub struct TemporalEdgePropExpr<E: Clone> {
    pub(crate) view_expr: E,
    pub(crate) name: String,
}

impl<E: Clone> TemporalEdgePropExpr<E> {
    pub fn new(view_expr: E, name: impl Into<String>) -> Self {
        Self { view_expr, name: name.into() }
    }
}

impl<E: crate::db::graph::views::filter::model::CreateView + Clone + Send + Sync + 'static>
    EntityExpr for TemporalEdgePropExpr<E>
{
}

impl<E: crate::db::graph::views::filter::model::CreateView + Clone + Send + Sync + 'static>
    EdgeExpr for TemporalEdgePropExpr<E>
{
    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let (prop_id, _) = graph
            .edge_meta()
            .get_prop_id_and_type(&self.name, false)
            .ok_or_else(|| GraphError::PropertyMissingError(self.name.clone()))?;
        let graph = self.view_expr.create_view(graph)?;
        Ok(Arc::new(TemporalEdgePropOp { graph, prop_id }))
    }
}
