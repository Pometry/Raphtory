//! EdgeExpr impls for the shared Property/Metadata structs and scalar types.

use super::{ops::{EdgeMetaOp, EdgePropOp}, EdgeExpr, EdgeOp};
use crate::{
    db::api::{state::ops::Const, view::internal::GraphView},
    db::graph::views::filter::model::{edge_filter::EdgeFilter, node_expr::EntityExpr, Metadata, Property},
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
        let prop_id = graph
            .edge_meta()
            .get_prop_id(&self.name, false)
            .ok_or_else(|| GraphError::PropertyMissingError(self.name.clone()))?;
        Ok(Arc::new(EdgePropOp { graph, prop_id }))
    }
}

impl EdgeExpr for Metadata {
    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let prop_id = graph
            .edge_meta()
            .get_prop_id(&self.name, true)
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

