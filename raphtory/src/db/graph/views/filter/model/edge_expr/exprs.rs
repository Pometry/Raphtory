//! EdgeExpr impls for the shared Property/Metadata structs and scalar types.

use super::{EdgeExpr, EdgeMetaOp, EdgeOp, EdgePropOp, LenEdgeOp, TemporalEdgePropOp};
use crate::db::graph::views::filter::model::property_filter::Op;
use crate::{
    db::api::{state::ops::Const, view::internal::GraphView},
    db::graph::views::filter::model::{Metadata, Property},
    errors::GraphError,
};
use raphtory_api::core::entities::properties::prop::{Prop, PropType};
use std::sync::Arc;
use crate::db::graph::views::filter::model::node_expr::{AvgEdgeOp, FirstEdgeOp, LastEdgeOp, MaxEdgeOp, MinEdgeOp, SumEdgeOp};
// ─────────────────────────────────────────────────────────────────────────────
// Property / Metadata — EdgeExpr impls
// ─────────────────────────────────────────────────────────────────────────────

impl EdgeExpr for Property {
    type Output = Option<Prop>;

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
    type Output = Option<Prop>;

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
// IntoPropEdgeExpr — normalises RHS values to EdgeExpr<Output = Option<Prop>>
// ─────────────────────────────────────────────────────────────────────────────

pub trait IntoPropEdgeExpr {
    type Expr: EdgeExpr<Output = Option<Prop>>;
    fn into_prop_edge_expr(self) -> Self::Expr;
}

impl<T: EdgeExpr<Output = Option<Prop>>> IntoPropEdgeExpr for T {
    type Expr = T;
    fn into_prop_edge_expr(self) -> T {
        self
    }
}


// ─────────────────────────────────────────────────────────────────────────────
// Scalar EdgeExpr impls — literal RHS values
// ─────────────────────────────────────────────────────────────────────────────

impl EdgeExpr for Prop {
    type Output = Option<Prop>;

    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        Ok(Arc::new(Const(Some(self.clone()))))
    }

    fn prop_type(&self) -> PropType {
        self.dtype()
    }
}

macro_rules! impl_edge_expr_for_numeric {
    ($prim:ty, $variant:ident) => {
        impl EdgeExpr for $prim {
            type Output = Option<Prop>;

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
    type Output = Option<Prop>;

    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        Ok(Arc::new(Const(Some(Prop::Str(
            raphtory_api::core::storage::arc_str::ArcStr::from(*self),
        )))))
    }

    fn prop_type(&self) -> PropType {
        PropType::Str
    }
}

impl EdgeExpr for usize {
    type Output = usize;

    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = usize> + 'g>, GraphError> {
        Ok(Arc::new(Const(*self)))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// TemporalEdgePropExpr<E> — all temporal values of a property in the view window
// ─────────────────────────────────────────────────────────────────────────────

/// Parallel to `TemporalPropertyExpr` but for edges: reads from `edge_meta()`.
///
/// Produced by `TemporalProp<E>` internal conversion when `E: EdgeFilterFactory`.
/// Returns `Prop::List` of all temporal values within the view window.
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
    EdgeExpr for TemporalEdgePropExpr<E>
{
    type Output = Prop;

    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Prop> + 'g>, GraphError> {
        let (prop_id, _) = graph
            .edge_meta()
            .get_prop_id_and_type(&self.name, false)
            .ok_or_else(|| GraphError::PropertyMissingError(self.name.clone()))?;
        let graph = self.view_expr.create_view(graph)?;
        Ok(Arc::new(TemporalEdgePropOp { graph, prop_id }))
    }
}

// LenEdgeExpr written explicitly: Output = usize, not Option<Prop>
#[derive(Clone)]
pub struct LenEdgeExpr<E: EdgeExpr<Output = Prop>>(pub E);

impl<E: EdgeExpr<Output = Prop>> EdgeExpr for LenEdgeExpr<E> {
    type Output = usize;

    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = usize> + 'g>, GraphError> {
        let inner = self.0.create_edge_op(graph)?;
        Ok(Arc::new(LenEdgeOp { inner }))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// UnwrapOptPropEdgeExpr<E> — bridges Option<Prop> → Prop for nested aggregation
// ─────────────────────────────────────────────────────────────────────────────

// Bridges `E: EdgeExpr<Output = Option<Prop>>` to `EdgeExpr<Output = Prop>`,
// enabling aggregator exprs to operate on values produced by a prior aggregation.
// 
// Used when chaining e.g. `.temporal().last().sum()`:
// `last()` produces `EdgeAggregated<LastEdgeExpr<...>>` with `Output = Option<Prop>`;
// `sum()` wraps in `SumEdgeExpr<UnwrapOptPropEdgeExpr<LastEdgeExpr<...>>>`.
// #[derive(Clone)]
// pub struct UnwrapOptPropEdgeExpr<E: EdgeExpr<Output = Option<Prop>>>(pub E);
//
// impl<E: EdgeExpr<Output = Option<Prop>>> EdgeExpr for UnwrapOptPropEdgeExpr<E> {
//     type Output = Prop;
//
//     fn create_edge_op<'g, G: crate::db::api::view::internal::GraphView + 'g>(
//         &self,
//         graph: G,
//     ) -> Result<std::sync::Arc<dyn EdgeOp<Output = Prop> + 'g>, crate::errors::GraphError> {
//         let inner = self.0.create_edge_op(graph)?;
//         Ok(std::sync::Arc::new(UnwrapOptPropEdgeOp { inner }))
//     }
// }

// ─────────────────────────────────────────────────────────────────────────────
// NestedMapEdgeExpr<E> — per-element aggregation / quantification on a Prop::List
// ─────────────────────────────────────────────────────────────────────────────

// Applies a per-element op to each element of a `Prop::List` produced by `E`.
//
// Used for chained expressions like `.temporal().any().sum()`.
// #[derive(Clone)]
// pub struct NestedMapEdgeExpr<E: EdgeExpr<Output = Prop>> {
//     pub inner: E,
//     pub op: Op,
// }
//
// impl<E: EdgeExpr<Output = Prop>> EdgeExpr for NestedMapEdgeExpr<E> {
//     type Output = Prop;
//
//     fn create_edge_op<'g, G: crate::db::api::view::internal::GraphView + 'g>(
//         &self,
//         graph: G,
//     ) -> Result<std::sync::Arc<dyn EdgeOp<Output = Prop> + 'g>, crate::errors::GraphError> {
//         let inner = self.inner.create_edge_op(graph)?;
//         Ok(std::sync::Arc::new(NestedMapEdgeOp { inner, op: self.op }))
//     }
// }
