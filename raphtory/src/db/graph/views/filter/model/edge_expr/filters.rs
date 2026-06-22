//! Filter types for edge expressions — bridge from EdgeExpr to a filtered graph.
//!
//! Parallel to `node_expr/filters.rs` but for edges: `create_filter` produces
//! an `EdgeExprFilteredGraph` instead of a `NodeFilteredGraph`.

use super::{
    ops::{
        BinaryCmpEdgeOp, ListAwareCmpEdgeOp, ListAwareSetEdgeOp,
        ListAwareStringEdgeOp, PropValueSetEdgeOp, StringEdgeOp, UnaryEdgeOp,
    },
    EdgeExpr, EdgeOp,
};
use crate::db::graph::views::filter::model::edge_filter::EdgeFilter;
use crate::db::graph::views::filter::model::node_expr::filters::PropValueSetExpr;
pub(crate) use crate::db::graph::views::filter::model::{BinaryCmpExpr, StringExpr, UnaryExpr};
use crate::{
    db::{
        api::{state::ops::NotANodeFilter, view::internal::GraphView},
        graph::views::filter::{
            edge_expr_filtered_graph::EdgeExprFilteredGraph,
            model::{
                edge_filter::CompositeEdgeFilter,
                filter_operator::BinaryOp

                , CompositeExplodedEdgeFilter,
                CompositeNodeFilter, CreateFilter, TryAsCompositeFilter,
            },
        },
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use raphtory_api::core::entities::properties::prop::{Prop, PropType};
use std::sync::Arc;
// ─────────────────────────────────────────────────────────────────────────────
// validate helpers
// ─────────────────────────────────────────────────────────────────────────────

fn validate_binary_op(op: &BinaryOp, prop_type: &PropType) -> Result<(), GraphError> {
    if *prop_type != PropType::Empty
        && matches!(op, BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge)
        && *prop_type == PropType::Bool
    {
        return Err(GraphError::InvalidFilter(format!(
            "operator {:?} is not valid for boolean properties",
            op
        )));
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// BinaryCmpExpr<L, R>
// ─────────────────────────────────────────────────────────────────────────────
impl<L, R> EdgeExpr for BinaryCmpExpr<L, R, EdgeFilter>
where
    L: EdgeExpr,
    R: EdgeExpr,
{
    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let left = self.left.create_edge_op(graph.clone())?;
        let right = self.right.create_edge_op(graph)?;
        Ok(Arc::new(ListAwareCmpEdgeOp { left, right, op: self.op }))
    }
}

impl<L, R> TryAsCompositeFilter for BinaryCmpExpr<L, R, EdgeFilter>
where
    L: EdgeExpr,
    R: EdgeExpr,
{
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }
}

impl<L, R> CreateFilter for BinaryCmpExpr<L, R, EdgeFilter>
where
    L: EdgeExpr,
    R: EdgeExpr,
{
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> =
        EdgeExprFilteredGraph<G, Arc<dyn EdgeOp<Output = bool> + 'graph>>;

    type NodeFilter<'graph, G: GraphView + 'graph> = NotANodeFilter;

    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphViewOps<'graph>;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        let left = self.left.create_edge_op(graph.clone())?;
        let right = self.right.create_edge_op(graph.clone())?;
        validate_binary_op(&self.op, &left.prop_type())?;
        let op: Arc<dyn EdgeOp<Output = bool> + 'graph> =
            Arc::new(BinaryCmpEdgeOp { left, right, op: self.op });
        Ok(EdgeExprFilteredGraph::new(graph, op))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        _graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        Err(GraphError::NotNodeFilter)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// UnaryExpr<E, I>
// ─────────────────────────────────────────────────────────────────────────────

impl<E> TryAsCompositeFilter for UnaryExpr<E, EdgeFilter>
where
    E: EdgeExpr,
{
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }

    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }
}

impl<E> CreateFilter for UnaryExpr<E, EdgeFilter>
where
    E: EdgeExpr,
{
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> =
        EdgeExprFilteredGraph<G, Arc<dyn EdgeOp<Output = bool> + 'graph>>;
    type NodeFilter<'graph, G: GraphView + 'graph> = NotANodeFilter;
    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphViewOps<'graph>;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        let inner = self.expr.create_edge_op(graph.clone())?;
        let op: Arc<dyn EdgeOp<Output = bool> + 'graph> =
            Arc::new(UnaryEdgeOp { inner, op: self.op });
        Ok(EdgeExprFilteredGraph::new(graph, op))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        _graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        Err(GraphError::NotNodeFilter)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// validate_string_op — reject non-string prop types at compile time
// ─────────────────────────────────────────────────────────────────────────────

fn validate_string_op(prop_type: &PropType) -> Result<(), GraphError> {
    if *prop_type != PropType::Empty && *prop_type != PropType::Str {
        return Err(GraphError::InvalidFilter(format!(
            "string operator requires a Str property, but the property type is {}",
            prop_type
        )));
    }
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// StringExpr<L, R> — string expression filter for edges
// ─────────────────────────────────────────────────────────────────────────────


impl<L, R> EdgeExpr for StringExpr<L, R, EdgeFilter>
where
    L: EdgeExpr,
    R: EdgeExpr,
{
    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let left = self.left.create_edge_op(graph.clone())?;
        let right = self.right.create_edge_op(graph)?;
        Ok(Arc::new(ListAwareStringEdgeOp { left, right, op: self.op }))
    }
}

impl<L, R> TryAsCompositeFilter for StringExpr<L, R, EdgeFilter>
where
    L: EdgeExpr,
    R: EdgeExpr,
{
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }
    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }
    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }
}

impl<L, R> CreateFilter for StringExpr<L, R, EdgeFilter>
where
    L: EdgeExpr,
    R: EdgeExpr,
{
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> =
        EdgeExprFilteredGraph<G, Arc<dyn EdgeOp<Output = bool> + 'graph>>;
    type NodeFilter<'graph, G: GraphView + 'graph> = NotANodeFilter;
    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphViewOps<'graph>;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        let left = self.left.create_edge_op(graph.clone())?;
        let right = self.right.create_edge_op(graph.clone())?;
        validate_string_op(&left.prop_type())?;
        let op: Arc<dyn EdgeOp<Output = bool> + 'graph> =
            Arc::new(StringEdgeOp { left, right, op: self.op });
        Ok(EdgeExprFilteredGraph::new(graph, op))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        _graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        Err(GraphError::NotNodeFilter)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// PropValueSetExpr<E, EdgeFilter> — is_in / is_not_in for edge-side exprs
// ─────────────────────────────────────────────────────────────────────────────

impl<E: EdgeExpr> EdgeExpr for PropValueSetExpr<E, EdgeFilter> {
    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let inner = self.expr.create_edge_op(graph)?;
        Ok(Arc::new(ListAwareSetEdgeOp { inner, values: self.values.clone(), op: self.op }))
    }
}


impl<E: EdgeExpr> TryAsCompositeFilter for PropValueSetExpr<E, EdgeFilter> {
    fn try_as_composite_node_filter(&self) -> Result<CompositeNodeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }
    fn try_as_composite_edge_filter(&self) -> Result<CompositeEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }
    fn try_as_composite_exploded_edge_filter(
        &self,
    ) -> Result<CompositeExplodedEdgeFilter, GraphError> {
        Err(GraphError::NotSupported)
    }
}

impl<E: EdgeExpr> CreateFilter for PropValueSetExpr<E, EdgeFilter> {
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> =
        EdgeExprFilteredGraph<G, Arc<dyn EdgeOp<Output = bool> + 'graph>>;
    type NodeFilter<'graph, G: GraphView + 'graph> = NotANodeFilter;
    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphViewOps<'graph>;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        let inner = self.expr.create_edge_op(graph.clone())?;
        let op: Arc<dyn EdgeOp<Output = bool> + 'graph> =
            Arc::new(PropValueSetEdgeOp { inner, values: self.values, op: self.op });
        Ok(EdgeExprFilteredGraph::new(graph, op))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        _graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        Err(GraphError::NotNodeFilter)
    }
}
