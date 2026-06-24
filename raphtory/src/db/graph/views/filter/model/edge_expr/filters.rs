//! Filter types for edge expressions — bridge from EdgeExpr to a filtered graph.
//!
//! Parallel to `node_expr/filters.rs` but for edges: `create_filter` produces
//! an `EdgeExprFilteredGraph` instead of a `NodeFilteredGraph`.

use super::{
    ops::{
        BinaryCmpEdgeOp, ListAwareCmpEdgeOp, ListAwareSetEdgeOp, ListAwareStringEdgeOp,
        PropValueSetEdgeOp, StringEdgeOp, UnaryEdgeOp,
    },
    EdgeOp,
};
use crate::db::graph::views::filter::exploded_edge_expr_filtered_graph::ExplodedEdgeExprFilteredGraph;
use crate::db::graph::views::filter::model::{validate_binary_op, validate_string_op, ExplodedEdgeFilter};
pub(crate) use crate::db::graph::views::filter::model::{BinaryCmpExpr, StringExpr, UnaryExpr};
use crate::{
    db::{
        api::{state::ops::NotANodeFilter, view::internal::GraphView},
        graph::views::filter::{
            edge_expr_filtered_graph::EdgeExprFilteredGraph,
            model::{
                edge_filter::{CompositeEdgeFilter, EdgeFilter}
                ,
                node_expr::{filters::PropValueSetExpr, CreateOp},
                CompositeExplodedEdgeFilter, CompositeNodeFilter, CreateFilter,
                TryAsCompositeFilter,
            },
        },
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use raphtory_api::core::entities::properties::prop::Prop;
use std::sync::Arc;
// ─────────────────────────────────────────────────────────────────────────────
// BinaryCmpExpr<L, R>
// ─────────────────────────────────────────────────────────────────────────────
impl<L, R> CreateOp for BinaryCmpExpr<L, R, EdgeFilter>
where
    L: CreateOp,
    R: CreateOp,
{
    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let left = self.left.create_edge_op(graph.clone())?;
        let right = self.right.create_edge_op(graph)?;
        Ok(Arc::new(ListAwareCmpEdgeOp {
            left,
            right,
            op: self.op,
        }))
    }
}

impl<L, R> TryAsCompositeFilter for BinaryCmpExpr<L, R, EdgeFilter>
where
    L: CreateOp,
    R: CreateOp,
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
    L: CreateOp,
    R: CreateOp,
{
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> =
        EdgeExprFilteredGraph<G, Arc<dyn EdgeOp<Output = bool> + 'graph>>;

    type NodeFilter<'graph, G: GraphView + 'graph> = NotANodeFilter;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        let left = self.left.create_edge_op(graph.clone())?;
        let right = self.right.create_edge_op(graph.clone())?;
        validate_binary_op(&self.op, &left.prop_type())?;
        let op: Arc<dyn EdgeOp<Output = bool> + 'graph> = Arc::new(BinaryCmpEdgeOp {
            left,
            right,
            op: self.op,
        });
        Ok(EdgeExprFilteredGraph::new(graph, op))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        _graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        Err(GraphError::NotNodeFilter)
    }
}

impl<L, R> CreateFilter for BinaryCmpExpr<L, R, ExplodedEdgeFilter>
where
    L: CreateOp,
    R: CreateOp,
{
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> =
    ExplodedEdgeExprFilteredGraph<G, Arc<dyn EdgeOp<Output = bool> + 'graph>>;

    type NodeFilter<'graph, G: GraphView + 'graph> = NotANodeFilter;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        let left = self.left.create_edge_op(graph.clone())?;
        let right = self.right.create_edge_op(graph.clone())?;
        validate_binary_op(&self.op, &left.prop_type())?;
        let op: Arc<dyn EdgeOp<Output = bool> + 'graph> = Arc::new(BinaryCmpEdgeOp {
            left,
            right,
            op: self.op,
        });
        Ok(ExplodedEdgeExprFilteredGraph::new(graph, op))
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
    E: CreateOp,
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
    E: CreateOp,
{
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> =
        EdgeExprFilteredGraph<G, Arc<dyn EdgeOp<Output = bool> + 'graph>>;
    type NodeFilter<'graph, G: GraphView + 'graph> = NotANodeFilter;

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
// StringExpr<L, R> — string expression filter for edges
// ─────────────────────────────────────────────────────────────────────────────

impl<L, R> CreateOp for StringExpr<L, R, EdgeFilter>
where
    L: CreateOp,
    R: CreateOp,
{
    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let left = self.left.create_edge_op(graph.clone())?;
        let right = self.right.create_edge_op(graph)?;
        Ok(Arc::new(ListAwareStringEdgeOp {
            left,
            right,
            op: self.op,
        }))
    }
}

impl<L, R> TryAsCompositeFilter for StringExpr<L, R, EdgeFilter>
where
    L: CreateOp,
    R: CreateOp,
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
    L: CreateOp,
    R: CreateOp,
{
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> =
        EdgeExprFilteredGraph<G, Arc<dyn EdgeOp<Output = bool> + 'graph>>;
    type NodeFilter<'graph, G: GraphView + 'graph> = NotANodeFilter;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        let left = self.left.create_edge_op(graph.clone())?;
        let right = self.right.create_edge_op(graph.clone())?;
        validate_string_op(&left.prop_type())?;
        let op: Arc<dyn EdgeOp<Output = bool> + 'graph> = Arc::new(StringEdgeOp {
            left,
            right,
            op: self.op,
        });
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

impl<E: CreateOp> CreateOp for PropValueSetExpr<E, EdgeFilter> {
    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let inner = self.expr.create_edge_op(graph)?;
        Ok(Arc::new(ListAwareSetEdgeOp {
            inner,
            values: self.values.clone(),
            op: self.op,
        }))
    }
}

impl<E: CreateOp> TryAsCompositeFilter for PropValueSetExpr<E, EdgeFilter> {
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

impl<E: CreateOp> CreateFilter for PropValueSetExpr<E, EdgeFilter> {
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> =
        EdgeExprFilteredGraph<G, Arc<dyn EdgeOp<Output = bool> + 'graph>>;
    type NodeFilter<'graph, G: GraphView + 'graph> = NotANodeFilter;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        let inner = self.expr.create_edge_op(graph.clone())?;
        let op: Arc<dyn EdgeOp<Output = bool> + 'graph> = Arc::new(PropValueSetEdgeOp {
            inner,
            values: self.values,
            op: self.op,
        });
        Ok(EdgeExprFilteredGraph::new(graph, op))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        _graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        Err(GraphError::NotNodeFilter)
    }
}
