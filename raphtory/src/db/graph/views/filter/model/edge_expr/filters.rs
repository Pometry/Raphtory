//! Filter types for edge expressions — bridge from EdgeExpr to a filtered graph.
//!
//! Parallel to `node_expr/filters.rs` but for edges: `create_filter` produces
//! an `EdgeExprFilteredGraph` instead of a `NodeFilteredGraph`.

use super::{
    ops::{BinaryCmpEdgeOp, PropValueSetEdgeOp, StringEdgeOp, UnaryEdgeOp},
    EdgeOp,
};
pub(crate) use crate::db::graph::views::filter::model::{BinaryCmpExpr, StringExpr, UnaryExpr};
use crate::{
    db::{
        api::{state::ops::NotANodeFilter, view::internal::GraphView},
        graph::views::filter::{
            edge_expr_filtered_graph::EdgeExprFilteredGraph,
            exploded_edge_expr_filtered_graph::ExplodedEdgeExprFilteredGraph,
            model::{
                edge_filter::EdgeFilter,
                node_expr::{filters::PropValueSetExpr, CreateOp},
                resolved_prop_type, validate_binary_op, validate_const_castable,
                validate_string_op, validate_types_compatible, CreateFilter, ExplodedEdgeFilter,
            },
        },
    },
    errors::GraphError,
};
use std::sync::Arc;
// ─────────────────────────────────────────────────────────────────────────────
// BinaryCmpExpr<L, R>
// ─────────────────────────────────────────────────────────────────────────────

impl<L, R> CreateFilter for BinaryCmpExpr<L, R, EdgeFilter>
where
    L: CreateOp,
    R: CreateOp,
{
    type EntityFiltered<'graph, G: GraphView + 'graph, F: GraphView + 'graph> =
        EdgeExprFilteredGraph<G, Arc<dyn EdgeOp<Output = bool> + 'graph>>;

    type NodeFilter<'graph, G: GraphView + 'graph, F: GraphView + 'graph> = NotANodeFilter;

    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Self::EntityFiltered<'graph, G, F>, GraphError> {
        let expr_pt = self.left.prop_type();
        let left = self.left.create_edge_op(filtered.clone())?;
        let right = self.right.create_edge_op(filtered.clone())?;
        let lhs_pt = resolved_prop_type(expr_pt, left.prop_type());
        let rhs_pt = resolved_prop_type(self.right.prop_type(), right.prop_type());
        validate_binary_op(&self.op, &lhs_pt)?;
        match right.const_value() {
            Some(c) => validate_const_castable(&lhs_pt, c.as_ref())?,
            None => validate_types_compatible(&lhs_pt, &rhs_pt)?,
        }
        let op: Arc<dyn EdgeOp<Output = bool> + 'graph> = Arc::new(BinaryCmpEdgeOp {
            left,
            right,
            op: self.op,
        });
        Ok(EdgeExprFilteredGraph::new(graph, op))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        _graph: G,
        _filtered: F,
    ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError> {
        Err(GraphError::NotNodeFilter)
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        Ok(graph)
    }
}

impl<L, R> CreateFilter for BinaryCmpExpr<L, R, ExplodedEdgeFilter>
where
    L: CreateOp,
    R: CreateOp,
{
    type EntityFiltered<'graph, G: GraphView + 'graph, F: GraphView + 'graph> =
        ExplodedEdgeExprFilteredGraph<G, Arc<dyn EdgeOp<Output = bool> + 'graph>>;

    type NodeFilter<'graph, G: GraphView + 'graph, F: GraphView + 'graph> = NotANodeFilter;

    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Self::EntityFiltered<'graph, G, F>, GraphError> {
        let expr_pt = self.left.prop_type();
        let left = self.left.create_edge_op(filtered.clone())?;
        let right = self.right.create_edge_op(filtered.clone())?;
        let lhs_pt = resolved_prop_type(expr_pt, left.prop_type());
        let rhs_pt = resolved_prop_type(self.right.prop_type(), right.prop_type());
        validate_binary_op(&self.op, &lhs_pt)?;
        match right.const_value() {
            Some(c) => validate_const_castable(&lhs_pt, c.as_ref())?,
            None => validate_types_compatible(&lhs_pt, &rhs_pt)?,
        }
        let op: Arc<dyn EdgeOp<Output = bool> + 'graph> = Arc::new(BinaryCmpEdgeOp {
            left,
            right,
            op: self.op,
        });
        Ok(ExplodedEdgeExprFilteredGraph::new(graph, op))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        _graph: G,
        _filtered: F,
    ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError> {
        Err(GraphError::NotNodeFilter)
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        Ok(graph)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// UnaryExpr<E, I>
// ─────────────────────────────────────────────────────────────────────────────

impl<E> CreateFilter for UnaryExpr<E, EdgeFilter>
where
    E: CreateOp,
{
    type EntityFiltered<'graph, G: GraphView + 'graph, F: GraphView + 'graph> =
        EdgeExprFilteredGraph<G, Arc<dyn EdgeOp<Output = bool> + 'graph>>;
    type NodeFilter<'graph, G: GraphView + 'graph, F: GraphView + 'graph> = NotANodeFilter;

    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Self::EntityFiltered<'graph, G, F>, GraphError> {
        let inner = self.expr.create_edge_op(filtered.clone())?;
        let op: Arc<dyn EdgeOp<Output = bool> + 'graph> =
            Arc::new(UnaryEdgeOp { inner, op: self.op });
        Ok(EdgeExprFilteredGraph::new(graph, op))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        _graph: G,
        _filtered: F,
    ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError> {
        Err(GraphError::NotNodeFilter)
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        Ok(graph)
    }
}

impl<E> CreateFilter for UnaryExpr<E, ExplodedEdgeFilter>
where
    E: CreateOp,
{
    type EntityFiltered<'graph, G: GraphView + 'graph, F: GraphView + 'graph> =
        ExplodedEdgeExprFilteredGraph<G, Arc<dyn EdgeOp<Output = bool> + 'graph>>;
    type NodeFilter<'graph, G: GraphView + 'graph, F: GraphView + 'graph> = NotANodeFilter;

    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Self::EntityFiltered<'graph, G, F>, GraphError> {
        let inner = self.expr.create_edge_op(filtered.clone())?;
        let op: Arc<dyn EdgeOp<Output = bool> + 'graph> =
            Arc::new(UnaryEdgeOp { inner, op: self.op });
        Ok(ExplodedEdgeExprFilteredGraph::new(graph, op))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        _graph: G,
        _filtered: F,
    ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError> {
        Err(GraphError::NotNodeFilter)
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        Ok(graph)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// StringExpr<L, R> — string expression filter for edges
// ─────────────────────────────────────────────────────────────────────────────

impl<L, R> CreateFilter for StringExpr<L, R, EdgeFilter>
where
    L: CreateOp,
    R: CreateOp,
{
    type EntityFiltered<'graph, G: GraphView + 'graph, F: GraphView + 'graph> =
        EdgeExprFilteredGraph<G, Arc<dyn EdgeOp<Output = bool> + 'graph>>;
    type NodeFilter<'graph, G: GraphView + 'graph, F: GraphView + 'graph> = NotANodeFilter;

    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Self::EntityFiltered<'graph, G, F>, GraphError> {
        let left = self.left.create_edge_op(filtered.clone())?;
        let right = self.right.create_edge_op(filtered.clone())?;
        validate_string_op(&left.prop_type())?;
        let op: Arc<dyn EdgeOp<Output = bool> + 'graph> = Arc::new(StringEdgeOp {
            left,
            right,
            op: self.op,
        });
        Ok(EdgeExprFilteredGraph::new(graph, op))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        _graph: G,
        _filtered: F,
    ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError> {
        Err(GraphError::NotNodeFilter)
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        Ok(graph)
    }
}

impl<L, R> CreateFilter for StringExpr<L, R, ExplodedEdgeFilter>
where
    L: CreateOp,
    R: CreateOp,
{
    type EntityFiltered<'graph, G: GraphView + 'graph, F: GraphView + 'graph> =
        ExplodedEdgeExprFilteredGraph<G, Arc<dyn EdgeOp<Output = bool> + 'graph>>;
    type NodeFilter<'graph, G: GraphView + 'graph, F: GraphView + 'graph> = NotANodeFilter;

    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Self::EntityFiltered<'graph, G, F>, GraphError> {
        let left = self.left.create_edge_op(filtered.clone())?;
        let right = self.right.create_edge_op(filtered.clone())?;
        validate_string_op(&left.prop_type())?;
        let op: Arc<dyn EdgeOp<Output = bool> + 'graph> = Arc::new(StringEdgeOp {
            left,
            right,
            op: self.op,
        });
        Ok(ExplodedEdgeExprFilteredGraph::new(graph, op))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        _graph: G,
        _filtered: F,
    ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError> {
        Err(GraphError::NotNodeFilter)
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        Ok(graph)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// PropValueSetExpr<E, EdgeFilter> — is_in / is_not_in for edge-side exprs
// ─────────────────────────────────────────────────────────────────────────────

impl<E: CreateOp> CreateFilter for PropValueSetExpr<E, EdgeFilter> {
    type EntityFiltered<'graph, G: GraphView + 'graph, F: GraphView + 'graph> =
        EdgeExprFilteredGraph<G, Arc<dyn EdgeOp<Output = bool> + 'graph>>;
    type NodeFilter<'graph, G: GraphView + 'graph, F: GraphView + 'graph> = NotANodeFilter;

    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Self::EntityFiltered<'graph, G, F>, GraphError> {
        let inner = self.expr.create_edge_op(filtered.clone())?;
        let op: Arc<dyn EdgeOp<Output = bool> + 'graph> = Arc::new(PropValueSetEdgeOp {
            inner,
            values: self.values,
            op: self.op,
        });
        Ok(EdgeExprFilteredGraph::new(graph, op))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        _graph: G,
        _filtered: F,
    ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError> {
        Err(GraphError::NotNodeFilter)
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        Ok(graph)
    }
}

impl<E: CreateOp> CreateFilter for PropValueSetExpr<E, ExplodedEdgeFilter> {
    type EntityFiltered<'graph, G: GraphView + 'graph, F: GraphView + 'graph> =
        ExplodedEdgeExprFilteredGraph<G, Arc<dyn EdgeOp<Output = bool> + 'graph>>;
    type NodeFilter<'graph, G: GraphView + 'graph, F: GraphView + 'graph> = NotANodeFilter;

    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphView + 'graph;

    fn create_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        graph: G,
        filtered: F,
    ) -> Result<Self::EntityFiltered<'graph, G, F>, GraphError> {
        let inner = self.expr.create_edge_op(filtered.clone())?;
        let op: Arc<dyn EdgeOp<Output = bool> + 'graph> = Arc::new(PropValueSetEdgeOp {
            inner,
            values: self.values,
            op: self.op,
        });
        Ok(ExplodedEdgeExprFilteredGraph::new(graph, op))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph, F: GraphView + 'graph>(
        self,
        _graph: G,
        _filtered: F,
    ) -> Result<Self::NodeFilter<'graph, G, F>, GraphError> {
        Err(GraphError::NotNodeFilter)
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        Ok(graph)
    }
}
