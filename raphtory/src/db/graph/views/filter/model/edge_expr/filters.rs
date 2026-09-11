//! Filter types for edge expressions — bridge from EdgeExpr to a filtered graph.
//!
//! Parallel to `node_expr/filters.rs` but for edges: `create_filter` produces
//! an `EdgeExprFilteredGraph` instead of a `NodeFilteredGraph`.

use super::{
    ops::{
        BinaryCmpEdgeOp, ListAwareCmpEdgeOp, ListAwareSetEdgeOp, ListAwareStringEdgeOp,
        ListAwareUnaryEdgeOp, PropValueSetEdgeOp, StringEdgeOp, UnaryEdgeOp,
    },
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
                coerce_set_values,
                edge_filter::EdgeFilter,
                elem_prop_type,
                filter_operator::ElemQual,
                node_expr::{
                    filters::PropValueSetExpr,
                    ops::{AllEdgeOp, AnyEdgeOp},
                    CreateOp,
                },
                resolved_prop_type, validate_binary_op, validate_const_castable,
                validate_string_op, validate_types_compatible, CreateFilter, ExplodedEdgeFilter,
            },
        },
    },
    errors::GraphError,
};
use raphtory_api::core::entities::{
    edges::edge_ref::EdgeRef,
    properties::prop::{Prop, PropType},
};
use raphtory_storage::graph::graph::GraphStorage;
use std::sync::Arc;
// ─────────────────────────────────────────────────────────────────────────────
// BinaryCmpExpr<L, R>
// ─────────────────────────────────────────────────────────────────────────────

/// Adapts an elementwise boolean edge op to the plain boolean output the
/// filtered-graph wrappers consume.
#[derive(Clone)]
struct TruthyEdgeOp<'g> {
    inner: Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>,
}

impl<'g> EdgeOp for TruthyEdgeOp<'g> {
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, edge: EdgeRef) -> bool {
        matches!(self.inner.apply(storage, edge), Some(Prop::Bool(true)))
    }
}

/// Collapse elementwise boolean results per the collected qualifiers,
/// innermost list level first, and adapt to a boolean edge filter.
pub(crate) fn qualify_edge_filter<'g>(
    elemwise: Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>,
    quals: &[ElemQual],
) -> Arc<dyn EdgeOp<Output = bool> + 'g> {
    let mut op = elemwise;
    // Qualifiers are collected in call order (outermost list level first);
    // wrapping starts at the innermost level, so iterate in reverse.
    for q in quals.iter().rev() {
        op = match q {
            ElemQual::Any => Arc::new(AnyEdgeOp { inner: op }),
            ElemQual::All => Arc::new(AllEdgeOp { inner: op }),
        };
    }
    Arc::new(TruthyEdgeOp { inner: op })
}

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
        let (left, quals) = self.left.create_qualified_edge_op(filtered.clone())?;
        let right = self.right.create_edge_op(filtered.clone())?;
        let lhs_pt = elem_prop_type(&resolved_prop_type(expr_pt, left.prop_type()), quals.len())?;
        let rhs_pt = resolved_prop_type(self.right.prop_type(), right.prop_type());
        validate_binary_op(&self.op, &lhs_pt)?;
        match right.const_value() {
            Some(c) => validate_const_castable(&lhs_pt, c.as_ref())?,
            None => validate_types_compatible(&lhs_pt, &rhs_pt)?,
        }
        let op: Arc<dyn EdgeOp<Output = bool> + 'graph> = if quals.is_empty() {
            Arc::new(BinaryCmpEdgeOp {
                left,
                right,
                op: self.op,
            })
        } else {
            qualify_edge_filter(
                Arc::new(ListAwareCmpEdgeOp {
                    left,
                    right,
                    op: self.op,
                }),
                &quals,
            )
        };
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
        let (left, quals) = self.left.create_qualified_edge_op(filtered.clone())?;
        let right = self.right.create_edge_op(filtered.clone())?;
        let lhs_pt = elem_prop_type(&resolved_prop_type(expr_pt, left.prop_type()), quals.len())?;
        let rhs_pt = resolved_prop_type(self.right.prop_type(), right.prop_type());
        validate_binary_op(&self.op, &lhs_pt)?;
        match right.const_value() {
            Some(c) => validate_const_castable(&lhs_pt, c.as_ref())?,
            None => validate_types_compatible(&lhs_pt, &rhs_pt)?,
        }
        let op: Arc<dyn EdgeOp<Output = bool> + 'graph> = if quals.is_empty() {
            Arc::new(BinaryCmpEdgeOp {
                left,
                right,
                op: self.op,
            })
        } else {
            qualify_edge_filter(
                Arc::new(ListAwareCmpEdgeOp {
                    left,
                    right,
                    op: self.op,
                }),
                &quals,
            )
        };
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
        let (inner, quals) = self.expr.create_qualified_edge_op(filtered.clone())?;
        let op: Arc<dyn EdgeOp<Output = bool> + 'graph> = if quals.is_empty() {
            Arc::new(UnaryEdgeOp { inner, op: self.op })
        } else {
            qualify_edge_filter(
                Arc::new(ListAwareUnaryEdgeOp { inner, op: self.op }),
                &quals,
            )
        };
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
        let (inner, quals) = self.expr.create_qualified_edge_op(filtered.clone())?;
        let op: Arc<dyn EdgeOp<Output = bool> + 'graph> = if quals.is_empty() {
            Arc::new(UnaryEdgeOp { inner, op: self.op })
        } else {
            qualify_edge_filter(
                Arc::new(ListAwareUnaryEdgeOp { inner, op: self.op }),
                &quals,
            )
        };
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
        let (left, quals) = self.left.create_qualified_edge_op(filtered.clone())?;
        let right = self.right.create_edge_op(filtered.clone())?;
        validate_string_op(&elem_prop_type(&left.prop_type(), quals.len())?)?;
        match right.const_value() {
            Some(c) => validate_const_castable(&PropType::Str, c.as_ref())?,
            None => {}
        }
        let op: Arc<dyn EdgeOp<Output = bool> + 'graph> = if quals.is_empty() {
            Arc::new(StringEdgeOp {
                left,
                right,
                op: self.op,
            })
        } else {
            qualify_edge_filter(
                Arc::new(ListAwareStringEdgeOp {
                    left,
                    right,
                    op: self.op,
                }),
                &quals,
            )
        };
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
        let (left, quals) = self.left.create_qualified_edge_op(filtered.clone())?;
        let right = self.right.create_edge_op(filtered.clone())?;
        validate_string_op(&elem_prop_type(&left.prop_type(), quals.len())?)?;
        match right.const_value() {
            Some(c) => validate_const_castable(&PropType::Str, c.as_ref())?,
            None => {}
        }
        let op: Arc<dyn EdgeOp<Output = bool> + 'graph> = if quals.is_empty() {
            Arc::new(StringEdgeOp {
                left,
                right,
                op: self.op,
            })
        } else {
            qualify_edge_filter(
                Arc::new(ListAwareStringEdgeOp {
                    left,
                    right,
                    op: self.op,
                }),
                &quals,
            )
        };
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
        let expr_pt = self.expr.prop_type();
        let (inner, quals) = self.expr.create_qualified_edge_op(filtered.clone())?;
        let lhs_pt = elem_prop_type(&resolved_prop_type(expr_pt, inner.prop_type()), quals.len())?;
        let values = coerce_set_values(&lhs_pt, self.values)?;
        let op: Arc<dyn EdgeOp<Output = bool> + 'graph> = if quals.is_empty() {
            Arc::new(PropValueSetEdgeOp {
                inner,
                values,
                op: self.op,
            })
        } else {
            qualify_edge_filter(
                Arc::new(ListAwareSetEdgeOp {
                    inner,
                    values,
                    op: self.op,
                }),
                &quals,
            )
        };
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
        let expr_pt = self.expr.prop_type();
        let (inner, quals) = self.expr.create_qualified_edge_op(filtered.clone())?;
        let lhs_pt = elem_prop_type(&resolved_prop_type(expr_pt, inner.prop_type()), quals.len())?;
        let values = coerce_set_values(&lhs_pt, self.values)?;
        let op: Arc<dyn EdgeOp<Output = bool> + 'graph> = if quals.is_empty() {
            Arc::new(PropValueSetEdgeOp {
                inner,
                values,
                op: self.op,
            })
        } else {
            qualify_edge_filter(
                Arc::new(ListAwareSetEdgeOp {
                    inner,
                    values,
                    op: self.op,
                }),
                &quals,
            )
        };
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
