//! Filter types for edge expressions — bridge from EdgeExpr to a filtered graph.
//!
//! Parallel to `node_expr/filters.rs` but for edges: `create_filter` produces
//! an `EdgeExprFilteredGraph` instead of a `NodeFilteredGraph`.

use super::{
    ops::{
        AllEdgeOp, AllPropEdgeOp, AnyEdgeOp, AnyPropEdgeOp, BinaryCmpEdgeOp, PropListEdgeCmpOp,
        PropListInSetEdgeOp, PropListStringEdgeOp, PropValueSetEdgeOp, SetEdgeOp, StringEdgeOp,
        UnaryEdgeOp,
    },
    EdgeExpr, EdgeOp,
};
use raphtory_api::core::storage::arc_str::ArcStr;
use crate::{
    db::{
        api::{state::ops::NotANodeFilter, view::internal::GraphView},
        graph::views::filter::{
            edge_expr_filtered_graph::EdgeExprFilteredGraph,
            model::{
                edge_filter::CompositeEdgeFilter,
                filter_operator::{
                    BinaryOp, Comparable, SetOp, StringComparable, StringOp, UnaryOp,
                },
                ComposableFilter, CompositeExplodedEdgeFilter, CompositeNodeFilter, CreateFilter,
                TryAsCompositeFilter,
            },
        },
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use raphtory_api::core::entities::properties::prop::{Prop, PropType};
use std::{collections::HashSet, hash::Hash, marker::PhantomData, sync::Arc};

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
// BinaryCmpEdgeFilter<L, R>
// ─────────────────────────────────────────────────────────────────────────────

pub struct BinaryCmpEdgeFilter<L, R>
where
    L: EdgeExpr,
    R: EdgeExpr<Output = L::Output>,
{
    pub left: L,
    pub op: BinaryOp,
    pub right: R,
}

impl<L, R> BinaryCmpEdgeFilter<L, R>
where
    L: EdgeExpr,
    R: EdgeExpr<Output = L::Output>,
{
    pub fn new(left: L, op: BinaryOp, right: R) -> Self {
        Self { left, op, right }
    }
}

impl<L, R> Clone for BinaryCmpEdgeFilter<L, R>
where
    L: EdgeExpr,
    R: EdgeExpr<Output = L::Output>,
{
    fn clone(&self) -> Self {
        Self {
            left: self.left.clone(),
            op: self.op,
            right: self.right.clone(),
        }
    }
}

impl<L, R> ComposableFilter for BinaryCmpEdgeFilter<L, R>
where
    L: EdgeExpr,
    R: EdgeExpr<Output = L::Output>,
{
}

impl<L, R> TryAsCompositeFilter for BinaryCmpEdgeFilter<L, R>
where
    L: EdgeExpr,
    R: EdgeExpr<Output = L::Output>,
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

impl<L, R> CreateFilter for BinaryCmpEdgeFilter<L, R>
where
    L: EdgeExpr,
    R: EdgeExpr<Output = L::Output>,
    L::Output: Comparable,
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

// ─────────────────────────────────────────────────────────────────────────────
// UnaryEdgeFilter<E, I>
// ─────────────────────────────────────────────────────────────────────────────

pub struct UnaryEdgeFilter<E, I>
where
    E: EdgeExpr<Output = Option<I>>,
    I: Clone + Send + Sync + 'static,
{
    pub expr: E,
    pub op: UnaryOp,
    pub(crate) _phantom: PhantomData<I>,
}

impl<E, I> Clone for UnaryEdgeFilter<E, I>
where
    E: EdgeExpr<Output = Option<I>>,
    I: Clone + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            expr: self.expr.clone(),
            op: self.op,
            _phantom: PhantomData,
        }
    }
}

impl<E, I> ComposableFilter for UnaryEdgeFilter<E, I>
where
    E: EdgeExpr<Output = Option<I>>,
    I: Clone + Send + Sync + 'static,
{
}

impl<E, I> TryAsCompositeFilter for UnaryEdgeFilter<E, I>
where
    E: EdgeExpr<Output = Option<I>>,
    I: Clone + Send + Sync + 'static,
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

impl<E, I> CreateFilter for UnaryEdgeFilter<E, I>
where
    E: EdgeExpr<Output = Option<I>>,
    I: Clone + Send + Sync + 'static,
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
// QuantifiedEdgeFilter<E, Q, R>
// ─────────────────────────────────────────────────────────────────────────────

pub struct QuantifiedEdgeFilter<E, Q, R>
where
    E: EdgeExpr<Output = Prop>,
    Q: super::super::node_expr::QuantifierMode,
    R: EdgeExpr<Output = Option<Prop>>,
{
    pub expr: E,
    pub op: BinaryOp,
    pub rhs: R,
    pub(crate) _q: PhantomData<Q>,
}

impl<E, Q, R> QuantifiedEdgeFilter<E, Q, R>
where
    E: EdgeExpr<Output = Prop>,
    Q: super::super::node_expr::QuantifierMode,
    R: EdgeExpr<Output = Option<Prop>>,
{
    pub fn new(expr: E, op: BinaryOp, rhs: R) -> Self {
        Self { expr, op, rhs, _q: PhantomData }
    }
}

impl<E, Q, R> Clone for QuantifiedEdgeFilter<E, Q, R>
where
    E: EdgeExpr<Output = Prop>,
    Q: super::super::node_expr::QuantifierMode,
    R: EdgeExpr<Output = Option<Prop>>,
{
    fn clone(&self) -> Self {
        Self {
            expr: self.expr.clone(),
            op: self.op,
            rhs: self.rhs.clone(),
            _q: PhantomData,
        }
    }
}

impl<E, Q, R> ComposableFilter for QuantifiedEdgeFilter<E, Q, R>
where
    E: EdgeExpr<Output = Prop>,
    Q: super::super::node_expr::QuantifierMode,
    R: EdgeExpr<Output = Option<Prop>>,
{
}

impl<E, Q, R> TryAsCompositeFilter for QuantifiedEdgeFilter<E, Q, R>
where
    E: EdgeExpr<Output = Prop>,
    Q: super::super::node_expr::QuantifierMode,
    R: EdgeExpr<Output = Option<Prop>>,
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

use super::super::node_expr::{AnyMode, AllMode};

impl<E, R> CreateFilter for QuantifiedEdgeFilter<E, AnyMode, R>
where
    E: EdgeExpr<Output = Prop>,
    R: EdgeExpr<Output = Option<Prop>>,
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
        let temporal_op = self.expr.create_edge_op(graph.clone())?;
        let rhs = self.rhs.create_edge_op(graph.clone())?;
        let list_cmp: Arc<dyn EdgeOp<Output = bool> + 'graph> =
            Arc::new(PropListEdgeCmpOp { temporal_op, rhs, cmp_op: self.op, any: true });
        let op: Arc<dyn EdgeOp<Output = bool> + 'graph> =
            Arc::new(AnyEdgeOp { inner: list_cmp });
        Ok(EdgeExprFilteredGraph::new(graph, op))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        _graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        Err(GraphError::NotNodeFilter)
    }
}

impl<E, R> CreateFilter for QuantifiedEdgeFilter<E, AllMode, R>
where
    E: EdgeExpr<Output = Prop>,
    R: EdgeExpr<Output = Option<Prop>>,
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
        let temporal_op = self.expr.create_edge_op(graph.clone())?;
        let rhs = self.rhs.create_edge_op(graph.clone())?;
        let list_cmp: Arc<dyn EdgeOp<Output = bool> + 'graph> =
            Arc::new(PropListEdgeCmpOp { temporal_op, rhs, cmp_op: self.op, any: false });
        let op: Arc<dyn EdgeOp<Output = bool> + 'graph> =
            Arc::new(AllEdgeOp { inner: list_cmp });
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
// StringEdgeFilter<L, R> — string expression filter for edges
// ─────────────────────────────────────────────────────────────────────────────

pub struct StringEdgeFilter<L, R>
where
    L: EdgeExpr,
    R: EdgeExpr<Output = L::Output>,
    L::Output: StringComparable,
{
    pub left: L,
    pub op: StringOp,
    pub right: R,
}

impl<L, R> StringEdgeFilter<L, R>
where
    L: EdgeExpr,
    R: EdgeExpr<Output = L::Output>,
    L::Output: StringComparable,
{
    pub fn new(left: L, op: StringOp, right: R) -> Self {
        Self { left, op, right }
    }
}

impl<L, R> Clone for StringEdgeFilter<L, R>
where
    L: EdgeExpr,
    R: EdgeExpr<Output = L::Output>,
    L::Output: StringComparable,
{
    fn clone(&self) -> Self {
        Self { left: self.left.clone(), op: self.op, right: self.right.clone() }
    }
}

impl<L, R> ComposableFilter for StringEdgeFilter<L, R>
where
    L: EdgeExpr,
    R: EdgeExpr<Output = L::Output>,
    L::Output: StringComparable,
{
}

impl<L, R> TryAsCompositeFilter for StringEdgeFilter<L, R>
where
    L: EdgeExpr,
    R: EdgeExpr<Output = L::Output>,
    L::Output: StringComparable,
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

impl<L, R> CreateFilter for StringEdgeFilter<L, R>
where
    L: EdgeExpr,
    R: EdgeExpr<Output = L::Output>,
    L::Output: StringComparable,
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
// PropValueSetEdgeFilter<E> — is_in / is_not_in for Option<Prop> (linear scan)
// ─────────────────────────────────────────────────────────────────────────────

pub struct PropValueSetEdgeFilter<E: EdgeExpr<Output = Option<Prop>>> {
    pub(crate) expr: E,
    pub(crate) values: Vec<Prop>,
    pub(crate) op: SetOp,
}

impl<E: EdgeExpr<Output = Option<Prop>>> Clone for PropValueSetEdgeFilter<E> {
    fn clone(&self) -> Self {
        Self { expr: self.expr.clone(), values: self.values.clone(), op: self.op }
    }
}

impl<E: EdgeExpr<Output = Option<Prop>>> ComposableFilter for PropValueSetEdgeFilter<E> {}

impl<E: EdgeExpr<Output = Option<Prop>>> TryAsCompositeFilter for PropValueSetEdgeFilter<E> {
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

impl<E: EdgeExpr<Output = Option<Prop>>> CreateFilter for PropValueSetEdgeFilter<E> {
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

// ─────────────────────────────────────────────────────────────────────────────
// SetEdgeFilter<E, I> — is_in / is_not_in for Option<I: Hash> (HashSet, O(1))
// ─────────────────────────────────────────────────────────────────────────────

pub struct SetEdgeFilter<E, I>
where
    E: EdgeExpr<Output = Option<I>>,
    I: Eq + Hash + Clone + Send + Sync + 'static,
{
    pub expr: E,
    pub op: SetOp,
    pub values: Arc<HashSet<I>>,
    pub(crate) _phantom: PhantomData<I>,
}

impl<E, I> Clone for SetEdgeFilter<E, I>
where
    E: EdgeExpr<Output = Option<I>>,
    I: Eq + Hash + Clone + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            expr: self.expr.clone(),
            op: self.op,
            values: self.values.clone(),
            _phantom: PhantomData,
        }
    }
}

impl<E, I> ComposableFilter for SetEdgeFilter<E, I>
where
    E: EdgeExpr<Output = Option<I>>,
    I: Eq + Hash + Clone + Send + Sync + 'static,
{}

impl<E, I> TryAsCompositeFilter for SetEdgeFilter<E, I>
where
    E: EdgeExpr<Output = Option<I>>,
    I: Eq + Hash + Clone + Send + Sync + 'static,
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

impl<E, I> CreateFilter for SetEdgeFilter<E, I>
where
    E: EdgeExpr<Output = Option<I>>,
    I: Eq + Hash + Clone + Send + Sync + 'static,
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
            Arc::new(SetEdgeOp { inner, values: self.values, op: self.op });
        Ok(EdgeExprFilteredGraph::new(graph, op))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        _graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        Err(GraphError::NotNodeFilter)
    }
}

use super::super::node_expr::{AnyMode as AnyM, AllMode as AllM, QuantifierMode};

// ─────────────────────────────────────────────────────────────────────────────
// QuantifiedIsInEdgeFilter<E, Q> — quantified set-membership filter for edges
// ─────────────────────────────────────────────────────────────────────────────

pub struct QuantifiedIsInEdgeFilter<E, Q>
where
    E: EdgeExpr<Output = Prop>,
    Q: QuantifierMode,
{
    pub(crate) expr: E,
    pub(crate) values: Vec<Prop>,
    pub(crate) op: SetOp,
    pub(crate) _q: PhantomData<Q>,
}

impl<E, Q> Clone for QuantifiedIsInEdgeFilter<E, Q>
where
    E: EdgeExpr<Output = Prop>,
    Q: QuantifierMode,
{
    fn clone(&self) -> Self {
        Self {
            expr: self.expr.clone(),
            values: self.values.clone(),
            op: self.op,
            _q: PhantomData,
        }
    }
}

impl<E, Q> ComposableFilter for QuantifiedIsInEdgeFilter<E, Q>
where
    E: EdgeExpr<Output = Prop>,
    Q: QuantifierMode,
{
}

impl<E, Q> TryAsCompositeFilter for QuantifiedIsInEdgeFilter<E, Q>
where
    E: EdgeExpr<Output = Prop>,
    Q: QuantifierMode,
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

impl<E> CreateFilter for QuantifiedIsInEdgeFilter<E, AnyM>
where
    E: EdgeExpr<Output = Prop>,
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
        let inner: Arc<dyn EdgeOp<Output = Prop> + 'graph> = Arc::new(PropListInSetEdgeOp {
            inner: self.expr.create_edge_op(graph.clone())?,
            values: self.values,
            op: self.op,
        });
        let op: Arc<dyn EdgeOp<Output = bool> + 'graph> = Arc::new(AnyPropEdgeOp { inner });
        Ok(EdgeExprFilteredGraph::new(graph, op))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        _graph: G,
    ) -> Result<NotANodeFilter, GraphError> {
        Err(GraphError::NotNodeFilter)
    }
}

impl<E> CreateFilter for QuantifiedIsInEdgeFilter<E, AllM>
where
    E: EdgeExpr<Output = Prop>,
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
        let inner: Arc<dyn EdgeOp<Output = Prop> + 'graph> = Arc::new(PropListInSetEdgeOp {
            inner: self.expr.create_edge_op(graph.clone())?,
            values: self.values,
            op: self.op,
        });
        let op: Arc<dyn EdgeOp<Output = bool> + 'graph> = Arc::new(AllPropEdgeOp { inner });
        Ok(EdgeExprFilteredGraph::new(graph, op))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        _graph: G,
    ) -> Result<NotANodeFilter, GraphError> {
        Err(GraphError::NotNodeFilter)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// QuantifiedStringEdgeFilter<E, Q> — quantified string-comparison filter for edges
// ─────────────────────────────────────────────────────────────────────────────

pub struct QuantifiedStringEdgeFilter<E, Q>
where
    E: EdgeExpr<Output = Prop>,
    Q: QuantifierMode,
{
    pub(crate) expr: E,
    pub(crate) rhs: ArcStr,
    pub(crate) op: StringOp,
    pub(crate) _q: PhantomData<Q>,
}

impl<E, Q> Clone for QuantifiedStringEdgeFilter<E, Q>
where
    E: EdgeExpr<Output = Prop>,
    Q: QuantifierMode,
{
    fn clone(&self) -> Self {
        Self {
            expr: self.expr.clone(),
            rhs: self.rhs.clone(),
            op: self.op,
            _q: PhantomData,
        }
    }
}

impl<E, Q> ComposableFilter for QuantifiedStringEdgeFilter<E, Q>
where
    E: EdgeExpr<Output = Prop>,
    Q: QuantifierMode,
{
}

impl<E, Q> TryAsCompositeFilter for QuantifiedStringEdgeFilter<E, Q>
where
    E: EdgeExpr<Output = Prop>,
    Q: QuantifierMode,
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

impl<E> CreateFilter for QuantifiedStringEdgeFilter<E, AnyM>
where
    E: EdgeExpr<Output = Prop>,
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
        let inner: Arc<dyn EdgeOp<Output = Prop> + 'graph> = Arc::new(PropListStringEdgeOp {
            inner: self.expr.create_edge_op(graph.clone())?,
            rhs: self.rhs,
            op: self.op,
        });
        let op: Arc<dyn EdgeOp<Output = bool> + 'graph> = Arc::new(AnyPropEdgeOp { inner });
        Ok(EdgeExprFilteredGraph::new(graph, op))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        _graph: G,
    ) -> Result<NotANodeFilter, GraphError> {
        Err(GraphError::NotNodeFilter)
    }
}

impl<E> CreateFilter for QuantifiedStringEdgeFilter<E, AllM>
where
    E: EdgeExpr<Output = Prop>,
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
        let inner: Arc<dyn EdgeOp<Output = Prop> + 'graph> = Arc::new(PropListStringEdgeOp {
            inner: self.expr.create_edge_op(graph.clone())?,
            rhs: self.rhs,
            op: self.op,
        });
        let op: Arc<dyn EdgeOp<Output = bool> + 'graph> = Arc::new(AllPropEdgeOp { inner });
        Ok(EdgeExprFilteredGraph::new(graph, op))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        _graph: G,
    ) -> Result<NotANodeFilter, GraphError> {
        Err(GraphError::NotNodeFilter)
    }
}
