//! The yes/no forms of the typed expression API: a comparison, a string test,
//! a presence test, a set membership test, `any`/`all` over an element-wise
//! result, and `and`/`or`/`not` of yes/no values.
//!
//! Each compiles itself to a node op and an edge op. The result shape (one
//! answer, or one per list element) is decided against the graph, because
//! only then are property types known. The builders hand each one out wrapped
//! in a [`Predicate`], which is what makes it a filter. A filter tree built
//! from data (python, GraphQL, a stored grant) builds these same expressions,
//! so there is one compiler and one set of checks whatever built the filter.

use super::{
    exprs::{AllExpr, AnyExpr},
    ops::{
        AllEdgeOp, AllNodeOp, AndValueNodeOp, AnyEdgeOp, AnyNodeOp, BinaryValueNodeOp,
        OrValueNodeOp, UnaryValueNodeOp,
    },
    predicate::{IndexQuery, IndexTerm, Pushdown},
    typing::{
        cmp_kernel, comparison_shape, not_kernel, presence_kernel, qualified_type, require_bool,
        set_kernel, set_shape, static_comparison_type, str_kernel, string_shape, SetMembers,
    },
    CreateOp, EntityExpr,
};
use crate::{
    db::{
        api::{state::NodeOp, view::internal::GraphView},
        graph::views::filter::model::{
            edge_expr::{
                ops::{AndValueEdgeOp, BinaryValueEdgeOp, OrValueEdgeOp, UnaryValueEdgeOp},
                EdgeOp,
            },
            expr::{
                stream::{
                    StreamedAllEdgeOp, StreamedAllNodeOp, StreamedAnyEdgeOp, StreamedAnyNodeOp,
                },
                DynCreateHistory, EdgeHistory, NodeHistory, ValueTest,
            },
            filter_operator::{BinaryOp, SetOp, StringOp, UnaryOp},
            resolved_prop_type, EntityMarker,
        },
    },
    errors::GraphError,
};
use raphtory_api::core::entities::properties::prop::{Prop, PropType};
use std::sync::Arc;

fn invalid(msg: impl Into<String>) -> GraphError {
    GraphError::InvalidFilter(msg.into())
}

// ── comparison ───────────────────────────────────────────────────────────────

/// Two values compared with a [`BinaryOp`]; either side may be a constant.
///
/// ```rust,ignore
/// NodeFilter.degree().gt(2usize)
/// NodeFilter.property("age").eq(30i64)
/// NodeFilter.out_degree().gt(NodeFilter.in_degree())
/// ```
#[derive(Clone)]
pub struct BinaryCmpExpr<L, R> {
    pub left: L,
    pub op: BinaryOp,
    pub right: R,
    pub entity: EntityMarker,
}

impl<L, R> BinaryCmpExpr<L, R> {
    pub fn new(left: L, op: BinaryOp, right: R, entity: EntityMarker) -> Self {
        Self {
            left,
            op,
            right,
            entity,
        }
    }
}

impl<L: EntityExpr, R: EntityExpr> EntityExpr for BinaryCmpExpr<L, R> {
    fn entity(&self) -> EntityMarker {
        self.entity
    }

    /// One yes/no, or one per element, when both sides' types are known
    /// before the filter meets a graph.
    fn prop_type(&self) -> PropType {
        static_comparison_type(&self.op, &self.left.prop_type(), &self.right.prop_type())
    }

    fn nullable(&self) -> bool {
        false
    }
}

impl<L: CreateOp, R: CreateOp> BinaryCmpExpr<L, R> {
    /// The indexable term and the constant it is compared with, the operator
    /// turned round when the constant stands on the left.
    fn term_and_constant(&self) -> Option<(IndexTerm, BinaryOp, Prop)> {
        if let (Some(term), Some(value)) = (self.left.index_term(), self.right.constant()) {
            return Some((term, self.op, value));
        }
        if let (Some(term), Some(value)) = (self.right.index_term(), self.left.constant()) {
            return Some((term, self.op.flipped(), value));
        }
        None
    }
}

impl<L: CreateOp, R: CreateOp> CreateOp for BinaryCmpExpr<L, R> {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let left = self.left.create_node_op(graph.clone())?;
        let right = self.right.create_node_op(graph)?;
        let lhs_pt = resolved_prop_type(self.left.prop_type(), left.prop_type());
        let rhs_pt = resolved_prop_type(self.right.prop_type(), right.prop_type());
        let rhs_const = right.const_value().flatten();
        let (out, shape) = comparison_shape(&self.op, &lhs_pt, &rhs_pt)
            .map_err(|e| e.into_error(&rhs_pt, rhs_const.as_ref()))?;
        Ok(Arc::new(BinaryValueNodeOp {
            left,
            right,
            param: self.op,
            kernel: cmp_kernel(shape),
            out,
        }))
    }

    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let left = self.left.create_edge_op(graph.clone())?;
        let right = self.right.create_edge_op(graph)?;
        let lhs_pt = resolved_prop_type(self.left.prop_type(), left.prop_type());
        let rhs_pt = resolved_prop_type(self.right.prop_type(), right.prop_type());
        let rhs_const = right.const_value().flatten();
        let (out, shape) = comparison_shape(&self.op, &lhs_pt, &rhs_pt)
            .map_err(|e| e.into_error(&rhs_pt, rhs_const.as_ref()))?;
        Ok(Arc::new(BinaryValueEdgeOp {
            left,
            right,
            param: self.op,
            kernel: cmp_kernel(shape),
            out,
        }))
    }

    fn index_query(&self) -> Option<IndexQuery> {
        let (term, op, value) = self.term_and_constant()?;
        IndexQuery::cmp(term, op, value)
    }

    /// Equality on the id, or on the name (the node's external id), names the
    /// node outright; any other indexable test asks the index for candidates.
    fn pushdown(&self) -> Option<Pushdown> {
        if let Some((IndexTerm::Id | IndexTerm::Name, BinaryOp::Eq, value)) =
            self.term_and_constant()
        {
            return Some(Pushdown::Ids(vec![value]));
        }
        self.index_query()
            .filter(|query| !query.is_history())
            .map(Pushdown::Index)
    }

    fn value_test(&self) -> Option<(Arc<dyn DynCreateHistory>, ValueTest)> {
        if let (Some(history), Some(value)) = (self.left.history(), self.right.constant()) {
            return Some((history, ValueTest::Cmp(self.op, value)));
        }
        if let (Some(history), Some(value)) = (self.right.history(), self.left.constant()) {
            return Some((history, ValueTest::Cmp(self.op.flipped(), value)));
        }
        None
    }
}

// ── string test ──────────────────────────────────────────────────────────────

/// A string test with a [`StringOp`]: `starts_with`, `contains`, `fuzzy_search`, …
#[derive(Clone)]
pub struct StringExpr<L, R> {
    pub left: L,
    pub op: StringOp,
    pub right: R,
    pub entity: EntityMarker,
}

impl<L, R> StringExpr<L, R> {
    pub fn new(left: L, op: StringOp, right: R, entity: EntityMarker) -> Self {
        Self {
            left,
            op,
            right,
            entity,
        }
    }
}

impl<L: EntityExpr, R: EntityExpr> EntityExpr for StringExpr<L, R> {
    fn entity(&self) -> EntityMarker {
        self.entity
    }

    fn nullable(&self) -> bool {
        false
    }
}

impl<L: CreateOp, R: CreateOp> CreateOp for StringExpr<L, R> {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let left = self.left.create_node_op(graph.clone())?;
        let right = self.right.create_node_op(graph)?;
        let lhs_pt = resolved_prop_type(self.left.prop_type(), left.prop_type());
        let rhs_pt = resolved_prop_type(self.right.prop_type(), right.prop_type());
        let rhs_const = right.const_value().flatten();
        let (out, shape) = string_shape(&lhs_pt, &rhs_pt)
            .map_err(|e| e.into_error(&rhs_pt, rhs_const.as_ref()))?;
        Ok(Arc::new(BinaryValueNodeOp {
            left,
            right,
            param: self.op,
            kernel: str_kernel(shape),
            out,
        }))
    }

    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let left = self.left.create_edge_op(graph.clone())?;
        let right = self.right.create_edge_op(graph)?;
        let lhs_pt = resolved_prop_type(self.left.prop_type(), left.prop_type());
        let rhs_pt = resolved_prop_type(self.right.prop_type(), right.prop_type());
        let rhs_const = right.const_value().flatten();
        let (out, shape) = string_shape(&lhs_pt, &rhs_pt)
            .map_err(|e| e.into_error(&rhs_pt, rhs_const.as_ref()))?;
        Ok(Arc::new(BinaryValueEdgeOp {
            left,
            right,
            param: self.op,
            kernel: str_kernel(shape),
            out,
        }))
    }

    fn index_query(&self) -> Option<IndexQuery> {
        IndexQuery::string(self.left.index_term()?, self.op, self.right.constant()?)
    }

    fn pushdown(&self) -> Option<Pushdown> {
        self.index_query()
            .filter(|query| !query.is_history())
            .map(Pushdown::Index)
    }

    fn value_test(&self) -> Option<(Arc<dyn DynCreateHistory>, ValueTest)> {
        let history = self.left.history()?;
        let value = self.right.constant()?;
        Some((history, ValueTest::Str(self.op, value)))
    }
}

// ── presence test ────────────────────────────────────────────────────────────

/// A presence test: `is_some()` / `is_none()`.
#[derive(Clone)]
pub struct UnaryExpr<E> {
    pub expr: E,
    pub op: UnaryOp,
    pub entity: EntityMarker,
}

impl<E: EntityExpr> EntityExpr for UnaryExpr<E> {
    fn entity(&self) -> EntityMarker {
        self.entity
    }

    fn nullable(&self) -> bool {
        false
    }
}

impl<E: CreateOp> UnaryExpr<E> {
    /// A presence test only means something on a value that can be missing.
    fn check(&self) -> Result<(), GraphError> {
        if self.expr.nullable() {
            return Ok(());
        }
        Err(invalid(format!(
            "{}() is not valid on an expression that always has a value",
            self.op
        )))
    }
}

impl<E: CreateOp> CreateOp for UnaryExpr<E> {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        self.check()?;
        Ok(Arc::new(UnaryValueNodeOp {
            inner: self.expr.create_node_op(graph)?,
            param: self.op,
            kernel: presence_kernel,
            out: PropType::Bool,
        }))
    }

    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        self.check()?;
        Ok(Arc::new(UnaryValueEdgeOp {
            inner: self.expr.create_edge_op(graph)?,
            param: self.op,
            kernel: presence_kernel,
            out: PropType::Bool,
        }))
    }
}

// ── membership test ──────────────────────────────────────────────────────────

/// A membership test against a fixed set of values: `is_in` / `is_not_in`.
#[derive(Clone)]
pub struct PropValueSetExpr<E> {
    pub(crate) expr: E,
    pub(crate) values: Vec<Prop>,
    pub(crate) op: SetOp,
    pub(crate) entity: EntityMarker,
}

impl<E: EntityExpr> EntityExpr for PropValueSetExpr<E> {
    fn entity(&self) -> EntityMarker {
        self.entity
    }

    fn nullable(&self) -> bool {
        false
    }
}

impl<E> PropValueSetExpr<E> {
    fn negated(&self) -> bool {
        matches!(self.op, SetOp::IsNotIn)
    }
}

impl<E: CreateOp> CreateOp for PropValueSetExpr<E> {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let inner = self.expr.create_node_op(graph)?;
        let pt = resolved_prop_type(self.expr.prop_type(), inner.prop_type());
        let (out, shape, members) = set_shape(&pt, &self.values);
        Ok(Arc::new(UnaryValueNodeOp {
            inner,
            param: SetMembers::new(members, self.negated()),
            kernel: set_kernel(shape),
            out,
        }))
    }

    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let inner = self.expr.create_edge_op(graph)?;
        let pt = resolved_prop_type(self.expr.prop_type(), inner.prop_type());
        let (out, shape, members) = set_shape(&pt, &self.values);
        Ok(Arc::new(UnaryValueEdgeOp {
            inner,
            param: SetMembers::new(members, self.negated()),
            kernel: set_kernel(shape),
            out,
        }))
    }

    fn index_query(&self) -> Option<IndexQuery> {
        if self.negated() {
            return None;
        }
        IndexQuery::members(self.expr.index_term()?, &self.values)
    }

    fn pushdown(&self) -> Option<Pushdown> {
        if !self.negated()
            && matches!(
                self.expr.index_term(),
                Some(IndexTerm::Id | IndexTerm::Name)
            )
        {
            return Some(Pushdown::Ids(self.values.clone()));
        }
        self.index_query()
            .filter(|query| !query.is_history())
            .map(Pushdown::Index)
    }

    fn value_test(&self) -> Option<(Arc<dyn DynCreateHistory>, ValueTest)> {
        let history = self.expr.history()?;
        Some((
            history,
            ValueTest::members(self.values.iter().cloned(), self.negated()),
        ))
    }
}

// ── any / all ────────────────────────────────────────────────────────────────

/// `any()` / `all()` over an element-wise yes/no result. A comparison of a
/// history with a constant walks the history and stops at the first value
/// that decides the answer; anything else collapses the list. `streamed`
/// builds the walking op, `list` the collapsing one.
fn qualify_node_op<'g, E: CreateOp, G: GraphView + 'g>(
    inner: &E,
    graph: G,
    streamed: impl FnOnce(
        Arc<dyn NodeHistory + 'g>,
        ValueTest,
    ) -> Arc<dyn NodeOp<Output = Option<Prop>> + 'g>,
    list: impl FnOnce(
        Arc<dyn NodeOp<Output = Option<Prop>> + 'g>,
    ) -> Arc<dyn NodeOp<Output = Option<Prop>> + 'g>,
) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
    if let Some((history, test)) = inner.value_test() {
        let history = history.create_node_history(graph.clone().into_dyn_graph_arc())?;
        if let Some(test) = test.for_history(&history.history_type()) {
            return Ok(streamed(history, test));
        }
    }
    let op = inner.create_node_op(graph)?;
    qualified_type(&resolved_prop_type(inner.prop_type(), op.prop_type()))?;
    Ok(list(op))
}

fn qualify_edge_op<'g, E: CreateOp, G: GraphView + 'g>(
    inner: &E,
    graph: G,
    streamed: impl FnOnce(
        Arc<dyn EdgeHistory + 'g>,
        ValueTest,
    ) -> Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>,
    list: impl FnOnce(
        Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>,
    ) -> Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>,
) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
    if let Some((history, test)) = inner.value_test() {
        let history = history.create_edge_history(graph.clone().into_dyn_graph_arc())?;
        if let Some(test) = test.for_history(&history.history_type()) {
            return Ok(streamed(history, test));
        }
    }
    let op = inner.create_edge_op(graph)?;
    qualified_type(&resolved_prop_type(inner.prop_type(), op.prop_type()))?;
    Ok(list(op))
}

impl<E: CreateOp> CreateOp for AnyExpr<E> {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        qualify_node_op(
            &self.0,
            graph,
            |history, test| Arc::new(StreamedAnyNodeOp { history, test }),
            |inner| Arc::new(AnyNodeOp { inner }),
        )
    }

    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        qualify_edge_op(
            &self.0,
            graph,
            |history, test| Arc::new(StreamedAnyEdgeOp { history, test }),
            |inner| Arc::new(AnyEdgeOp { inner }),
        )
    }

    /// Under `any()` the history test narrows: any value ever held may match.
    fn pushdown(&self) -> Option<Pushdown> {
        self.0
            .index_query()
            .filter(|query| query.is_history())
            .map(Pushdown::Index)
    }
}

impl<E: CreateOp> CreateOp for AllExpr<E> {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        qualify_node_op(
            &self.0,
            graph,
            |history, test| Arc::new(StreamedAllNodeOp { history, test }),
            |inner| Arc::new(AllNodeOp { inner }),
        )
    }

    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        qualify_edge_op(
            &self.0,
            graph,
            |history, test| Arc::new(StreamedAllEdgeOp { history, test }),
            |inner| Arc::new(AllEdgeOp { inner }),
        )
    }
}

// ── and / or / not of yes/no values ──────────────────────────────────────────

/// `and` of yes/no values of one entity. Built from a filter tree; a typed
/// `and` of two filters is an [`AndFilter`](super::super::and_filter::AndFilter).
#[derive(Clone)]
pub struct AndExpr<E> {
    pub items: Vec<E>,
    pub entity: EntityMarker,
}

impl<E: EntityExpr> EntityExpr for AndExpr<E> {
    fn entity(&self) -> EntityMarker {
        self.entity
    }

    fn nullable(&self) -> bool {
        false
    }
}

impl<E: CreateOp> CreateOp for AndExpr<E> {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        Ok(Arc::new(AndValueNodeOp {
            items: bool_node_ops(&self.items, graph, "and")?,
        }))
    }

    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        Ok(Arc::new(AndValueEdgeOp {
            items: bool_edge_ops(&self.items, graph, "and")?,
        }))
    }
}

/// `or` of yes/no values of one entity.
#[derive(Clone)]
pub struct OrExpr<E> {
    pub items: Vec<E>,
    pub entity: EntityMarker,
}

impl<E: EntityExpr> EntityExpr for OrExpr<E> {
    fn entity(&self) -> EntityMarker {
        self.entity
    }

    fn nullable(&self) -> bool {
        false
    }
}

impl<E: CreateOp> CreateOp for OrExpr<E> {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        Ok(Arc::new(OrValueNodeOp {
            items: bool_node_ops(&self.items, graph, "or")?,
        }))
    }

    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        Ok(Arc::new(OrValueEdgeOp {
            items: bool_edge_ops(&self.items, graph, "or")?,
        }))
    }
}

/// The yes/no ops of `items`, each checked to answer yes/no. An empty list has
/// no meaning either way, so it is refused.
fn bool_node_ops<'g, E: CreateOp, G: GraphView + 'g>(
    items: &[E],
    graph: G,
    name: &str,
) -> Result<Vec<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>>, GraphError> {
    if items.is_empty() {
        return Err(invalid(format!("`{name}` needs at least one operand")));
    }
    items
        .iter()
        .map(|item| {
            let op = item.create_node_op(graph.clone())?;
            require_bool(&resolved_prop_type(item.prop_type(), op.prop_type()), name)?;
            Ok(op)
        })
        .collect()
}

fn bool_edge_ops<'g, E: CreateOp, G: GraphView + 'g>(
    items: &[E],
    graph: G,
    name: &str,
) -> Result<Vec<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>>, GraphError> {
    if items.is_empty() {
        return Err(invalid(format!("`{name}` needs at least one operand")));
    }
    items
        .iter()
        .map(|item| {
            let op = item.create_edge_op(graph.clone())?;
            require_bool(&resolved_prop_type(item.prop_type(), op.prop_type()), name)?;
            Ok(op)
        })
        .collect()
}

/// `not` of a yes/no value: the opposite answer on the same entity.
#[derive(Clone)]
pub struct NotExpr<E>(pub E);

impl<E: EntityExpr> EntityExpr for NotExpr<E> {
    fn entity(&self) -> EntityMarker {
        self.0.entity()
    }

    fn nullable(&self) -> bool {
        false
    }
}

impl<E: CreateOp> CreateOp for NotExpr<E> {
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let inner = self.0.create_node_op(graph)?;
        require_bool(
            &resolved_prop_type(self.0.prop_type(), inner.prop_type()),
            "not",
        )?;
        Ok(Arc::new(UnaryValueNodeOp {
            inner,
            param: (),
            kernel: not_kernel,
            out: PropType::Bool,
        }))
    }

    fn create_edge_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn EdgeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let inner = self.0.create_edge_op(graph)?;
        require_bool(
            &resolved_prop_type(self.0.prop_type(), inner.prop_type()),
            "not",
        )?;
        Ok(Arc::new(UnaryValueEdgeOp {
            inner,
            param: (),
            kernel: not_kernel,
            out: PropType::Bool,
        }))
    }
}

#[cfg(test)]
mod streaming_tests {
    use super::*;
    use crate::{
        db::graph::views::filter::model::{
            expr::{Expr, NodeLeaf},
            node_expr::{typing::list, Compiled},
            node_filter::NodeFilter,
            EntityExprFilterOps, PropertyExprFactory,
        },
        prelude::EntityAggOps,
    };
    use raphtory_api::core::entities::properties::prop::IntoProp;

    #[test]
    fn a_history_term_walks_and_a_latest_value_does_not() {
        assert!(NodeFilter
            .property("score")
            .temporal()
            .compiled()
            .history()
            .is_some());
        assert!(NodeFilter.property("score").compiled().history().is_none());
        assert!(NodeFilter
            .property("score")
            .temporal()
            .sum()
            .compiled()
            .history()
            .is_none());
    }

    #[test]
    fn a_history_compared_with_a_constant_walks_from_either_side() {
        let gt = NodeFilter.property("score").temporal().gt(4i64);
        assert!(matches!(
            gt.compiled().value_test(),
            Some((_, ValueTest::Cmp(BinaryOp::Gt, Prop::I64(4))))
        ));
        let mirrored =
            Expr::<NodeLeaf>::Const(Prop::I64(4)).lt(NodeFilter.property("score").temporal());
        assert!(matches!(
            mirrored.compiled().value_test(),
            Some((_, ValueTest::Cmp(BinaryOp::Gt, Prop::I64(4))))
        ));
        let contains = NodeFilter.property("name").temporal().contains("a");
        assert!(matches!(
            contains.compiled().value_test(),
            Some((_, ValueTest::Str(StringOp::Contains, _)))
        ));
        let is_in = NodeFilter
            .property("score")
            .temporal()
            .is_not_in([1i64.into_prop()]);
        assert!(matches!(
            is_in.compiled().value_test(),
            Some((_, ValueTest::In(_, true)))
        ));
    }

    #[test]
    fn anything_else_under_any_keeps_the_list() {
        let latest = NodeFilter.property("score").gt(4i64);
        assert!(latest.compiled().value_test().is_none());
        let two_reads = NodeFilter
            .property("score")
            .temporal()
            .gt(NodeFilter.property("other").temporal());
        assert!(two_reads.compiled().value_test().is_none());
        let aggregated = NodeFilter.property("score").temporal().sum().gt(4i64);
        assert!(aggregated.compiled().value_test().is_none());
    }

    #[test]
    fn only_a_one_answer_per_value_test_walks() {
        let history = list(PropType::I64);
        let gt = ValueTest::Cmp(BinaryOp::Gt, 4i64.into_prop());
        assert!(gt.for_history(&history).is_some());
        // A constant list compares against the whole history, not each value.
        let whole = ValueTest::Cmp(BinaryOp::Eq, Prop::list([1i64, 2i64]).unwrap());
        assert!(whole.for_history(&history).is_none());
        // A mismatch is left to the list path, which reports it.
        assert!(gt.for_history(&list(PropType::Str)).is_none());
        // A history of lists answers per element, not per value.
        assert!(gt.for_history(&list(list(PropType::I64))).is_none());
        // A set no value can be in still answers per value, as the list path does.
        let none = ValueTest::members(vec!["x".into_prop()], false);
        assert!(
            matches!(none.for_history(&history), Some(ValueTest::In(members, false)) if members.is_empty())
        );
        // An empty set is a whole-history test, which the list path refuses.
        let empty = ValueTest::members(Vec::new(), false);
        assert!(empty.for_history(&history).is_none());
    }
}

#[cfg(test)]
mod pushdown_tests {
    use super::*;
    use crate::{
        db::graph::views::filter::model::{
            expr::{Expr, NodeLeaf},
            node_expr::{predicate::IndexTest, Compiled},
            node_filter::{NodeFilter, NodeFilterFactory},
            EntityExprFilterOps, PropertyExprFactory, ViewWrapOps,
        },
        prelude::EntityAggOps,
    };
    use raphtory_api::core::entities::properties::prop::IntoProp;

    fn property(name: &str, ever: bool) -> IndexTerm {
        IndexTerm::Property {
            name: name.to_owned(),
            metadata: false,
            ever,
        }
    }

    fn index_of(pushdown: Option<Pushdown>) -> IndexQuery {
        match pushdown {
            Some(Pushdown::Index(query)) => query,
            other => panic!("expected an index query, got {other:?}"),
        }
    }

    #[test]
    fn plain_property_tests_reach_the_index_from_either_side() {
        let gt = index_of(NodeFilter.property("score").gt(4i64).compiled().pushdown());
        assert_eq!(gt.term(), &property("score", false));
        assert_eq!(gt.test(), &IndexTest::Gt(4i64.into_prop()));
        let flipped = Expr::<NodeLeaf>::Const(Prop::I64(4)).lt(NodeFilter.property("score"));
        assert_eq!(
            index_of(flipped.compiled().pushdown()).test(),
            &IndexTest::Gt(4i64.into_prop())
        );
        let contains = index_of(
            NodeFilter
                .property("name")
                .contains("acme")
                .compiled()
                .pushdown(),
        );
        assert_eq!(contains.term(), &property("name", false));
        assert_eq!(contains.test(), &IndexTest::Contains("acme".to_owned()));
        let members = index_of(
            NodeFilter
                .property("tag")
                .is_in(["a".into_prop(), "b".into_prop()])
                .compiled()
                .pushdown(),
        );
        assert_eq!(members.term(), &property("tag", false));
        let metadata = index_of(NodeFilter.metadata("kind").eq("x").compiled().pushdown());
        assert_eq!(
            metadata.term(),
            &IndexTerm::Property {
                name: "kind".to_owned(),
                metadata: true,
                ever: false
            }
        );
    }

    #[test]
    fn a_history_under_any_asks_for_every_value_ever_held() {
        let any = NodeFilter
            .property("score")
            .temporal()
            .eq(4i64)
            .any()
            .compiled()
            .pushdown();
        assert_eq!(index_of(any).term(), &property("score", true));
        // The latest update of a history is the property's latest value.
        let latest = NodeFilter
            .property("score")
            .temporal()
            .latest()
            .eq(4i64)
            .compiled()
            .pushdown();
        assert_eq!(index_of(latest).term(), &property("score", false));
        // A history outside any() has no single value the index can test.
        assert!(NodeFilter
            .property("score")
            .temporal()
            .eq(4i64)
            .compiled()
            .pushdown()
            .is_none());
        // all() must see every value, so it scans.
        assert!(NodeFilter
            .property("score")
            .temporal()
            .eq(4i64)
            .all()
            .compiled()
            .pushdown()
            .is_none());
    }

    #[test]
    fn what_the_index_cannot_answer_scans() {
        assert!(NodeFilter
            .latest()
            .property("score")
            .eq(4i64)
            .compiled()
            .pushdown()
            .is_none());
        assert!(NodeFilter
            .property("score")
            .ne(4i64)
            .compiled()
            .pushdown()
            .is_none());
        assert!(NodeFilter
            .property("tag")
            .is_not_in(["a".into_prop()])
            .compiled()
            .pushdown()
            .is_none());
        // A pattern on the name goes to the id index; equality resolves the node outright.
        let prefix = index_of(NodeFilter.name().starts_with("bo").compiled().pushdown());
        assert_eq!(prefix.term(), &IndexTerm::Name);
        assert_eq!(prefix.test(), &IndexTest::StartsWith("bo".to_owned()));
        assert!(NodeFilter
            .property("a")
            .eq(NodeFilter.property("b"))
            .compiled()
            .pushdown()
            .is_none());
    }

    #[test]
    fn the_ids_a_predicate_names_are_resolved_outright() {
        assert_eq!(
            NodeFilter.id().eq(1u64).compiled().pushdown(),
            Some(Pushdown::Ids(vec![1u64.into_prop()]))
        );
        assert_eq!(
            NodeFilter.id().is_in([1u64, 2u64]).compiled().pushdown(),
            Some(Pushdown::Ids(vec![1u64.into_prop(), 2u64.into_prop()]))
        );
        // The name is the node's external id, so equality on it resolves the node too.
        assert_eq!(
            NodeFilter.name().eq("bob").compiled().pushdown(),
            Some(Pushdown::Ids(vec!["bob".into_prop()]))
        );
        assert_eq!(
            NodeFilter
                .name()
                .is_in(["bob", "carol"])
                .compiled()
                .pushdown(),
            Some(Pushdown::Ids(vec!["bob".into_prop(), "carol".into_prop()]))
        );
        // Only equality names nodes; an ordering on the id is a scan.
        assert!(NodeFilter.id().gt(1u64).compiled().pushdown().is_none());
        assert!(NodeFilter.name().ne("bob").compiled().pushdown().is_none());
        // A view that can hide nodes takes the id off the index.
        assert!(NodeFilter
            .latest()
            .id()
            .eq(1u64)
            .compiled()
            .pushdown()
            .is_none());
        assert!(NodeFilter
            .latest()
            .name()
            .eq("bob")
            .compiled()
            .pushdown()
            .is_none());
    }
}
