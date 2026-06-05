use crate::{
    db::{
        api::{
            properties::PropertiesOps,
            state::ops::{Const, Degree, Name, NodeOp, Type},
            view::{internal::GraphView, NodeViewOps},
        },
        graph::views::filter::{
            model::{
                edge_filter::CompositeEdgeFilter,
                filter_operator::{BinaryOp, Comparable, SetOp, UnaryOp},
                ComposableFilter, CompositeExplodedEdgeFilter, CompositeNodeFilter, CreateFilter,
                TryAsCompositeFilter,
            },
            node_filtered_graph::NodeFilteredGraph,
        },
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use raphtory_api::core::{
    entities::{properties::prop::Prop, VID},
    storage::arc_str::ArcStr,
    Direction,
};
use raphtory_storage::graph::graph::GraphStorage;
use std::{collections::HashSet, hash::Hash, marker::PhantomData, sync::Arc};

// ─────────────────────────────────────────────────────────────────────────────
// NodeExpr — typed node expression with associated Output type
// ─────────────────────────────────────────────────────────────────────────────

/// A typed expression that produces a value per node.
///
/// `Output` carries nullability only where the value can genuinely be absent:
/// `Option<Prop>` for properties/metadata, `Option<ArcStr>` for node type.
/// Always-present values use non-optional types: `usize` for degree, `String` for name.
///
/// Calling `create_node_op` resolves name→ID lookups once against the graph,
/// returning a `NodeOp` that evaluates in O(1) per node.
///
/// Usage:
/// ```rust,ignore
/// NodeFilter::degree().gt(2usize)
/// NodeFilter::out_degree().gt(NodeFilter::in_degree())
/// NodeFilter::property("age").gt(30i64)
/// NodeFilter::name().eq("Alice")
/// ```
///
pub trait NodeExpr: Clone + Send + Sync + 'static {
    type Output: Comparable + Clone + Send + Sync + 'static;

    /// Compile the expression against a specific graph view.
    ///
    /// Any name→ID resolution (property, metadata) happens here, once.
    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Self::Output> + 'g>, GraphError>;
}

// ─────────────────────────────────────────────────────────────────────────────
// NodePropOp<G> / NodeMetaOp<G> — prop_id resolved at creation time
// ─────────────────────────────────────────────────────────────────────────────

/// Evaluates a temporal property by pre-resolved column ID.
#[derive(Clone)]
pub(crate) struct NodePropOp<G> {
    graph: G,
    prop_id: usize,
}

impl<G: GraphView> NodeOp for NodePropOp<G> {
    type Output = Option<Prop>;

    fn apply(&self, _storage: &GraphStorage, node: VID) -> Option<Prop> {
        self.graph.node(node)?.properties().get_by_id(self.prop_id)
    }
}

/// Evaluates a metadata (static) field by pre-resolved column ID.
#[derive(Clone)]
pub(crate) struct NodeMetaOp<G> {
    graph: G,
    prop_id: usize,
}

impl<G: GraphView> NodeOp for NodeMetaOp<G> {
    type Output = Option<Prop>;

    fn apply(&self, _storage: &GraphStorage, node: VID) -> Option<Prop> {
        self.graph.node(node)?.metadata().get_by_id(self.prop_id)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Concrete expression structs
// ─────────────────────────────────────────────────────────────────────────────

/// Wraps a `Direction` so it can be used as a `NodeExpr` for degree filtering.
///
/// Delegates to `Degree<G>` from `db/api/state/ops/node.rs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DegreeExpr(pub Direction);

impl NodeExpr for DegreeExpr {
    type Output = usize;

    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = usize> + 'g>, GraphError> {
        Ok(Arc::new(Degree {
            dir: self.0,
            view: graph,
        }))
    }
}

/// Current (latest) value of a named property.
///
/// The property name is resolved to a column ID once at `create_node_op` time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Property {
    pub name: String,
}

impl Property {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl NodeExpr for Property {
    type Output = Option<Prop>;

    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let (prop_id, _) = graph
            .node_meta()
            .get_prop_id_and_type(&self.name, false)
            .ok_or_else(|| GraphError::PropertyMissingError(self.name.clone()))?;
        Ok(Arc::new(NodePropOp { graph, prop_id }))
    }
}

/// Static metadata field.
///
/// The metadata name is resolved to a column ID once at `create_node_op` time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Metadata {
    pub name: String,
}

impl Metadata {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl NodeExpr for Metadata {
    type Output = Option<Prop>;

    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        let (prop_id, _) = graph
            .node_meta()
            .get_prop_id_and_type(&self.name, true)
            .ok_or_else(|| GraphError::MetadataMissingError(self.name.clone()))?;
        Ok(Arc::new(NodeMetaOp { graph, prop_id }))
    }
}

/// `Type` from `db/api/state/ops/node.rs` used as a node expression.
///
/// `Type: NodeOp<Output = Option<ArcStr>>` — used directly, no conversion.
impl NodeExpr for Type {
    type Output = Option<ArcStr>;

    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<ArcStr>> + 'g>, GraphError> {
        Ok(Arc::new(Type))
    }
}

/// `Name` from `db/api/state/ops/node.rs` used as a node expression.
impl NodeExpr for Name {
    type Output = String;

    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = String> + 'g>, GraphError> {
        Ok(Arc::new(Name))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// NodeExpr impls for constant value types
//
// Allows passing raw values directly to filter operators:
//   NodeFilter::degree().gt(2usize)
//   NodeFilter::name().eq("Alice")
//   NodeFilter::property("age").gt(30i64)
// ─────────────────────────────────────────────────────────────────────────────

impl NodeExpr for usize {
    type Output = usize;

    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = usize> + 'g>, GraphError> {
        Ok(Arc::new(Const(*self)))
    }
}

impl NodeExpr for String {
    type Output = String;

    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = String> + 'g>, GraphError> {
        Ok(Arc::new(Const(self.clone())))
    }
}

impl NodeExpr for ArcStr {
    type Output = Option<ArcStr>;

    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<ArcStr>> + 'g>, GraphError> {
        Ok(Arc::new(Const(Some(self.clone()))))
    }
}

impl NodeExpr for &'static str {
    type Output = String;

    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = String> + 'g>, GraphError> {
        Ok(Arc::new(Const(self.to_string())))
    }
}

impl NodeExpr for Prop {
    type Output = Option<Prop>;

    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
        Ok(Arc::new(Const(Some(self.clone()))))
    }
}

macro_rules! impl_node_expr_for_numeric {
    ($prim:ty, $variant:ident) => {
        impl NodeExpr for $prim {
            type Output = Option<Prop>;

            fn create_node_op<'g, G: GraphView + 'g>(
                &self,
                _graph: G,
            ) -> Result<Arc<dyn NodeOp<Output = Option<Prop>> + 'g>, GraphError> {
                Ok(Arc::new(Const(Some(Prop::$variant(*self)))))
            }
        }
    };
}

impl_node_expr_for_numeric!(i32, I32);
impl_node_expr_for_numeric!(i64, I64);
impl_node_expr_for_numeric!(u32, U32);
impl_node_expr_for_numeric!(u64, U64);
impl_node_expr_for_numeric!(f32, F32);
impl_node_expr_for_numeric!(f64, F64);
impl_node_expr_for_numeric!(bool, Bool);
impl_node_expr_for_numeric!(u8, U8);
impl_node_expr_for_numeric!(u16, U16);

/// A constant expression for custom output types not covered by the built-in impls.
///
/// Built-in types (`usize`, `String`, `Prop`, etc.) can be passed directly;
/// `ConstExpr<T>` is only needed for custom attribute output types.
#[derive(Clone)]
pub struct ConstExpr<T: Comparable + Clone + Send + Sync + 'static>(pub T)
where
    Option<T>: Comparable;

impl<T: Comparable + Clone + Send + Sync + 'static> NodeExpr for ConstExpr<T>
where
    Option<T>: Comparable,
{
    type Output = Option<T>;

    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<T>> + 'g>, GraphError> {
        Ok(Arc::new(Const(Some(self.0.clone()))))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// BinOpNodeOp<'g, T> — compares two NodeOp<Output = T> using BinaryOp
// ─────────────────────────────────────────────────────────────────────────────

/// Execution op for `BinOpNodeFilter`.
///
/// Holds two compiled `NodeOp<Output = T>` (type-erased via `Arc<dyn NodeOp>`)
/// and applies `T::binary_cmp`.  The `'g` lifetime bounds both ops to the graph
/// view they were compiled against.
#[derive(Clone)]
pub struct BinOpNodeOp<'g, T: Comparable> {
    pub(crate) left: Arc<dyn NodeOp<Output = T> + 'g>,
    pub(crate) right: Arc<dyn NodeOp<Output = T> + 'g>,
    pub(crate) op: BinaryOp,
}

impl<'g, T: Comparable + Clone + Send + Sync + 'static> NodeOp for BinOpNodeOp<'g, T> {
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, node: VID) -> bool {
        let lv = self.left.apply(storage, node);
        let rv = self.right.apply(storage, node);
        T::binary_cmp(&self.op, &lv, &rv)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// UnaryNodeOp<'g, T> — evaluates is_some / is_none
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct UnaryNodeOp<'g, I: Clone + Send + Sync + 'static> {
    inner: Arc<dyn NodeOp<Output = Option<I>> + 'g>,
    op: UnaryOp,
}

impl<'g, I: Clone + Send + Sync + 'static> NodeOp for UnaryNodeOp<'g, I> {
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, node: VID) -> bool {
        let v = self.inner.apply(storage, node);
        match self.op {
            UnaryOp::IsSome => v.is_some(),
            UnaryOp::IsNone => v.is_none(),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SetNodeOp<'g, T> — evaluates is_in / is_not_in
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct SetNodeOp<'g, I: Eq + Hash + Clone + Send + Sync + 'static> {
    inner: Arc<dyn NodeOp<Output = Option<I>> + 'g>,
    op: SetOp,
    values: Arc<HashSet<I>>,
}

impl<'g, I: Eq + Hash + Clone + Send + Sync + 'static> NodeOp for SetNodeOp<'g, I> {
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, node: VID) -> bool {
        let v = self.inner.apply(storage, node);
        match self.op {
            SetOp::IsIn => v.as_ref().map(|x| self.values.contains(x)).unwrap_or(false),
            SetOp::IsNotIn => v
                .as_ref()
                .map(|x| !self.values.contains(x))
                .unwrap_or(false),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// BinOpNodeFilter<L, R> — binary expression filter (no PhantomData)
// ─────────────────────────────────────────────────────────────────────────────

/// A node filter that compares two `NodeExpr` values using a `BinaryOp`.
///
/// The output type is determined by the left expression (`L::Output`);
/// the right expression must produce the same type.  No `PhantomData` required
/// because the output type is encoded as an associated type of `L`.
///
/// Created by `NodeExprFilterOps`:
/// ```rust,ignore
/// DegreeExpr(Direction::BOTH).gt(2usize)
/// DegreeExpr(Direction::OUT).gt(DegreeExpr(Direction::IN))
/// NodeFilter::property("age").gt(30i64)
/// NodeFilter::name().eq("Alice")
/// ```
pub struct BinOpNodeFilter<L, R>
where
    L: NodeExpr,
    R: NodeExpr<Output = L::Output>,
{
    pub left: L,
    pub op: BinaryOp,
    pub right: R,
}

impl<L, R> BinOpNodeFilter<L, R>
where
    L: NodeExpr,
    R: NodeExpr<Output = L::Output>,
{
    pub fn new(left: L, op: BinaryOp, right: R) -> Self {
        Self { left, op, right }
    }
}

impl<L, R> Clone for BinOpNodeFilter<L, R>
where
    L: NodeExpr,
    R: NodeExpr<Output = L::Output>,
{
    fn clone(&self) -> Self {
        Self {
            left: self.left.clone(),
            op: self.op,
            right: self.right.clone(),
        }
    }
}

impl<L, R> ComposableFilter for BinOpNodeFilter<L, R>
where
    L: NodeExpr,
    R: NodeExpr<Output = L::Output>,
{
}

impl<L, R> CreateFilter for BinOpNodeFilter<L, R>
where
    L: NodeExpr,
    R: NodeExpr<Output = L::Output>,
    L::Output: Comparable,
{
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> =
        NodeFilteredGraph<G, BinOpNodeOp<'graph, L::Output>>;

    type NodeFilter<'graph, G: GraphView + 'graph> = BinOpNodeOp<'graph, L::Output>;

    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphViewOps<'graph>;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        let filter = self.create_node_filter(graph.clone())?;
        Ok(NodeFilteredGraph::new(graph, filter))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        let left = self.left.create_node_op(graph.clone())?;
        let right = self.right.create_node_op(graph)?;
        Ok(BinOpNodeOp {
            left,
            right,
            op: self.op,
        })
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        Ok(graph)
    }
}

impl<L, R> TryAsCompositeFilter for BinOpNodeFilter<L, R>
where
    L: NodeExpr,
    R: NodeExpr<Output = L::Output>,
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

// ─────────────────────────────────────────────────────────────────────────────
// UnaryNodeFilter<E> — is_some / is_none on nullable expressions
// ─────────────────────────────────────────────────────────────────────────────

/// A node filter that tests the presence of an `Option`-valued expression.
///
/// Created by `.is_some()` and `.is_none()` on any `NodeExpr<Output = Option<I>>`.
pub struct UnaryNodeFilter<E, I>
where
    E: NodeExpr<Output = Option<I>>,
    I: Clone + Send + Sync + 'static,
{
    pub expr: E,
    pub op: UnaryOp,
    _phantom: PhantomData<I>,
}

impl<E, I> Clone for UnaryNodeFilter<E, I>
where
    E: NodeExpr<Output = Option<I>>,
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

impl<E, I> ComposableFilter for UnaryNodeFilter<E, I>
where
    E: NodeExpr<Output = Option<I>>,
    I: Clone + Send + Sync + 'static,
{
}

impl<E, I> CreateFilter for UnaryNodeFilter<E, I>
where
    E: NodeExpr<Output = Option<I>>,
    I: Clone + Send + Sync + 'static,
{
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> =
        NodeFilteredGraph<G, UnaryNodeOp<'graph, I>>;

    type NodeFilter<'graph, G: GraphView + 'graph> = UnaryNodeOp<'graph, I>;

    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphViewOps<'graph>;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        let filter = self.create_node_filter(graph.clone())?;
        Ok(NodeFilteredGraph::new(graph, filter))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        let inner = self.expr.create_node_op(graph)?;
        Ok(UnaryNodeOp { inner, op: self.op })
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        Ok(graph)
    }
}

impl<E, I> TryAsCompositeFilter for UnaryNodeFilter<E, I>
where
    E: NodeExpr<Output = Option<I>>,
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

// ─────────────────────────────────────────────────────────────────────────────
// SetNodeFilter<E> — is_in / is_not_in on nullable expressions
// ─────────────────────────────────────────────────────────────────────────────

/// A node filter that checks whether the inner value of an `Option`-valued
/// expression is contained in (or absent from) a fixed set.
///
/// Created by `.is_in(values)` and `.is_not_in(values)`.
pub struct SetNodeFilter<E, I>
where
    E: NodeExpr<Output = Option<I>>,
    I: Eq + Hash + Clone + Send + Sync + 'static,
{
    pub expr: E,
    pub op: SetOp,
    pub values: Arc<HashSet<I>>,
    _phantom: PhantomData<I>,
}

impl<E, I> Clone for SetNodeFilter<E, I>
where
    E: NodeExpr<Output = Option<I>>,
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

impl<E, I> ComposableFilter for SetNodeFilter<E, I>
where
    E: NodeExpr<Output = Option<I>>,
    I: Eq + Hash + Clone + Send + Sync + 'static,
{
}

impl<E, I> CreateFilter for SetNodeFilter<E, I>
where
    E: NodeExpr<Output = Option<I>>,
    I: Eq + Hash + Clone + Send + Sync + 'static,
{
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> =
        NodeFilteredGraph<G, SetNodeOp<'graph, I>>;

    type NodeFilter<'graph, G: GraphView + 'graph> = SetNodeOp<'graph, I>;

    type FilteredGraph<'graph, G>
        = G
    where
        Self: 'graph,
        G: GraphViewOps<'graph>;

    fn create_filter<'graph, G: GraphViewOps<'graph>>(
        self,
        graph: G,
    ) -> Result<Self::EntityFiltered<'graph, G>, GraphError> {
        let filter = self.create_node_filter(graph.clone())?;
        Ok(NodeFilteredGraph::new(graph, filter))
    }

    fn create_node_filter<'graph, G: GraphView + 'graph>(
        self,
        graph: G,
    ) -> Result<Self::NodeFilter<'graph, G>, GraphError> {
        let inner = self.expr.create_node_op(graph)?;
        Ok(SetNodeOp {
            inner,
            op: self.op,
            values: self.values,
        })
    }

    fn filter_graph_view<'graph, G: GraphView + 'graph>(
        &self,
        graph: G,
    ) -> Result<Self::FilteredGraph<'graph, G>, GraphError> {
        Ok(graph)
    }
}

impl<E, I> TryAsCompositeFilter for SetNodeFilter<E, I>
where
    E: NodeExpr<Output = Option<I>>,
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

// ─────────────────────────────────────────────────────────────────────────────
// NodeExprFilterOps — comparison and set operators on NodeExpr
// ─────────────────────────────────────────────────────────────────────────────

/// Comparison, string, set, and presence operators on any `NodeExpr`.
///
/// `gt(rhs)` accepts any `R: NodeExpr<Output = Self::Output>`:
/// ```rust,ignore
/// DegreeExpr(Direction::BOTH).gt(2usize)
/// DegreeExpr(Direction::OUT).gt(DegreeExpr(Direction::IN))
/// NodeFilter::property("age").gt(30i64)
/// DegreeExpr(Direction::BOTH).is_in([2usize, 3usize])
/// ```
pub trait NodeExprFilterOps: NodeExpr + Sized {
    fn gt<R: NodeExpr<Output = Self::Output>>(self, rhs: R) -> BinOpNodeFilter<Self, R> {
        BinOpNodeFilter::new(self, BinaryOp::Gt, rhs)
    }

    fn ge<R: NodeExpr<Output = Self::Output>>(self, rhs: R) -> BinOpNodeFilter<Self, R> {
        BinOpNodeFilter::new(self, BinaryOp::Ge, rhs)
    }

    fn lt<R: NodeExpr<Output = Self::Output>>(self, rhs: R) -> BinOpNodeFilter<Self, R> {
        BinOpNodeFilter::new(self, BinaryOp::Lt, rhs)
    }

    fn le<R: NodeExpr<Output = Self::Output>>(self, rhs: R) -> BinOpNodeFilter<Self, R> {
        BinOpNodeFilter::new(self, BinaryOp::Le, rhs)
    }

    fn eq<R: NodeExpr<Output = Self::Output>>(self, rhs: R) -> BinOpNodeFilter<Self, R> {
        BinOpNodeFilter::new(self, BinaryOp::Eq, rhs)
    }

    fn ne<R: NodeExpr<Output = Self::Output>>(self, rhs: R) -> BinOpNodeFilter<Self, R> {
        BinOpNodeFilter::new(self, BinaryOp::Ne, rhs)
    }

    fn starts_with<R: NodeExpr<Output = Self::Output>>(self, rhs: R) -> BinOpNodeFilter<Self, R> {
        BinOpNodeFilter::new(self, BinaryOp::StartsWith, rhs)
    }

    fn ends_with<R: NodeExpr<Output = Self::Output>>(self, rhs: R) -> BinOpNodeFilter<Self, R> {
        BinOpNodeFilter::new(self, BinaryOp::EndsWith, rhs)
    }

    fn contains<R: NodeExpr<Output = Self::Output>>(self, rhs: R) -> BinOpNodeFilter<Self, R> {
        BinOpNodeFilter::new(self, BinaryOp::Contains, rhs)
    }

    fn not_contains<R: NodeExpr<Output = Self::Output>>(self, rhs: R) -> BinOpNodeFilter<Self, R> {
        BinOpNodeFilter::new(self, BinaryOp::NotContains, rhs)
    }

    fn fuzzy_search<R: NodeExpr<Output = Self::Output>>(
        self,
        rhs: R,
        levenshtein_distance: usize,
        prefix_match: bool,
    ) -> BinOpNodeFilter<Self, R> {
        BinOpNodeFilter::new(
            self,
            BinaryOp::FuzzySearch {
                levenshtein_distance,
                prefix_match,
            },
            rhs,
        )
    }

    fn is_some<Inner>(self) -> UnaryNodeFilter<Self, Inner>
    where
        Self: NodeExpr<Output = Option<Inner>>,
        Inner: Clone + Send + Sync + 'static,
    {
        UnaryNodeFilter {
            expr: self,
            op: UnaryOp::IsSome,
            _phantom: PhantomData,
        }
    }

    fn is_none<Inner>(self) -> UnaryNodeFilter<Self, Inner>
    where
        Self: NodeExpr<Output = Option<Inner>>,
        Inner: Clone + Send + Sync + 'static,
    {
        UnaryNodeFilter {
            expr: self,
            op: UnaryOp::IsNone,
            _phantom: PhantomData,
        }
    }

    fn is_in<Inner, Iter>(self, values: Iter) -> SetNodeFilter<Self, Inner>
    where
        Self: NodeExpr<Output = Option<Inner>>,
        Inner: Eq + Hash + Clone + Send + Sync + 'static,
        Iter: IntoIterator<Item = Inner>,
    {
        let set: HashSet<_> = values.into_iter().collect();
        SetNodeFilter {
            expr: self,
            op: SetOp::IsIn,
            values: Arc::new(set),
            _phantom: PhantomData,
        }
    }

    fn is_not_in<Inner, Iter>(self, values: Iter) -> SetNodeFilter<Self, Inner>
    where
        Self: NodeExpr<Output = Option<Inner>>,
        Inner: Eq + Hash + Clone + Send + Sync + 'static,
        Iter: IntoIterator<Item = Inner>,
    {
        let set: HashSet<_> = values.into_iter().collect();
        SetNodeFilter {
            expr: self,
            op: SetOp::IsNotIn,
            values: Arc::new(set),
            _phantom: PhantomData,
        }
    }
}

impl<E: NodeExpr> NodeExprFilterOps for E {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prelude::{AdditionOps, Graph, GraphViewOps, NodeViewOps, NO_PROPS};

    // Test graph: a→b, a→c, b→c
    // All nodes have total degree 2; in-degrees: a=0, b=1, c=2
    fn build_test_graph() -> Graph {
        let g = Graph::new();
        g.add_edge(0, "a", "b", NO_PROPS, None).unwrap();
        g.add_edge(0, "a", "c", NO_PROPS, None).unwrap();
        g.add_edge(0, "b", "c", NO_PROPS, None).unwrap();
        g
    }

    fn filtered_names<F>(filter: F, g: Graph) -> Vec<String>
    where
        F: CreateFilter,
        for<'graph> F::EntityFiltered<'graph, Graph>: GraphViewOps<'graph>,
    {
        let mut names: Vec<String> = filter
            .create_filter(g)
            .unwrap()
            .nodes()
            .iter()
            .map(|n| n.name())
            .collect();
        names.sort();
        names
    }

    // ── DegreeExpr comparison operators ──────────────────────────────────────

    #[test]
    fn degree_ge_2_keeps_all_nodes() {
        let g = build_test_graph();
        assert_eq!(
            filtered_names(DegreeExpr(Direction::BOTH).ge(2usize), g),
            vec!["a", "b", "c"]
        );
    }

    #[test]
    fn degree_eq_1_keeps_no_nodes() {
        let g = build_test_graph();
        assert!(filtered_names(DegreeExpr(Direction::BOTH).eq(1usize), g).is_empty());
    }

    #[test]
    fn degree_le_2_keeps_all_nodes() {
        let g = build_test_graph();
        assert_eq!(
            filtered_names(DegreeExpr(Direction::BOTH).le(2usize), g),
            vec!["a", "b", "c"]
        );
    }

    #[test]
    fn degree_gt_2_keeps_no_nodes() {
        let g = build_test_graph();
        assert!(filtered_names(DegreeExpr(Direction::BOTH).gt(2usize), g).is_empty());
    }

    #[test]
    fn degree_ne_2_keeps_no_nodes_when_all_are_2() {
        let g = build_test_graph();
        assert!(filtered_names(DegreeExpr(Direction::BOTH).ne(2usize), g).is_empty());
    }

    // ── expression-vs-expression: RHS can be another NodeExpr ────────────────

    #[test]
    fn total_gt_in_degree_selects_nodes_with_outgoing_edges() {
        // total=2, in-degrees: a=0, b=1, c=2 → total > in for a and b only
        let g = build_test_graph();
        assert_eq!(
            filtered_names(DegreeExpr(Direction::BOTH).gt(DegreeExpr(Direction::IN)), g),
            vec!["a", "b"]
        );
    }

    // ── ConstExpr for custom output types ────────────────────────────────────

    #[test]
    fn const_expr_works() {
        let filter = BinOpNodeFilter::new(ConstExpr(2usize), BinaryOp::Eq, ConstExpr(2usize));
        let g = build_test_graph();
        assert_eq!(filtered_names(filter, g), vec!["a", "b", "c"]);
    }
}
