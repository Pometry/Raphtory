use crate::{
    db::{
        api::{
            properties::PropertiesOps,
            state::ops::{Const, Degree, Id, Name, NodeOp, Type},
            view::{
                internal::{GraphView, NodeList},
                NodeViewOps,
            },
        },
        graph::views::filter::{
            model::{
                edge_filter::CompositeEdgeFilter,
                filter_operator::{BinaryOp, Comparable, SetOp, UnaryOp},
                property_filter::{evaluate::aggregate_values, Op},
                ComposableFilter, CompositeExplodedEdgeFilter, CompositeNodeFilter, CreateFilter,
                TryAsCompositeFilter, Wrap,
            },
            node_filtered_graph::NodeFilteredGraph,
        },
    },
    errors::GraphError,
    prelude::GraphViewOps,
};
use raphtory_api::core::{
    entities::{properties::prop::Prop, GID, VID},
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
    type Output: Clone + Send + Sync + 'static;

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

/// `Id` from `db/api/state/ops/node.rs` used as a node expression.
impl NodeExpr for Id {
    type Output = GID;

    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = GID> + 'g>, GraphError> {
        Ok(Arc::new(Id))
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
    type Output = &'static str;

    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = &'static str> + 'g>, GraphError> {
        Ok(Arc::new(Const(*self)))
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

impl NodeExpr for GID {
    type Output = GID;

    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Self::Output> + 'g>, GraphError> {
        Ok(Arc::new(Const(self.clone())))
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

#[derive(Debug, Clone, Copy)]
struct AsProp<E>(E);

#[derive(Debug, Clone, Copy)]
struct AsPropOp<Op>(Op);

impl<Op: NodeOp<Output: Into<Prop>>> NodeOp for AsPropOp<Op> {
    type Output = Prop;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Self::Output {
        self.0.apply(storage, node).into()
    }

    fn domain(&self, storage: &GraphStorage) -> NodeList {
        self.0.domain(storage)
    }

    fn const_value_in_domain(&self) -> Option<Self::Output> {
        self.0.const_value_in_domain().map(|v| v.into())
    }

    fn const_value(&self) -> Option<Self::Output> {
        self.0.const_value().map(|v| v.into())
    }
}

impl<E: NodeExpr<Output: Into<Prop>>> NodeExpr for AsProp<E> {
    type Output = Prop;

    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Self::Output> + 'g>, GraphError> {
        Ok(Arc::new(AsPropOp(self.0.create_node_op(graph)?)))
    }
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
pub struct ConstExpr<T>(pub T);

impl<T: Comparable + Clone + Send + Sync + 'static> NodeExpr for ConstExpr<T> {
    type Output = T;

    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = T> + 'g>, GraphError> {
        Ok(Arc::new(Const(self.0.clone())))
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

// ─────────────────────────────────────────────────────────────────────────────
// Sealed trait for QuantifierMode
// ─────────────────────────────────────────────────────────────────────────────

mod sealed {
    pub trait Sealed {}
}

// ─────────────────────────────────────────────────────────────────────────────
// QuantifierMode — AnyMode / AllMode
// ─────────────────────────────────────────────────────────────────────────────

pub trait QuantifierMode: sealed::Sealed + Clone + Copy + Send + Sync + 'static {
    const IS_ANY: bool;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AnyMode;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AllMode;

impl sealed::Sealed for AnyMode {}
impl sealed::Sealed for AllMode {}
impl QuantifierMode for AnyMode {
    const IS_ANY: bool = true;
}
impl QuantifierMode for AllMode {
    const IS_ANY: bool = false;
}

// ─────────────────────────────────────────────────────────────────────────────
// TemporalNodePropOp<G> — returns all temporal values for a property
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub(crate) struct TemporalNodePropOp<G> {
    graph: G,
    prop_id: usize,
}

impl<G: GraphView> NodeOp for TemporalNodePropOp<G> {
    type Output = Vec<Prop>;

    fn apply(&self, _storage: &GraphStorage, node: VID) -> Vec<Prop> {
        self.graph
            .node(node)
            .and_then(|n| {
                n.properties()
                    .temporal()
                    .get_by_id(self.prop_id)
                    .map(|tpv| tpv.values().collect())
            })
            .unwrap_or_default()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// TemporalPropertyExpr — NodeExpr<Output = Vec<Prop>>
// ─────────────────────────────────────────────────────────────────────────────

/// All temporal values of a named property over the current view window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemporalPropertyExpr {
    pub name: String,
}

impl TemporalPropertyExpr {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

impl NodeExpr for TemporalPropertyExpr {
    type Output = Vec<Prop>;

    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Vec<Prop>> + 'g>, GraphError> {
        let (prop_id, _) = graph
            .node_meta()
            .get_prop_id_and_type(&self.name, false)
            .ok_or_else(|| GraphError::PropertyMissingError(self.name.clone()))?;
        Ok(Arc::new(TemporalNodePropOp { graph, prop_id }))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Aggregator NodeOps — compile-time resolved against a concrete graph view
// ─────────────────────────────────────────────────────────────────────────────

macro_rules! impl_agg_node_op {
    ($name:ident, $output:ty, $body:expr) => {
        pub struct $name<'g> {
            pub(crate) inner: Arc<dyn NodeOp<Output = Vec<Prop>> + 'g>,
        }

        impl<'g> Clone for $name<'g> {
            fn clone(&self) -> Self {
                Self {
                    inner: self.inner.clone(),
                }
            }
        }

        impl<'g> NodeOp for $name<'g> {
            type Output = $output;

            fn apply(&self, storage: &GraphStorage, node: VID) -> $output {
                let vals = self.inner.apply(storage, node);
                ($body)(vals)
            }
        }
    };
}

impl_agg_node_op!(SumNodeOp, Option<Prop>, |vals: Vec<Prop>| {
    aggregate_values(&vals, Op::Sum)
});
impl_agg_node_op!(AvgNodeOp, Option<Prop>, |vals: Vec<Prop>| {
    aggregate_values(&vals, Op::Avg)
});
impl_agg_node_op!(MinNodeOp, Option<Prop>, |vals: Vec<Prop>| {
    aggregate_values(&vals, Op::Min)
});
impl_agg_node_op!(MaxNodeOp, Option<Prop>, |vals: Vec<Prop>| {
    aggregate_values(&vals, Op::Max)
});
impl_agg_node_op!(FirstNodeOp, Option<Prop>, |vals: Vec<Prop>| {
    vals.into_iter().next()
});
impl_agg_node_op!(LastNodeOp, Option<Prop>, |vals: Vec<Prop>| {
    vals.into_iter().last()
});
impl_agg_node_op!(LenNodeOp, usize, |vals: Vec<Prop>| { vals.len() });

// ─────────────────────────────────────────────────────────────────────────────
// Aggregator Exprs — NodeExpr wrappers producing a single scalar
// ─────────────────────────────────────────────────────────────────────────────

macro_rules! impl_agg_expr {
    ($expr:ident, $op_ty:ident, $output:ty) => {
        pub struct $expr<E: NodeExpr<Output = Vec<Prop>>>(pub E);

        impl<E: NodeExpr<Output = Vec<Prop>>> Clone for $expr<E> {
            fn clone(&self) -> Self {
                $expr(self.0.clone())
            }
        }

        impl<E: NodeExpr<Output = Vec<Prop>>> NodeExpr for $expr<E> {
            type Output = $output;

            fn create_node_op<'g, G: GraphView + 'g>(
                &self,
                graph: G,
            ) -> Result<Arc<dyn NodeOp<Output = $output> + 'g>, GraphError> {
                let inner = self.0.create_node_op(graph)?;
                Ok(Arc::new($op_ty { inner }))
            }
        }
    };
}

impl_agg_expr!(SumExpr, SumNodeOp, Option<Prop>);
impl_agg_expr!(AvgExpr, AvgNodeOp, Option<Prop>);
impl_agg_expr!(MinExpr, MinNodeOp, Option<Prop>);
impl_agg_expr!(MaxExpr, MaxNodeOp, Option<Prop>);
impl_agg_expr!(FirstExpr, FirstNodeOp, Option<Prop>);
impl_agg_expr!(LastExpr, LastNodeOp, Option<Prop>);
impl_agg_expr!(LenExpr, LenNodeOp, usize);

// ─────────────────────────────────────────────────────────────────────────────
// AnyNodeOp / AllNodeOp — quantified comparison over a temporal sequence
// ─────────────────────────────────────────────────────────────────────────────

pub struct AnyNodeOp<'g> {
    inner: Arc<dyn NodeOp<Output = Vec<Prop>> + 'g>,
    rhs: Arc<dyn NodeOp<Output = Option<Prop>> + 'g>,
    op: BinaryOp,
}

impl<'g> Clone for AnyNodeOp<'g> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            rhs: self.rhs.clone(),
            op: self.op,
        }
    }
}

impl<'g> NodeOp for AnyNodeOp<'g> {
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, node: VID) -> bool {
        let vals = self.inner.apply(storage, node);
        let Some(rhs) = self.rhs.apply(storage, node) else {
            return false;
        };
        vals.iter().any(|v| Prop::binary_cmp(&self.op, v, &rhs))
    }
}

pub struct AllNodeOp<'g> {
    inner: Arc<dyn NodeOp<Output = Vec<Prop>> + 'g>,
    rhs: Arc<dyn NodeOp<Output = Option<Prop>> + 'g>,
    op: BinaryOp,
}

impl<'g> Clone for AllNodeOp<'g> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            rhs: self.rhs.clone(),
            op: self.op,
        }
    }
}

impl<'g> NodeOp for AllNodeOp<'g> {
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, node: VID) -> bool {
        let vals = self.inner.apply(storage, node);
        let Some(rhs) = self.rhs.apply(storage, node) else {
            return false;
        };
        !vals.is_empty() && vals.iter().all(|v| Prop::binary_cmp(&self.op, v, &rhs))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// QuantifiedNodeFilter<E, Q, R> — leaf filter wrapping a quantified comparison
// ─────────────────────────────────────────────────────────────────────────────

pub struct QuantifiedNodeFilter<E, Q, R>
where
    E: NodeExpr<Output = Vec<Prop>>,
    Q: QuantifierMode,
    R: NodeExpr<Output = Option<Prop>>,
{
    pub expr: E,
    pub rhs: R,
    pub op: BinaryOp,
    _q: PhantomData<Q>,
}

impl<E, Q, R> QuantifiedNodeFilter<E, Q, R>
where
    E: NodeExpr<Output = Vec<Prop>>,
    Q: QuantifierMode,
    R: NodeExpr<Output = Option<Prop>>,
{
    pub fn new(expr: E, op: BinaryOp, rhs: R) -> Self {
        Self {
            expr,
            rhs,
            op,
            _q: PhantomData,
        }
    }
}

impl<E, Q, R> Clone for QuantifiedNodeFilter<E, Q, R>
where
    E: NodeExpr<Output = Vec<Prop>>,
    Q: QuantifierMode,
    R: NodeExpr<Output = Option<Prop>>,
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

impl<E, Q, R> ComposableFilter for QuantifiedNodeFilter<E, Q, R>
where
    E: NodeExpr<Output = Vec<Prop>>,
    Q: QuantifierMode,
    R: NodeExpr<Output = Option<Prop>>,
{
}

impl<E, R> CreateFilter for QuantifiedNodeFilter<E, AnyMode, R>
where
    E: NodeExpr<Output = Vec<Prop>>,
    R: NodeExpr<Output = Option<Prop>>,
{
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> = NodeFilteredGraph<G, AnyNodeOp<'graph>>;
    type NodeFilter<'graph, G: GraphView + 'graph> = AnyNodeOp<'graph>;
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
        Ok(AnyNodeOp {
            inner: self.expr.create_node_op(graph.clone())?,
            rhs: self.rhs.create_node_op(graph)?,
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

impl<E, R> CreateFilter for QuantifiedNodeFilter<E, AllMode, R>
where
    E: NodeExpr<Output = Vec<Prop>>,
    R: NodeExpr<Output = Option<Prop>>,
{
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> = NodeFilteredGraph<G, AllNodeOp<'graph>>;
    type NodeFilter<'graph, G: GraphView + 'graph> = AllNodeOp<'graph>;
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
        Ok(AllNodeOp {
            inner: self.expr.create_node_op(graph.clone())?,
            rhs: self.rhs.create_node_op(graph)?,
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

impl<E, Q, R> TryAsCompositeFilter for QuantifiedNodeFilter<E, Q, R>
where
    E: NodeExpr<Output = Vec<Prop>>,
    Q: QuantifierMode,
    R: NodeExpr<Output = Option<Prop>>,
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
// Context builders — carry wrap context through the builder chain
// ─────────────────────────────────────────────────────────────────────────────

/// Builder returned from `.any()` / `.all()` on a temporal expression.
///
/// Carries the wrapper context `W` (identity for `NodeFilter`, `Windowed` for windowed filters).
/// Call `.eq(rhs)`, `.gt(rhs)` etc. to produce the final filter wrapped in `W`.
pub struct QuantifiedContextBuilder<W, E, Q>
where
    W: Wrap + Clone,
    E: NodeExpr<Output = Vec<Prop>>,
    Q: QuantifierMode,
{
    pub(crate) wrap_ctx: W,
    pub(crate) expr: E,
    pub(crate) _q: PhantomData<Q>,
}

impl<W, E, Q> QuantifiedContextBuilder<W, E, Q>
where
    W: Wrap + Clone,
    E: NodeExpr<Output = Vec<Prop>>,
    Q: QuantifierMode,
{
    fn finish<R: NodeExpr<Output = Option<Prop>>>(
        self,
        op: BinaryOp,
        rhs: R,
    ) -> W::Wrapped<QuantifiedNodeFilter<E, Q, R>> {
        self.wrap_ctx
            .wrap(QuantifiedNodeFilter::new(self.expr, op, rhs))
    }

    pub fn eq<R: NodeExpr<Output = Option<Prop>>>(
        self,
        rhs: R,
    ) -> W::Wrapped<QuantifiedNodeFilter<E, Q, R>> {
        self.finish(BinaryOp::Eq, rhs)
    }

    pub fn ne<R: NodeExpr<Output = Option<Prop>>>(
        self,
        rhs: R,
    ) -> W::Wrapped<QuantifiedNodeFilter<E, Q, R>> {
        self.finish(BinaryOp::Ne, rhs)
    }

    pub fn gt<R: NodeExpr<Output = Option<Prop>>>(
        self,
        rhs: R,
    ) -> W::Wrapped<QuantifiedNodeFilter<E, Q, R>> {
        self.finish(BinaryOp::Gt, rhs)
    }

    pub fn ge<R: NodeExpr<Output = Option<Prop>>>(
        self,
        rhs: R,
    ) -> W::Wrapped<QuantifiedNodeFilter<E, Q, R>> {
        self.finish(BinaryOp::Ge, rhs)
    }

    pub fn lt<R: NodeExpr<Output = Option<Prop>>>(
        self,
        rhs: R,
    ) -> W::Wrapped<QuantifiedNodeFilter<E, Q, R>> {
        self.finish(BinaryOp::Lt, rhs)
    }

    pub fn le<R: NodeExpr<Output = Option<Prop>>>(
        self,
        rhs: R,
    ) -> W::Wrapped<QuantifiedNodeFilter<E, Q, R>> {
        self.finish(BinaryOp::Le, rhs)
    }
}

/// Builder returned from aggregators (`.sum()`, `.avg()` etc.) on a temporal expression.
///
/// Carries the wrapper context `W` and the aggregator expression `E`.
/// Call `.eq(rhs)`, `.gt(rhs)` etc. to produce the final filter wrapped in `W`.
pub struct NodeExprContextBuilder<W, E>
where
    W: Wrap + Clone,
    E: NodeExpr,
{
    pub(crate) wrap_ctx: W,
    pub(crate) expr: E,
}

impl<W, E> NodeExprContextBuilder<W, E>
where
    W: Wrap + Clone,
    E: NodeExpr,
{
    fn finish<R: NodeExpr<Output = E::Output>>(
        self,
        op: BinaryOp,
        rhs: R,
    ) -> W::Wrapped<BinOpNodeFilter<E, R>> {
        self.wrap_ctx.wrap(BinOpNodeFilter::new(self.expr, op, rhs))
    }

    pub fn eq<R: NodeExpr<Output = E::Output>>(self, rhs: R) -> W::Wrapped<BinOpNodeFilter<E, R>> {
        self.finish(BinaryOp::Eq, rhs)
    }

    pub fn ne<R: NodeExpr<Output = E::Output>>(self, rhs: R) -> W::Wrapped<BinOpNodeFilter<E, R>> {
        self.finish(BinaryOp::Ne, rhs)
    }

    pub fn gt<R: NodeExpr<Output = E::Output>>(self, rhs: R) -> W::Wrapped<BinOpNodeFilter<E, R>> {
        self.finish(BinaryOp::Gt, rhs)
    }

    pub fn ge<R: NodeExpr<Output = E::Output>>(self, rhs: R) -> W::Wrapped<BinOpNodeFilter<E, R>> {
        self.finish(BinaryOp::Ge, rhs)
    }

    pub fn lt<R: NodeExpr<Output = E::Output>>(self, rhs: R) -> W::Wrapped<BinOpNodeFilter<E, R>> {
        self.finish(BinaryOp::Lt, rhs)
    }

    pub fn le<R: NodeExpr<Output = E::Output>>(self, rhs: R) -> W::Wrapped<BinOpNodeFilter<E, R>> {
        self.finish(BinaryOp::Le, rhs)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// TemporalPropContext<W> — entry point returned from `.temporal_property(name)`
// ─────────────────────────────────────────────────────────────────────────────

/// Builder returned from `.temporal_property(name)`.
///
/// `W` carries the wrapping context so that windowed temporal filters are correctly
/// produced when called on a `Windowed<NodeFilter>`.
///
/// Usage:
/// ```rust,ignore
/// NodeFilter::temporal_property("score").any().gt(10i64)
/// NodeFilter.window(0, 100).temporal_property("score").any().gt(10i64)
/// NodeFilter::temporal_property("price").sum().gt(100i64)
/// ```
pub struct TemporalPropContext<W: Wrap + Clone> {
    wrap_ctx: W,
    expr: TemporalPropertyExpr,
}

impl<W: Wrap + Clone> TemporalPropContext<W> {
    pub(crate) fn new(wrap_ctx: W, name: impl Into<String>) -> Self {
        Self {
            wrap_ctx,
            expr: TemporalPropertyExpr::new(name),
        }
    }

    pub fn any(self) -> QuantifiedContextBuilder<W, TemporalPropertyExpr, AnyMode> {
        QuantifiedContextBuilder {
            wrap_ctx: self.wrap_ctx,
            expr: self.expr,
            _q: PhantomData,
        }
    }

    pub fn all(self) -> QuantifiedContextBuilder<W, TemporalPropertyExpr, AllMode> {
        QuantifiedContextBuilder {
            wrap_ctx: self.wrap_ctx,
            expr: self.expr,
            _q: PhantomData,
        }
    }

    pub fn sum(self) -> NodeExprContextBuilder<W, SumExpr<TemporalPropertyExpr>> {
        NodeExprContextBuilder {
            wrap_ctx: self.wrap_ctx,
            expr: SumExpr(self.expr),
        }
    }

    pub fn avg(self) -> NodeExprContextBuilder<W, AvgExpr<TemporalPropertyExpr>> {
        NodeExprContextBuilder {
            wrap_ctx: self.wrap_ctx,
            expr: AvgExpr(self.expr),
        }
    }

    pub fn min(self) -> NodeExprContextBuilder<W, MinExpr<TemporalPropertyExpr>> {
        NodeExprContextBuilder {
            wrap_ctx: self.wrap_ctx,
            expr: MinExpr(self.expr),
        }
    }

    pub fn max(self) -> NodeExprContextBuilder<W, MaxExpr<TemporalPropertyExpr>> {
        NodeExprContextBuilder {
            wrap_ctx: self.wrap_ctx,
            expr: MaxExpr(self.expr),
        }
    }

    pub fn first(self) -> NodeExprContextBuilder<W, FirstExpr<TemporalPropertyExpr>> {
        NodeExprContextBuilder {
            wrap_ctx: self.wrap_ctx,
            expr: FirstExpr(self.expr),
        }
    }

    pub fn last(self) -> NodeExprContextBuilder<W, LastExpr<TemporalPropertyExpr>> {
        NodeExprContextBuilder {
            wrap_ctx: self.wrap_ctx,
            expr: LastExpr(self.expr),
        }
    }

    pub fn len(self) -> NodeExprContextBuilder<W, LenExpr<TemporalPropertyExpr>> {
        NodeExprContextBuilder {
            wrap_ctx: self.wrap_ctx,
            expr: LenExpr(self.expr),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// TemporalExprOps — blanket trait for E: NodeExpr<Output = Vec<Prop>>
// ─────────────────────────────────────────────────────────────────────────────

/// Quantifier and aggregator operators for temporal property sequences.
///
/// Available on any `NodeExpr<Output = Vec<Prop>>` (e.g. `TemporalPropertyExpr`).
pub trait TemporalExprOps: NodeExpr<Output = Vec<Prop>> + Sized {
    fn any(self) -> QuantifiedContextBuilder<NoWrap, Self, AnyMode> {
        QuantifiedContextBuilder {
            wrap_ctx: NoWrap,
            expr: self,
            _q: PhantomData,
        }
    }

    fn all(self) -> QuantifiedContextBuilder<NoWrap, Self, AllMode> {
        QuantifiedContextBuilder {
            wrap_ctx: NoWrap,
            expr: self,
            _q: PhantomData,
        }
    }

    fn sum(self) -> SumExpr<Self> {
        SumExpr(self)
    }

    fn avg(self) -> AvgExpr<Self> {
        AvgExpr(self)
    }

    fn min(self) -> MinExpr<Self> {
        MinExpr(self)
    }

    fn max(self) -> MaxExpr<Self> {
        MaxExpr(self)
    }

    fn first(self) -> FirstExpr<Self> {
        FirstExpr(self)
    }

    fn last(self) -> LastExpr<Self> {
        LastExpr(self)
    }

    fn len(self) -> LenExpr<Self> {
        LenExpr(self)
    }
}

impl<E: NodeExpr<Output = Vec<Prop>>> TemporalExprOps for E {}

/// Identity wrapper — used by `TemporalExprOps` blanket to avoid wrapping.
#[derive(Debug, Clone, Copy)]
pub struct NoWrap;

impl Wrap for NoWrap {
    type Wrapped<T> = T;

    fn wrap<T>(&self, value: T) -> T {
        value
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::{
            api::view::filter_ops::NodeSelect,
            graph::views::filter::model::{
                node_filter::{NodeFilter, TemporalNodeExprBuilderOps},
                ViewWrapOps,
            },
        },
        prelude::{AdditionOps, Graph, GraphViewOps, NodeViewOps, NO_PROPS},
    };
    use raphtory_api::core::entities::properties::prop::IntoProp;

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

    #[test]
    fn test_id_filter_expr() {
        let g = Graph::new();
        g.add_node(0, 1, NO_PROPS, None, None).unwrap();
        g.add_node(0, 6, NO_PROPS, None, None).unwrap();
        let filter = Id.ge(GID::U64(5u64));

        assert_eq!(g.nodes().select(filter).unwrap().id(), [6u64])
    }

    // ── Temporal property helpers ─────────────────────────────────────────────

    /// Graph with three nodes; "alice" has scores [1, 5, 10] at times 1, 2, 3
    ///                           "bob"   has scores [2, 3]    at times 1, 2
    ///                           "carol" has no score property
    fn build_temporal_graph() -> Graph {
        let g = Graph::new();
        g.add_node(1, "alice", [("score", 1i64.into_prop())], None, None)
            .unwrap();
        g.add_node(2, "alice", [("score", 5i64.into_prop())], None, None)
            .unwrap();
        g.add_node(3, "alice", [("score", 10i64.into_prop())], None, None)
            .unwrap();
        g.add_node(1, "bob", [("score", 2i64.into_prop())], None, None)
            .unwrap();
        g.add_node(2, "bob", [("score", 3i64.into_prop())], None, None)
            .unwrap();
        g.add_node(1, "carol", NO_PROPS, None, None).unwrap();
        let _ = NodeFilter; // suppress unused warning
        g
    }

    fn temporal_filtered_names<F>(filter: F, g: Graph) -> Vec<String>
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

    // ── any() quantifier ─────────────────────────────────────────────────────

    #[test]
    fn temporal_any_eq_selects_nodes_with_matching_value() {
        // alice has 1, 5, 10; bob has 2, 3; carol has none
        // any == 5 → alice only
        let g = build_temporal_graph();
        let filter = TemporalPropertyExpr::new("score").any().eq(5i64);
        assert_eq!(temporal_filtered_names(filter, g), vec!["alice"]);
    }

    #[test]
    fn temporal_any_gt_selects_nodes_with_at_least_one_value_above_threshold() {
        // any > 4 → alice (has 5, 10), not bob (max 3), not carol (none)
        let g = build_temporal_graph();
        let filter = TemporalPropertyExpr::new("score").any().gt(4i64);
        assert_eq!(temporal_filtered_names(filter, g), vec!["alice"]);
    }

    #[test]
    fn temporal_any_gt_both_nodes_qualify() {
        // any > 1 → alice (5, 10), bob (2, 3) — both qualify
        let g = build_temporal_graph();
        let filter = TemporalPropertyExpr::new("score").any().gt(1i64);
        assert_eq!(temporal_filtered_names(filter, g), vec!["alice", "bob"]);
    }

    // ── all() quantifier ─────────────────────────────────────────────────────

    #[test]
    fn temporal_all_gt_requires_every_value() {
        // all > 0 → alice (1,5,10 all > 0 ✓), bob (2,3 all > 0 ✓), carol excluded (empty)
        let g = build_temporal_graph();
        let filter = TemporalPropertyExpr::new("score").all().gt(0i64);
        assert_eq!(temporal_filtered_names(filter, g), vec!["alice", "bob"]);
    }

    #[test]
    fn temporal_all_gt_rejects_if_any_value_fails() {
        // all > 4 → alice (1 fails) not included, bob (2, 3 fail) not included
        let g = build_temporal_graph();
        let filter = TemporalPropertyExpr::new("score").all().gt(4i64);
        assert!(temporal_filtered_names(filter, g).is_empty());
    }

    #[test]
    fn temporal_all_requires_non_empty_sequence() {
        // carol has no score → "all" over empty sequence returns false
        let g = build_temporal_graph();
        let filter = TemporalPropertyExpr::new("score").all().ge(0i64);
        let names = temporal_filtered_names(filter, g);
        assert!(!names.contains(&"carol".to_string()));
    }

    // ── sum() aggregator ──────────────────────────────────────────────────────

    #[test]
    fn temporal_sum_gt_threshold() {
        // alice sum = 16, bob sum = 5 → sum > 10 → alice only
        let g = build_temporal_graph();
        let filter = TemporalPropertyExpr::new("score").sum().gt(10i64);
        assert_eq!(temporal_filtered_names(filter, g), vec!["alice"]);
    }

    #[test]
    fn temporal_sum_eq() {
        // bob sum = 5 → sum == 5 → bob only
        let g = build_temporal_graph();
        let filter = TemporalPropertyExpr::new("score").sum().eq(5i64);
        assert_eq!(temporal_filtered_names(filter, g), vec!["bob"]);
    }

    // ── first() / last() aggregators ─────────────────────────────────────────

    #[test]
    fn temporal_first_value() {
        // alice first = 1, bob first = 2 → first == 1 → alice only
        let g = build_temporal_graph();
        let filter = TemporalPropertyExpr::new("score").first().eq(1i64);
        assert_eq!(temporal_filtered_names(filter, g), vec!["alice"]);
    }

    #[test]
    fn temporal_last_value() {
        // alice last = 10 → last > 9 → alice only
        let g = build_temporal_graph();
        let filter = TemporalPropertyExpr::new("score").last().gt(9i64);
        assert_eq!(temporal_filtered_names(filter, g), vec!["alice"]);
    }

    // ── len() aggregator ──────────────────────────────────────────────────────

    #[test]
    fn temporal_len_count() {
        // alice has 3 updates, bob has 2 → len == 3 → alice only
        let g = build_temporal_graph();
        let filter = TemporalPropertyExpr::new("score").len().eq(3usize);
        assert_eq!(temporal_filtered_names(filter, g), vec!["alice"]);
    }

    #[test]
    fn temporal_len_ge_2() {
        // alice (3), bob (2) both have len >= 2; carol has 0
        let g = build_temporal_graph();
        let filter = TemporalPropertyExpr::new("score").len().ge(2usize);
        assert_eq!(temporal_filtered_names(filter, g), vec!["alice", "bob"]);
    }

    // ── NodeFilter entry point ────────────────────────────────────────────────

    #[test]
    fn node_filter_temporal_property_entry_point() {
        let g = build_temporal_graph();
        let filter = NodeFilter::temporal_property("score").any().eq(5i64);
        assert_eq!(temporal_filtered_names(filter, g), vec!["alice"]);
    }

    // ── TemporalExprOps blanket ───────────────────────────────────────────────

    #[test]
    fn temporal_expr_ops_blanket_any() {
        // Using the blanket TemporalExprOps on TemporalPropertyExpr directly
        let g = build_temporal_graph();
        let filter = TemporalPropertyExpr::new("score").any().eq(10i64);
        assert_eq!(temporal_filtered_names(filter, g), vec!["alice"]);
    }

    // ── Windowed temporal filter ──────────────────────────────────────────────

    /// Apply a filter using the full two-step pipeline (filter_graph_view → create_filter).
    /// Required for windowed filters where filter_graph_view applies the window.
    fn windowed_filtered_names<F>(filter: F, g: Graph) -> Vec<String>
    where
        F: CreateFilter + Clone,
        for<'graph> F::EntityFiltered<'graph, F::FilteredGraph<'graph, Graph>>:
            GraphViewOps<'graph>,
    {
        let fg = filter.filter_graph_view(g).unwrap();
        let mut names: Vec<String> = filter
            .create_filter(fg)
            .unwrap()
            .nodes()
            .iter()
            .map(|n| n.name())
            .collect();
        names.sort();
        names
    }

    #[test]
    fn windowed_temporal_any_restricts_to_window() {
        // alice scores: t1=1, t2=5, t3=10
        // window [1, 2) → only t=1 visible → score=1 only
        // any == 5 in window [1,2) → false for all nodes
        let g = build_temporal_graph();
        let filter = NodeFilter
            .window(1, 2)
            .temporal_property("score")
            .any()
            .eq(5i64);
        // window [1,2) shows t=1 only → alice has score=1, not 5
        assert!(windowed_filtered_names(filter, g).is_empty());
    }

    #[test]
    fn windowed_temporal_any_matches_in_window() {
        // window [2, 3) → alice has score=5 (t=2), bob has score=3 (t=2)
        let g = build_temporal_graph();
        let filter = NodeFilter
            .window(2, 3)
            .temporal_property("score")
            .any()
            .eq(5i64);
        assert_eq!(windowed_filtered_names(filter, g), vec!["alice"]);
    }

    // ── Layered temporal filter ───────────────────────────────────────────────

    /// Graph where temporal "score" updates are split across two named layers.
    ///
    /// alice: score [1, 5, 10] at t=1,2,3 — all added in "layer_a"
    /// bob:   score [2, 3]     at t=1,2   — all added in "layer_b"
    /// carol: no score property            — added in "layer_a" (makes her visible there)
    ///
    /// Because updates added without an explicit layer go into the static layer
    /// (and are always visible regardless of the active LayeredGraph), we must use
    /// an explicit layer on every `add_node` call that carries a property we want
    /// to isolate.
    fn build_layered_temporal_graph() -> Graph {
        let g = Graph::new();
        g.add_node(
            1,
            "alice",
            [("score", 1i64.into_prop())],
            None,
            Some("layer_a"),
        )
        .unwrap();
        g.add_node(
            2,
            "alice",
            [("score", 5i64.into_prop())],
            None,
            Some("layer_a"),
        )
        .unwrap();
        g.add_node(
            3,
            "alice",
            [("score", 10i64.into_prop())],
            None,
            Some("layer_a"),
        )
        .unwrap();
        g.add_node(
            1,
            "bob",
            [("score", 2i64.into_prop())],
            None,
            Some("layer_b"),
        )
        .unwrap();
        g.add_node(
            2,
            "bob",
            [("score", 3i64.into_prop())],
            None,
            Some("layer_b"),
        )
        .unwrap();
        g.add_node(1, "carol", NO_PROPS, None, Some("layer_a"))
            .unwrap();
        g
    }

    /// Run the full filter_graph_view → create_filter pipeline for a layered filter.
    /// Identical in structure to `windowed_filtered_names`; factored separately for clarity.
    fn layered_filtered_names<F>(filter: F, g: Graph) -> Vec<String>
    where
        F: CreateFilter + Clone,
        for<'graph> F::EntityFiltered<'graph, F::FilteredGraph<'graph, Graph>>:
            GraphViewOps<'graph>,
    {
        let fg = filter.filter_graph_view(g).unwrap();
        let mut names: Vec<String> = filter
            .create_filter(fg)
            .unwrap()
            .nodes()
            .iter()
            .map(|n| n.name())
            .collect();
        names.sort();
        names
    }

    #[test]
    fn layered_temporal_any_restricts_to_layer_a_updates() {
        // layer_a view: alice has scores [1, 5, 10], carol has none, bob has none
        // any == 5 → only alice qualifies
        let g = build_layered_temporal_graph();
        let filter = NodeFilter
            .layer("layer_a")
            .temporal_property("score")
            .any()
            .eq(5i64);
        assert_eq!(layered_filtered_names(filter, g), vec!["alice"]);
    }

    #[test]
    fn layered_temporal_any_restricts_to_layer_b_updates() {
        // layer_b view: bob has scores [2, 3], alice has none, carol has none
        // any > 2 → bob qualifies (score=3 > 2), alice and carol do not
        let g = build_layered_temporal_graph();
        let filter = NodeFilter
            .layer("layer_b")
            .temporal_property("score")
            .any()
            .gt(2i64);
        assert_eq!(layered_filtered_names(filter, g), vec!["bob"]);
    }

    #[test]
    fn layered_temporal_sum_is_layer_scoped() {
        // layer_a: alice sum = 1+5+10 = 16; layer_b: bob sum = 2+3 = 5
        // layer_a sum > 10 → alice (16 > 10); carol (no score) excluded
        let g = build_layered_temporal_graph();
        let filter = NodeFilter
            .layer("layer_a")
            .temporal_property("score")
            .sum()
            .gt(10i64);
        assert_eq!(layered_filtered_names(filter, g), vec!["alice"]);
    }
}
