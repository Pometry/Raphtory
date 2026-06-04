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
                filter_operator::{BinaryOp, SetOp, UnaryOp},
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
    Direction,
};
use raphtory_storage::graph::graph::GraphStorage;
use std::{collections::HashSet, hash::Hash, sync::Arc};
use strsim::levenshtein;

// ─────────────────────────────────────────────────────────────────────────────
// Comparable — type-driven dispatch for BinOpNodeOp
// ─────────────────────────────────────────────────────────────────────────────

/// Comparison trait used by `BinOpNodeOp` to evaluate a `BinaryOp` against two values.
///
/// Implemented for `usize`, `String`, `Prop`, and `Option<T: Comparable>`.
/// The `Option` impl handles `None` symmetrically: `(None, None)` is equal,
/// one `None` is unequal, and ordering ops return `false` when either side is `None`.
pub trait Comparable: Clone + Send + Sync + 'static {
    fn binary_cmp(op: &BinaryOp, left: &Self, right: &Self) -> bool;
}

impl Comparable for usize {
    fn binary_cmp(op: &BinaryOp, left: &usize, right: &usize) -> bool {
        match op {
            BinaryOp::Eq => left == right,
            BinaryOp::Ne => left != right,
            BinaryOp::Lt => left < right,
            BinaryOp::Le => left <= right,
            BinaryOp::Gt => left > right,
            BinaryOp::Ge => left >= right,
            _ => false,
        }
    }
}

impl Comparable for String {
    fn binary_cmp(op: &BinaryOp, left: &String, right: &String) -> bool {
        // Coerce to &str to avoid ambiguity with NodeExprFilterOps methods of the same name.
        let (l, r): (&str, &str) = (left, right);
        match op {
            BinaryOp::Eq => left == right,
            BinaryOp::Ne => left != right,
            BinaryOp::Lt => left < right,
            BinaryOp::Le => left <= right,
            BinaryOp::Gt => left > right,
            BinaryOp::Ge => left >= right,
            BinaryOp::StartsWith => l.starts_with(r),
            BinaryOp::EndsWith => l.ends_with(r),
            BinaryOp::Contains => l.contains(r),
            BinaryOp::NotContains => !l.contains(r),
            BinaryOp::FuzzySearch {
                levenshtein_distance,
                prefix_match,
            } => {
                let l = l.to_lowercase();
                let r = r.to_lowercase();
                let lev = levenshtein(&r, &l) <= *levenshtein_distance;
                let prefix = *prefix_match && l.as_str().starts_with(r.as_str());
                lev || prefix
            }
        }
    }
}

impl Comparable for Prop {
    fn binary_cmp(op: &BinaryOp, left: &Prop, right: &Prop) -> bool {
        use std::cmp::Ordering::*;
        match op {
            BinaryOp::Eq => left == right,
            BinaryOp::Ne => left != right,
            BinaryOp::Lt => left.partial_cmp(right).map(|o| o == Less).unwrap_or(false),
            BinaryOp::Le => left
                .partial_cmp(right)
                .map(|o| o != Greater)
                .unwrap_or(false),
            BinaryOp::Gt => left
                .partial_cmp(right)
                .map(|o| o == Greater)
                .unwrap_or(false),
            BinaryOp::Ge => left.partial_cmp(right).map(|o| o != Less).unwrap_or(false),
            _ => false,
        }
    }
}

impl<T: Comparable> Comparable for Option<T> {
    fn binary_cmp(op: &BinaryOp, left: &Option<T>, right: &Option<T>) -> bool {
        match (left, right) {
            (Some(l), Some(r)) => T::binary_cmp(op, l, r),
            (None, None) => matches!(op, BinaryOp::Eq),
            (None, Some(_)) | (Some(_), None) => matches!(op, BinaryOp::Ne),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Unwrap — constrains Output = Option<Inner>
// ─────────────────────────────────────────────────────────────────────────────

pub trait Unwrap {
    type Inner;
    fn is_some(&self) -> bool;
    fn is_none(&self) -> bool;
    fn unwrap_inner(self) -> Option<Self::Inner>;
}

impl<T> Unwrap for Option<T> {
    type Inner = T;
    fn is_some(&self) -> bool {
        Option::is_some(self)
    }
    fn is_none(&self) -> bool {
        Option::is_none(self)
    }
    fn unwrap_inner(self) -> Option<T> {
        self
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// NodeExpr — typed node expression with associated Output type
// ─────────────────────────────────────────────────────────────────────────────

/// A typed expression that produces a value per node.
///
/// `Output` carries nullability directly: `Option<Prop>` for properties that
/// may be absent, `Option<String>` for name/type, `Option<usize>` for degree.
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
// OptionWrapOp<O> — adapts NodeOp<Output = T> to NodeOp<Output = Option<T>>
// ─────────────────────────────────────────────────────────────────────────────

/// Wraps an inner `NodeOp` and returns `Some(inner.apply(...))`.
///
/// Used by `DegreeExpr` and `Name` to produce `Option`-wrapped outputs from
/// the existing `Degree<G>` and `Name` ops in `db/api/state/ops/node.rs`,
/// without reimplementing their logic.
#[derive(Clone)]
pub(crate) struct OptionWrapOp<O>(O);

impl<O: NodeOp + Clone + Send + Sync> NodeOp for OptionWrapOp<O>
where
    O::Output: Clone + Send + Sync + 'static,
{
    type Output = Option<O::Output>;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Option<O::Output> {
        Some(self.0.apply(storage, node))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// NodeTypeStringOp — maps Type's Option<ArcStr> to Option<String>
// ─────────────────────────────────────────────────────────────────────────────

/// Evaluates `Type` from `node.rs` and converts `ArcStr` to `String`.
///
/// `Type: NodeOp<Output = Option<ArcStr>>` — this op converts to `Option<String>`
/// without reimplementing the type-id lookup logic.
#[derive(Clone)]
pub(crate) struct NodeTypeStringOp;

impl NodeOp for NodeTypeStringOp {
    type Output = Option<String>;

    fn apply(&self, storage: &GraphStorage, node: VID) -> Option<String> {
        Type.apply(storage, node).map(|a| a.to_string())
    }
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
    type Output = Option<usize>;

    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<usize>> + 'g>, GraphError> {
        Ok(Arc::new(OptionWrapOp(Degree {
            dir: self.0,
            view: graph,
        })))
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
/// `Type: NodeOp<Output = Option<ArcStr>>` — this impl converts to `Option<String>`
/// via `NodeTypeStringOp` without reimplementing the type-id lookup.
impl NodeExpr for Type {
    type Output = Option<String>;

    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<String>> + 'g>, GraphError> {
        Ok(Arc::new(NodeTypeStringOp))
    }
}

/// `Name` from `db/api/state/ops/node.rs` used as a node expression.
///
/// Wraps the existing `Name` op via `OptionWrapOp` so it fits the
/// `NodeExpr<Output = Option<String>>` interface without reimplementation.
impl NodeExpr for Name {
    type Output = Option<String>;

    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<String>> + 'g>, GraphError> {
        Ok(Arc::new(OptionWrapOp(Name)))
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
    type Output = Option<usize>;

    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<usize>> + 'g>, GraphError> {
        Ok(Arc::new(Const(Some(*self))))
    }
}

impl NodeExpr for String {
    type Output = Option<String>;

    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<String>> + 'g>, GraphError> {
        Ok(Arc::new(Const(Some(self.clone()))))
    }
}

impl NodeExpr for &'static str {
    type Output = Option<String>;

    fn create_node_op<'g, G: GraphView + 'g>(
        &self,
        _graph: G,
    ) -> Result<Arc<dyn NodeOp<Output = Option<String>> + 'g>, GraphError> {
        Ok(Arc::new(Const(Some(self.to_string()))))
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
pub struct UnaryNodeOp<'g, T: Unwrap + Clone + Send + Sync + 'static>
where
    T::Inner: Clone + Send + Sync + 'static,
{
    inner: Arc<dyn NodeOp<Output = T> + 'g>,
    op: UnaryOp,
}

impl<'g, T: Unwrap + Clone + Send + Sync + 'static> NodeOp for UnaryNodeOp<'g, T>
where
    T::Inner: Clone + Send + Sync + 'static,
{
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
pub struct SetNodeOp<'g, T: Unwrap + Clone + Send + Sync + 'static>
where
    T::Inner: Eq + Hash + Clone + Send + Sync + 'static,
{
    inner: Arc<dyn NodeOp<Output = T> + 'g>,
    op: SetOp,
    values: Arc<HashSet<T::Inner>>,
}

impl<'g, T: Unwrap + Clone + Send + Sync + 'static> NodeOp for SetNodeOp<'g, T>
where
    T::Inner: Eq + Hash + Clone + Send + Sync + 'static,
{
    type Output = bool;

    fn apply(&self, storage: &GraphStorage, node: VID) -> bool {
        let v = self.inner.apply(storage, node).unwrap_inner();
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
/// Created by `.is_some()` and `.is_none()` on any `NodeExpr` whose `Output`
/// implements `Unwrap` (i.e., is an `Option<T>`).
pub struct UnaryNodeFilter<E: NodeExpr>
where
    E::Output: Unwrap,
{
    pub expr: E,
    pub op: UnaryOp,
}

impl<E: NodeExpr> Clone for UnaryNodeFilter<E>
where
    E::Output: Unwrap,
{
    fn clone(&self) -> Self {
        Self {
            expr: self.expr.clone(),
            op: self.op,
        }
    }
}

impl<E: NodeExpr> ComposableFilter for UnaryNodeFilter<E> where E::Output: Unwrap {}

impl<E: NodeExpr> CreateFilter for UnaryNodeFilter<E>
where
    E::Output: Unwrap + Clone + Send + Sync + 'static,
    <E::Output as Unwrap>::Inner: Clone + Send + Sync + 'static,
{
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> =
        NodeFilteredGraph<G, UnaryNodeOp<'graph, E::Output>>;

    type NodeFilter<'graph, G: GraphView + 'graph> = UnaryNodeOp<'graph, E::Output>;

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

impl<E: NodeExpr> TryAsCompositeFilter for UnaryNodeFilter<E>
where
    E::Output: Unwrap + Clone + Send + Sync + 'static,
    <E::Output as Unwrap>::Inner: Clone + Send + Sync + 'static,
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
#[derive(Clone)]
pub struct SetNodeFilter<E: NodeExpr>
where
    E::Output: Unwrap,
    <E::Output as Unwrap>::Inner: Eq + Hash + Clone,
{
    pub expr: E,
    pub op: SetOp,
    pub values: Arc<HashSet<<E::Output as Unwrap>::Inner>>,
}

// impl<E: NodeExpr> Clone for SetNodeFilter<E>
// where
//     E::Output: Unwrap,
//     <E::Output as Unwrap>::Inner: Eq + Hash + Clone,
// {
//     fn clone(&self) -> Self {
//         Self { expr: self.expr.clone(), op: self.op, values: self.values.clone() }
//     }
// }

impl<E: NodeExpr> ComposableFilter for SetNodeFilter<E>
where
    E::Output: Unwrap,
    <E::Output as Unwrap>::Inner: Eq + Hash + Clone,
{
}

impl<E: NodeExpr> CreateFilter for SetNodeFilter<E>
where
    E::Output: Unwrap + Clone + Send + Sync + 'static,
    <E::Output as Unwrap>::Inner: Eq + Hash + Clone + Send + Sync + 'static,
{
    type EntityFiltered<'graph, G: GraphViewOps<'graph>> =
        NodeFilteredGraph<G, SetNodeOp<'graph, E::Output>>;

    type NodeFilter<'graph, G: GraphView + 'graph> = SetNodeOp<'graph, E::Output>;

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

impl<E: NodeExpr> TryAsCompositeFilter for SetNodeFilter<E>
where
    E::Output: Unwrap + Clone + Send + Sync + 'static,
    <E::Output as Unwrap>::Inner: Eq + Hash + Clone + Send + Sync + 'static,
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
/// AttrNodeExpr(MyAttr).is_in([2usize, 3usize])
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

    fn is_some(self) -> UnaryNodeFilter<Self>
    where
        Self::Output: Unwrap,
    {
        UnaryNodeFilter {
            expr: self,
            op: UnaryOp::IsSome,
        }
    }

    fn is_none(self) -> UnaryNodeFilter<Self>
    where
        Self::Output: Unwrap,
    {
        UnaryNodeFilter {
            expr: self,
            op: UnaryOp::IsNone,
        }
    }

    fn is_in<I>(self, values: I) -> SetNodeFilter<Self>
    where
        Self::Output: Unwrap,
        <Self::Output as Unwrap>::Inner: Eq + Hash + Clone,
        I: IntoIterator<Item = <Self::Output as Unwrap>::Inner>,
    {
        let set: HashSet<_> = values.into_iter().collect();
        SetNodeFilter {
            expr: self,
            op: SetOp::IsIn,
            values: Arc::new(set),
        }
    }

    fn is_not_in<I>(self, values: I) -> SetNodeFilter<Self>
    where
        Self::Output: Unwrap,
        <Self::Output as Unwrap>::Inner: Eq + Hash + Clone,
        I: IntoIterator<Item = <Self::Output as Unwrap>::Inner>,
    {
        let set: HashSet<_> = values.into_iter().collect();
        SetNodeFilter {
            expr: self,
            op: SetOp::IsNotIn,
            values: Arc::new(set),
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

    // ── unary ops ────────────────────────────────────────────────────────────

    #[test]
    fn degree_is_some_keeps_all_nodes() {
        let g = build_test_graph();
        assert_eq!(
            filtered_names(DegreeExpr(Direction::BOTH).is_some(), g),
            vec!["a", "b", "c"]
        );
    }

    #[test]
    fn degree_is_none_keeps_no_nodes() {
        let g = build_test_graph();
        assert!(filtered_names(DegreeExpr(Direction::BOTH).is_none(), g).is_empty());
    }

    // ── set ops ──────────────────────────────────────────────────────────────

    #[test]
    fn degree_is_in_set() {
        let g = build_test_graph();
        assert_eq!(
            filtered_names(DegreeExpr(Direction::BOTH).is_in([2usize]), g),
            vec!["a", "b", "c"]
        );
    }

    #[test]
    fn degree_is_not_in_set_excludes_matching_nodes() {
        let g = build_test_graph();
        assert!(filtered_names(DegreeExpr(Direction::BOTH).is_not_in([2usize]), g).is_empty());
    }

    // ── ConstExpr for custom output types ────────────────────────────────────

    #[test]
    fn const_expr_works() {
        let filter = BinOpNodeFilter::new(ConstExpr(2usize), BinaryOp::Eq, ConstExpr(2usize));
        let g = build_test_graph();
        assert_eq!(filtered_names(filter, g), vec!["a", "b", "c"]);
    }
}
